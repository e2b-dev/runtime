//go:build linux

package fc

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
)

// featuresLinePrefix starts the line of the version output that lists the
// features of the binary, space-separated: "Features: a b".
const featuresLinePrefix = "Features:"

// featureProbeTimeout bounds one run of the binary's version output.
const featureProbeTimeout = 10 * time.Second

// ErrFeatureProbe is the error of a version output that could not be read.
var ErrFeatureProbe = errors.New("the firecracker binary did not report its version")

// MissingFeaturesError refuses a Firecracker binary that does not report every
// feature the guest profile requires. The binary of a version directory is
// the same on every node, so the refusal holds wherever the sandbox is placed.
type MissingFeaturesError struct {
	// Version is the binary's version directory.
	Version string
	// Path is the binary.
	Path string
	// Missing lists the required features the binary does not report.
	Missing []string
	// Reported lists the features the binary reports; nil when its version
	// output has no Features line at all (a build before the line existed).
	Reported []string
}

func (e *MissingFeaturesError) Error() string {
	if e.Reported == nil {
		return fmt.Sprintf("the firecracker binary %q does not report the features %s that the guest requires: its version output has no Features line",
			e.Path, strings.Join(e.Missing, ", "))
	}

	return fmt.Sprintf("the firecracker binary %q does not report the features %s that the guest requires (reported: %s)",
		e.Path, strings.Join(e.Missing, ", "), strings.Join(e.Reported, " "))
}

// Refusal is the error a client may see: it names the version and the
// missing features, not the binary's path on this host or what the binary
// printed, and unwraps to e.
func (e *MissingFeaturesError) Refusal() error {
	return &featuresRefusalError{missing: e}
}

type featuresRefusalError struct{ missing *MissingFeaturesError }

func (r *featuresRefusalError) Error() string {
	return fmt.Sprintf("the firecracker version %q does not report the features %s that the guest requires",
		r.missing.Version, strings.Join(r.missing.Missing, ", "))
}

func (r *featuresRefusalError) Unwrap() error { return r.missing }

// reportedFeatures caches the features a binary reports, keyed by its path,
// size and modification time, so a version directory is asked once per
// process and a binary replaced in place is asked again. A failed read is
// not kept, so a binary that could not be run is asked again; concurrent
// reads of one binary share one run.
var (
	reportedFeatures sync.Map // binaryKey -> []string
	featureProbes    singleflight.Group
)

// binaryKey identifies the file at path: a replaced binary gets another key.
func binaryKey(path string) (string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s|%d|%d", path, info.Size(), info.ModTime().UnixNano()), nil
}

// RequireFeatures refuses the Firecracker binary of the configuration when it
// does not report a feature the guest profile requires. A profile that
// requires none never runs the binary. A binary whose version output cannot
// be read is refused with the path and the output.
func (t Config) RequireFeatures(ctx context.Context, config cfg.BuilderConfig) error {
	required := t.GuestProfile().Firecracker().RequiredFeatures
	if len(required) == 0 {
		return nil
	}
	path := t.FirecrackerPath(config)
	reported, err := binaryFeatures(ctx, path)
	if err != nil {
		return err
	}
	var missing []string
	for _, feature := range required {
		if !slices.Contains(reported, feature) {
			missing = append(missing, feature)
		}
	}
	if len(missing) > 0 {
		return &MissingFeaturesError{Version: t.FirecrackerVersion, Path: path, Missing: missing, Reported: reported}
	}

	return nil
}

// requireAcceptedBinary refuses a launch whose Firecracker version the guest
// profile refuses by name, or whose binary does not report a feature the
// profile requires. Every launch passes here, a served sandbox's, a build
// layer's or a standalone tool's, so the server's and the builder's own
// refusals are only earlier and better mapped, never the only ones.
func requireAcceptedBinary(ctx context.Context, versions Config, config cfg.BuilderConfig) error {
	if err := requireAcceptedVersion(versions); err != nil {
		return err
	}

	return versions.RequireFeatures(ctx, config)
}

// binaryFeatures returns the features the binary at path reports: the words
// after the Features line of its version output, nil when the output has no
// such line.
func binaryFeatures(ctx context.Context, path string) ([]string, error) {
	key, err := binaryKey(path)
	if err != nil {
		return nil, fmt.Errorf("%w: %q: %w", ErrFeatureProbe, path, err)
	}
	if cached, ok := reportedFeatures.Load(key); ok {
		features, _ := cached.([]string)

		return features, nil
	}
	result, err, _ := featureProbes.Do(key, func() (any, error) {
		probeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), featureProbeTimeout)
		defer cancel()
		output, err := exec.CommandContext(probeCtx, path, "--version").CombinedOutput()
		if err != nil {
			return nil, fmt.Errorf("%w: %q: %w (output: %q)", ErrFeatureProbe, path, err, strings.TrimSpace(string(output)))
		}
		features := parseFeatures(output)
		reportedFeatures.Store(key, features)

		return features, nil
	})
	if err != nil {
		return nil, err
	}
	features, _ := result.([]string)

	return features, nil
}

// parseFeatures returns the words of the first Features line of a version
// output, an empty slice for a line without words, nil without the line.
func parseFeatures(output []byte) []string {
	scanner := bufio.NewScanner(bytes.NewReader(output))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if rest, ok := strings.CutPrefix(line, featuresLinePrefix); ok {
			return append([]string{}, strings.Fields(rest)...)
		}
	}

	return nil
}
