//go:build linux

package fc

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/guest"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// requiringProfile requires features of the Firecracker binary.
type requiringProfile struct {
	guest.Profile

	features []string
}

func (p requiringProfile) Name() string  { return "requiring" }
func (p requiringProfile) Label() string { return "requiring" }
func (p requiringProfile) Firecracker() guest.FirecrackerSelection {
	return guest.FirecrackerSelection{RequiredFeatures: p.features}
}

// fakeFirecrackerBinary writes a version script as the binary of version in dir and
// returns the configuration that finds it. The script appends one line to
// calls on every run.
func fakeFirecrackerBinary(t *testing.T, version, script string) (cfg.BuilderConfig, string) {
	t.Helper()
	dir := t.TempDir()
	versionDir := filepath.Join(dir, version, utils.TargetArch())
	require.NoError(t, os.MkdirAll(versionDir, 0o755))
	calls := filepath.Join(dir, "calls")
	path := filepath.Join(versionDir, "firecracker")
	require.NoError(t, os.WriteFile(path, []byte("#!/bin/sh\necho run >> '"+calls+"'\n"+script), 0o755))

	return cfg.BuilderConfig{FirecrackerVersionsDir: dir}, calls
}

// The tests that write a script and run it are serial: a fork in a parallel
// test can hold the script's write descriptor until its exec, and running
// the script then fails with ETXTBSY (golang/go#22315).

//nolint:paralleltest // writes a script and runs it
func TestRequireFeaturesAcceptsAReportingBinary(t *testing.T) {
	config, calls := fakeFirecrackerBinary(t, "v1.17-100.0.1", "printf 'Firecracker v1.17.0\\nFeatures: alpha beta\\n\\n'")
	versions := Config{FirecrackerVersion: "v1.17-100.0.1", Guest: requiringProfile{Profile: guest.Default(), features: []string{"alpha", "beta"}}}

	require.NoError(t, versions.RequireFeatures(t.Context(), config))
	require.NoError(t, versions.RequireFeatures(t.Context(), config))
	runs, err := os.ReadFile(calls)
	require.NoError(t, err)
	assert.Equal(t, "run\n", string(runs), "the binary is asked once per process")
}

//nolint:paralleltest // writes a script and runs it
func TestRequireFeaturesRefusesAMissingFeature(t *testing.T) {
	config, _ := fakeFirecrackerBinary(t, "v1.17-0.2.0", "printf 'Firecracker v1.17.0\\n\\n'")
	versions := Config{FirecrackerVersion: "v1.17-0.2.0", Guest: requiringProfile{Profile: guest.Default(), features: []string{"alpha"}}}

	err := versions.RequireFeatures(t.Context(), config)
	var missing *MissingFeaturesError
	require.ErrorAs(t, err, &missing)
	assert.Equal(t, versions.FirecrackerPath(config), missing.Path)
	assert.Equal(t, []string{"alpha"}, missing.Missing)
	assert.Nil(t, missing.Reported)
	assert.Contains(t, err.Error(), missing.Path)
	assert.Contains(t, err.Error(), "has no Features line")
	refusal := missing.Refusal()
	assert.Contains(t, refusal.Error(), `"v1.17-0.2.0"`)
	assert.Contains(t, refusal.Error(), "alpha")
	assert.NotContains(t, refusal.Error(), missing.Path, "a client sees no path of this host")
	var unwrapped *MissingFeaturesError
	require.ErrorAs(t, refusal, &unwrapped, "the refusal keeps the detail for the logs")

	config, _ = fakeFirecrackerBinary(t, "v1.17-0.2.0", "printf 'Firecracker v1.17.0\\nFeatures: beta\\n'")
	err = versions.RequireFeatures(t.Context(), config)
	require.ErrorAs(t, err, &missing)
	assert.Equal(t, []string{"beta"}, missing.Reported)
	assert.Contains(t, err.Error(), "(reported: beta)")
	assert.NotContains(t, missing.Refusal().Error(), "beta", "a client sees nothing the binary printed")
}

// A binary replaced under the same path is asked again.
//
//nolint:paralleltest // writes a script and runs it
func TestRequireFeaturesAsksAReplacedBinaryAgain(t *testing.T) {
	config, calls := fakeFirecrackerBinary(t, "v1.17-100.0.1", "printf 'Firecracker v1.17.0\\n'")
	versions := Config{FirecrackerVersion: "v1.17-100.0.1", Guest: requiringProfile{Profile: guest.Default(), features: []string{"alpha"}}}
	var missing *MissingFeaturesError
	require.ErrorAs(t, versions.RequireFeatures(t.Context(), config), &missing)

	path := versions.FirecrackerPath(config)
	require.NoError(t, os.WriteFile(path, []byte("#!/bin/sh\necho run >> '"+calls+"'\nprintf 'Firecracker v1.17.0\\nFeatures: alpha\\n'\n"), 0o755))
	require.NoError(t, versions.RequireFeatures(t.Context(), config))
	runs, err := os.ReadFile(calls)
	require.NoError(t, err)
	assert.Equal(t, "run\nrun\n", string(runs))
}

//nolint:paralleltest // writes a script and runs it
func TestRequireFeaturesRefusesAFailedProbe(t *testing.T) {
	config, _ := fakeFirecrackerBinary(t, "v1.17-100.0.0", "echo 'no such option'; exit 2")
	versions := Config{FirecrackerVersion: "v1.17-100.0.0", Guest: requiringProfile{Profile: guest.Default(), features: []string{"alpha"}}}

	err := versions.RequireFeatures(t.Context(), config)
	require.ErrorIs(t, err, ErrFeatureProbe)
	assert.Contains(t, err.Error(), versions.FirecrackerPath(config))
	assert.Contains(t, err.Error(), "no such option")
}

func TestRequireFeaturesAsksNothingOfTheDefaultProfile(t *testing.T) {
	t.Parallel()

	// No binary exists at all: a probe would fail.
	config := cfg.BuilderConfig{FirecrackerVersionsDir: t.TempDir()}
	for name, versions := range map[string]Config{
		"nil profile":     {FirecrackerVersion: "v1.17-0.2.0"},
		"default profile": {FirecrackerVersion: "v1.17-0.2.0", Guest: guest.Default()},
		"requires none":   {FirecrackerVersion: "v1.17-0.2.0", Guest: requiringProfile{Profile: guest.Default()}},
	} {
		require.NoError(t, versions.RequireFeatures(t.Context(), config), name)
	}
}

// The version rule runs before the probe, so a refused version never runs
// the binary, and an accepted one is still probed.
//
//nolint:paralleltest // writes a script and runs it
func TestRequireAcceptedBinaryRefusesTheVersionBeforeTheProbe(t *testing.T) {
	config, calls := fakeFirecrackerBinary(t, "v1.17-0.2.0", "printf 'Firecracker v1.17.0\\nFeatures: alpha\\n'")
	versions := Config{FirecrackerVersion: "v1.17-0.2.0", Guest: lineProfile{Profile: guest.Default(), line: "v1.17-100.0.1", features: []string{"alpha"}}}

	err := requireAcceptedBinary(t.Context(), versions, config)
	require.ErrorIs(t, err, errNotTheLine)
	_, statErr := os.Stat(calls)
	require.ErrorIs(t, statErr, os.ErrNotExist, "a refused version never runs the binary")

	versions.FirecrackerVersion = "v1.17-100.0.1"
	err = requireAcceptedBinary(t.Context(), versions, config)
	require.ErrorIs(t, err, ErrFeatureProbe, "an accepted version is probed: no binary of that version exists")

	config, calls = fakeFirecrackerBinary(t, "v1.17-100.0.1", "printf 'Firecracker v1.17.0\\nFeatures: alpha\\n'")
	require.NoError(t, requireAcceptedBinary(t.Context(), versions, config))
	runs, err := os.ReadFile(calls)
	require.NoError(t, err)
	assert.Equal(t, "run\n", string(runs))

	require.NoError(t, requireAcceptedBinary(t.Context(), Config{FirecrackerVersion: "v0"}, cfg.BuilderConfig{}), "the default profile accepts every version and asks nothing")
}

var errNotTheLine = errors.New("not the line")

// lineProfile accepts only the version line and requires features of the binary.
type lineProfile struct {
	guest.Profile

	line     string
	features []string
}

func (p lineProfile) Name() string  { return "line" }
func (p lineProfile) Label() string { return "line" }
func (p lineProfile) Firecracker() guest.FirecrackerSelection {
	return guest.FirecrackerSelection{
		AcceptVersion: func(version string) error {
			if version != p.line {
				return errNotTheLine
			}

			return nil
		},
		RequiredFeatures: p.features,
	}
}

func TestParseFeatures(t *testing.T) {
	t.Parallel()

	assert.Equal(t, []string{"alpha", "beta"}, parseFeatures([]byte("Firecracker v1.17.0\nFeatures: alpha beta\n\n")))
	assert.Equal(t, []string{"a"}, parseFeatures([]byte("Firecracker v1.17.0\n\nFeatures:   a  \nFeatures: b\n")))
	assert.Equal(t, []string{}, parseFeatures([]byte("Firecracker v1.17.0\nFeatures:\n")))
	assert.Nil(t, parseFeatures([]byte("Firecracker v1.17.0\n\n")))
}
