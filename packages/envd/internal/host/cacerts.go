package host

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"golang.org/x/sync/semaphore"

	"github.com/e2b-dev/infra/packages/envd/internal/cabundle"
)

var (
	ErrCAInstallInProgress = errors.New("CA install already in progress")
	managedCertFilename    = regexp.MustCompile(`^e2b-([0-9a-f]{64})\.crt$`)
)

const (
	CaBundlePath     = "/etc/ssl/certs/ca-certificates.crt"
	caCertDir        = "/usr/local/share/ca-certificates"
	legacyCACertFile = "e2b-ca.crt"
)

type caState struct {
	DesiredPEM string   `json:"desiredPEM"`
	Owned      []string `json:"owned"`
}

// CACertInstaller owns orchestrator-supplied certificates, including preexisting copies.
// It records intent before publishing trust; one Run loop retires old roots.
type CACertInstaller struct {
	mu        *semaphore.Weighted
	logger    *zerolog.Logger
	statePath string
	state     *caState
	requested cabundle.Bundle
	loaded    bool
	installed bool
	pending   bool
	wake      chan struct{}
}

func NewCACertInstaller(logger *zerolog.Logger) *CACertInstaller {
	return &CACertInstaller{logger: logger, mu: semaphore.NewWeighted(1), statePath: "/run/e2b/ca/state.json", wake: make(chan struct{}, 1)}
}

func (c *CACertInstaller) Install(ctx context.Context, bundlePEM string) error {
	return c.install(ctx, bundlePEM, CaBundlePath)
}

func (c *CACertInstaller) install(ctx context.Context, bundlePEM, bundlePath string) error {
	if bundlePEM == "" {
		return nil
	}
	certs, err := cabundle.Parse(bundlePEM)
	if err != nil {
		return fmt.Errorf("parse CA bundle: %w", err)
	}
	err = c.updateBundle(ctx, certs, bundlePath)
	select {
	case c.wake <- struct{}{}:
	default:
	}

	return err
}

// updateBundle records every pending retirement before publishing new trust.
func (c *CACertInstaller) updateBundle(ctx context.Context, certs cabundle.Bundle, bundlePath string) error {
	if err := c.mu.Acquire(ctx, 1); err != nil {
		return fmt.Errorf("%w: %w", ErrCAInstallInProgress, err)
	}
	defer c.mu.Release(1)
	if err := c.loadState(); err != nil {
		return err
	}
	if c.installed && c.requested.Equal(certs) {
		return nil
	}
	recovering := !c.installed && c.state != nil
	var owned []string
	if c.state != nil {
		owned = slices.Clone(c.state.Owned)
	}
	var desired strings.Builder
	for _, cert := range certs {
		if !slices.Contains(owned, cert.Fingerprint) {
			owned = append(owned, cert.Fingerprint)
		}
		desired.Write(cert.PEM)
	}
	state := &caState{DesiredPEM: desired.String(), Owned: owned}
	if err := c.saveState(state); err != nil {
		return err
	}
	c.state, c.requested = state, certs
	c.installed, c.pending = false, true
	data, err := os.ReadFile(bundlePath)
	if err != nil {
		return fmt.Errorf("read CA bundle: %w", err)
	}
	if recovering {
		// Repair an interrupted older append before init can report success.
		data = cabundle.Reconcile(data, state.Owned, certs)
	} else {
		// Ordinary installs copy existing bytes without decoding the bundle.
		data = append(data, '\n')
		data = append(data, state.DesiredPEM...)
	}
	if err := writeFileAtomic(bundlePath, data); err != nil {
		return fmt.Errorf("publish CA bundle: %w", err)
	}
	c.installed = true

	return nil
}

// Run resumes persisted intent immediately and retries interrupted work once a second.
func (c *CACertInstaller) Run(ctx context.Context) {
	c.run(ctx, CaBundlePath, caCertDir)
}

func (c *CACertInstaller) run(ctx context.Context, bundlePath, certDir string) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var failed bool
	for {
		err := c.persist(ctx, bundlePath, certDir)
		if err == nil {
			failed = false
		} else if !failed && ctx.Err() == nil {
			failed = true
			c.logger.Error().Err(err).Msg("Failed to reconcile CA certificates")
		}
		select {
		case <-ctx.Done():
			return
		case <-c.wake:
		case <-ticker.C:
		}
	}
}

// persist runs in one worker. Source writes run outside mu; legacy removal checks the latest request.
func (c *CACertInstaller) persist(ctx context.Context, bundlePath, certDir string) error {
	if err := c.mu.Acquire(ctx, 1); err != nil {
		return err
	}
	if err := c.loadState(); err != nil {
		c.mu.Release(1)

		return err
	}
	if !c.pending {
		c.mu.Release(1)

		return nil
	}
	state, certs := c.state, c.requested
	c.mu.Release(1)

	content, err := os.ReadFile(bundlePath)
	if err != nil {
		return fmt.Errorf("read CA bundle for cleanup: %w", err)
	}
	var nextOwned []string
	for _, cert := range certs {
		nextOwned = append(nextOwned, cert.Fingerprint)
	}
	content = cabundle.Reconcile(content, state.Owned, certs)
	tmp, err := prepareFile(bundlePath, content, 0o644)
	if err != nil {
		return err
	}
	defer os.Remove(tmp)
	if err := c.mu.Acquire(ctx, 1); err != nil {
		return err
	}
	if c.state != state {
		c.mu.Release(1)

		return nil
	}
	err = os.Rename(tmp, bundlePath)
	if err == nil {
		c.installed = true
	}
	c.mu.Release(1)
	if err != nil {
		return fmt.Errorf("publish CA bundle: %w", err)
	}

	// Rootfs failures must not prevent retirement in the active bundle.
	if err := writeCertFiles(certDir, certs); err != nil {
		return err
	}
	if err := c.removeRetiredCertFiles(ctx, certDir, certs); err != nil {
		return err
	}
	nextState := &caState{DesiredPEM: state.DesiredPEM, Owned: nextOwned}
	stateData, err := json.Marshal(nextState)
	if err != nil {
		return err
	}
	stateTmp, err := prepareFile(c.statePath, stateData, 0o600)
	if err != nil {
		return err
	}
	defer os.Remove(stateTmp)

	if err := c.mu.Acquire(ctx, 1); err != nil {
		return err
	}
	defer c.mu.Release(1)
	if c.state != state {
		return nil
	}
	// Forget retired identities only after source files and the aggregate are cleaned.
	if err := os.Rename(stateTmp, c.statePath); err != nil {
		return fmt.Errorf("publish CA state: %w", err)
	}
	c.state = nextState
	c.installed, c.pending = true, false

	return nil
}

// loadState runs under mu. Missing state never authorizes removal of existing trust.
func (c *CACertInstaller) loadState() error {
	if c.loaded {
		return nil
	}
	data, err := os.ReadFile(c.statePath)
	if errors.Is(err, os.ErrNotExist) {
		c.loaded = true

		return nil
	}
	if err != nil {
		return fmt.Errorf("read CA state: %w", err)
	}
	var state caState
	if err := json.Unmarshal(data, &state); err != nil {
		return fmt.Errorf("decode CA state: %w", err)
	}
	var certs cabundle.Bundle
	if state.DesiredPEM != "" {
		certs, err = cabundle.Parse(state.DesiredPEM)
		if err != nil {
			return fmt.Errorf("parse CA state: %w", err)
		}
	}
	c.state, c.requested = &state, certs
	c.loaded, c.pending = true, true

	return nil
}

func (c *CACertInstaller) saveState(state *caState) error {
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(c.statePath), 0o700); err != nil {
		return fmt.Errorf("create CA state directory: %w", err)
	}
	tmp, err := prepareFile(c.statePath, data, 0o600)
	if err != nil {
		return fmt.Errorf("prepare CA state: %w", err)
	}
	defer os.Remove(tmp)
	if err := os.Rename(tmp, c.statePath); err != nil {
		return fmt.Errorf("publish CA state: %w", err)
	}

	return nil
}

func writeCertFiles(certDir string, certs cabundle.Bundle) error {
	if err := os.MkdirAll(certDir, 0o755); err != nil {
		return fmt.Errorf("create CA cert dir: %w", err)
	}
	for _, cert := range certs {
		name := makeCertName(cert.Fingerprint)
		if err := writeFileAtomic(filepath.Join(certDir, name), cert.PEM); err != nil {
			return fmt.Errorf("persist CA cert: %w", err)
		}
	}

	return nil
}

func (c *CACertInstaller) removeRetiredCertFiles(ctx context.Context, certDir string, certs cabundle.Bundle) error {
	wanted := make(map[string]struct{}, len(certs))
	for _, cert := range certs {
		wanted[cert.Fingerprint] = struct{}{}
	}
	entries, err := os.ReadDir(certDir)
	if err != nil {
		return fmt.Errorf("read CA cert dir: %w", err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if name == legacyCACertFile {
			if err := c.removeLegacyCert(ctx, filepath.Join(certDir, name)); err != nil {
				return err
			}

			continue
		}
		match := managedCertFilename.FindStringSubmatch(name)
		if match == nil {
			continue
		}
		if _, keep := wanted[match[1]]; keep {
			continue
		}
		if err := os.Remove(filepath.Join(certDir, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove stale CA cert: %w", err)
		}
	}

	return nil
}

// Existing OpenSSL hash links still resolve through the legacy source filename.
func (c *CACertInstaller) removeLegacyCert(ctx context.Context, path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read legacy CA cert: %w", err)
	}
	legacy, parseErr := cabundle.Parse(string(data))
	if err := c.mu.Acquire(ctx, 1); err != nil {
		return err
	}
	defer c.mu.Release(1)
	// Check the latest request and unlink together, so init cannot adopt this alias between them.
	if parseErr == nil && len(legacy) == 1 {
		for _, cert := range c.requested {
			if cert.Fingerprint == legacy[0].Fingerprint {
				return nil
			}
		}
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove legacy CA cert: %w", err)
	}

	return nil
}

func makeCertName(fingerprint string) string {
	return fmt.Sprintf("e2b-%s.crt", fingerprint)
}

// prepareFile stages a complete replacement beside its destination.
func prepareFile(path string, data []byte, mode os.FileMode) (string, error) {
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".tmp*")
	if err != nil {
		return "", fmt.Errorf("create temp file: %w", err)
	}
	tmpPath := tmp.Name()
	err = tmp.Chmod(mode)
	if err == nil {
		_, err = tmp.Write(data)
	}
	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		os.Remove(tmpPath)

		return "", err
	}

	return tmpPath, nil
}

func writeFileAtomic(path string, data []byte) error {
	tmp, err := prepareFile(path, data, 0o644)
	if err != nil {
		return err
	}
	defer os.Remove(tmp)

	return os.Rename(tmp, path)
}
