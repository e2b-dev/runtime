package featureflags

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The candidate checks run before the version probe, so a bounded probe alone
// does not keep a stalled mount off the resume path: the stat must be the
// injected one too, and its stall must not read as a misconfigured target.
func TestACandidateStatThatStallsIsSourceStalledNotNotStaged(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	promoted := filepath.Join(dir, "envd")
	require.NoError(t, os.WriteFile(promoted, []byte("x"), 0o755))

	stalled := func(context.Context, string) (os.FileInfo, error) {
		return nil, fmt.Errorf("stat: %w", ErrEnvdSourceStalled)
	}
	probed := false
	getVersion := func(context.Context, string) (string, error) {
		probed = true

		return "0.7.0", nil
	}

	for _, target := range []string{"promoted", "v0.9.0"} {
		path, version, reason := resolveEnvdUpgradePath(t.Context(), target, "0.6.0", promoted, getVersion, stalled)
		assert.Empty(t, path, target)
		assert.Empty(t, version, target)
		assert.Equal(t, ReasonSourceStalled, reason, target)
	}
	assert.False(t, probed, "a stalled candidate check must not go on to probe the binary")
}

func TestAProbeThatStallsIsSourceStalled(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	promoted := filepath.Join(dir, "envd")
	require.NoError(t, os.WriteFile(promoted, []byte("x"), 0o755))

	for name, probeErr := range map[string]error{
		"stall":    fmt.Errorf("lookup: %w", ErrEnvdSourceStalled),
		"deadline": context.DeadlineExceeded,
	} {
		getVersion := func(context.Context, string) (string, error) { return "", probeErr }

		_, _, reason := resolveEnvdUpgradePath(t.Context(), "promoted", "0.6.0", promoted, getVersion, nil)
		assert.Equal(t, ReasonSourceStalled, reason, name)
	}
}

// A stat that answers "missing" is still a target that is not staged.
func TestACandidateThatIsMissingIsStillNotStaged(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	promoted := filepath.Join(dir, "envd")
	missing := func(_ context.Context, path string) (os.FileInfo, error) {
		return nil, &os.PathError{Op: "stat", Path: path, Err: os.ErrNotExist}
	}
	getVersion := func(context.Context, string) (string, error) {
		return "", errors.New("must not be probed")
	}

	for _, target := range []string{"promoted", "v0.9.0"} {
		_, _, reason := resolveEnvdUpgradePath(t.Context(), target, "0.6.0", promoted, getVersion, missing)
		assert.Equal(t, "not_staged", reason, target)
	}
}

// A version-pinned target is stat-ed once per layout tried and not again: the
// winner's own stat is the one that found it.
func TestAVersionPinnedCandidateIsNotStatedTwice(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	promoted := filepath.Join(dir, "envd")
	flat := filepath.Join(dir, "envd.v0.9.0")
	require.NoError(t, os.WriteFile(flat, []byte("x"), 0o755))

	stats := map[string]int{}
	stat := func(_ context.Context, path string) (os.FileInfo, error) {
		stats[path]++

		return os.Stat(path)
	}

	candidate, reason := envdUpgradeCandidate(t.Context(), "v0.9.0", promoted, stat)
	require.Empty(t, reason)
	require.Equal(t, flat, candidate)
	require.Equal(t, map[string]int{flat: 1}, stats)
}
