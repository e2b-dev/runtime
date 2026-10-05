package featureflags

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestResolveEnvdOfflineUpgrade_ReadsSiblingFlag proves the offline resolver
// is driven by envd-offline-upgrade-target and is independent of the
// live-path flag envd-upgrade-target — so the two mechanisms ramp separately.
// It goes through the real Client so the flag plumbing, not just the pure
// decision, is exercised.
func TestResolveEnvdOfflineUpgrade_ReadsSiblingFlag(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	promoted := filepath.Join(dir, "envd")
	require.NoError(t, os.WriteFile(promoted, []byte("x"), 0o755))
	getVersion := func(_ context.Context, _ string) (string, error) { return "0.6.12", nil }

	// Live-path flag ON, offline flag OFF (its default): the offline resolver must
	// still be a no-op — it must not read the live flag.
	source := ldtestdata.DataSource()
	source.Update(source.Flag(EnvdUpgradeTargetFlag.Key()).ValueForAll(ldvalue.String("promoted")))
	ff, err := NewClientWithDatasource(source)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ff.Close(context.WithoutCancel(t.Context())) })

	path, _, reason := ResolveEnvdOfflineUpgrade(t.Context(), ff, "0.6.5", promoted, getVersion, nil)
	assert.Empty(t, path, "offline resolver must ignore the live-path flag")
	assert.Equal(t, "off", reason)

	// Now turn the offline flag on: the swap resolves.
	source.Update(source.Flag(EnvdOfflineUpgradeTargetFlag.Key()).ValueForAll(ldvalue.String("promoted")))
	path, version, reason := ResolveEnvdOfflineUpgrade(t.Context(), ff, "0.6.5", promoted, getVersion, nil)
	assert.Equal(t, promoted, path)
	assert.Equal(t, "0.6.12", version)
	assert.Empty(t, reason)
}

// TestOfflineUpgradeConvergence covers the convergence behavior: the offline swap keys on the
// snapshot's BUILT-WITH version (there is no running envd at cold-boot swap
// time), and the swap never advances that recorded version. So a re-resumed,
// already-upgraded snapshot resolves an upgrade AGAIN on every cold boot — an
// accepted, idempotent re-fire — until a re-pause re-bakes the version. This
// test walks the two resume cycles the PoC exercises on the cluster.
func TestOfflineUpgradeConvergence(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	promoted := filepath.Join(dir, "envd")
	require.NoError(t, os.WriteFile(promoted, []byte("x"), 0o755))

	const (
		builtWith = "0.6.5"  // the snapshot's recorded (built-with) version
		target    = "0.6.12" // the staged upgrade target
	)
	getVersion := func(_ context.Context, path string) (string, error) {
		if path == promoted {
			return target, nil
		}

		return "", fmt.Errorf("unknown binary %s", path)
	}

	// Cycle 1 — first resume of the pre-upgrade snapshot: built-with < target, so
	// the resolver returns a swap.
	path, version, reason := resolveEnvdUpgradePath(t.Context(), "promoted", builtWith, promoted, getVersion, nil)
	require.Equal(t, promoted, path, "cycle 1 must resolve a swap")
	require.Equal(t, target, version)
	require.Empty(t, reason)

	// Cycle 2 — resume AGAIN without a re-pause. The on-disk binary is now the
	// target, but the snapshot's built-with is unchanged (the swap does not update
	// it, and pause does not persist LiveEnvdVersion), so the resolver STILL sees
	// built-with < target and re-fires. Harmless: the swap replaces the target
	// binary with the same target binary (idempotent).
	path2, version2, reason2 := resolveEnvdUpgradePath(t.Context(), "promoted", builtWith, promoted, getVersion, nil)
	assert.Equal(t, promoted, path2, "cycle 2 re-fires because built-with never advances")
	assert.Equal(t, target, version2)
	assert.Empty(t, reason2)

	// Convergence only after a re-pause re-bakes the running version as built-with:
	// then built-with == target and the resolver no-ops (same_version). This is the
	// deferred cross-pause improvement; without it, the re-fire above is the
	// accepted steady state.
	pathConverged, _, reasonConverged := resolveEnvdUpgradePath(t.Context(), "promoted", target, promoted, getVersion, nil)
	assert.Empty(t, pathConverged, "once built-with == target the swap no-ops")
	assert.Equal(t, "same_version", reasonConverged)
}
