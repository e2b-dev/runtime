//go:build linux

package sandbox

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Stopping counts lifecycles that left the live map but have not finished
// their cleanup: a kill, pause or checkpoint still releasing its resources. A
// lifecycle that is tracked but not yet live counts too, by design: the count
// errs toward "something is being released", which costs its caller a retry.
func TestMapStopping(t *testing.T) {
	t.Parallel()

	sandboxes := NewSandboxesMap()
	assert.Equal(t, 0, sandboxes.Stopping(), "an empty map releases nothing")

	r, err := sandboxes.Reserve("sandbox-1")
	require.NoError(t, err)
	sbx := testMapSandbox(t, "lifecycle-1")
	require.NoError(t, r.MarkRunning(t.Context(), sbx))
	r.Release()
	assert.Equal(t, 0, sandboxes.Stopping(), "a live sandbox is not stopping")

	require.True(t, sandboxes.MarkStopping(t.Context(), "sandbox-1", "lifecycle-1"))
	assert.Equal(t, 1, sandboxes.Stopping(), "out of the live map, cleanup outstanding")

	sandboxes.MarkStopped(t.Context(), sbx)
	assert.Equal(t, 0, sandboxes.Stopping(), "cleanup done")

	tracked := testMapSandbox(t, "lifecycle-2")
	sandboxes.TrackLifecycle(t.Context(), tracked)
	assert.Equal(t, 1, sandboxes.Stopping(), "tracked but not live counts, the accepted over-count")
	sandboxes.MarkStopped(t.Context(), tracked)
	assert.Equal(t, 0, sandboxes.Stopping())
}

// StoppingMatching counts only the stopping lifecycles match accepts; a
// live one never counts.
func TestMapStoppingMatching(t *testing.T) {
	t.Parallel()

	hugePages := func(sbx *Sandbox) bool { return sbx.Config.HugePages }
	sandboxes := NewSandboxesMap()

	r, err := sandboxes.Reserve("sandbox-1")
	require.NoError(t, err)
	huge := testMapSandbox(t, "lifecycle-huge")
	huge.Config.HugePages = true
	require.NoError(t, r.MarkRunning(t.Context(), huge))
	r.Release()
	assert.Equal(t, 0, sandboxes.StoppingMatching(hugePages), "a live sandbox is not stopping")

	plain := testMapSandbox(t, "lifecycle-plain")
	plain.Runtime.SandboxID = "sandbox-2"
	sandboxes.TrackLifecycle(t.Context(), plain)
	assert.Equal(t, 1, sandboxes.Stopping())
	assert.Equal(t, 0, sandboxes.StoppingMatching(hugePages), "match leaves the other lifecycle out")

	require.True(t, sandboxes.MarkStopping(t.Context(), "sandbox-1", "lifecycle-huge"))
	assert.Equal(t, 1, sandboxes.StoppingMatching(hugePages))

	sandboxes.MarkStopped(t.Context(), huge)
	sandboxes.MarkStopped(t.Context(), plain)
	assert.Equal(t, 0, sandboxes.StoppingMatching(hugePages))
}
