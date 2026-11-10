package nodemanager

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
)

func TestUnreachableSince_DefaultReachable(t *testing.T) {
	t.Parallel()

	n := NewTestNode("test-node", api.NodeStatusReady, 0, 1)

	_, unreachable := n.UnreachableSince()
	assert.False(t, unreachable)
}

func TestMarkUnreachable_KeepsTheFirstObservation(t *testing.T) {
	t.Parallel()

	n := NewTestNode("test-node", api.NodeStatusReady, 0, 1)

	n.markUnreachable()
	first, unreachable := n.UnreachableSince()
	require.True(t, unreachable)

	// Consecutive silent cycles must preserve the first timestamp so the
	// unreachable duration keeps accumulating rather than restarting.
	time.Sleep(5 * time.Millisecond)
	n.markUnreachable()
	second, unreachable := n.UnreachableSince()
	require.True(t, unreachable)
	assert.Equal(t, first, second)
}

// TestMarkUnhealthyLocal_DoesNotImplyUnreachable pins the separation the two
// signals need. A sync can fail on a node this replica demonstrably reached —
// ServiceInfo answers and the sandbox list call then errors — so marking a node
// unhealthy must not also assert that this replica cannot reach it. Conflating
// them lets one failed call present a live node as a candidate for reclamation.
func TestMarkUnhealthyLocal_DoesNotImplyUnreachable(t *testing.T) {
	t.Parallel()

	n := NewTestNode("test-node", api.NodeStatusReady, 0, 1)

	n.markUnhealthyLocal(t.Context())

	assert.Equal(t, api.NodeStatusUnhealthy, n.Status())
	_, unreachable := n.UnreachableSince()
	assert.False(t, unreachable, "an unhealthy node that answered is still reachable")
}

func TestMarkReachable_ClearsUnreachableSince(t *testing.T) {
	t.Parallel()

	n := NewTestNode("test-node", api.NodeStatusReady, 0, 1)

	n.markUnreachable()
	n.markReachable()

	_, unreachable := n.UnreachableSince()
	assert.False(t, unreachable)

	// The next silent cycle starts a fresh clock.
	n.markUnreachable()
	since, unreachable := n.UnreachableSince()
	require.True(t, unreachable)
	assert.WithinDuration(t, time.Now(), since, time.Second)
}

// TestSync_NodeThatAnsweredIsNotUnreachable drives the reported scenario end to
// end. The node answers ServiceInfo, but the sandbox list call fails, so the
// sync never completes — four times over, exhausting the retries.
//
// The node has demonstrably answered this replica, so it must not come out of
// that unreachable. It does come out unhealthy: the cycle genuinely failed.
//
// A nil store is safe here precisely because the sync cannot get far enough to
// reconcile; if that ever changes this test panics rather than passing quietly.
func TestSync_NodeThatAnsweredIsNotUnreachable(t *testing.T) {
	t.Parallel()

	n := NewTestNode("test-node", api.NodeStatusReady, 0, 1, WithFailingSandboxList())

	require.NoError(t, n.Sync(t.Context(), nil), "a transient sync failure is handled locally, not escalated")

	_, unreachable := n.UnreachableSince()
	assert.False(t, unreachable, "a node that answered every RPC must not be marked unreachable")
	assert.Equal(t, api.NodeStatusUnhealthy, n.Status(), "the failed sync must still degrade status")
}

// TestSync_SilentNodeIsUnreachable is the other half: a node that never answers
// must be marked unreachable, or the signal reports nothing at all.
func TestSync_SilentNodeIsUnreachable(t *testing.T) {
	t.Parallel()

	n := NewTestNode("test-node", api.NodeStatusReady, 0, 1, WithSilentInfoClient())

	require.NoError(t, n.Sync(t.Context(), nil), "an unreachable node is handled locally, not escalated")

	since, unreachable := n.UnreachableSince()
	require.True(t, unreachable, "a node that never answered must be marked unreachable")
	assert.WithinDuration(t, time.Now(), since, time.Second)
	assert.Equal(t, api.NodeStatusUnhealthy, n.Status())
}

// TestSelfReportedUnhealthyIsNotUnreachable pins the distinction this signal
// exists for: a node whose orchestrator self-reports Unhealthy over a
// successful sync is responsive, and must never count as unreachable.
func TestSelfReportedUnhealthyIsNotUnreachable(t *testing.T) {
	t.Parallel()

	n := NewTestNode("test-node", api.NodeStatusReady, 0, 1)
	n.setStatus(t.Context(), api.NodeStatusUnhealthy, "", time.Now().Add(-time.Hour))

	_, unreachable := n.UnreachableSince()
	assert.False(t, unreachable)
}
