package placement

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
)

// TestPlaceSandbox_InterruptedCreateReportsNode: when a node's SandboxCreate is
// interrupted by the request context being cancelled, that node is reported as
// InterruptedNode so the caller can compensate for an instance the node may
// have completed server-side (issue #3637).
func TestPlaceSandbox_InterruptedCreateReportsNode(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	node := nodemanager.NewTestNode("node1", api.NodeStatusReady, 0, 4)
	// Cancels the request context, then returns as the node create would when
	// the deadline lands mid-flight (the orchestrator collapses this to Internal).
	node.SetSandboxClient(erroringClient(cancel, status.Error(codes.Internal, "context canceled")))

	result, err := PlaceSandbox(ctx, failIfCalled(t), []*nodemanager.Node{node}, node, testSbxRequest("test-sandbox"), CPURequirement{}, false, nil)

	require.Error(t, err)
	assert.True(t, result.TimedOut)
	require.NotNil(t, result.InterruptedNode, "the interrupted node must be reported for compensation")
	assert.Equal(t, node.ID, result.InterruptedNode.ID)
}

// TestPlaceSandbox_ResourceExhaustedInterruptNotCompensated: a node that refused
// with ResourceExhausted never started a create, so even when the deadline
// lands on it, it must NOT be reported as InterruptedNode — killing it would be
// a pointless RPC against a node that holds nothing.
func TestPlaceSandbox_ResourceExhaustedInterruptNotCompensated(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	node := nodemanager.NewTestNode("node1", api.NodeStatusReady, 0, 4)
	node.SetSandboxClient(erroringClient(cancel, status.Error(codes.ResourceExhausted, "no capacity")))

	result, err := PlaceSandbox(ctx, failIfCalled(t), []*nodemanager.Node{node}, node, testSbxRequest("test-sandbox"), CPURequirement{}, false, nil)

	require.Error(t, err)
	assert.True(t, result.TimedOut)
	assert.Nil(t, result.InterruptedNode, "a ResourceExhausted refusal must not be compensated")
}

// TestPlaceSandbox_HardFailureNoCancelNoInterruptedNode: a hard create failure
// while the context is still live is a genuine node failure, not an interrupt.
// It must not be reported for compensation (the node cleaned up itself).
func TestPlaceSandbox_HardFailureNoCancelNoInterruptedNode(t *testing.T) {
	t.Parallel()

	node := nodemanager.NewTestNode("node1", api.NodeStatusReady, 0, 4,
		nodemanager.WithSandboxCreateError(status.Error(codes.Internal, "create failed")))

	result, err := PlaceSandbox(t.Context(), failIfCalled(t), []*nodemanager.Node{node}, node, testSbxRequest("test-sandbox"), CPURequirement{}, false, nil)

	require.Error(t, err)
	assert.False(t, result.TimedOut)
	assert.Nil(t, result.InterruptedNode)
}
