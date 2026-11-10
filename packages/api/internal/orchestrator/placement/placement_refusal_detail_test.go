package placement

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
)

// countingCreate is a node that refuses every create with err and counts
// how often it was asked.
func countingCreate(t *testing.T, id string, err error) (*nodemanager.Node, *atomic.Int32) {
	t.Helper()

	var calls atomic.Int32
	node := nodemanager.NewTestNode(id, api.NodeStatusReady, 0, 4)
	node.SetSandboxClient(&nodemanager.MockSandboxClientCustom{CreateFunc: func() error {
		calls.Add(1)

		return err
	}})

	return node, &calls
}

func refusedForGood(t *testing.T, message string) error {
	t.Helper()

	st, err := status.New(codes.InvalidArgument, message).WithDetails(&orchestrator.UserError{Code: orchestrator.UserErrorCode_SANDBOX_CREATE_REFUSED, Message: message})
	require.NoError(t, err)

	return st.Err()
}

// A create the node refused with the typed detail ends placement there: the
// answer is the same on every node, so no other node is asked, and it says
// nothing about the node, so it is skipped rather than counted as failing.
// A refusal without the detail (a node that predates it) still walks the
// nodes and counts.
func TestPlaceSandbox_StopsOnACreateRefusedForGood(t *testing.T) {
	t.Parallel()

	for name, tt := range map[string]struct {
		firstErr     error
		wantAttempts int
		wantOthers   int32
	}{
		"typed detail":      {firstErr: refusedForGood(t, "vcpu 0 must be at least 1"), wantAttempts: 1, wantOthers: 1},
		"message only":      {firstErr: status.Error(codes.InvalidArgument, "vcpu 0 must be at least 1"), wantAttempts: 3, wantOthers: 1},
		"transient refusal": {firstErr: status.Error(codes.FailedPrecondition, "node is draining"), wantAttempts: 3, wantOthers: 1},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			first, firstCalls := countingCreate(t, "node1", tt.firstErr)
			second, secondCalls := countingCreate(t, "node2", status.Error(codes.Internal, "create failed on second node"))
			third, thirdCalls := countingCreate(t, "node3", status.Error(codes.Internal, "create failed on third node"))
			nodes := []*nodemanager.Node{first, second, third}
			algorithm := stubAlgorithm{choose: func(excluded map[string]struct{}) (*nodemanager.Node, error) {
				for _, n := range nodes {
					if _, ok := excluded[n.ID]; !ok {
						return n, nil
					}
				}

				return nil, assert.AnError
			}}

			_, err := PlaceSandbox(t.Context(), algorithm, nodes, nil, testSbxRequest("test-sandbox"), Requirements{}, false, nil)

			var createErr SandboxCreateError
			require.ErrorAs(t, err, &createErr)
			assert.Equal(t, tt.wantAttempts, createErr.Attempts)
			assert.EqualValues(t, 1, firstCalls.Load())
			assert.Zero(t, first.PlacementMetrics.InProgressCount(), "the refused sandbox is still counted as placing")
			if tt.wantAttempts == 1 {
				require.ErrorIs(t, createErr.LastErr, tt.firstErr)
				assert.Zero(t, secondCalls.Load(), "a node was asked after a refusal for good")
				assert.Zero(t, thirdCalls.Load(), "a node was asked after a refusal for good")
				assert.Zero(t, first.PlacementMetrics.FailsCount(), "a refusal for good counted against the node")
			} else {
				assert.Equal(t, tt.wantOthers, secondCalls.Load())
				assert.Equal(t, tt.wantOthers, thirdCalls.Load())
				assert.EqualValues(t, 1, first.PlacementMetrics.FailsCount())
			}
		})
	}
}

// A refusal for good that lands as the request context ends is still the
// refusal, not a timeout: the result names no warmed node and no timeout,
// so a resume's origin is not remapped to the node that just refused it
// for good.
func TestPlaceSandbox_RefusedForGoodAfterTheContextEndedIsNoTimeout(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())

	refused := refusedForGood(t, "vcpu 0 must be at least 1")
	node := nodemanager.NewTestNode("node-refusing", api.NodeStatusReady, 0, 8)
	node.SetSandboxClient(erroringClient(cancel, refused))

	result, err := PlaceSandbox(ctx, failIfCalled(t), []*nodemanager.Node{node}, node, testSbxRequest("sbx-refused"), Requirements{}, false, nil)

	var createErr SandboxCreateError
	require.ErrorAs(t, err, &createErr)
	require.ErrorIs(t, createErr.LastErr, refused)
	assert.False(t, result.TimedOut, "a refusal for good reported as a timeout")
	assert.Nil(t, result.WarmedNode, "a refusal for good surfaced the refusing node as warmed")
}
