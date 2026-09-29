package placement

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
)

// refusingClient answers the first `times` creates with the refusal a node gives
// when its start slots are taken (negative: every create), counting them all.
func refusingClient(creates *atomic.Int64, times int64) *nodemanager.MockSandboxClientCustom {
	return &nodemanager.MockSandboxClientCustom{
		CreateFunc: func() error {
			if n := creates.Add(1); times < 0 || n <= times {
				return status.Error(codes.ResourceExhausted, "too many sandboxes starting on this node, please retry")
			}

			return nil
		},
	}
}

// firstNotExcluded picks the first node, in order, the placement has not
// excluded, recording each pick.
func firstNotExcluded(nodes []*nodemanager.Node, picks *[]string) stubAlgorithm {
	return stubAlgorithm{choose: func(excluded map[string]struct{}) (*nodemanager.Node, error) {
		for _, n := range nodes {
			if _, ok := excluded[n.ID]; !ok {
				*picks = append(*picks, n.ID)

				return n, nil
			}
		}

		return nil, FailedToPlaceSandboxError{}
	}}
}

// A refusal is capacity that frees up, not a failure: the node stays eligible
// and a later retry can land on it.
func TestPlaceSandbox_RefusedNodeIsRetried(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	var creates atomic.Int64
	only := nodemanager.NewTestNode("only", api.NodeStatusReady, 0, 8)
	only.SetSandboxClient(refusingClient(&creates, 2))
	nodes := []*nodemanager.Node{only}

	var picks []string
	result, err := placeSandbox(ctx, firstNotExcluded(nodes, &picks), nodes, nil, testSbxRequest("sbx-retry"),
		CPURequirement{}, FeatureRequirement{}, false, nil, refusalBackoff{})

	require.NoError(t, err)
	assert.Equal(t, only.ID, result.Node.ID)
	assert.Equal(t, int64(3), creates.Load())
	assert.Equal(t, []string{only.ID, only.ID, only.ID}, picks)
}

// With every node refusing, the default backoff paces the retries: without it
// the loop re-sends a create as fast as the refusals return, thousands in the
// window below.
func TestPlaceSandbox_RefusalsArePaced(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()

	var creates atomic.Int64
	busyA := nodemanager.NewTestNode("busy-a", api.NodeStatusReady, 0, 8)
	busyA.SetSandboxClient(refusingClient(&creates, -1))
	busyB := nodemanager.NewTestNode("busy-b", api.NodeStatusReady, 0, 8)
	busyB.SetSandboxClient(refusingClient(&creates, -1))
	nodes := []*nodemanager.Node{busyA, busyB}

	var picks []string
	result, err := PlaceSandbox(ctx, firstNotExcluded(nodes, &picks), nodes, nil, testSbxRequest("sbx-paced"), CPURequirement{}, false, nil)

	var noNodesErr NoNodesAvailableError
	require.ErrorAs(t, err, &noNodesErr, "refusals until the deadline are still classified as capacity")
	assert.True(t, result.TimedOut)
	assert.Less(t, creates.Load(), int64(30))
}

// The deadline ends a backoff wait instead of the wait outliving the request.
func TestPlaceSandbox_DeadlineEndsRefusalBackoff(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	var creates atomic.Int64
	busy := nodemanager.NewTestNode("busy", api.NodeStatusReady, 0, 8)
	busy.SetSandboxClient(refusingClient(&creates, -1))
	nodes := []*nodemanager.Node{busy}

	var picks []string
	start := time.Now()
	result, err := placeSandbox(ctx, firstNotExcluded(nodes, &picks), nodes, nil, testSbxRequest("sbx-deadline"),
		CPURequirement{}, FeatureRequirement{}, false, nil, refusalBackoff{base: time.Hour, max: time.Hour})

	assert.Less(t, time.Since(start), 5*time.Second)

	var noNodesErr NoNodesAvailableError
	require.ErrorAs(t, err, &noNodesErr)
	assert.True(t, result.TimedOut)
	assert.LessOrEqual(t, creates.Load(), int64(2), "an hour-wide backoff leaves no time for more creates")
}
