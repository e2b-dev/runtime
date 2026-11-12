package placement

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
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

// statusRefusingClient answers the first `times` creates with the refusal of a
// node whose status admits nothing new (negative: every create), counting them
// all. The detail is what tells it from a capacity refusal.
func statusRefusingClient(creates *atomic.Int64, times int64) *nodemanager.MockSandboxClientCustom {
	return &nodemanager.MockSandboxClientCustom{
		CreateFunc: func() error {
			if n := creates.Add(1); times < 0 || n <= times {
				st, err := status.New(codes.ResourceExhausted, "node is not accepting new sandboxes (status Standby), please retry").
					WithDetails(&orchestrator.UserError{Code: orchestrator.UserErrorCode_NODE_NOT_ACCEPTING_SANDBOXES})
				if err != nil {
					return err
				}

				return st.Err()
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
		Requirements{}, false, nil, refusalBackoff{}, time.Time{})

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

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	var creates atomic.Int64
	refuse := refusingClient(&creates, -1)
	var openWindow sync.Once
	// The window opens at the first create, so a slow start cannot spend it before any create.
	paced := &nodemanager.MockSandboxClientCustom{CreateFunc: func() error {
		openWindow.Do(func() { time.AfterFunc(300*time.Millisecond, cancel) })

		return refuse.CreateFunc()
	}}
	busyA := nodemanager.NewTestNode("busy-a", api.NodeStatusReady, 0, 8)
	busyA.SetSandboxClient(paced)
	busyB := nodemanager.NewTestNode("busy-b", api.NodeStatusReady, 0, 8)
	busyB.SetSandboxClient(paced)
	nodes := []*nodemanager.Node{busyA, busyB}

	var picks []string
	result, err := PlaceSandbox(ctx, firstNotExcluded(nodes, &picks), nodes, nil, testSbxRequest("sbx-paced"), Requirements{}, false, nil)

	var noNodesErr NoNodesAvailableError
	require.ErrorAs(t, err, &noNodesErr, "refusals until the caller leaves are still classified as capacity")
	assert.True(t, result.TimedOut)
	assert.Less(t, creates.Load(), int64(30))
}

// The deadline ends a backoff wait instead of the wait outliving the request.
// The clock is synctest's, so the deadline cannot pass before the first create.
func TestPlaceSandbox_DeadlineEndsRefusalBackoff(t *testing.T) {
	t.Parallel()

	// Built outside the bubble: the test node's gRPC connection runs goroutines of its own.
	busy := nodemanager.NewTestNode("busy", api.NodeStatusReady, 0, 8)
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancel()

		var creates atomic.Int64
		busy.SetSandboxClient(refusingClient(&creates, -1))
		nodes := []*nodemanager.Node{busy}

		var picks []string
		start := time.Now()
		result, err := placeSandbox(ctx, firstNotExcluded(nodes, &picks), nodes, nil, testSbxRequest("sbx-deadline"),
			Requirements{}, false, nil, refusalBackoff{base: time.Hour, max: time.Hour}, time.Time{})

		assert.Equal(t, 50*time.Millisecond, time.Since(start), "the wait ends at the deadline")

		var noNodesErr NoNodesAvailableError
		require.ErrorAs(t, err, &noNodesErr)
		assert.True(t, result.TimedOut)
		assert.LessOrEqual(t, creates.Load(), int64(2), "an hour-wide backoff leaves no time for more creates")
	})
}

// A node whose status refused a create is left out of the next sample while
// another candidate exists, so the retry does not go straight back to the
// parked node. It is not excluded: the sample after that may pick it again.
func TestPlaceSandbox_StatusRefusedNodeIsSkippedNextSampleWhenOthersExist(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	var busyCreates, otherCreates atomic.Int64
	busy := nodemanager.NewTestNode("busy", api.NodeStatusReady, 0, 8)
	busy.SetSandboxClient(statusRefusingClient(&busyCreates, 1))
	other := nodemanager.NewTestNode("other", api.NodeStatusReady, 0, 8)
	other.SetSandboxClient(refusingClient(&otherCreates, 0))
	nodes := []*nodemanager.Node{busy, other}

	var picks []string
	result, err := placeSandbox(ctx, firstNotExcluded(nodes, &picks), nodes, nil, testSbxRequest("sbx-skip"),
		Requirements{}, false, nil, refusalBackoff{}, time.Time{})

	require.NoError(t, err)
	assert.Equal(t, other.ID, result.Node.ID)
	assert.Equal(t, []string{busy.ID, other.ID}, picks)
	assert.Equal(t, int64(1), busyCreates.Load())
	assert.Equal(t, int64(1), otherCreates.Load())
	assert.Zero(t, busy.PlacementMetrics.FailsCount())
	assert.Zero(t, other.PlacementMetrics.FailsCount())
}

// A capacity refusal skips nothing: the node that refused stays the first
// candidate and the paced retry lands on it, as it did before the skip.
func TestPlaceSandbox_CapacityRefusedNodeIsRetriedNextSample(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	var busyCreates, otherCreates atomic.Int64
	busy := nodemanager.NewTestNode("busy", api.NodeStatusReady, 0, 8)
	busy.SetSandboxClient(refusingClient(&busyCreates, 1))
	other := nodemanager.NewTestNode("other", api.NodeStatusReady, 0, 8)
	other.SetSandboxClient(refusingClient(&otherCreates, 0))
	nodes := []*nodemanager.Node{busy, other}

	var picks []string
	result, err := placeSandbox(ctx, firstNotExcluded(nodes, &picks), nodes, nil, testSbxRequest("sbx-capacity"),
		Requirements{}, false, nil, refusalBackoff{}, time.Time{})
	require.NoError(t, err)
	assert.Equal(t, busy.ID, result.Node.ID)
	assert.Equal(t, []string{busy.ID, busy.ID}, picks)
	assert.Equal(t, int64(2), busyCreates.Load())
	assert.Zero(t, otherCreates.Load())
}

// Each refusal is counted under what caused it. Not parallel: it swaps the
// package counter.
//
//nolint:paralleltest
func TestPlaceSandbox_CountsRefusalsByCause(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	counter, err := telemetry.GetCounter(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("github.com/e2b-dev/infra/packages/api/internal/orchestrator/placement"), telemetry.ApiOrchestratorCreateRefusals)
	require.NoError(t, err)
	previous := createRefusals
	createRefusals = counter
	t.Cleanup(func() { createRefusals = previous })

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	// Two refusals of one cause and one of the other: with equal counts, swapped
	// labels would pass.
	var parkedCreates, fullCreates atomic.Int64
	parked := nodemanager.NewTestNode("parked", api.NodeStatusReady, 0, 8)
	parked.SetSandboxClient(statusRefusingClient(&parkedCreates, 2))
	full := nodemanager.NewTestNode("full", api.NodeStatusReady, 0, 8)
	full.SetSandboxClient(refusingClient(&fullCreates, 1))
	nodes := []*nodemanager.Node{parked, full}

	var picks []string
	_, err = placeSandbox(ctx, firstNotExcluded(nodes, &picks), nodes, nil, testSbxRequest("sbx-count"),
		Requirements{}, false, nil, refusalBackoff{}, time.Time{})
	require.NoError(t, err)

	var collected metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &collected))
	byCause := map[string]int64{}
	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok)
			for _, point := range sum.DataPoints {
				cause, _ := point.Attributes.Value("cause")
				byCause[cause.AsString()] = point.Value
			}
		}
	}
	assert.Equal(t, map[string]int64{"status": 2, "capacity": 1}, byCause)
}

// When no other candidate can take the sandbox, the skip is withdrawn and the
// refusing node is retried, as a single-node cluster is.
func TestPlaceSandbox_StatusRefusedNodeIsRetriedWhenOthersCannotTakeIt(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	var creates atomic.Int64
	busy := nodemanager.NewTestNode("busy", api.NodeStatusReady, 0, 8)
	busy.SetSandboxClient(statusRefusingClient(&creates, 1))
	small := nodemanager.NewTestNode("small", api.NodeStatusReady, 0, 8)
	nodes := []*nodemanager.Node{busy, small}

	// The small node never fits the sandbox: a sample without the busy node finds nothing.
	var picks []string
	onlyBusyFits := stubAlgorithm{choose: func(excluded map[string]struct{}) (*nodemanager.Node, error) {
		if _, ok := excluded[busy.ID]; ok {
			return nil, FailedToPlaceSandboxError{}
		}
		picks = append(picks, busy.ID)

		return busy, nil
	}}

	result, err := placeSandbox(ctx, onlyBusyFits, nodes, nil, testSbxRequest("sbx-only-fit"),
		Requirements{}, false, nil, refusalBackoff{}, time.Time{})

	require.NoError(t, err)
	assert.Equal(t, busy.ID, result.Node.ID)
	assert.Equal(t, []string{busy.ID, busy.ID}, picks)
	assert.Equal(t, int64(2), creates.Load())
}
