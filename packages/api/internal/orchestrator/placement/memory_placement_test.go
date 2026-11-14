package placement

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
)

func hugepageSbxRequest() *orchestrator.SandboxCreateRequest {
	req := testSbxRequest("sbx-1")
	req.Sandbox.HugePages = true

	return req
}

func TestPlaceSandbox_PreferredNodeWithAFullPoolFallsBack(t *testing.T) {
	t.Parallel()

	origin := hugepageNode("origin", 1600, 1600, 300, 2000)
	other := hugepageNode("other", 1600, 100, 0, 2000)
	var picks []string

	result, err := PlaceSandbox(t.Context(), firstNotExcluded([]*nodemanager.Node{other}, &picks), []*nodemanager.Node{origin, other}, origin,
		hugepageSbxRequest(), Requirements{Memory: limit95}, false, nil)

	require.NoError(t, err)
	assert.Equal(t, other.ID, result.Node.ID)
	assert.Equal(t, []string{other.ID}, picks)
}

func TestPlaceSandbox_PreferredNodeWithRoomIsKept(t *testing.T) {
	t.Parallel()

	origin := hugepageNode("origin", 1600, 100, 0, 2000)

	result, err := PlaceSandbox(t.Context(), failIfCalled(t), []*nodemanager.Node{origin}, origin,
		hugepageSbxRequest(), Requirements{Memory: limit95}, false, nil)

	require.NoError(t, err)
	assert.Equal(t, origin.ID, result.Node.ID)
}

func TestPlaceSandbox_GateOffKeepsAFullPreferredNode(t *testing.T) {
	t.Parallel()

	origin := hugepageNode("origin", 1600, 1600, 300, 2000)

	result, err := PlaceSandbox(t.Context(), failIfCalled(t), []*nodemanager.Node{origin}, origin,
		hugepageSbxRequest(), Requirements{}, false, nil)

	require.NoError(t, err)
	assert.Equal(t, origin.ID, result.Node.ID)
}

// The algorithm's pick filled up before the placement was recorded (the stub
// ignores memory, as a pick made before a concurrent placement would): the
// node is skipped without a create, and placement moves on.
func TestPlaceSandbox_NodeThatFilledBeforePlacingIsSkipped(t *testing.T) {
	t.Parallel()

	filled := hugepageNode("filled", 1600, 1600, 300, 2000)
	filled.SetSandboxClient(failingCreateClient(t))
	roomy := hugepageNode("roomy", 1600, 100, 0, 2000)
	var picks []string

	result, err := PlaceSandbox(t.Context(), firstNotExcluded([]*nodemanager.Node{filled, roomy}, &picks), []*nodemanager.Node{filled, roomy}, nil,
		hugepageSbxRequest(), Requirements{Memory: limit95}, false, nil)

	require.NoError(t, err)
	assert.Equal(t, roomy.ID, result.Node.ID)
	assert.Equal(t, []string{filled.ID, roomy.ID}, picks)
	assert.Zero(t, filled.PlacementMetrics.InProgressCount())
}

// Once refused at the record step, a full node is excluded, so chooseNode no
// longer sees its pool; the incompatible rest must not turn that into a mismatch.
func TestPlaceSandbox_FullPreferredNodeBesideIncompatibleNodesIsACapacityShortage(t *testing.T) {
	t.Parallel()

	origin := hugepageNode("origin", 1600, 1600, 300, 2000)
	draining := nodemanager.NewTestNode("draining", api.NodeStatusDraining, 0, 8)

	_, err := PlaceSandbox(t.Context(), NewBestOfK(DefaultBestOfKConfig()), []*nodemanager.Node{origin, draining}, origin,
		hugepageSbxRequest(), Requirements{Memory: limit95}, false, nil)

	require.ErrorAs(t, err, &NoNodesAvailableError{})
}

func TestPlaceSandbox_NodeThatFilledBeforePlacingBesideIncompatibleNodesIsACapacityShortage(t *testing.T) {
	t.Parallel()

	filled := hugepageNode("filled", 1600, 1600, 300, 2000)
	filled.SetSandboxClient(failingCreateClient(t))
	draining := nodemanager.NewTestNode("draining", api.NodeStatusDraining, 0, 8)
	var picks []string

	_, err := PlaceSandbox(t.Context(), firstNotExcluded([]*nodemanager.Node{filled}, &picks), []*nodemanager.Node{filled, draining}, nil,
		hugepageSbxRequest(), Requirements{Memory: limit95}, false, nil)

	require.ErrorAs(t, err, &NoNodesAvailableError{})
}

func TestPlaceSandbox_EveryPoolFullIsACapacityShortage(t *testing.T) {
	t.Parallel()

	nodes := []*nodemanager.Node{
		hugepageNode("a", 1600, 1600, 300, 2000),
		hugepageNode("b", 1600, 1700, 200, 2000),
	}

	_, err := PlaceSandbox(t.Context(), NewBestOfK(DefaultBestOfKConfig()), nodes, nil,
		hugepageSbxRequest(), Requirements{Memory: limit95}, false, nil)

	require.ErrorAs(t, err, &NoNodesAvailableError{})
}

// A deadline that runs out between pool-full refusals is still a capacity
// shortage, not a slow placement.
func TestPlaceSandbox_DeadlineDuringPoolFullRefusalsIsACapacityShortage(t *testing.T) {
	t.Parallel()

	full := hugepageNode("full", 1600, 1600, 300, 2000)
	alsoFull := hugepageNode("also-full", 1600, 1700, 200, 2000)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	algorithm := stubAlgorithm{choose: func(map[string]struct{}) (*nodemanager.Node, error) {
		cancel()

		return full, nil
	}}

	_, err := PlaceSandbox(ctx, algorithm, []*nodemanager.Node{full, alsoFull}, nil,
		hugepageSbxRequest(), Requirements{Memory: limit95}, false, nil)

	require.ErrorAs(t, err, &NoNodesAvailableError{})
}

// A preferred node that has left the cluster list does not count against the
// list, so refusing it cannot end placement before the listed nodes are tried.
func TestPlaceSandbox_FullPreferredNodeOffTheListLeavesTheListToTry(t *testing.T) {
	t.Parallel()

	origin := hugepageNode("origin", 1600, 1600, 300, 2000)
	joined := hugepageNode("joined", 1600, 100, 0, 2000)
	var picks []string

	result, err := PlaceSandbox(t.Context(), firstNotExcluded([]*nodemanager.Node{joined}, &picks), []*nodemanager.Node{joined}, origin,
		hugepageSbxRequest(), Requirements{Memory: limit95}, false, nil)

	require.NoError(t, err)
	assert.Equal(t, joined.ID, result.Node.ID)
	assert.Equal(t, []string{joined.ID}, picks)
}

func TestChooseNode_SkipsAFullPool(t *testing.T) {
	t.Parallel()

	full := hugepageNode("full", 1600, 1600, 300, 2000)
	roomy := hugepageNode("roomy", 1600, 1000, 0, 2000)
	algo := NewBestOfK(DefaultBestOfKConfig()).(*BestOfK)

	for range 20 {
		selected, err := algo.chooseNode(t.Context(), []*nodemanager.Node{full, roomy}, nil, hugepageSandbox, Requirements{Memory: limit95}, false, nil)
		require.NoError(t, err)
		assert.Equal(t, roomy.ID, selected.ID)
	}
}

// A full pool on a compatible node is capacity; nodes that are merely
// incompatible still report the requirement mismatch. The draining node lacks
// the required label, so it is a mismatch and not an unavailable fit.
func TestChooseNode_ClassifiesFullPoolsApartFromIncompatibleNodes(t *testing.T) {
	t.Parallel()

	algo := NewBestOfK(DefaultBestOfKConfig()).(*BestOfK)
	draining := nodemanager.NewTestNode("draining", api.NodeStatusDraining, 0, 8)
	full := nodemanager.NewTestNode("full", api.NodeStatusReady, 0, 8,
		nodemanager.WithHugePages(1600, 1600, 300, testPageBytes),
		nodemanager.WithHugePagesCapacity(2000),
		nodemanager.WithLabels([]string{"gpu"}),
	)
	labels := []string{"gpu"}

	_, err := algo.chooseNode(t.Context(), []*nodemanager.Node{draining, full}, nil, hugepageSandbox, Requirements{Memory: limit95}, true, labels)
	require.ErrorAs(t, err, &NoNodesAvailableError{})

	_, err = algo.chooseNode(t.Context(), []*nodemanager.Node{draining}, nil, hugepageSandbox, Requirements{Memory: limit95}, true, labels)
	require.ErrorAs(t, err, &FailedToPlaceSandboxError{})
}

// failingCreateClient fails the test if a create reaches the node.
func failingCreateClient(t *testing.T) *nodemanager.MockSandboxClientCustom {
	t.Helper()

	return &nodemanager.MockSandboxClientCustom{CreateFunc: func() error {
		t.Error("create sent to a node that had no room")

		return nil
	}}
}

// Placements racing for the last room on one node, each finishing as soon as
// it starts, admit exactly what fits: a finishing placement is counted either
// in flight or in the node view at every moment a check can look, never both.
func TestPlaceSandbox_ConcurrentPlacementsNeverOverfillANode(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		node   func() *nodemanager.Node
		memory MemoryRequirement
	}{
		{
			// 768 pages hold exactly three 512 MiB sandboxes at limit 1.0.
			name:   "limit",
			node:   func() *nodemanager.Node { return hugepageNode("node", 1600, 0, 0, 768) },
			memory: MemoryRequirement{Limit: 1},
		},
		{
			// A 768-page base pool holds exactly three 512 MiB sandboxes at a cap of 1.
			name: "overcommit cap",
			node: func() *nodemanager.Node {
				return nodemanager.NewTestNode("node", api.NodeStatusReady, 0, 8,
					nodemanager.WithHugePages(768, 0, 0, testPageBytes),
					nodemanager.WithHugePagesBase(768),
				)
			},
			memory: MemoryRequirement{MaxOvercommit: 1},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// With few racers a placement rarely finishes inside another's check
			// unless the race detector slows the check down.
			const racers = 128
			for range 75 {
				node := tc.node()
				var placed atomic.Int32
				var wg sync.WaitGroup
				for i := range racers {
					wg.Go(func() {
						req := hugepageSbxRequest()
						req.Sandbox.SandboxId = fmt.Sprintf("sbx-%d", i)
						var picks []string
						if _, err := PlaceSandbox(t.Context(), firstNotExcluded([]*nodemanager.Node{node}, &picks), []*nodemanager.Node{node}, node,
							req, Requirements{Memory: tc.memory}, false, nil); err == nil {
							placed.Add(1)
						}
					})
				}
				wg.Wait()
				require.Equal(t, int32(3), placed.Load())
			}
		})
	}
}

// The capacity shortage carries a classified reason for logs and traces. Every
// pool full is the hugepage-pool reason, not a bare unknown, so an operator can
// tell a memory shortage from node refusals without reading the message text.
func TestPlaceSandbox_EveryPoolFull_ReasonIsHugepagePool(t *testing.T) {
	t.Parallel()

	nodes := []*nodemanager.Node{
		hugepageNode("a", 1600, 1600, 300, 2000),
		hugepageNode("b", 1600, 1700, 200, 2000),
	}

	_, err := PlaceSandbox(t.Context(), NewBestOfK(DefaultBestOfKConfig()), nodes, nil,
		hugepageSbxRequest(), Requirements{Memory: limit95}, false, nil)

	var noNodes NoNodesAvailableError
	require.ErrorAs(t, err, &noNodes)
	assert.Equal(t, ShortageHugepagePool, noNodes.Reason)
	assert.True(t, noNodes.PoolsFull)
	assert.Equal(t, 0, noNodes.Attempts)
}
