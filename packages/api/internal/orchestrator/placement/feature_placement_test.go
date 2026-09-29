package placement

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
)

// requireHighFeature is the requirement testFeatureHigh produces. Passed to
// placeSandbox directly: PlaceSandbox binds featureGates.
func requireHighFeature(t *testing.T) FeatureRequirement {
	t.Helper()

	return featuresFrom(testSbxRequest("sbx-1"), []featureGate{gateOn(testFeatureHigh, true)})
}

// TestPlaceSandbox_FleetTooOldReported is the rollout scenario: a capability
// enabled before the orchestrators carrying it are everywhere. The caller must
// be told the cluster cannot serve it, not handed a timeout.
func TestPlaceSandbox_FleetTooOldReported(t *testing.T) {
	t.Parallel()

	nodes := []*nodemanager.Node{
		nodeAtVersion("node-old", "0.10.0"),
		nodeAtVersion("node-older", "0.9.0"),
	}

	_, err := placeSandbox(
		t.Context(),
		failIfCalled(t),
		nodes,
		nil,
		testSbxRequest("sbx-1"),
		CPURequirement{},
		requireHighFeature(t),
		false,
		nil,
		defaultRefusalBackoff,
	)

	var unsupportedErr UnsupportedFeatureError
	require.ErrorAs(t, err, &unsupportedErr)
	assert.Equal(t, "0.11.0", unsupportedErr.MinVersion)
	assert.Equal(t, []string{"high"}, unsupportedErr.Features)
}

// TestPlaceSandbox_EmptyClusterIsNotFleetTooOld: an empty cluster runs no
// release to be out of date, so it stays NoNodesAvailableError.
func TestPlaceSandbox_EmptyClusterIsNotFleetTooOld(t *testing.T) {
	t.Parallel()

	_, err := placeSandbox(
		t.Context(),
		failIfCalled(t),
		nil,
		nil,
		testSbxRequest("sbx-1"),
		CPURequirement{},
		requireHighFeature(t),
		false,
		nil,
		defaultRefusalBackoff,
	)

	var noNodesErr NoNodesAvailableError
	require.ErrorAs(t, err, &noNodesErr)
}

// TestPlaceSandbox_OneCapableNodeIsNotFleetTooOld: a single capable node must
// reach the placement loop, not be reported as an unsupported cluster.
func TestPlaceSandbox_OneCapableNodeIsNotFleetTooOld(t *testing.T) {
	t.Parallel()

	capable := nodeAtVersion("node-new", "0.11.0")

	result, err := placeSandbox(
		t.Context(),
		stubAlgorithm{
			choose: func(map[string]struct{}) (*nodemanager.Node, error) { return capable, nil },
		},
		[]*nodemanager.Node{nodeAtVersion("node-old", "0.10.0"), capable},
		nil,
		testSbxRequest("sbx-1"),
		CPURequirement{},
		requireHighFeature(t),
		false,
		nil,
		defaultRefusalBackoff,
	)

	require.NoError(t, err)
	require.NotNil(t, result.Node)
	assert.Equal(t, capable.ID, result.Node.ID)
}

// TestPlaceSandbox_PreferredNodeTooOldIsNotUsed covers the resume path, which
// skips chooseNode. Without the vet, snapshot affinity returns every resume to
// the node it was taken on regardless of that node's release.
func TestPlaceSandbox_PreferredNodeTooOldIsNotUsed(t *testing.T) {
	t.Parallel()

	stale := nodeAtVersion("node-old", "0.10.0")
	capable := nodeAtVersion("node-new", "0.11.0")

	var chosen atomic.Int64
	result, err := placeSandbox(
		t.Context(),
		stubAlgorithm{
			choose: func(map[string]struct{}) (*nodemanager.Node, error) {
				chosen.Add(1)

				return capable, nil
			},
		},
		[]*nodemanager.Node{stale, capable},
		stale,
		testSbxRequest("sbx-1"),
		CPURequirement{},
		requireHighFeature(t),
		false,
		nil,
		defaultRefusalBackoff,
	)

	require.NoError(t, err)
	assert.Equal(t, int64(1), chosen.Load(), "expected the stale preferred node to fall through to chooseNode")
	require.NotNil(t, result.Node)
	assert.Equal(t, capable.ID, result.Node.ID)
}

// TestPlaceSandbox_PreferredNodeCapableIsUsed is the same path when affinity is
// legitimate: the vet must not cost a warm resume its own node. The cluster list
// holds only the stale node, so the preferred node is also what keeps the fleet
// check from firing; dropping either half fails here.
func TestPlaceSandbox_PreferredNodeCapableIsUsed(t *testing.T) {
	t.Parallel()

	capable := nodeAtVersion("node-new", "0.11.0")

	result, err := placeSandbox(
		t.Context(),
		failIfCalled(t),
		[]*nodemanager.Node{nodeAtVersion("node-old", "0.10.0")},
		capable,
		testSbxRequest("sbx-1"),
		CPURequirement{},
		requireHighFeature(t),
		false,
		nil,
		defaultRefusalBackoff,
	)

	require.NoError(t, err)
	require.NotNil(t, result.Node)
	assert.Equal(t, capable.ID, result.Node.ID)
}

// TestPlaceSandbox_CancelledRequestStaysATimeout keeps client cancellations out
// of the unsupported-feature code, which means the orchestrators need rolling.
func TestPlaceSandbox_CancelledRequestStaysATimeout(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err := placeSandbox(
		ctx,
		failIfCalled(t),
		[]*nodemanager.Node{nodeAtVersion("node-old", "0.10.0")},
		nil,
		testSbxRequest("sbx-1"),
		CPURequirement{},
		requireHighFeature(t),
		false,
		nil,
		defaultRefusalBackoff,
	)

	var timeoutErr PlacementTimeoutError
	require.ErrorAs(t, err, &timeoutErr)
}

// TestChooseNode_SkipsNodesBelowTheFloor covers the per-node filter, which keeps
// a mixed fleet correct once at least one node qualifies.
func TestChooseNode_SkipsNodesBelowTheFloor(t *testing.T) {
	t.Parallel()

	algo, ok := NewBestOfK(BestOfKConfig{R: 4, K: 10, Alpha: 0.5}).(*BestOfK)
	require.True(t, ok)

	// The stale node is the emptiest, so it wins on score alone.
	stale := nodemanager.NewTestNode("node-old", api.NodeStatusReady, 0, 8, nodemanager.WithOrchestratorVersion("0.10.0"))
	capable := nodemanager.NewTestNode("node-new", api.NodeStatusReady, 6, 8, nodemanager.WithOrchestratorVersion("0.11.0"))

	selected, err := algo.chooseNode(
		t.Context(),
		[]*nodemanager.Node{stale, capable},
		make(map[string]struct{}),
		nodemanager.SandboxResources{CPUs: 1, MiBMemory: 512},
		CPURequirement{},
		requireHighFeature(t),
		false,
		nil,
	)

	require.NoError(t, err)
	assert.Equal(t, capable.ID, selected.ID)
}

// TestChooseNode_AllNodesBelowTheFloorReportsTheRequirement checks the message
// names the floor, which is the release an operator has to roll to.
func TestChooseNode_AllNodesBelowTheFloorReportsTheRequirement(t *testing.T) {
	t.Parallel()

	algo, ok := NewBestOfK(DefaultBestOfKConfig()).(*BestOfK)
	require.True(t, ok)

	_, err := algo.chooseNode(
		t.Context(),
		[]*nodemanager.Node{nodeAtVersion("node-old", "0.10.0")},
		make(map[string]struct{}),
		nodemanager.SandboxResources{CPUs: 1, MiBMemory: 512},
		CPURequirement{},
		requireHighFeature(t),
		false,
		nil,
	)

	var placeErr FailedToPlaceSandboxError
	require.ErrorAs(t, err, &placeErr)
	assert.Contains(t, placeErr.Error(), "min_orchestrator_version=0.11.0")
	assert.Contains(t, placeErr.Error(), "features=[high]")
}
