package placement

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	orchestratorinfo "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator-info"
	"github.com/e2b-dev/infra/packages/shared/pkg/units"
)

var testPageBytes = uint64(units.MBToBytes(2))

// hugepageNode reports a pool of total pages, used+reserved committed, and a
// ceiling of capacity pages (base pool plus overcommit allowance).
func hugepageNode(id string, total, used, reserved, capacity uint64) *nodemanager.Node {
	return nodemanager.NewTestNode(id, api.NodeStatusReady, 0, 8,
		nodemanager.WithHugePages(total, used, reserved, testPageBytes),
		nodemanager.WithHugePagesCapacity(capacity),
	)
}

// 512 MiB is 256 pages.
var hugepageSandbox = nodemanager.SandboxResources{CPUs: 1, MiBMemory: 512, HugePages: true}

var limit95 = MemoryRequirement{Limit: 0.95}

func TestNodeSatisfiesMemory_RefusesPastTheLimit(t *testing.T) {
	t.Parallel()

	// 1800 committed + 256 requested > 0.95 * 2000.
	assert.False(t, NodeSatisfiesMemory(hugepageNode("n", 1600, 1600, 200, 2000), hugepageSandbox, limit95))
}

func TestNodeSatisfiesMemory_ExactFitAtTheLimitPasses(t *testing.T) {
	t.Parallel()

	// 1644 committed + 256 requested == 0.95 * 2000.
	assert.True(t, NodeSatisfiesMemory(hugepageNode("n", 1600, 1600, 44, 2000), hugepageSandbox, limit95))
}

// A node borrowing surplus pages has Used+Reserved == HugePages_Total; with
// overcommit allowance left it still has room.
func TestNodeSatisfiesMemory_NodeOnSurplusWithAllowanceLeftPasses(t *testing.T) {
	t.Parallel()

	assert.True(t, NodeSatisfiesMemory(hugepageNode("n", 1000, 900, 100, 1500), hugepageSandbox, limit95))
}

// A pool that is all overcommit (no base pages) grows HugePages_Total only as
// pages are borrowed, so Total always equals what is committed.
func TestNodeSatisfiesMemory_PoolThatIsAllOvercommitPasses(t *testing.T) {
	t.Parallel()

	assert.True(t, NodeSatisfiesMemory(hugepageNode("n", 300, 200, 100, 54097), hugepageSandbox, limit95))
}

func TestNodeSatisfiesMemory_CountsHugepagePlacementsInFlight(t *testing.T) {
	t.Parallel()

	node := hugepageNode("n", 1600, 1200, 0, 2000)
	assert.True(t, NodeSatisfiesMemory(node, hugepageSandbox, limit95))

	node.PlacementMetrics.StartPlacingIf("ordinary", nodemanager.SandboxResources{MiBMemory: 4096}, func() bool { return true })
	assert.True(t, NodeSatisfiesMemory(node, hugepageSandbox, limit95), "an ordinary-page placement takes nothing from the pool")

	node.PlacementMetrics.StartPlacingIf("hugepage", nodemanager.SandboxResources{MiBMemory: 1024, HugePages: true}, func() bool { return true })
	assert.False(t, NodeSatisfiesMemory(node, hugepageSandbox, limit95))
}

// A deleted sandbox may have held fewer pages than its RAM, so the node stays
// as full as last reported until the next sync instead of admitting too much.
func TestNodeSatisfiesMemory_DeleteDoesNotFreeRoomBeforeTheNextSync(t *testing.T) {
	t.Parallel()

	// 1 GiB is 512 pages; the limit is 40 GiB of a 50 GiB ceiling.
	limit := MemoryRequirement{Limit: 0.8}
	node := nodemanager.NewTestNode("n", api.NodeStatusReady, 2, 8,
		nodemanager.WithAllocatedMemoryBytes(uint64(units.MBToBytes(16*1024))),
		nodemanager.WithHugePages(19*1024, 19*1024, 0, testPageBytes),
		nodemanager.WithHugePagesCapacity(25*1024),
	)
	sixteenGiB := nodemanager.SandboxResources{CPUs: 2, MiBMemory: 16 * 1024, HugePages: true}
	eightGiB := nodemanager.SandboxResources{CPUs: 2, MiBMemory: 8 * 1024, HugePages: true}

	// Other sandboxes hold 30 GiB and the deleted one 8 GiB of its 16.
	node.OptimisticRemove(t.Context(), sixteenGiB)

	assert.False(t, NodeSatisfiesMemory(node, sixteenGiB, limit))

	node.UpdateMetricsFromSync(&orchestratorinfo.ServiceInfoResponse{
		MetricHugepagesTotal:    15 * 1024,
		MetricHugepagesUsed:     15 * 1024,
		MetricHugepageSizeBytes: testPageBytes,
		MetricHugepagesCapacity: 25 * 1024,
	}, time.Now())

	assert.False(t, NodeSatisfiesMemory(node, sixteenGiB, limit))
	assert.True(t, NodeSatisfiesMemory(node, eightGiB, limit))
}

func TestNodeSatisfiesMemory_PassesWhenTheCheckDoesNotApply(t *testing.T) {
	t.Parallel()

	full := hugepageNode("full", 2000, 2000, 0, 2000)
	assert.True(t, NodeSatisfiesMemory(full, hugepageSandbox, MemoryRequirement{}), "limit 0 turns the check off")
	fullPoolRoomyMemory := nodemanager.NewTestNode("n", api.NodeStatusReady, 0, 8,
		nodemanager.WithHugePages(2000, 2000, 0, testPageBytes),
		nodemanager.WithHugePagesCapacity(2000),
		nodemanager.WithMemoryTotalBytes(uint64(units.MBToBytes(8192))),
		nodemanager.WithMemoryUsedBytes(uint64(units.MBToBytes(1024))),
	)
	assert.True(t, NodeSatisfiesMemory(fullPoolRoomyMemory, nodemanager.SandboxResources{MiBMemory: 512}, limit95), "ordinary pages do not use the pool")
	assert.True(t, NodeSatisfiesMemory(hugepageNode("silent", 2000, 2000, 0, 0), hugepageSandbox, limit95), "a node that does not report its ceiling is not refused")
}

// The reason travels with the admission so the caller records what was
// checked, not what the metrics say later.
func TestMemoryAdmission_NamesWhyANodePassedUnbounded(t *testing.T) {
	t.Parallel()

	ok, missing := MemoryAdmission(hugepageNode("n", 1000, 900, 100, 0), hugepageSandbox, limit95)
	assert.True(t, ok)
	assert.Equal(t, NoHugepageCapacity, missing)

	ok, missing = MemoryAdmission(hugepageNode("n", 1600, 1600, 44, 2000), hugepageSandbox, limit95)
	assert.True(t, ok)
	assert.Empty(t, missing)

	ok, missing = MemoryAdmission(hugepageNode("n", 1600, 1600, 200, 2000), hugepageSandbox, limit95)
	assert.False(t, ok)
	assert.Empty(t, missing)

	ok, missing = MemoryAdmission(hugepageNode("n", 1000, 900, 100, 0), hugepageSandbox, MemoryRequirement{})
	assert.True(t, ok)
	assert.Empty(t, missing)
}

// baseNode reports a base pool of 1000 pages, a ceiling a quarter above it as
// node-init sizes it, nothing held, and allocatedMiB of template RAM.
func baseNode(id string, allocatedMiB int64) *nodemanager.Node {
	const base = 1000

	return nodemanager.NewTestNode(id, api.NodeStatusReady, 0, 8,
		nodemanager.WithAllocatedMemoryBytes(uint64(units.MBToBytes(allocatedMiB))),
		nodemanager.WithHugePages(base, 0, 0, testPageBytes),
		nodemanager.WithHugePagesBase(base),
		nodemanager.WithHugePagesCapacity(base*5/4),
	)
}

// A base pool of 1000 pages is 2000 MiB.
var (
	atBasePool   = MemoryRequirement{MaxOvercommit: 1}
	overcommit15 = MemoryRequirement{MaxOvercommit: 1.5}
)

func TestNodeSatisfiesMemory_OvercommitRefusesPastTheCap(t *testing.T) {
	t.Parallel()

	// 1600 allocated + 512 requested > 2000.
	assert.False(t, NodeSatisfiesMemory(baseNode("n", 1600), hugepageSandbox, atBasePool))
	// 2600 allocated + 512 requested > 1.5 * 2000.
	assert.False(t, NodeSatisfiesMemory(baseNode("n", 2600), hugepageSandbox, overcommit15))
}

func TestNodeSatisfiesMemory_OvercommitExactFitAtTheCapPasses(t *testing.T) {
	t.Parallel()

	// 1488 allocated + 512 requested == 2000.
	assert.True(t, NodeSatisfiesMemory(baseNode("n", 1488), hugepageSandbox, atBasePool))
	// 2488 allocated + 512 requested == 1.5 * 2000: a cap above 1 admits past the base pool.
	assert.True(t, NodeSatisfiesMemory(baseNode("n", 2488), hugepageSandbox, overcommit15))
}

// The node reports one allocated total for every sandbox, so placements in
// flight count whether or not they use hugepages.
func TestNodeSatisfiesMemory_OvercommitCountsEveryPlacementInFlight(t *testing.T) {
	t.Parallel()

	node := baseNode("n", 1000)
	assert.True(t, NodeSatisfiesMemory(node, hugepageSandbox, atBasePool))

	node.PlacementMetrics.StartPlacingIf("ordinary", nodemanager.SandboxResources{MiBMemory: 512}, func() bool { return true })
	assert.False(t, NodeSatisfiesMemory(node, hugepageSandbox, atBasePool))
}

// A pool that is all overcommit has no base to multiply, so the cap applies
// to the ceiling, as the memory score does.
func TestNodeSatisfiesMemory_OvercommitOnAPoolThatIsAllOvercommitUsesTheCeiling(t *testing.T) {
	t.Parallel()

	node := func(allocatedMiB int64) *nodemanager.Node {
		return nodemanager.NewTestNode("n", api.NodeStatusReady, 0, 8,
			nodemanager.WithAllocatedMemoryBytes(uint64(units.MBToBytes(allocatedMiB))),
			nodemanager.WithHugePages(0, 0, 0, testPageBytes),
			nodemanager.WithHugePagesBase(0),
			nodemanager.WithHugePagesCapacity(1000),
		)
	}

	assert.True(t, NodeSatisfiesMemory(node(1488), hugepageSandbox, atBasePool))
	assert.False(t, NodeSatisfiesMemory(node(1600), hugepageSandbox, atBasePool))
}

// Each check refuses on its own: a node inside one can still be past the other.
func TestNodeSatisfiesMemory_LimitAndOvercommitEachRefuse(t *testing.T) {
	t.Parallel()

	both := MemoryRequirement{Limit: 0.95, MaxOvercommit: 1}

	pastTheCap := baseNode("n", 1600)
	assert.True(t, NodeSatisfiesMemory(pastTheCap, hugepageSandbox, MemoryRequirement{Limit: 0.95}))
	assert.False(t, NodeSatisfiesMemory(pastTheCap, hugepageSandbox, both))

	// 1200 pages held + 256 requested > 0.95 * 1250, with 512 MiB allocated.
	pastTheLimit := nodemanager.NewTestNode("n", api.NodeStatusReady, 0, 8,
		nodemanager.WithAllocatedMemoryBytes(uint64(units.MBToBytes(512))),
		nodemanager.WithHugePages(1200, 1200, 0, testPageBytes),
		nodemanager.WithHugePagesBase(1000),
		nodemanager.WithHugePagesCapacity(1250),
	)
	assert.True(t, NodeSatisfiesMemory(pastTheLimit, hugepageSandbox, atBasePool))
	assert.False(t, NodeSatisfiesMemory(pastTheLimit, hugepageSandbox, both))
}

// usedMemoryNode reports 4096 MiB of total memory, usedMiB of it in use, the
// pool's 2000 MiB included, and nothing held in the pool.
func usedMemoryNode(usedMiB int64) *nodemanager.Node {
	return nodemanager.NewTestNode("n", api.NodeStatusReady, 0, 8,
		nodemanager.WithMemoryTotalBytes(uint64(units.MBToBytes(4096))),
		nodemanager.WithMemoryUsedBytes(uint64(units.MBToBytes(usedMiB))),
		nodemanager.WithHugePages(1000, 0, 0, testPageBytes),
		nodemanager.WithHugePagesCapacity(1250),
	)
}

// A sandbox without hugepages draws from the node's memory outside the pool,
// so the limit measures used memory there, and the pool's room decides nothing.
func TestNodeSatisfiesMemory_LimitMeasuresOrdinaryPagesAgainstUsedMemory(t *testing.T) {
	t.Parallel()

	// Outside the pool: 1300 used + 512 requested <= 0.95 * 2096 < 1500 + 512.
	assert.True(t, NodeSatisfiesMemory(usedMemoryNode(3300), ordinarySandbox, limit95))
	assert.False(t, NodeSatisfiesMemory(usedMemoryNode(3500), ordinarySandbox, limit95))
	assert.True(t, NodeSatisfiesMemory(usedMemoryNode(3500), hugepageSandbox, limit95), "a hugepage sandbox is measured against the pool")
}

// A pool that is most of the node's memory counts as used, but ordinary pages
// cannot have it anyway: the limit's share is of what they can have.
func TestNodeSatisfiesMemory_LimitLeavesTheHugepagePoolOutOfOrdinaryPages(t *testing.T) {
	t.Parallel()

	// 7000 MiB pool of 8192 MiB; 100 MiB used outside it. 7100 + 512 is past
	// 0.9 * 8192, but 100 + 512 is well inside 0.9 * 1192.
	node := nodemanager.NewTestNode("n", api.NodeStatusReady, 0, 8,
		nodemanager.WithMemoryTotalBytes(uint64(units.MBToBytes(8192))),
		nodemanager.WithMemoryUsedBytes(uint64(units.MBToBytes(7100))),
		nodemanager.WithHugePages(3500, 0, 0, testPageBytes),
		nodemanager.WithHugePagesCapacity(3500),
	)

	assert.True(t, NodeSatisfiesMemory(node, ordinarySandbox, MemoryRequirement{Limit: 0.9}))
}

// A hugepage placement the full pool cannot back borrows surplus pages out of
// ordinary memory, which the limit counts before the next sync shows it:
// while in flight, and once placed.
func TestNodeSatisfiesMemory_LimitCountsSurplusHugepagesAsUsedMemory(t *testing.T) {
	t.Parallel()

	// A full 2000 MiB pool of 4096; 1000 MiB used outside it, and 1000 + 512
	// requested fits 0.95 * 2096. 512 MiB more of surplus does not.
	fullPool := func() *nodemanager.Node {
		return nodemanager.NewTestNode("n", api.NodeStatusReady, 0, 8,
			nodemanager.WithMemoryTotalBytes(uint64(units.MBToBytes(4096))),
			nodemanager.WithMemoryUsedBytes(uint64(units.MBToBytes(3000))),
			nodemanager.WithHugePages(1000, 1000, 0, testPageBytes),
			nodemanager.WithHugePagesCapacity(1250),
		)
	}
	borrowing := nodemanager.SandboxResources{MiBMemory: 512, HugePages: true}

	inFlight := fullPool()
	assert.True(t, NodeSatisfiesMemory(inFlight, ordinarySandbox, limit95))
	inFlight.PlacementMetrics.StartPlacingIf("borrowing", borrowing, func() bool { return true })
	assert.False(t, NodeSatisfiesMemory(inFlight, ordinarySandbox, limit95))

	placed := fullPool()
	placed.PlacementMetrics.StartPlacingIf("borrowing", borrowing, func() bool { return true })
	placed.CompletePlacement("borrowing", borrowing)
	assert.False(t, NodeSatisfiesMemory(placed, ordinarySandbox, limit95))
}

func TestNodeSatisfiesMemory_LimitCountsOrdinaryPlacementsInFlight(t *testing.T) {
	t.Parallel()

	node := usedMemoryNode(3000)
	assert.True(t, NodeSatisfiesMemory(node, ordinarySandbox, limit95))

	node.PlacementMetrics.StartPlacingIf("hugepage", nodemanager.SandboxResources{MiBMemory: 1024, HugePages: true}, func() bool { return true })
	assert.True(t, NodeSatisfiesMemory(node, ordinarySandbox, limit95), "a hugepage placement draws from the pool")

	node.PlacementMetrics.StartPlacingIf("ordinary", nodemanager.SandboxResources{MiBMemory: 512}, func() bool { return true })
	assert.False(t, NodeSatisfiesMemory(node, ordinarySandbox, limit95))
}

// A node that reports no memory has none to give a sandbox without hugepages.
func TestNodeSatisfiesMemory_NodeWithoutMemoryTakesNoOrdinaryPages(t *testing.T) {
	t.Parallel()

	node := hugepageNode("n", 2000, 0, 0, 2000)
	assert.False(t, NodeSatisfiesMemory(node, ordinarySandbox, limit95))
	assert.False(t, NodeSatisfiesMemory(node, ordinarySandbox, atBasePool))
}

func TestNodeSatisfiesMemory_OvercommitOffPassesAFullNode(t *testing.T) {
	t.Parallel()

	assert.True(t, NodeSatisfiesMemory(baseNode("full", 4000), hugepageSandbox, MemoryRequirement{}))
}

var ordinarySandbox = nodemanager.SandboxResources{CPUs: 1, MiBMemory: 512}

// A sandbox without hugepages takes nothing from the pool, so the cap
// measures it against the node's total memory instead.
func TestNodeSatisfiesMemory_OvercommitMeasuresOrdinaryPagesAgainstTotalMemory(t *testing.T) {
	t.Parallel()

	node := func(allocatedMiB int64) *nodemanager.Node {
		return nodemanager.NewTestNode("n", api.NodeStatusReady, 0, 8,
			nodemanager.WithAllocatedMemoryBytes(uint64(units.MBToBytes(allocatedMiB))),
			nodemanager.WithMemoryTotalBytes(uint64(units.MBToBytes(4096))),
			nodemanager.WithHugePages(1000, 0, 0, testPageBytes),
			nodemanager.WithHugePagesBase(1000),
		)
	}

	// 3584 allocated + 512 requested is past the 2000 MiB pool but == 4096 total.
	assert.False(t, NodeSatisfiesMemory(node(3584), hugepageSandbox, atBasePool))
	assert.True(t, NodeSatisfiesMemory(node(3584), ordinarySandbox, atBasePool))
	assert.False(t, NodeSatisfiesMemory(node(3600), ordinarySandbox, atBasePool))
}

// A node that reports no hugepage pool has nothing for the cap to multiply,
// and no room for a hugepage sandbox.
func TestNodeSatisfiesMemory_OvercommitGivesANodeWithoutAPoolNoRoom(t *testing.T) {
	t.Parallel()

	noPageSize := nodemanager.NewTestNode("n", api.NodeStatusReady, 0, 8,
		nodemanager.WithHugePagesBase(1000),
	)
	assert.False(t, NodeSatisfiesMemory(noPageSize, hugepageSandbox, atBasePool))

	noPool := nodemanager.NewTestNode("n", api.NodeStatusReady, 0, 8,
		nodemanager.WithHugePages(0, 0, 0, testPageBytes),
	)
	assert.False(t, NodeSatisfiesMemory(noPool, hugepageSandbox, atBasePool))
}

// The limit's reason travels with the admission when the cap still bounds the node.
func TestMemoryAdmission_NamesWhyTheLimitPassedUnboundedUnderTheCap(t *testing.T) {
	t.Parallel()

	noCeiling := nodemanager.NewTestNode("n", api.NodeStatusReady, 0, 8,
		nodemanager.WithAllocatedMemoryBytes(uint64(units.MBToBytes(1000))),
		nodemanager.WithHugePages(1000, 0, 0, testPageBytes),
		nodemanager.WithHugePagesBase(1000),
	)
	ok, missing := MemoryAdmission(noCeiling, hugepageSandbox, MemoryRequirement{Limit: 0.95, MaxOvercommit: 1})
	assert.True(t, ok)
	assert.Equal(t, NoHugepageCapacity, missing)

	ok, missing = MemoryAdmission(baseNode("n", 1000), hugepageSandbox, atBasePool)
	assert.True(t, ok)
	assert.Empty(t, missing)
}
