package placement

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	orchestratorinfo "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator-info"
	"github.com/e2b-dev/infra/packages/shared/pkg/machineinfo"
	"github.com/e2b-dev/infra/packages/shared/pkg/units"
)

// emptyHugePool is a reported pool large enough that a typical sandbox
// request scores well below CPU load, so CPU-ranking tests still rank on CPU.
func emptyHugePool() nodemanager.TestOptions {
	return nodemanager.WithHugePages(100_000, 0, 0, uint64(units.MBToBytes(2)))
}

func TestBestOfK_Score(t *testing.T) {
	t.Parallel()
	config := DefaultBestOfKConfig()
	algo := NewBestOfK(config).(*BestOfK)

	// Create a test node with known metrics
	// The node's CpuUsage is set to 50 via the constructor
	node := nodemanager.NewTestNode("test-node", api.NodeStatusReady, 2, 4, emptyHugePool())

	resources := nodemanager.SandboxResources{
		CPUs:      1,
		MiBMemory: 512,
	}

	score := algo.Score(node, resources, config)

	// Score should be non-negative
	assert.GreaterOrEqual(t, score, 0.0)

	// Test with different CPU usage
	node2 := nodemanager.NewTestNode("test-node2", api.NodeStatusReady, 10, 4, emptyHugePool())
	score2 := algo.Score(node2, resources, config)

	// Higher CPU usage should result in higher score (worse)
	assert.Greater(t, score2, score)
}

func TestBestOfK_Score_PreferBiggerNode(t *testing.T) {
	t.Parallel()
	config := DefaultBestOfKConfig()
	algo := NewBestOfK(config).(*BestOfK)

	// Create a test node with known metrics
	// The node's CpuUsage is set to 50 via the constructor
	node := nodemanager.NewTestNode("test-node", api.NodeStatusReady, 5, 4, emptyHugePool())

	resources := nodemanager.SandboxResources{
		CPUs:      1,
		MiBMemory: 512,
	}

	score := algo.Score(node, resources, config)

	// Score should be non-negative
	assert.GreaterOrEqual(t, score, 0.0)

	// Test with different CPU usage
	node2 := nodemanager.NewTestNode("test-node2", api.NodeStatusReady, 1, 8, emptyHugePool())
	score2 := algo.Score(node2, resources, config)

	// Lower CPU count should result in higher score (worse) as the expected load is higher
	assert.Greater(t, score, score2)
}

func TestBestOfK_Score_WithPendingResources(t *testing.T) {
	t.Parallel()
	config := DefaultBestOfKConfig()
	algo := NewBestOfK(config).(*BestOfK)

	// Create two nodes with identical base loads
	nodeNormal := nodemanager.NewTestNode("node-normal", api.NodeStatusReady, 0, 4, emptyHugePool())
	nodeWithPending := nodemanager.NewTestNode("node-pending", api.NodeStatusReady, 0, 4, emptyHugePool())

	// Inject InProgress resources into nodeWithPending using StartPlacing
	// This simulates a Sandbox that is currently being placed but hasn't fully started
	pendingRes := nodemanager.SandboxResources{
		CPUs:      2,
		MiBMemory: 1024,
	}
	nodeWithPending.PlacementMetrics.StartPlacing("pending-sbx-1", pendingRes)

	reqResources := nodemanager.SandboxResources{
		CPUs:      1,
		MiBMemory: 512,
	}

	scoreNormal := algo.Score(nodeNormal, reqResources, config)
	scorePending := algo.Score(nodeWithPending, reqResources, config)

	// A node with pending resources has a higher 'reserved' CPU count,
	// so its calculated Score should be greater (meaning worse/lower priority)
	assert.Greater(t, scorePending, scoreNormal, "Node with pending resources should receive a higher (worse) score")
}

// A node with no hugepage pool scores 0.5, not as free.
func TestBestOfK_Score_NoHugePagesIsHalf(t *testing.T) {
	t.Parallel()
	config := DefaultBestOfKConfig()
	algo := NewBestOfK(config).(*BestOfK)

	node := nodemanager.NewTestNode("no-pool", api.NodeStatusReady, 2, 4, nodemanager.WithAllocatedMemoryBytes(uint64(units.MBToBytes(64*1024))))

	score := algo.Score(node, nodemanager.SandboxResources{CPUs: 1, MiBMemory: 512}, config)

	// CPU load is 3/16; unknown pool is 0.5.
	assert.InDelta(t, 0.5, score, 1e-9)
}

// Ordinary-page sandbox RAM must not fill the hugepage pool.
func TestBestOfK_Score_OrdinaryRAMDoesNotFillPool(t *testing.T) {
	t.Parallel()
	config := DefaultBestOfKConfig()
	algo := NewBestOfK(config).(*BestOfK)

	pageBytes := uint64(units.MBToBytes(2))
	node := nodemanager.NewTestNode("mixed", api.NodeStatusReady, 0, 8,
		nodemanager.WithHugePages(1000, 0, 0, pageBytes),
		nodemanager.WithAllocatedMemoryBytes(uint64(units.MBToBytes(64*1024))),
	)

	score := algo.Score(node, nodemanager.SandboxResources{CPUs: 1, MiBMemory: 512}, config)

	// Memory load is only the 512 MiB request over a 2000 MiB pool.
	// CPU load is 1/32 = 0.03125, so memory (0.256) wins.
	assert.InDelta(t, 512.0/2000.0, score, 1e-9)
}

// Same CPU on both nodes; the one whose pool is nearly committed scores worse,
// and the memory load is what the score reports once it exceeds the CPU load.
func TestBestOfK_Score_HugePageMemoryDominates(t *testing.T) {
	t.Parallel()
	config := DefaultBestOfKConfig()
	algo := NewBestOfK(config).(*BestOfK)

	pageBytes := uint64(units.MBToBytes(2))
	// 1000 pages of 2 MiB: a 2000 MiB pool.
	// tight: 500 used + 250 reserved = 1500 MiB committed
	tight := nodemanager.NewTestNode("tight", api.NodeStatusReady, 4, 8,
		nodemanager.WithHugePages(1000, 500, 250, pageBytes),
	)
	// roomy: 50 reserved = 100 MiB committed
	roomy := nodemanager.NewTestNode("roomy", api.NodeStatusReady, 4, 8,
		nodemanager.WithHugePages(1000, 0, 50, pageBytes),
	)

	resources := nodemanager.SandboxResources{CPUs: 1, MiBMemory: 512}

	tightScore := algo.Score(tight, resources, config)
	roomyScore := algo.Score(roomy, resources, config)

	// CPU load is (1 + 4) / (4 * 8) = 0.15625 on both, below either memory load.
	// tight: (512 requested + 1500 committed + 0.5 * 1000 used) / 2000
	assert.InDelta(t, 2512.0/2000.0, tightScore, 1e-9)
	// roomy: (512 + 100 + 0) / 2000
	assert.InDelta(t, 612.0/2000.0, roomyScore, 1e-9)
	assert.Greater(t, tightScore, roomyScore)
}

// Memory of an in-flight placement is committed before the node reports it.
func TestBestOfK_Score_PendingMemoryCounts(t *testing.T) {
	t.Parallel()
	config := DefaultBestOfKConfig()
	algo := NewBestOfK(config).(*BestOfK)

	pageBytes := uint64(units.MBToBytes(2))
	idle := nodemanager.NewTestNode("idle", api.NodeStatusReady, 0, 8, nodemanager.WithHugePages(1000, 0, 0, pageBytes))
	pending := nodemanager.NewTestNode("pending", api.NodeStatusReady, 0, 8, nodemanager.WithHugePages(1000, 0, 0, pageBytes))
	pending.PlacementMetrics.StartPlacing("in-flight", nodemanager.SandboxResources{CPUs: 0, MiBMemory: 1024})

	resources := nodemanager.SandboxResources{CPUs: 1, MiBMemory: 512}

	idleScore := algo.Score(idle, resources, config)
	pendingScore := algo.Score(pending, resources, config)

	// 1024 MiB in flight over a 2000 MiB pool.
	assert.InDelta(t, 1024.0/2000.0, pendingScore-idleScore, 1e-9)
}

// A heartbeat after place moves the sandbox into Used and clears Reserved.
// Remove must still drop commitment so the next score matches an empty node.
func TestBestOfK_Score_RemoveAfterSyncDropsCommitment(t *testing.T) {
	t.Parallel()
	config := DefaultBestOfKConfig()
	algo := NewBestOfK(config).(*BestOfK)

	pageBytes := uint64(units.MBToBytes(2))
	empty := nodemanager.NewTestNode("empty", api.NodeStatusReady, 0, 8, nodemanager.WithHugePages(1000, 0, 0, pageBytes))
	live := nodemanager.NewTestNode("live", api.NodeStatusReady, 0, 8, nodemanager.WithHugePages(1000, 0, 0, pageBytes))

	placed := nodemanager.SandboxResources{CPUs: 2, MiBMemory: 1024}
	live.OptimisticAdd(placed)
	live.UpdateMetricsFromServiceInfoResponse(&orchestratorinfo.ServiceInfoResponse{
		MetricCpuAllocated:         2,
		MetricCpuCount:             8,
		MetricMemoryAllocatedBytes: uint64(units.MBToBytes(1024)),
		MetricHugepagesTotal:       1000,
		MetricHugepagesUsed:        512,
		MetricHugepagesReserved:    0,
		MetricHugepageSizeBytes:    pageBytes,
	})

	next := nodemanager.SandboxResources{CPUs: 1, MiBMemory: 512}
	afterSync := algo.Score(live, next, config)
	// (512 requested + 1024 used + 0.5 * 1024 used) / 2000
	assert.InDelta(t, 2048.0/2000.0, afterSync, 1e-9)
	assert.Greater(t, afterSync, algo.Score(empty, next, config))

	live.OptimisticRemove(t.Context(), placed)

	assert.InDelta(t, algo.Score(empty, next, config), algo.Score(live, next, config), 1e-9)
}

// The pick follows the tighter resource: a CPU-light node with a full pool
// loses to a CPU-heavy node with room in its pool.
func TestBestOfK_ChooseNode_AvoidsFullHugePages(t *testing.T) {
	t.Parallel()
	config := BestOfKConfig{R: 4, Alpha: 0.5, K: 2, ScoreHugepages: true}
	algo := NewBestOfK(config).(*BestOfK)

	pageBytes := uint64(units.MBToBytes(2))
	memFull := nodemanager.NewTestNode("cpu-light-mem-full", api.NodeStatusReady, 2, 8,
		nodemanager.WithHugePages(1000, 900, 50, pageBytes),
	)
	memFree := nodemanager.NewTestNode("cpu-heavy-mem-free", api.NodeStatusReady, 20, 8,
		nodemanager.WithHugePages(1000, 100, 0, pageBytes),
	)

	resources := nodemanager.SandboxResources{CPUs: 1, MiBMemory: 512}

	// K covers both nodes, so the choice is the score alone.
	for range 20 {
		selected, err := algo.chooseNode(t.Context(), []*nodemanager.Node{memFull, memFree}, nil, resources, CPURequirement{}, FeatureRequirement{}, false, nil)
		require.NoError(t, err)
		assert.Equal(t, "cpu-heavy-mem-free", selected.ID)
	}
}

// Unknown pool scores 0.5, so a silent node loses to a same-CPU reporter with room.
func TestBestOfK_ChooseNode_UnknownPoolLosesToReporter(t *testing.T) {
	t.Parallel()
	config := BestOfKConfig{R: 4, Alpha: 0.5, K: 2, ScoreHugepages: true}
	algo := NewBestOfK(config).(*BestOfK)

	pageBytes := uint64(units.MBToBytes(2))
	silent := nodemanager.NewTestNode("silent", api.NodeStatusReady, 4, 8)
	reporting := nodemanager.NewTestNode("reporting", api.NodeStatusReady, 4, 8,
		nodemanager.WithHugePages(1000, 0, 50, pageBytes),
	)

	resources := nodemanager.SandboxResources{CPUs: 1, MiBMemory: 512}

	for range 20 {
		selected, err := algo.chooseNode(t.Context(), []*nodemanager.Node{silent, reporting}, nil, resources, CPURequirement{}, FeatureRequirement{}, false, nil)
		require.NoError(t, err)
		assert.Equal(t, "reporting", selected.ID)
	}
}

// Off, a full hugepage pool does not change the score: CPU alone decides.
func TestBestOfK_Score_HugepageFlagOffIgnoresPool(t *testing.T) {
	t.Parallel()
	config := DefaultBestOfKConfig()
	config.ScoreHugepages = false
	algo := NewBestOfK(config).(*BestOfK)

	pageBytes := uint64(units.MBToBytes(2))
	tight := nodemanager.NewTestNode("tight", api.NodeStatusReady, 4, 8,
		nodemanager.WithHugePages(1000, 500, 250, pageBytes),
	)
	roomy := nodemanager.NewTestNode("roomy", api.NodeStatusReady, 4, 8,
		nodemanager.WithHugePages(1000, 0, 50, pageBytes),
	)

	resources := nodemanager.SandboxResources{CPUs: 1, MiBMemory: 512}

	// (1 requested + 4 allocated) / (R=4 * 8 cores)
	assert.InDelta(t, 5.0/32.0, algo.Score(tight, resources, config), 1e-9)
	assert.InDelta(t, 5.0/32.0, algo.Score(roomy, resources, config), 1e-9)
}

func TestBestOfK_ChooseNode_HugepageFlagOffPrefersCPU(t *testing.T) {
	t.Parallel()
	config := BestOfKConfig{R: 4, Alpha: 0.5, K: 2}
	algo := NewBestOfK(config).(*BestOfK)

	pageBytes := uint64(units.MBToBytes(2))
	memFull := nodemanager.NewTestNode("cpu-light-mem-full", api.NodeStatusReady, 2, 8,
		nodemanager.WithHugePages(1000, 900, 50, pageBytes),
	)
	memFree := nodemanager.NewTestNode("cpu-heavy-mem-free", api.NodeStatusReady, 20, 8,
		nodemanager.WithHugePages(1000, 100, 0, pageBytes),
	)

	resources := nodemanager.SandboxResources{CPUs: 1, MiBMemory: 512}

	for range 20 {
		selected, err := algo.chooseNode(t.Context(), []*nodemanager.Node{memFull, memFree}, nil, resources, CPURequirement{}, FeatureRequirement{}, false, nil)
		require.NoError(t, err)
		assert.Equal(t, "cpu-light-mem-full", selected.ID)
	}
}

func TestBestOfK_ChooseNode(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	config := BestOfKConfig{
		R:     10, // Higher overcommit ratio to ensure nodes can fit
		Alpha: 0.5,
		K:     3, // Sample all nodes
	}
	algo := NewBestOfK(config).(*BestOfK)

	// Create test nodes with different loads
	node1 := nodemanager.NewTestNode("node1", api.NodeStatusReady, 8, 4, emptyHugePool())
	node2 := nodemanager.NewTestNode("node2", api.NodeStatusReady, 2, 4, emptyHugePool())
	node3 := nodemanager.NewTestNode("node3", api.NodeStatusReady, 5, 4, emptyHugePool())

	nodes := []*nodemanager.Node{node1, node2, node3}
	excludedNodes := make(map[string]struct{})
	resources := nodemanager.SandboxResources{
		CPUs:      1, // Small resource request
		MiBMemory: 512,
	}

	// Test selection - should work with proper config
	selected, err := algo.chooseNode(ctx, nodes, excludedNodes, resources, CPURequirement{}, FeatureRequirement{}, false, nil)
	require.NoError(t, err)
	assert.NotNil(t, selected)
	assert.Contains(t, []string{"node1", "node2", "node3"}, selected.ID)
}

func TestBestOfK_ChooseNode_WithExclusions(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	config := BestOfKConfig{
		R:     10,
		Alpha: 0.5,
		K:     3,
	}
	algo := NewBestOfK(config).(*BestOfK)

	// Create test nodes
	node1 := nodemanager.NewTestNode("node1", api.NodeStatusReady, 8, 4, emptyHugePool())
	node2 := nodemanager.NewTestNode("node2", api.NodeStatusReady, 2, 4, emptyHugePool())
	node3 := nodemanager.NewTestNode("node3", api.NodeStatusReady, 5, 4, emptyHugePool())

	nodes := []*nodemanager.Node{node1, node2, node3}

	// Exclude the best node (node2)
	excludedNodes := map[string]struct{}{
		"node2": {},
	}

	resources := nodemanager.SandboxResources{
		CPUs:      1,
		MiBMemory: 512,
	}

	selected, err := algo.chooseNode(ctx, nodes, excludedNodes, resources, CPURequirement{}, FeatureRequirement{}, false, nil)
	require.NoError(t, err)
	// Should not select excluded node
	assert.NotEqual(t, "node2", selected.ID)
	assert.Contains(t, []string{"node1", "node3"}, selected.ID)
}

func TestBestOfK_ChooseNode_NoAvailableNodes(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	config := DefaultBestOfKConfig()
	algo := NewBestOfK(config).(*BestOfK)

	// Create unhealthy nodes
	node1 := nodemanager.NewTestNode("node1", api.NodeStatusUnhealthy, 8, 4)
	node2 := nodemanager.NewTestNode("node2", api.NodeStatusUnhealthy, 2, 4)

	nodes := []*nodemanager.Node{node1, node2}
	excludedNodes := make(map[string]struct{})
	resources := nodemanager.SandboxResources{
		CPUs:      2,
		MiBMemory: 1024,
	}

	selected, err := algo.chooseNode(ctx, nodes, excludedNodes, resources, CPURequirement{}, FeatureRequirement{}, false, nil)
	require.Error(t, err)
	assert.Nil(t, selected)
	assert.Contains(t, err.Error(), "no node available")
}

func TestFailedToPlaceSandboxError_Error(t *testing.T) {
	t.Parallel()

	machine := machineinfo.MachineInfo{
		CPUArchitecture: "x86_64",
		CPUFamily:       "6",
		CPUModel:        machineinfo.IceLakeModel,
		CPUModelName:    "Intel Ice Lake",
	}

	tests := []struct {
		name           string
		filterByLabels bool
		pinnedModel    string
		requiredLabels []string
		wantContains   []string
		wantNotContain string
	}{
		{
			name:           "without label filtering",
			filterByLabels: false,
			requiredLabels: nil,
			wantContains: []string{
				"no node available with required metadata",
				fmt.Sprintf("machine=%v", machine),
			},
			wantNotContain: "labels=",
		},
		{
			name:           "cpu model pinned",
			filterByLabels: false,
			pinnedModel:    machineinfo.IceLakeModel,
			wantContains: []string{
				"no node available with required metadata",
				"cpu_model_pinned=" + machineinfo.IceLakeModel,
			},
		},
		{
			name:           "cpu model not pinned",
			filterByLabels: false,
			wantContains: []string{
				"no node available with required metadata",
			},
			wantNotContain: "cpu_model_pinned",
		},
		{
			name:           "with label filtering",
			filterByLabels: true,
			requiredLabels: []string{"gpu", "fast-disk"},
			wantContains: []string{
				"no node available with required metadata",
				fmt.Sprintf("machine=%v", machine),
				"labels=[gpu fast-disk]",
			},
		},
		{
			name:           "label filtering enabled with no labels",
			filterByLabels: true,
			requiredLabels: nil,
			wantContains: []string{
				"no node available with required metadata",
				"labels=[]",
			},
		},
		{
			name:           "labels present but filtering disabled",
			filterByLabels: false,
			requiredLabels: []string{"gpu"},
			wantContains: []string{
				"no node available with required metadata",
			},
			wantNotContain: "labels=",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := FailedToPlaceSandboxError{
				filterByLabels: tt.filterByLabels,
				requiredLabels: tt.requiredLabels,
				cpu:            CPURequirement{Build: machine, PinnedModel: tt.pinnedModel},
			}

			msg := err.Error()

			for _, want := range tt.wantContains {
				assert.Contains(t, msg, want)
			}

			if tt.wantNotContain != "" {
				assert.NotContains(t, msg, tt.wantNotContain)
			}
		})
	}
}

func TestFailedToPlaceSandboxError_IsError(t *testing.T) {
	t.Parallel()

	// The error returned by chooseNode when no node is available should be a
	// FailedToPlaceSandboxError carrying the placement constraints.
	var err error = FailedToPlaceSandboxError{
		filterByLabels: true,
		requiredLabels: []string{"gpu"},
		cpu:            CPURequirement{Build: machineinfo.MachineInfo{CPUModelName: "Intel Ice Lake"}},
	}

	var placeErr FailedToPlaceSandboxError
	require.ErrorAs(t, err, &placeErr)
	assert.True(t, placeErr.filterByLabels)
	assert.Equal(t, []string{"gpu"}, placeErr.requiredLabels)
	assert.Equal(t, "Intel Ice Lake", placeErr.cpu.Build.CPUModelName)
}

func TestBestOfK_ChooseNode_ReturnsPlacementError(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	config := DefaultBestOfKConfig()
	algo := NewBestOfK(config).(*BestOfK)

	// Unhealthy nodes cannot be placed on, forcing the error path.
	node := nodemanager.NewTestNode("node1", api.NodeStatusUnhealthy, 2, 4)
	nodes := []*nodemanager.Node{node}
	resources := nodemanager.SandboxResources{CPUs: 1, MiBMemory: 512}
	machine := machineinfo.MachineInfo{CPUModelName: "Intel Ice Lake"}
	requiredLabels := []string{"gpu"}

	selected, err := algo.chooseNode(ctx, nodes, make(map[string]struct{}), resources, CPURequirement{Build: machine}, FeatureRequirement{}, true, requiredLabels)
	require.Error(t, err)
	assert.Nil(t, selected)

	// The error should be a FailedToPlaceSandboxError with the constraints that
	// were requested, and its message should surface them.
	var placeErr FailedToPlaceSandboxError
	require.ErrorAs(t, err, &placeErr)
	assert.Equal(t, requiredLabels, placeErr.requiredLabels)
	assert.Contains(t, err.Error(), "labels=[gpu]")
	assert.Contains(t, err.Error(), fmt.Sprintf("machine=%v", machine))
}

func TestBestOfK_Sample(t *testing.T) {
	t.Parallel()
	config := DefaultBestOfKConfig()
	algo := NewBestOfK(config).(*BestOfK)

	// Create many test nodes
	var nodes []*nodemanager.Node
	for i := range 10 {
		node := nodemanager.NewTestNode(string(rune('a'+i)), api.NodeStatusReady, int64(i), 4)
		nodes = append(nodes, node)
	}

	excludedNodes := make(map[string]struct{})

	// Test sampling fewer nodes than available
	sampled := algo.sample(nodes, config, excludedNodes, CPURequirement{}, FeatureRequirement{}, false, nil)
	assert.LessOrEqual(t, len(sampled), 3)

	// Check all sampled nodes are unique
	seen := make(map[string]bool)
	for _, n := range sampled {
		assert.False(t, seen[n.ID])
		seen[n.ID] = true
	}

	// Test sampling with exclusions
	excludedNodes["a"] = struct{}{}
	excludedNodes["b"] = struct{}{}
	sampled = algo.sample(nodes, config, excludedNodes, CPURequirement{}, FeatureRequirement{}, false, nil)

	for _, n := range sampled {
		assert.NotEqual(t, "a", n.ID)
		assert.NotEqual(t, "b", n.ID)
	}
}

func TestBestOfK_PowerOfKChoices(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	config := BestOfKConfig{
		R:     10,
		Alpha: 0.5,
		K:     3,
	}
	algo := NewBestOfK(config).(*BestOfK)

	// Create many nodes with varying loads
	var nodes []*nodemanager.Node
	for i := range 20 {
		node := nodemanager.NewTestNode(string(rune('A'+i)), api.NodeStatusReady, int64(float64(i)*0.5), 4, emptyHugePool())
		nodes = append(nodes, node)
	}

	excludedNodes := make(map[string]struct{})
	resources := nodemanager.SandboxResources{
		CPUs:      1,
		MiBMemory: 512,
	}

	// Run multiple times to see distribution
	selectedCounts := make(map[string]int)
	successCount := 0
	for range 100 {
		selected, err := algo.chooseNode(ctx, nodes, excludedNodes, resources, CPURequirement{}, FeatureRequirement{}, false, nil)
		if err == nil && selected != nil {
			selectedCounts[selected.ID]++
			successCount++
		}
	}

	// We should see multiple different nodes selected due to random sampling
	assert.GreaterOrEqual(t, len(selectedCounts), 1)

	// Lower-loaded nodes should be selected more often if we have enough samples
	if successCount > 10 {
		var earlyNodeCount, lateNodeCount int
		for id, count := range selectedCounts {
			if id[0] < 'J' {
				earlyNodeCount += count
			} else {
				lateNodeCount += count
			}
		}
		// Early nodes (lower load) should be selected more often
		assert.GreaterOrEqual(t, earlyNodeCount, lateNodeCount)
	}
}
