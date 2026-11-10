package nodemanager

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	orchestratorinfo "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator-info"
	"github.com/e2b-dev/infra/packages/shared/pkg/units"
)

func TestHugePagesFor(t *testing.T) {
	t.Parallel()

	pageBytes := uint64(units.MBToBytes(2))
	for _, tc := range []struct {
		name     string
		miB      int64
		pageSize uint64
		want     uint64
	}{
		{name: "whole pages", miB: 1024, pageSize: pageBytes, want: 512},
		{name: "partial page rounds up", miB: 3, pageSize: pageBytes, want: 2},
		{name: "no pool", miB: 1024, pageSize: 0, want: 0},
		{name: "zero size", miB: 0, pageSize: pageBytes, want: 0},
		{name: "negative size", miB: -1024, pageSize: pageBytes, want: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.want, HugePagesFor(tc.miB, tc.pageSize))
		})
	}
}

func TestNode_OptimisticAdd(t *testing.T) {
	t.Parallel()

	node := NewTestNode("test-node", api.NodeStatusReady, 0, 4)
	initialMetrics := node.Metrics()

	res := SandboxResources{
		CPUs:      2,
		MiBMemory: 1024,
	}
	node.OptimisticAdd(res)

	newMetrics := node.Metrics()
	assert.Equal(t, initialMetrics.CpuAllocated+uint32(res.CPUs), newMetrics.CpuAllocated)
	assert.Equal(t, initialMetrics.MemoryAllocatedBytes+uint64(res.MiBMemory)*1024*1024, newMetrics.MemoryAllocatedBytes)
	assert.Equal(t, initialMetrics.HugePagesReserved, newMetrics.HugePagesReserved)
}

func TestNode_OptimisticAdd_ReservesHugePages(t *testing.T) {
	t.Parallel()

	pageBytes := uint64(units.MBToBytes(2))
	node := NewTestNode("test-node", api.NodeStatusReady, 0, 4, WithHugePages(1000, 0, 0, pageBytes))

	node.OptimisticAdd(SandboxResources{CPUs: 2, MiBMemory: 1024, HugePages: true})

	assert.Equal(t, uint64(512), node.Metrics().HugePagesReserved)
}

func TestNode_OptimisticAdd_OrdinaryPagesLeaveThePoolAlone(t *testing.T) {
	t.Parallel()

	pageBytes := uint64(units.MBToBytes(2))
	node := NewTestNode("test-node", api.NodeStatusReady, 0, 4, WithHugePages(1000, 0, 0, pageBytes))

	node.OptimisticAdd(SandboxResources{CPUs: 2, MiBMemory: 1024})

	assert.Zero(t, node.Metrics().HugePagesReserved)
}

// A sync sample requested before a placement was recorded may not include it.
func TestNode_UpdateMetricsFromSync_KeepsPlacementsRecordedAfterTheRequest(t *testing.T) {
	t.Parallel()

	pageBytes := uint64(units.MBToBytes(2))
	node := NewTestNode("test-node", api.NodeStatusReady, 0, 4, WithHugePages(1000, 0, 0, pageBytes))
	sample := &orchestratorinfo.ServiceInfoResponse{
		MetricHugepagesTotal:    1000,
		MetricHugepageSizeBytes: pageBytes,
		MetricHugepagesCapacity: 1250,
	}

	requestedAt := time.Now().Add(-time.Second)
	node.OptimisticAdd(SandboxResources{CPUs: 2, MiBMemory: 1024, HugePages: true})
	node.UpdateMetricsFromSync(sample, requestedAt)

	assert.Equal(t, uint64(512), node.Metrics().HugePagesReserved)
	assert.Equal(t, uint64(units.MBToBytes(1024)), node.Metrics().MemoryAllocatedBytes)
	assert.Equal(t, uint64(1250), node.Metrics().HugePagesCapacity)

	node.UpdateMetricsFromSync(sample, time.Now().Add(time.Second))

	assert.Zero(t, node.Metrics().HugePagesReserved)
	assert.Zero(t, node.Metrics().MemoryAllocatedBytes)
}

// Ordinary pages fault in on use, so until a sync says otherwise a placement
// counts its whole RAM as used memory, and no hugepages.
func TestNode_OptimisticAdd_CountsOrdinaryPagesAsUsedMemory(t *testing.T) {
	t.Parallel()

	pageBytes := uint64(units.MBToBytes(2))
	node := NewTestNode("test-node", api.NodeStatusReady, 0, 4, WithHugePages(1000, 0, 0, pageBytes))

	node.OptimisticAdd(SandboxResources{CPUs: 2, MiBMemory: 1024})
	assert.Equal(t, uint64(units.MBToBytes(1024)), node.Metrics().MemoryUsedBytes)

	node.OptimisticAdd(SandboxResources{CPUs: 2, MiBMemory: 1024, HugePages: true})
	assert.Equal(t, uint64(units.MBToBytes(1024)), node.Metrics().MemoryUsedBytes, "a hugepage placement takes from the pool")
}

// An ordinary-page placement keeps the allocated and used memory it added,
// but has no hugepage counts to keep.
func TestNode_UpdateMetricsFromSync_KeepsOrdinaryPlacementsMemory(t *testing.T) {
	t.Parallel()

	pageBytes := uint64(units.MBToBytes(2))
	node := NewTestNode("test-node", api.NodeStatusReady, 0, 4, WithHugePages(1000, 0, 300, pageBytes))
	sample := &orchestratorinfo.ServiceInfoResponse{
		MetricHugepagesTotal:    1000,
		MetricHugepageSizeBytes: pageBytes,
	}

	node.OptimisticAdd(SandboxResources{CPUs: 2, MiBMemory: 1024})
	node.UpdateMetricsFromSync(sample, time.Now().Add(-time.Second))

	assert.Equal(t, uint64(units.MBToBytes(1024)), node.Metrics().MemoryAllocatedBytes)
	assert.Equal(t, uint64(units.MBToBytes(1024)), node.Metrics().MemoryUsedBytes)
	assert.Zero(t, node.Metrics().HugePagesReserved)

	node.UpdateMetricsFromSync(sample, time.Now().Add(time.Second))

	assert.Zero(t, node.Metrics().MemoryAllocatedBytes)
	assert.Zero(t, node.Metrics().MemoryUsedBytes)
}

// The sample also carries what other sandboxes did since the view was last
// synced, so the view keeps a placement's additions only while they are higher.
func TestNode_UpdateMetricsFromSync_KeepsTheHigherOfViewAndSample(t *testing.T) {
	t.Parallel()

	node := NewTestNode("test-node", api.NodeStatusReady, 0, 4)
	node.OptimisticAdd(SandboxResources{CPUs: 2, MiBMemory: 1024})
	node.UpdateMetricsFromSync(&orchestratorinfo.ServiceInfoResponse{
		MetricMemoryUsedBytes:      uint64(units.MBToBytes(5 * 1024)),
		MetricMemoryAllocatedBytes: uint64(units.MBToBytes(3 * 1024)),
	}, time.Now().Add(-time.Second))

	assert.Equal(t, uint64(units.MBToBytes(5*1024)), node.Metrics().MemoryUsedBytes)
	assert.Equal(t, uint64(units.MBToBytes(3*1024)), node.Metrics().MemoryAllocatedBytes)
}

// A placement completing while another is being checked waits for the check,
// so the check never sees it in flight and in the node view at once.
func TestNode_CompletePlacementWaitsForAPlacementCheck(t *testing.T) {
	t.Parallel()

	pageBytes := uint64(units.MBToBytes(2))
	node := NewTestNode("test-node", api.NodeStatusReady, 0, 4, WithHugePages(1000, 0, 0, pageBytes))
	res := SandboxResources{CPUs: 2, MiBMemory: 1024, HugePages: true}
	assert.True(t, node.PlacementMetrics.StartPlacingIf("first", res, func() bool { return true }))

	checking, release := make(chan struct{}), make(chan struct{})
	go node.PlacementMetrics.StartPlacingIf("second", res, func() bool {
		close(checking)
		<-release

		return false
	})
	<-checking

	completed := make(chan struct{})
	go func() {
		node.CompletePlacement("first", res)
		close(completed)
	}()

	select {
	case <-completed:
		t.Fatal("the placement completed in the middle of another's check")
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	<-completed
	assert.Equal(t, uint64(512), node.Metrics().HugePagesReserved)
	assert.Zero(t, node.PlacementMetrics.InProgressCount())
}

func TestNode_OptimisticRemove(t *testing.T) {
	t.Parallel()

	// Node with resources already allocated at initialization
	node := NewTestNode("test-node", api.NodeStatusReady, 4, 8192, WithAllocatedMemoryBytes(8192*1024*1024))
	initialMetrics := node.Metrics()

	res := SandboxResources{
		CPUs:      2,
		MiBMemory: 1024,
	}
	node.OptimisticRemove(t.Context(), res)

	newMetrics := node.Metrics()
	assert.Equal(t, initialMetrics.CpuAllocated-uint32(res.CPUs), newMetrics.CpuAllocated)
	assert.Equal(t, initialMetrics.MemoryAllocatedBytes-uint64(res.MiBMemory)*1024*1024, newMetrics.MemoryAllocatedBytes)
	assert.Equal(t, initialMetrics.HugePagesReserved, newMetrics.HugePagesReserved)
}

// A deleted sandbox may hold fewer pages than its RAM once its balloon has
// returned free pages, so the pool counts wait for the next sync.
func TestNode_OptimisticRemove_KeepsHugePagesUntilTheNextSync(t *testing.T) {
	t.Parallel()

	pageBytes := uint64(units.MBToBytes(2))
	node := NewTestNode("test-node", api.NodeStatusReady, 4, 8,
		WithAllocatedMemoryBytes(uint64(units.MBToBytes(1024))),
		WithHugePages(1000, 256, 512, pageBytes),
	)

	node.OptimisticRemove(t.Context(), SandboxResources{CPUs: 2, MiBMemory: 1024, HugePages: true})

	got := node.Metrics()
	assert.Equal(t, uint32(2), got.CpuAllocated)
	assert.Zero(t, got.MemoryAllocatedBytes)
	assert.Equal(t, uint64(512), got.HugePagesReserved)
	assert.Equal(t, uint64(256), got.HugePagesUsed)

	node.UpdateMetricsFromSync(&orchestratorinfo.ServiceInfoResponse{
		MetricHugepagesTotal:    1000,
		MetricHugepagesUsed:     100,
		MetricHugepageSizeBytes: pageBytes,
	}, time.Now())

	assert.Zero(t, node.Metrics().HugePagesReserved)
	assert.Equal(t, uint64(100), node.Metrics().HugePagesUsed)
}

func TestNode_OptimisticRemove_OrdinaryPagesLeaveThePoolAlone(t *testing.T) {
	t.Parallel()

	pageBytes := uint64(units.MBToBytes(2))
	node := NewTestNode("test-node", api.NodeStatusReady, 4, 8,
		WithAllocatedMemoryBytes(uint64(units.MBToBytes(1024))),
		WithHugePages(1000, 100, 512, pageBytes),
	)

	node.OptimisticRemove(t.Context(), SandboxResources{CPUs: 2, MiBMemory: 1024})

	assert.Equal(t, uint64(512), node.Metrics().HugePagesReserved)
	assert.Equal(t, uint64(100), node.Metrics().HugePagesUsed)
}

func TestNode_OptimisticRemove_SkipsWhenItWouldUnderflow(t *testing.T) {
	t.Parallel()

	// Node with less allocated than what will be removed: 1 CPU, 512 MiB
	node := NewTestNode("test-node", api.NodeStatusReady, 1, 8192, WithAllocatedMemoryBytes(512*1024*1024))
	initialMetrics := node.Metrics()

	res := SandboxResources{
		CPUs:      2,
		MiBMemory: 1024,
	}
	node.OptimisticRemove(t.Context(), res)

	// Counters must never wrap to ~2^32/2^64; subtraction is skipped instead
	newMetrics := node.Metrics()
	assert.Equal(t, initialMetrics.CpuAllocated, newMetrics.CpuAllocated)
	assert.Equal(t, initialMetrics.MemoryAllocatedBytes, newMetrics.MemoryAllocatedBytes)
}

func TestNode_OptimisticRemove_FreshNodeDoesNotUnderflow(t *testing.T) {
	t.Parallel()

	// Fresh node: nothing allocated yet (e.g. poll overwrote counters after sandbox already left the orchestrator)
	node := NewTestNode("test-node", api.NodeStatusReady, 0, 8192)

	node.OptimisticRemove(t.Context(), SandboxResources{CPUs: 2, MiBMemory: 1024})

	newMetrics := node.Metrics()
	assert.Equal(t, uint32(0), newMetrics.CpuAllocated)
	assert.Equal(t, uint64(0), newMetrics.MemoryAllocatedBytes)
}

// After a heartbeat the sandbox sits in HugePagesUsed and Reserved is the
// kernel's HugePagesRsvd (often 0). Remove must still drop CPU and
// MemoryAllocatedBytes.
func TestNode_OptimisticRemove_AfterMetricsSyncReleasesCPUAndMemory(t *testing.T) {
	t.Parallel()

	pageBytes := uint64(units.MBToBytes(2))
	node := NewTestNode("test-node", api.NodeStatusReady, 0, 8, WithHugePages(1000, 0, 0, pageBytes))
	res := SandboxResources{CPUs: 2, MiBMemory: 1024, HugePages: true}

	node.OptimisticAdd(res)
	node.UpdateMetricsFromServiceInfoResponse(&orchestratorinfo.ServiceInfoResponse{
		MetricCpuAllocated:         2,
		MetricCpuCount:             8,
		MetricMemoryAllocatedBytes: uint64(units.MBToBytes(1024)),
		MetricHugepagesTotal:       1000,
		MetricHugepagesUsed:        512,
		MetricHugepagesReserved:    0,
		MetricHugepageSizeBytes:    pageBytes,
	})

	node.OptimisticRemove(t.Context(), res)

	got := node.Metrics()
	assert.Equal(t, uint32(0), got.CpuAllocated)
	assert.Equal(t, uint64(0), got.MemoryAllocatedBytes)
	assert.Equal(t, uint64(512), got.HugePagesUsed)
}

// A CPU underflow must not skip memory release.
func TestNode_OptimisticRemove_CPUUnderflowDoesNotSkipMemory(t *testing.T) {
	t.Parallel()

	pageBytes := uint64(units.MBToBytes(2))
	node := NewTestNode("test-node", api.NodeStatusReady, 1, 8,
		WithAllocatedMemoryBytes(uint64(units.MBToBytes(1024))),
		WithHugePages(1000, 0, 512, pageBytes),
	)

	node.OptimisticRemove(t.Context(), SandboxResources{CPUs: 2, MiBMemory: 1024, HugePages: true})

	got := node.Metrics()
	assert.Equal(t, uint32(1), got.CpuAllocated)
	assert.Equal(t, uint64(0), got.MemoryAllocatedBytes)
}
