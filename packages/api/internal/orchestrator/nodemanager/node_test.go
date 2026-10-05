package nodemanager

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	orchestratorinfo "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator-info"
	"github.com/e2b-dev/infra/packages/shared/pkg/units"
)

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

	node.OptimisticAdd(SandboxResources{CPUs: 2, MiBMemory: 1024})

	assert.Equal(t, uint64(512), node.Metrics().HugePagesReserved)
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

func TestNode_OptimisticRemove_ReleasesHugePages(t *testing.T) {
	t.Parallel()

	pageBytes := uint64(units.MBToBytes(2))
	node := NewTestNode("test-node", api.NodeStatusReady, 4, 8,
		WithAllocatedMemoryBytes(uint64(units.MBToBytes(1024))),
		WithHugePages(1000, 0, 512, pageBytes),
	)

	node.OptimisticRemove(t.Context(), SandboxResources{CPUs: 2, MiBMemory: 1024})

	assert.Equal(t, uint64(0), node.Metrics().HugePagesReserved)
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
// MemoryAllocatedBytes, and take the pages off Used.
func TestNode_OptimisticRemove_AfterMetricsSyncReleasesEachCounter(t *testing.T) {
	t.Parallel()

	pageBytes := uint64(units.MBToBytes(2))
	node := NewTestNode("test-node", api.NodeStatusReady, 0, 8, WithHugePages(1000, 0, 0, pageBytes))
	res := SandboxResources{CPUs: 2, MiBMemory: 1024}

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
	assert.Equal(t, uint64(0), got.HugePagesReserved)
	assert.Equal(t, uint64(0), got.HugePagesUsed)
}

// A leftover reservation after sync is drained first; the rest comes off Used.
func TestNode_OptimisticRemove_AfterMetricsSyncTakesRemainderFromUsed(t *testing.T) {
	t.Parallel()

	pageBytes := uint64(units.MBToBytes(2))
	node := NewTestNode("test-node", api.NodeStatusReady, 2, 8,
		WithAllocatedMemoryBytes(uint64(units.MBToBytes(1024))),
		WithHugePages(1000, 500, 12, pageBytes),
	)

	node.OptimisticRemove(t.Context(), SandboxResources{CPUs: 2, MiBMemory: 1024})

	got := node.Metrics()
	assert.Equal(t, uint32(0), got.CpuAllocated)
	assert.Equal(t, uint64(0), got.MemoryAllocatedBytes)
	assert.Equal(t, uint64(0), got.HugePagesReserved)
	assert.Equal(t, uint64(0), got.HugePagesUsed)
}

// A reserved-page underflow must not leave CPU or allocated RAM stuck.
func TestNode_OptimisticRemove_HugepageUnderflowDoesNotSkipCPUOrMemory(t *testing.T) {
	t.Parallel()

	pageBytes := uint64(units.MBToBytes(2))
	node := NewTestNode("test-node", api.NodeStatusReady, 4, 8,
		WithAllocatedMemoryBytes(uint64(units.MBToBytes(2048))),
		WithHugePages(1000, 0, 0, pageBytes),
	)

	node.OptimisticRemove(t.Context(), SandboxResources{CPUs: 2, MiBMemory: 1024})

	got := node.Metrics()
	assert.Equal(t, uint32(2), got.CpuAllocated)
	assert.Equal(t, uint64(units.MBToBytes(1024)), got.MemoryAllocatedBytes)
	assert.Equal(t, uint64(0), got.HugePagesReserved)
	assert.Equal(t, uint64(0), got.HugePagesUsed)
}

// A CPU underflow must not skip memory or hugepage release.
func TestNode_OptimisticRemove_CPUUnderflowDoesNotSkipMemory(t *testing.T) {
	t.Parallel()

	pageBytes := uint64(units.MBToBytes(2))
	node := NewTestNode("test-node", api.NodeStatusReady, 1, 8,
		WithAllocatedMemoryBytes(uint64(units.MBToBytes(1024))),
		WithHugePages(1000, 0, 512, pageBytes),
	)

	node.OptimisticRemove(t.Context(), SandboxResources{CPUs: 2, MiBMemory: 1024})

	got := node.Metrics()
	assert.Equal(t, uint32(1), got.CpuAllocated)
	assert.Equal(t, uint64(0), got.MemoryAllocatedBytes)
	assert.Equal(t, uint64(0), got.HugePagesReserved)
}
