package nodemanager

import (
	"time"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	orchestratorinfo "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator-info"
)

type DiskMetrics struct {
	MountPoint     string
	Device         string
	FilesystemType string
	UsedBytes      uint64
	TotalBytes     uint64
}
type Metrics struct {
	// Host metrics
	CpuAllocated         uint32
	CpuPercent           uint32
	CpuCount             uint32
	MemoryAllocatedBytes uint64
	MemoryUsedBytes      uint64
	MemoryTotalBytes     uint64
	SandboxCount         uint32
	MaxSandboxes         int64
	OutstandingWork      uint64

	// Hugepage pool metrics (page counts)
	HugePagesTotal    uint64
	HugePagesUsed     uint64
	HugePagesReserved uint64
	HugePageSizeBytes uint64
	HugePagesCapacity uint64 // pool ceiling, base pool plus overcommit allowance; 0 when not reported
	HugePagesBase     uint64 // base pool, the pages allocated at boot
	HugePagesBaseSet  bool   // the orchestrator reported HugePagesBase; 0 then is a pool that is all overcommit

	// Detailed disk metrics
	HostDisks []DiskMetrics
}

func (n *Node) UpdateMetricsFromServiceInfoResponse(info *orchestratorinfo.ServiceInfoResponse) {
	n.metricsMu.Lock()
	defer n.metricsMu.Unlock()

	n.updateMetrics(info)
}

// UpdateMetricsFromSync applies a sample requested at requestedAt. A
// placement recorded after that may be missing from the sample, so the view
// keeps its allocated memory, and the counts that placement added (hugepages,
// or used memory for ordinary pages), until the next sync. Allocated and used
// memory keep the higher of the view and the sample: the sample also carries
// what other sandboxes did meanwhile.
func (n *Node) UpdateMetricsFromSync(info *orchestratorinfo.ServiceInfoResponse, requestedAt time.Time) {
	n.metricsMu.Lock()
	defer n.metricsMu.Unlock()

	used, reserved := n.metrics.HugePagesUsed, n.metrics.HugePagesReserved
	allocated, memoryUsed := n.metrics.MemoryAllocatedBytes, n.metrics.MemoryUsedBytes
	n.updateMetrics(info)
	hugePageAdded, ordinaryPageAdded := n.lastHugePageAdd.After(requestedAt), n.lastOrdinaryPageAdd.After(requestedAt)
	if hugePageAdded {
		n.metrics.HugePagesUsed, n.metrics.HugePagesReserved = used, reserved
	}
	if ordinaryPageAdded {
		n.metrics.MemoryUsedBytes = max(n.metrics.MemoryUsedBytes, memoryUsed)
	}
	if hugePageAdded || ordinaryPageAdded {
		n.metrics.MemoryAllocatedBytes = max(n.metrics.MemoryAllocatedBytes, allocated)
	}
}

func (n *Node) updateMetrics(info *orchestratorinfo.ServiceInfoResponse) {
	// Update host usage metrics
	n.metrics.CpuPercent = info.GetMetricCpuPercent()
	n.metrics.MemoryUsedBytes = info.GetMetricMemoryUsedBytes()

	// Update host total metrics
	n.metrics.CpuCount = info.GetMetricCpuCount()
	n.metrics.MemoryTotalBytes = info.GetMetricMemoryTotalBytes()

	// Update allocated resources
	n.metrics.CpuAllocated = info.GetMetricCpuAllocated()
	n.metrics.MemoryAllocatedBytes = info.GetMetricMemoryAllocatedBytes()

	// Update total sandbox count
	n.metrics.SandboxCount = info.GetMetricSandboxesRunning()

	n.metrics.MaxSandboxes = info.GetMaxSandboxes()

	n.metrics.OutstandingWork = info.GetOutstandingWork()

	// Update hugepage metrics
	n.metrics.HugePagesTotal = info.GetMetricHugepagesTotal()
	n.metrics.HugePagesUsed = info.GetMetricHugepagesUsed()
	n.metrics.HugePagesReserved = info.GetMetricHugepagesReserved()
	n.metrics.HugePageSizeBytes = info.GetMetricHugepageSizeBytes()
	n.metrics.HugePagesCapacity = info.GetMetricHugepagesCapacity()
	n.metrics.HugePagesBase = info.GetMetricHugepagesBase()
	n.metrics.HugePagesBaseSet = info.MetricHugepagesBase != nil

	// Update detailed disk metrics
	disks := info.GetMetricDisks()
	n.metrics.HostDisks = make([]DiskMetrics, len(disks))
	for i, disk := range disks {
		n.metrics.HostDisks[i] = DiskMetrics{
			MountPoint:     disk.GetMountPoint(),
			Device:         disk.GetDevice(),
			FilesystemType: disk.GetFilesystemType(),
			UsedBytes:      disk.GetUsedBytes(),
			TotalBytes:     disk.GetTotalBytes(),
		}
	}
}

func (n *Node) Metrics() Metrics {
	n.metricsMu.RLock()
	defer n.metricsMu.RUnlock()

	result := Metrics{
		CpuAllocated:         n.metrics.CpuAllocated,
		CpuPercent:           n.metrics.CpuPercent,
		CpuCount:             n.metrics.CpuCount,
		MemoryAllocatedBytes: n.metrics.MemoryAllocatedBytes,
		MemoryUsedBytes:      n.metrics.MemoryUsedBytes,
		MemoryTotalBytes:     n.metrics.MemoryTotalBytes,
		SandboxCount:         n.metrics.SandboxCount,
		MaxSandboxes:         n.metrics.MaxSandboxes,
		OutstandingWork:      n.metrics.OutstandingWork,

		HugePagesTotal:    n.metrics.HugePagesTotal,
		HugePagesUsed:     n.metrics.HugePagesUsed,
		HugePagesReserved: n.metrics.HugePagesReserved,
		HugePageSizeBytes: n.metrics.HugePageSizeBytes,
		HugePagesCapacity: n.metrics.HugePagesCapacity,
		HugePagesBase:     n.metrics.HugePagesBase,
		HugePagesBaseSet:  n.metrics.HugePagesBaseSet,

		HostDisks: make([]DiskMetrics, len(n.metrics.HostDisks)),
	}

	copy(result.HostDisks, n.metrics.HostDisks)

	return result
}

func (n *Node) GetAPIMetric() api.NodeMetrics {
	n.metricsMu.Lock()
	defer n.metricsMu.Unlock()

	metrics := n.metrics
	diskMetrics := make([]api.DiskMetrics, len(metrics.HostDisks))
	for i, disk := range metrics.HostDisks {
		diskMetrics[i] = api.DiskMetrics{
			Device:         disk.Device,
			FilesystemType: disk.FilesystemType,
			MountPoint:     disk.MountPoint,
			TotalBytes:     disk.TotalBytes,
			UsedBytes:      disk.UsedBytes,
		}
	}

	return api.NodeMetrics{
		AllocatedMemoryBytes: metrics.MemoryAllocatedBytes,
		MemoryUsedBytes:      metrics.MemoryUsedBytes,
		MemoryTotalBytes:     metrics.MemoryTotalBytes,
		AllocatedCPU:         metrics.CpuAllocated,
		CpuPercent:           metrics.CpuPercent,
		CpuCount:             metrics.CpuCount,
		HugePagesTotal:       metrics.HugePagesTotal,
		HugePagesUsed:        metrics.HugePagesUsed,
		HugePagesReserved:    metrics.HugePagesReserved,
		HugePageSizeBytes:    metrics.HugePageSizeBytes,
		Disks:                diskMetrics,
	}
}
