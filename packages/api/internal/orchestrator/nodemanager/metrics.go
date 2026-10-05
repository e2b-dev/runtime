package nodemanager

import (
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

	// Detailed disk metrics
	HostDisks []DiskMetrics
}

func (n *Node) UpdateMetricsFromServiceInfoResponse(info *orchestratorinfo.ServiceInfoResponse) {
	n.metricsMu.Lock()
	defer n.metricsMu.Unlock()

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
