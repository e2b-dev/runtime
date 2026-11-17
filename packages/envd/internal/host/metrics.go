package host

import (
	"math"
	"time"

	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/mem"
)

type Metrics struct {
	Timestamp int64 `json:"ts"` // Unix Timestamp in UTC

	CPUCount       uint32  `json:"cpu_count"`    // CPU cores online
	CPUUsedPercent float32 `json:"cpu_used_pct"` // Percent rounded to 2 decimal places
	// CPUPossible and CPUTarget are filled by the API from the CPU manager, not here.
	CPUPossible uint32 `json:"cpu_possible"` // CPUs the guest can bring online
	CPUTarget   uint32 `json:"cpu_target"`   // Online count last requested
	// CPUTargetAttempts counts attempts at reaching CPUTarget since it was set.
	CPUTargetAttempts uint32 `json:"cpu_target_attempts"`
	// CPUWritePendingMs is how long a CPU online/offline write has been stuck in the
	// kernel, 0 when none is.
	CPUWritePendingMs uint64 `json:"cpu_write_pending_ms"`

	MemTotal uint64 `json:"mem_total"` // Total virtual memory in bytes
	MemUsed  uint64 `json:"mem_used"`  // Used virtual memory in bytes
	MemCache uint64 `json:"mem_cache"` // Cached memory (page cache) in bytes
	// MemLimit is filled by the API from the memory limit manager, not here.
	MemLimit uint64 `json:"mem_limit"` // Limit envd last applied to the user and PTY cgroups in bytes, 0 when none
	// MemMovable, MemKernel and MemOnlinePolicy are filled by the API from the memory limit
	// manager's last read of ReadMemoryLayout.
	MemMovable      uint64 `json:"mem_movable"`                 // Memory managed in ZONE_MOVABLE in bytes
	MemKernel       uint64 `json:"mem_kernel"`                  // Memory managed in every other zone in bytes
	MemOnlinePolicy string `json:"mem_online_policy,omitempty"` // memory_hotplug.online_policy, empty when the kernel has none

	DiskUsed  uint64 `json:"disk_used"`  // Used disk space in bytes
	DiskTotal uint64 `json:"disk_total"` // Total disk space in bytes

	OomKills *[]OOMKill `json:"oom_kills,omitempty"` // Latest OOM kills; nil when not known
}

func GetMetrics() (*Metrics, error) {
	v, err := mem.VirtualMemory()
	if err != nil {
		return nil, err
	}

	cpuTotal, err := cpu.Counts(true)
	if err != nil {
		return nil, err
	}

	cpuUsedPcts, err := cpu.Percent(0, false)
	if err != nil {
		return nil, err
	}

	cpuUsedPct := cpuUsedPcts[0]
	cpuUsedPctRounded := float32(cpuUsedPct)
	if cpuUsedPct > 0 {
		cpuUsedPctRounded = float32(math.Round(cpuUsedPct*100) / 100)
	}

	diskMetrics, err := diskStats(diskRoot())
	if err != nil {
		return nil, err
	}

	return &Metrics{
		Timestamp:      time.Now().UTC().Unix(),
		CPUCount:       uint32(cpuTotal),
		CPUUsedPercent: cpuUsedPctRounded,
		MemTotal:       v.Total,
		MemUsed:        v.Used,
		MemCache:       v.Cached,
		DiskUsed:       diskMetrics.Total - diskMetrics.Available,
		DiskTotal:      diskMetrics.Total,
	}, nil
}

type diskSpace struct {
	Total     uint64
	Available uint64
}

// MemTotal is the guest memory the kernel has online, in bytes.
func MemTotal() (uint64, error) {
	v, err := mem.VirtualMemory()
	if err != nil {
		return 0, err
	}

	return v.Total, nil
}
