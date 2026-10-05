package host

import (
	"math"
	"time"

	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/mem"
	"golang.org/x/sys/unix"
)

type Metrics struct {
	Timestamp int64 `json:"ts"` // Unix Timestamp in UTC

	CPUCount       uint32  `json:"cpu_count"`    // Total CPU cores
	CPUUsedPercent float32 `json:"cpu_used_pct"` // Percent rounded to 2 decimal places

	MemTotal uint64 `json:"mem_total"` // Total virtual memory in bytes
	MemUsed  uint64 `json:"mem_used"`  // Used virtual memory in bytes
	MemCache uint64 `json:"mem_cache"` // Cached memory (page cache) in bytes

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

	diskMetrics, err := diskStats("/")
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

func diskStats(path string) (diskSpace, error) {
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		return diskSpace{}, err
	}

	block := uint64(st.Bsize)

	// all data blocks
	total := st.Blocks * block
	// blocks available
	available := st.Bavail * block

	return diskSpace{Total: total, Available: available}, nil
}
