//go:build linux

package metrics

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/disk"
	"github.com/shirou/gopsutil/v4/mem"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

const (
	// hostPollInterval is how often HostMetrics resamples all host metrics.
	// cpu.Percent(0, ...) is non-blocking but stateful — it diffs CPU counters
	// against the previous call — so the interval must be wide enough for the
	// delta to be meaningful. Disk shares the same interval.
	hostPollInterval = 10 * time.Second

	nrHugePagesPath           = "/proc/sys/vm/nr_hugepages"
	nrOvercommitHugePagesPath = "/proc/sys/vm/nr_overcommit_hugepages"
)

type CPUMetrics struct {
	UsedPercent float64
	Count       uint32
}

type MemoryMetrics struct {
	UsedBytes  uint64
	TotalBytes uint64
	// Hugepage pool metrics from /proc/meminfo, in page counts.
	HugePagesTotal    uint64 // HugePages_Total: live pool size, surplus pages included
	HugePagesUsed     uint64 // HugePages_Total - HugePages_Free
	HugePagesReserved uint64 // HugePages_Rsvd: committed but not yet faulted
	HugePageSizeBytes uint64 // Hugepagesize
	HugePagesCapacity uint64 // nr_hugepages + nr_overcommit_hugepages; 0 when unknown
	HugePagesBase     uint64 // nr_hugepages, the part allocated at boot
	HugePagesBaseRead bool   // whether HugePagesBase was read; a pool that is all overcommit reads 0
}

type DiskInfo struct {
	MountPoint     string
	Device         string
	FilesystemType string
	UsedBytes      uint64
	TotalBytes     uint64
	UsedPercent    float64
}

// HostMetrics samples CPU and disk in the background so that callers on the
// request path never block on expensive OS calls; memory is read on request
// (see GetMemoryMetrics).
type HostMetrics struct {
	mu         sync.RWMutex
	cpu        *CPUMetrics
	cpuErr     error
	memory     *MemoryMetrics
	disks      []DiskInfo
	disksErr   error
	closed     chan struct{}
	closedOnce sync.Once
}

func NewHostMetrics() *HostMetrics {
	return &HostMetrics{closed: make(chan struct{})}
}

// Start primes the cache immediately then resamples every hostPollInterval
// until Close is called. Intended to be run via startService.
func (h *HostMetrics) Start() error {
	h.sample()

	ticker := time.NewTicker(hostPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-h.closed:
			return nil
		case <-ticker.C:
			h.sample()
		}
	}
}

func (h *HostMetrics) Close(_ context.Context) error {
	h.closedOnce.Do(func() { close(h.closed) })

	return nil
}

// GetCPUMetrics returns the most recent CPU sample. Non-blocking.
// Returns zero-value CPUMetrics if no sample has been taken yet.
func (h *HostMetrics) GetCPUMetrics() (*CPUMetrics, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if h.cpu == nil {
		return &CPUMetrics{}, h.cpuErr
	}

	// Valid cache exists — serve it regardless of transient errors.
	return h.cpu, nil
}

// GetMemoryMetrics reads memory now rather than from the last poll:
// placement gates on the hugepage counters, and a sample up to
// hostPollInterval old would hide reservations made since. A failed meminfo
// read serves the last good sample. An unreadable pool size reports capacity
// 0, which placement treats as unknown and does not limit.
func (h *HostMetrics) GetMemoryMetrics(ctx context.Context) (*MemoryMetrics, error) {
	return h.keepLastGood(readMemory(ctx))
}

func (h *HostMetrics) keepLastGood(memory *MemoryMetrics, err error) (*MemoryMetrics, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if err == nil {
		h.memory = memory

		return memory, nil
	}

	if h.memory != nil {
		return h.memory, nil
	}

	return nil, err
}

// GetDiskMetrics returns the most recent disk sample. Non-blocking.
// Returns an empty slice if no sample has been taken yet.
func (h *HostMetrics) GetDiskMetrics() ([]DiskInfo, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if h.disks == nil {
		return []DiskInfo{}, h.disksErr
	}

	return h.disks, nil
}

// sample resamples CPU and disk together and writes them atomically.
func (h *HostMetrics) sample() {
	cpu, cpuErr := readCPU()
	disks, disksErr := readDisk()

	h.mu.Lock()
	defer h.mu.Unlock()

	if cpuErr == nil {
		h.cpu = cpu
	}
	h.cpuErr = cpuErr

	if disksErr == nil {
		h.disks = disks
	}
	h.disksErr = disksErr
}

func readCPU() (*CPUMetrics, error) {
	// cpu.Percent(0) is non-blocking: it diffs kernel CPU counters against
	// the previous call. The ticker spacing (hostPollInterval) keeps the
	// inter-sample window wide enough for the delta to be meaningful.
	percents, err := cpu.Percent(0, false)
	if err != nil {
		return nil, err
	}

	usedPercent := float64(0)
	if len(percents) > 0 {
		usedPercent = percents[0]
	}

	return &CPUMetrics{
		UsedPercent: usedPercent,
		Count:       uint32(runtime.NumCPU()),
	}, nil
}

func readMemory(ctx context.Context) (*MemoryMetrics, error) {
	memInfo, err := mem.VirtualMemory()
	if err != nil {
		return nil, err
	}

	hugePagesUsed := uint64(0)
	if memInfo.HugePagesTotal > memInfo.HugePagesFree {
		hugePagesUsed = memInfo.HugePagesTotal - memInfo.HugePagesFree
	}

	pool := hugePagesPool(ctx)

	return &MemoryMetrics{
		UsedBytes:         memInfo.Used,
		TotalBytes:        memInfo.Total,
		HugePagesTotal:    memInfo.HugePagesTotal,
		HugePagesUsed:     hugePagesUsed,
		HugePagesReserved: memInfo.HugePagesRsvd,
		HugePageSizeBytes: memInfo.HugePageSize,
		HugePagesCapacity: pool.Capacity,
		HugePagesBase:     pool.Base,
		HugePagesBaseRead: pool.BaseRead,
	}, nil
}

var lastPoolWarning atomic.Int64

// hugePagePool is the pool's configured size, in pages.
type hugePagePool struct {
	Base     uint64 // nr_hugepages
	BaseRead bool   // Base was read; false leaves it unknown
	Capacity uint64 // nr_hugepages + nr_overcommit_hugepages; 0 when unknown
}

// hugePagesPool reports what it could read: a ceiling it cannot read is 0,
// which placement treats as unknown, and it warns at most once a minute so the
// node is not silently left unlimited. A kernel without hugetlb has no such
// files.
func hugePagesPool(ctx context.Context) hugePagePool {
	pool, err := readHugePagesPool(os.ReadFile)
	if err == nil || errors.Is(err, fs.ErrNotExist) {
		return pool
	}

	nowSec := time.Now().Unix()
	if last := lastPoolWarning.Load(); nowSec-last >= 60 && lastPoolWarning.CompareAndSwap(last, nowSec) {
		logger.L().Warn(ctx, "Cannot read the hugepage pool size, reporting it as unknown", zap.Error(err))
	}

	return pool
}

// readHugePagesPool reads the pool size from the hugetlb sysctls. Base is valid
// whenever BaseRead is true, even when err != nil: a base that was read is kept
// when only the overcommit allowance cannot be, and Capacity is then 0.
func readHugePagesPool(readFile func(string) ([]byte, error)) (hugePagePool, error) {
	read := func(path string) (uint64, error) {
		raw, err := readFile(path)
		if err != nil {
			return 0, err
		}

		pages, err := strconv.ParseUint(strings.TrimSpace(string(raw)), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("parse %s: %w", path, err)
		}

		return pages, nil
	}

	base, err := read(nrHugePagesPath)
	if err != nil {
		return hugePagePool{}, err
	}
	overcommit, err := read(nrOvercommitHugePagesPath)
	if err != nil {
		return hugePagePool{Base: base, BaseRead: true}, err
	}

	return hugePagePool{Base: base, BaseRead: true, Capacity: base + overcommit}, nil
}

func readDisk() ([]DiskInfo, error) {
	partitions, err := disk.Partitions(false)
	if err != nil {
		return nil, err
	}

	var disks []DiskInfo
	for _, partition := range partitions {
		if !isRealDisk(partition) {
			continue
		}

		usage, err := disk.Usage(partition.Mountpoint)
		if err != nil {
			// Skip partitions we can't read.
			continue
		}

		disks = append(disks, DiskInfo{
			MountPoint:     partition.Mountpoint,
			Device:         partition.Device,
			FilesystemType: partition.Fstype,
			UsedBytes:      usage.Used,
			TotalBytes:     usage.Total,
			UsedPercent:    usage.UsedPercent,
		})
	}

	return disks, nil
}

func isRealDisk(p disk.PartitionStat) bool {
	// Must not be a boot partition
	if strings.HasPrefix(p.Mountpoint, "/boot/") {
		return false
	}

	// Drop obvious virtuals
	if strings.HasPrefix(p.Device, "/dev/loop") || strings.HasPrefix(p.Device, "/dev/zram") {
		return false
	}

	return true
}
