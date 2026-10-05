//go:build linux

// Package testutils reports the process-wide kernel resources a benchmark
// consumes, so a change to how volumes are confined can be judged on its
// footprint (OS threads, mount namespaces, file descriptors, resident memory)
// and not only on latency.
package testutils

import (
	"bufio"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// ProcStats is a snapshot of the calling process' resource usage.
type ProcStats struct {
	// Threads is the number of OS threads in the process.
	Threads int
	// MountNamespaces is the number of distinct mount namespaces the
	// process' threads are members of.
	MountNamespaces int
	// OpenFDs is the number of open file descriptors.
	OpenFDs int
	// RSSBytes is the resident set size.
	RSSBytes int64
	// KernelStackBytes is the system-wide memory in kernel stacks. It is
	// not scoped to the process, but every OS thread costs one kernel stack,
	// and user-space RSS never shows that.
	KernelStackBytes int64
}

// SnapshotProcStats reads the current resource usage from procfs.
func SnapshotProcStats(tb testing.TB) ProcStats {
	tb.Helper()

	var stats ProcStats

	status, err := os.Open("/proc/self/status")
	if err != nil {
		tb.Fatalf("failed to open /proc/self/status: %v", err)
	}
	defer status.Close()

	scanner := bufio.NewScanner(status)
	for scanner.Scan() {
		key, value, ok := strings.Cut(scanner.Text(), ":")
		if !ok {
			continue
		}

		fields := strings.Fields(value)
		if len(fields) == 0 {
			continue
		}

		switch key {
		case "Threads":
			stats.Threads, err = strconv.Atoi(fields[0])
		case "VmRSS":
			var kb int64
			kb, err = strconv.ParseInt(fields[0], 10, 64)
			stats.RSSBytes = kb * 1024
		}
		if err != nil {
			tb.Fatalf("failed to parse %s from /proc/self/status: %v", key, err)
		}
	}
	if err := scanner.Err(); err != nil {
		tb.Fatalf("failed to read /proc/self/status: %v", err)
	}

	meminfo, err := os.Open("/proc/meminfo")
	if err != nil {
		tb.Fatalf("failed to open /proc/meminfo: %v", err)
	}
	defer meminfo.Close()

	scanner = bufio.NewScanner(meminfo)
	for scanner.Scan() {
		key, value, ok := strings.Cut(scanner.Text(), ":")
		if !ok || key != "KernelStack" {
			continue
		}

		fields := strings.Fields(value)
		if len(fields) == 0 {
			continue
		}

		kb, err := strconv.ParseInt(fields[0], 10, 64)
		if err != nil {
			tb.Fatalf("failed to parse KernelStack from /proc/meminfo: %v", err)
		}
		stats.KernelStackBytes = kb * 1024
	}
	if err := scanner.Err(); err != nil {
		tb.Fatalf("failed to read /proc/meminfo: %v", err)
	}

	fds, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		tb.Fatalf("failed to list /proc/self/fd: %v", err)
	}
	// ReadDir itself holds one descriptor open while listing.
	stats.OpenFDs = len(fds) - 1

	tasks, err := os.ReadDir("/proc/self/task")
	if err != nil {
		tb.Fatalf("failed to list /proc/self/task: %v", err)
	}

	namespaces := make(map[string]struct{}, 1)
	for _, task := range tasks {
		ns, err := os.Readlink(filepath.Join("/proc/self/task", task.Name(), "ns/mnt"))
		if err != nil {
			// The thread exited between the listing and the readlink.
			continue
		}
		namespaces[ns] = struct{}{}
	}
	stats.MountNamespaces = len(namespaces)

	return stats
}

// Sub returns the difference between two snapshots.
func (s ProcStats) Sub(o ProcStats) ProcStats {
	return ProcStats{
		Threads:          s.Threads - o.Threads,
		MountNamespaces:  s.MountNamespaces - o.MountNamespaces,
		OpenFDs:          s.OpenFDs - o.OpenFDs,
		RSSBytes:         s.RSSBytes - o.RSSBytes,
		KernelStackBytes: s.KernelStackBytes - o.KernelStackBytes,
	}
}

// ReportProcStatsDelta records the growth between before and after as
// benchmark metrics, divided by per. Pass per=1 for absolute deltas.
func ReportProcStatsDelta(b *testing.B, before, after ProcStats, per int) {
	b.Helper()

	delta := after.Sub(before)
	divisor := float64(per)

	b.ReportMetric(float64(delta.Threads)/divisor, "threads")
	b.ReportMetric(float64(delta.MountNamespaces)/divisor, "mntns")
	b.ReportMetric(float64(delta.OpenFDs)/divisor, "fds")
	b.ReportMetric(float64(delta.RSSBytes)/divisor/1024, "KiB-rss")
	b.ReportMetric(float64(delta.KernelStackBytes)/divisor/1024, "KiB-kstack")
}
