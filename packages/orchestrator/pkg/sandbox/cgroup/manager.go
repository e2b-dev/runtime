package cgroup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sys/unix"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

const (
	// standard kernel mount point for cgroups v2
	cgroupV2MountPoint = "/sys/fs/cgroup"

	// RootCgroupPath is the base path for all E2B sandbox cgroups
	RootCgroupPath = cgroupV2MountPoint + "/e2b"

	// NoCgroupFD is a sentinel value indicating that no cgroup file descriptor
	// is available (e.g. cgroup accounting is disabled or the FD has been released).
	NoCgroupFD = -1

	cgroupKillTimeout      = 2 * time.Second
	cgroupKillPollInterval = 100 * time.Millisecond

	// vcpuCgroupName is the threaded child holding only the vCPU threads, so a quota there spares the VMM.
	vcpuCgroupName = "vcpu"
	// VcpuThreadPrefix is how Firecracker names its vCPU threads ("fc_vcpu 3").
	VcpuThreadPrefix = "fc_vcpu"
	// cpuPeriodUsec is the cpu.max period; only quota/period sets the CPU count, the period only the granularity.
	cpuPeriodUsec = 100000
)

// ErrNoCgroup is a limit asked of a nil, noop or removed handle: nothing would enforce it.
var ErrNoCgroup = errors.New("sandbox has no cgroup to limit")

// Stats contains resource usage statistics from a cgroup
type Stats struct {
	CPUUsageUsec  uint64 // microseconds
	CPUUserUsec   uint64 // microseconds
	CPUSystemUsec uint64 // microseconds

	MemoryUsageBytes uint64 // bytes
	MemoryPeakBytes  uint64 // bytes, reset after each GetStats() call
}

// CgroupHandle represents a created cgroup for a sandbox.
// It encapsulates the cgroup lifecycle, resource access, and cleanup.
//
// Lifecycle: Create → GetFD → cmd.Start() → ReleaseCgroupFD → GetStats (repeatedly) → Remove
//
// The caller MUST call ReleaseCgroupFD() right after cmd.Start() (regardless of
// whether Start succeeded or failed). Remove() closes the memory.peak FD,
// closes the cgroup directory FD as a safety net if ReleaseCgroupFD() was not
// called, and deletes the cgroup directory.
type CgroupHandle struct {
	cgroupName          string
	path                string
	file                *os.File // Open FD to the cgroup directory (nil after ReleaseCgroupFD)
	memoryPeakFile      *os.File // Open FD to memory.peak for per-FD reset (nil after Remove or if not available)
	manager             *managerImpl
	removed             bool
	noop                bool // true for handles created by NoopManager (no real cgroup backing)
	vcpuThreadsConfined bool // true only after every expected vCPU thread was moved to the child cgroup
}

// CanLimit reports whether a limit set on this handle would hold anything: false for nil, noop and removed.
func (h *CgroupHandle) CanLimit() bool {
	return h != nil && !h.noop && !h.removed
}

// SetSandboxCpuLimit sets cpu.max on the whole cgroup; 0 lifts it.
func (h *CgroupHandle) SetSandboxCpuLimit(ctx context.Context, limitCpus int64) error {
	if !h.CanLimit() {
		if limitCpus > 0 {
			return ErrNoCgroup
		}

		return nil
	}
	if err := os.WriteFile(filepath.Join(h.path, "cpu.max"), []byte(cpuMaxValue(limitCpus)), 0); err != nil {
		return fmt.Errorf("failed to write cpu.max: %w", err)
	}

	logger.L().Debug(ctx, "limited sandbox cgroup",
		zap.String("cgroup_name", h.cgroupName),
		zap.Int64("cpus", limitCpus))

	return nil
}

// cpuMaxValue renders a CPU count as a cpu.max quota, "max" for 0.
func cpuMaxValue(limitCpus int64) string {
	if limitCpus <= 0 {
		return "max"
	}

	return fmt.Sprintf("%d %d", limitCpus*cpuPeriodUsec, cpuPeriodUsec)
}

// ConfineVcpuThreads limits the VM's vCPU threads to targetOnlineVcpus. It requires all
// firecrackerVcpuCount threads to be found so none can escape the limit.
func (h *CgroupHandle) ConfineVcpuThreads(ctx context.Context, targetOnlineVcpus, firecrackerVcpuCount int64) error {
	if targetOnlineVcpus <= 0 || firecrackerVcpuCount <= targetOnlineVcpus {
		return nil
	}
	if !h.CanLimit() {
		return ErrNoCgroup
	}
	if h.vcpuThreadsConfined {
		return h.setVcpuCpuLimit(targetOnlineVcpus)
	}
	tids, err := vcpuThreads(h.path)
	if err != nil {
		return err
	}
	if int64(len(tids)) != firecrackerVcpuCount {
		return fmt.Errorf("vcpu threads in sandbox cgroup do not match the vm: found %d of %d", len(tids), firecrackerVcpuCount)
	}
	if err := h.createVcpuCgroup(); err != nil {
		return err
	}
	if err := h.setVcpuCpuLimit(targetOnlineVcpus); err != nil {
		return err
	}
	moved, err := h.moveVcpuThreads(tids)
	if err != nil {
		return err
	}
	if int64(moved) != firecrackerVcpuCount {
		return fmt.Errorf("vcpu threads moved to child cgroup do not match the vm: moved %d of %d", moved, firecrackerVcpuCount)
	}
	h.vcpuThreadsConfined = true

	logger.L().Debug(ctx, "confined vcpu threads",
		zap.String("cgroup_name", h.cgroupName),
		zap.Int64("cpus", targetOnlineVcpus),
		zap.Int("threads_moved", moved))

	return nil
}

func (h *CgroupHandle) vcpuCgroupPath() string {
	return filepath.Join(h.path, vcpuCgroupName)
}

// createVcpuCgroup makes the threaded vcpu child with the cpu controller, removing it again on failure.
func (h *CgroupHandle) createVcpuCgroup() (err error) {
	vcpuPath := h.vcpuCgroupPath()
	if err := os.Mkdir(vcpuPath, 0o755); err != nil {
		return fmt.Errorf("failed to create vcpu cgroup: %w", err)
	}
	defer func() {
		if err != nil {
			_ = os.Remove(vcpuPath)
		}
	}()
	if err := os.WriteFile(filepath.Join(vcpuPath, "cgroup.type"), []byte("threaded"), 0); err != nil {
		return fmt.Errorf("failed to make vcpu cgroup threaded: %w", err)
	}
	// cpu is a threaded controller, so the kernel allows this below a cgroup that has processes.
	if err := os.WriteFile(filepath.Join(h.path, "cgroup.subtree_control"), []byte("+cpu"), 0); err != nil {
		return fmt.Errorf("failed to enable cpu controller for vcpu cgroup: %w", err)
	}

	return nil
}

// setVcpuCpuLimit sets the vcpu child's quota; a repeat changes it in place.
func (h *CgroupHandle) setVcpuCpuLimit(limitCpus int64) error {
	if err := os.WriteFile(filepath.Join(h.vcpuCgroupPath(), "cpu.max"), []byte(cpuMaxValue(limitCpus)), 0); err != nil {
		return fmt.Errorf("failed to write vcpu cpu.max: %w", err)
	}

	return nil
}

// moveVcpuThreads moves the given threads into the child; one that exited in between is skipped.
func (h *CgroupHandle) moveVcpuThreads(tids []int) (int, error) {
	threadsPath := filepath.Join(h.vcpuCgroupPath(), "cgroup.threads")
	moved := 0
	for _, tid := range tids {
		err := os.WriteFile(threadsPath, []byte(strconv.Itoa(tid)), 0)
		if errors.Is(err, unix.ESRCH) {
			continue
		}
		if err != nil {
			return moved, fmt.Errorf("failed to move thread %d into vcpu cgroup: %w", tid, err)
		}
		moved++
	}

	return moved, nil
}

// vcpuThreads lists the fc_vcpu threads in the sandbox cgroup itself, not in its vcpu child.
func vcpuThreads(cgroupPath string) ([]int, error) {
	data, err := os.ReadFile(filepath.Join(cgroupPath, "cgroup.threads"))
	if err != nil {
		return nil, fmt.Errorf("failed to read cgroup.threads: %w", err)
	}

	var tids []int
	for _, tid := range parseUniqueThreadIDs(data) {
		comm, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(tid), "comm"))
		if err != nil {
			// The thread exited between the listing and the read.
			continue
		}
		if strings.HasPrefix(string(comm), VcpuThreadPrefix) {
			tids = append(tids, tid)
		}
	}

	return tids, nil
}

// parseUniqueThreadIDs deduplicates cgroup.threads entries before they count as vCPU threads.
func parseUniqueThreadIDs(data []byte) []int {
	var tids []int
	for field := range strings.FieldsSeq(string(data)) {
		tid, err := strconv.Atoi(field)
		if err != nil || slices.Contains(tids, tid) {
			continue
		}
		tids = append(tids, tid)
	}

	return tids
}

// GetFD returns the file descriptor for use with SysProcAttr.CgroupFD.
// Returns NoCgroupFD if the directory FD has been released.
// The FD is valid until ReleaseCgroupFD() is called.
func (h *CgroupHandle) GetFD() int {
	if h == nil || h.file == nil {
		return NoCgroupFD
	}

	return int(h.file.Fd())
}

// ReleaseCgroupFD releases the cgroup directory file descriptor.
// Call this after cmd.Start() — the kernel has already placed the process in
// the cgroup atomically during clone, so the directory FD is no longer needed.
//
// The memory.peak FD is intentionally kept open because the per-FD reset
// mechanism requires the same FD for the lifetime of stats collection.
// That FD is closed later by Remove().
//
// Safe to call multiple times.
func (h *CgroupHandle) ReleaseCgroupFD() error {
	if h == nil || h.file == nil {
		return nil
	}

	err := h.file.Close()
	h.file = nil

	return err
}

// GetStats retrieves current resource usage statistics for this cgroup.
// Returns (nil, nil) for noop handles (no real cgroup backing).
// Returns error if the handle is nil (unexpected) or stats cannot be read.
func (h *CgroupHandle) GetStats(ctx context.Context) (*Stats, error) {
	if h == nil {
		return nil, errors.New("cgroup handle is nil")
	}

	if h.noop {
		return nil, nil
	}

	return h.manager.getStatsForPath(ctx, h.path, h.memoryPeakFile)
}

// Kill terminates all processes currently in this cgroup.
// Safe to call multiple times. Returns nil if the cgroup is already empty or gone.
func (h *CgroupHandle) Kill(ctx context.Context) error {
	if h == nil || h.noop || h.removed {
		return nil
	}

	return h.kill(ctx)
}

func (h *CgroupHandle) kill(ctx context.Context) error {
	if h == nil || h.noop {
		return nil
	}

	populated, err := h.populated()
	if err != nil {
		return err
	}
	if !populated {
		return nil
	}

	if err := os.WriteFile(filepath.Join(h.path, "cgroup.kill"), []byte("1"), 0); err != nil {
		if os.IsNotExist(err) {
			return nil
		}

		return fmt.Errorf("failed to write cgroup.kill: %w", err)
	}

	events, err := os.Open(filepath.Join(h.path, "cgroup.events"))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to open cgroup.events: %w", err)
	}
	defer events.Close()

	deadline := time.Now().Add(cgroupKillTimeout)

	for {
		populated, err := cgroupEventsPopulated(events)
		if err != nil {
			return err
		}
		if !populated {
			return nil
		}

		if err := ctx.Err(); err != nil {
			return err
		}

		remaining := time.Until(deadline)
		if remaining <= 0 {
			return fmt.Errorf("cgroup %s still has processes after cgroup.kill", h.cgroupName)
		}

		pollTimeout := min(remaining, cgroupKillPollInterval)
		pollTimeoutMillis := int(pollTimeout / time.Millisecond)
		if pollTimeoutMillis == 0 {
			pollTimeoutMillis = 1
		}

		fds := []unix.PollFd{{
			Fd:     int32(events.Fd()),
			Events: unix.POLLPRI | unix.POLLERR,
		}}

		_, err = unix.Poll(fds, pollTimeoutMillis)
		if err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}

			return fmt.Errorf("failed to poll cgroup.events: %w", err)
		}
	}
}

// Remove closes all open FDs and deletes the cgroup directory.
// After an error, the handle may only be used to retry Remove.
// Safe to call multiple times. Returns error if removal fails
// (but tolerates the cgroup having been auto-cleaned by the kernel).
func (h *CgroupHandle) Remove(ctx context.Context) error {
	if h == nil || h.noop || h.removed {
		return nil
	}

	// Close the directory FD if ReleaseCgroupFD() was not called
	if h.file != nil {
		h.file.Close()
		h.file = nil
	}

	if h.memoryPeakFile != nil {
		h.memoryPeakFile.Close()
		h.memoryPeakFile = nil
	}

	// Try plain rmdir first. The kernel removes the directory once the
	// last process exits, so the common path is a single rmdir.
	rmErr := h.rmdir()
	if rmErr == nil || os.IsNotExist(rmErr) {
		h.removed = true
		logger.L().Debug(ctx, "removed cgroup for sandbox",
			zap.String("cgroup_name", h.cgroupName),
			zap.String("path", h.path))

		return nil
	}

	// rmdir failed (almost always EBUSY because something is still in the
	// cgroup, e.g. a leaked firecracker). Log so the leak is visible, then
	// fall back to cgroup.kill (cgroups v2, kernel 5.14+) and retry rmdir.
	logger.L().Warn(ctx, "cgroup rmdir failed, falling back to cgroup.kill",
		zap.String("cgroup_name", h.cgroupName),
		zap.String("path", h.path),
		zap.Error(rmErr))

	if err := h.kill(ctx); err != nil {
		logger.L().Warn(ctx, "failed to kill cgroup processes",
			zap.String("cgroup_name", h.cgroupName),
			zap.String("path", h.path),
			zap.Error(err))
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		err := h.rmdir()
		if err == nil || os.IsNotExist(err) {
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("failed to remove cgroup: %w", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}

	h.removed = true
	logger.L().Debug(ctx, "removed cgroup for sandbox after cgroup.kill",
		zap.String("cgroup_name", h.cgroupName),
		zap.String("path", h.path))

	return nil
}

// rmdir removes the vcpu child first, or the parent stays busy.
func (h *CgroupHandle) rmdir() error {
	if err := os.Remove(filepath.Join(h.path, vcpuCgroupName)); err != nil && !os.IsNotExist(err) {
		return err
	}

	return os.Remove(h.path)
}

// Path returns the filesystem path to the cgroup directory
func (h *CgroupHandle) Path() string {
	if h == nil {
		return ""
	}

	return h.path
}

// CgroupName returns the name of the cgroup
func (h *CgroupHandle) CgroupName() string {
	if h == nil {
		return ""
	}

	return h.cgroupName
}

func (h *CgroupHandle) populated() (bool, error) {
	data, err := os.ReadFile(filepath.Join(h.path, "cgroup.events"))
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("failed to read cgroup.events: %w", err)
	}

	return parseCgroupEventsPopulated(data)
}

func cgroupEventsPopulated(file *os.File) (bool, error) {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}

		return false, fmt.Errorf("failed to seek cgroup.events: %w", err)
	}

	data, err := io.ReadAll(file)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("failed to read cgroup.events: %w", err)
	}

	return parseCgroupEventsPopulated(data)
}

func parseCgroupEventsPopulated(data []byte) (bool, error) {
	for line := range strings.SplitSeq(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 || fields[0] != "populated" {
			continue
		}

		switch fields[1] {
		case "0":
			return false, nil
		case "1":
			return true, nil
		default:
			return false, fmt.Errorf("invalid populated value in cgroup.events: %q", fields[1])
		}
	}

	return false, errors.New("missing populated value in cgroup.events")
}

// Manager handles initialization and creation of cgroups
// Individual cgroup operations are performed through CgroupHandle
type Manager interface {
	// Initialize creates the root cgroup directory and enables controllers
	// Should be called once at orchestrator startup
	Initialize(ctx context.Context) error

	// Create creates a cgroup with the given name and returns a handle
	// The handle provides access to the cgroup's FD, stats, and cleanup
	// Returns error if cgroup creation fails
	Create(ctx context.Context, cgroupName string) (*CgroupHandle, error)

	// Destroy kills any remaining processes in an already-created sandbox
	// cgroup and removes it. Intended for reclaiming cgroups leaked by
	// sandboxes that did not shut down cleanly.
	Destroy(ctx context.Context, cgroupName string) error
}

type managerImpl struct{}

// NewManager creates a new cgroup manager
// Returns error if cgroups v2 is not available on the system
func NewManager() (Manager, error) {
	if _, err := os.Stat(filepath.Join(cgroupV2MountPoint, "cgroup.controllers")); err != nil {
		return nil, fmt.Errorf("cgroups v2 not available: %w", err)
	}

	return &managerImpl{}, nil
}

func (m *managerImpl) Initialize(ctx context.Context) error {
	if err := os.MkdirAll(RootCgroupPath, 0o755); err != nil {
		return fmt.Errorf("failed to create root cgroup directory: %w", err)
	}

	controllersPath := filepath.Join(RootCgroupPath, "cgroup.subtree_control")
	if err := os.WriteFile(controllersPath, []byte("+cpu +memory"), 0o644); err != nil {
		return fmt.Errorf("failed to enable controllers: %w", err)
	}

	logger.L().Info(ctx, "initialized root cgroup", zap.String("path", RootCgroupPath))

	return nil
}

func (m *managerImpl) Create(ctx context.Context, cgroupName string) (*CgroupHandle, error) {
	cgroupPath := m.cgroupPath(cgroupName)

	if err := os.MkdirAll(cgroupPath, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create cgroup directory: %w", err)
	}

	// FD for CLONE_INTO_CGROUP — atomic process placement during fork
	file, err := os.Open(cgroupPath)
	if err != nil {
		os.Remove(cgroupPath)

		return nil, fmt.Errorf("failed to open cgroup directory: %w", err)
	}

	// O_RDWR FD must stay open for per-FD peak reset across GetStats() calls
	memPeakPath := filepath.Join(cgroupPath, "memory.peak")
	memoryPeakFile, peakErr := os.OpenFile(memPeakPath, os.O_RDWR, 0)
	if peakErr != nil {
		// Not fatal — memory.peak may not exist on older kernels
		logger.L().Warn(ctx, "failed to open memory.peak for reset",
			zap.String("cgroup_name", cgroupName),
			zap.String("path", memPeakPath),
			zap.Error(peakErr))
		memoryPeakFile = nil
	}

	handle := &CgroupHandle{
		cgroupName:     cgroupName,
		path:           cgroupPath,
		file:           file,
		memoryPeakFile: memoryPeakFile,
		manager:        m,
	}

	logger.L().Debug(ctx, "created cgroup for sandbox",
		zap.String("cgroup_name", cgroupName),
		zap.String("path", cgroupPath),
		zap.Int("fd", handle.GetFD()),
		zap.Bool("peak_reset_available", memoryPeakFile != nil))

	return handle, nil
}

// Destroy kills any remaining processes in an existing sandbox cgroup and
// removes it.
func (m *managerImpl) Destroy(ctx context.Context, cgroupName string) error {
	handle, err := m.openExisting(ctx, cgroupName)
	if err != nil {
		return err
	}

	// Remove kills any remaining processes and deletes the cgroup.
	return handle.Remove(ctx)
}

// openExisting returns a handle for an already-created sandbox cgroup, intended
// for teardown of leaked cgroups. It deliberately does not open memory.peak
// (unlike Create), since callers only Kill/Remove the cgroup and never read peak
// memory.
func (m *managerImpl) openExisting(ctx context.Context, cgroupName string) (*CgroupHandle, error) {
	cgroupPath := m.cgroupPath(cgroupName)
	info, err := os.Stat(cgroupPath)
	if err != nil {
		return nil, fmt.Errorf("failed to stat cgroup directory: %w", err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("cgroup path is not a directory: %s", cgroupPath)
	}

	logger.L().Debug(ctx, "opened existing cgroup for sandbox",
		zap.String("cgroup_name", cgroupName),
		zap.String("path", cgroupPath))

	return &CgroupHandle{
		cgroupName: cgroupName,
		path:       cgroupPath,
		manager:    m,
	}, nil
}

func (m *managerImpl) getStatsForPath(ctx context.Context, cgroupPath string, memoryPeakFile *os.File) (*Stats, error) {
	stats := &Stats{}

	cpuStatPath := filepath.Join(cgroupPath, "cpu.stat")
	cpuData, err := os.ReadFile(cpuStatPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read cpu.stat: %w", err)
	}

	for line := range strings.SplitSeq(string(cpuData), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		value, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			continue
		}

		switch fields[0] {
		case "usage_usec":
			stats.CPUUsageUsec = value
		case "user_usec":
			stats.CPUUserUsec = value
		case "system_usec":
			stats.CPUSystemUsec = value
		}
	}

	memCurrentPath := filepath.Join(cgroupPath, "memory.current")
	memData, err := os.ReadFile(memCurrentPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read memory.current: %w", err)
	}
	stats.MemoryUsageBytes, _ = strconv.ParseUint(strings.TrimSpace(string(memData)), 10, 64)

	if memoryPeakFile != nil {
		peakBytes, err := m.readAndResetMemoryPeak(ctx, memoryPeakFile)
		if err != nil {
			logger.L().Warn(ctx, "failed to read memory.peak", zap.Error(err))
		} else {
			stats.MemoryPeakBytes = peakBytes
		}
	}

	return stats, nil
}

// readAndResetMemoryPeak reads the current peak memory value and resets it for the next interval.
// It uses the persistent FD kept open in CgroupHandle for per-FD reset tracking.
// The cgroups v2 kernel interface works as follows:
//   - Read requires file position 0 (seq_file), so we seek before reading.
//   - Write resets the per-FD peak to current memory usage. The kernel ignores
//     both the written content and the file offset, so no seek before write is needed.
func (m *managerImpl) readAndResetMemoryPeak(ctx context.Context, memoryPeakFile *os.File) (uint64, error) {
	if _, err := memoryPeakFile.Seek(0, io.SeekStart); err != nil {
		return 0, fmt.Errorf("failed to seek memory.peak for read: %w", err)
	}

	var buf [64]byte
	n, err := memoryPeakFile.Read(buf[:])
	if err != nil && err != io.EOF {
		return 0, fmt.Errorf("failed to read memory.peak: %w", err)
	}

	peakBytes, parseErr := strconv.ParseUint(strings.TrimSpace(string(buf[:n])), 10, 64)
	if parseErr != nil {
		return 0, fmt.Errorf("failed to parse memory.peak value %q: %w", strings.TrimSpace(string(buf[:n])), parseErr)
	}

	// Reset per-FD peak for next interval
	if _, err := memoryPeakFile.WriteString("0"); err != nil {
		logger.L().Warn(ctx, "failed to reset memory.peak, interval peak semantics degraded", zap.Error(err))
	}

	return peakBytes, nil
}

// cgroupPath returns the filesystem path for a sandbox's cgroup
func (m *managerImpl) cgroupPath(cgroupName string) string {
	return filepath.Join(RootCgroupPath, cgroupName)
}

func IsSandboxCgroupName(name string) bool {
	return strings.HasPrefix(name, "sbx-") && len(name) > len("sbx-")
}

func ListSandboxCgroups(root string) ([]string, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}

		return nil, fmt.Errorf("failed to read cgroup root: %w", err)
	}

	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() || !IsSandboxCgroupName(entry.Name()) {
			continue
		}

		names = append(names, entry.Name())
	}

	slices.Sort(names)

	return names, nil
}
