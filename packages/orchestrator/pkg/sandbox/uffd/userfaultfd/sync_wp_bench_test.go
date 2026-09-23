//go:build linux

package userfaultfd

// A set of benchmarks that measure:
// 1. the cost per-fault round-trip cost of synchronous vs asynchronous
//    write protection via UFFD on a 2 MiB hugepage, i.e. the overhead of
//    synchronous write protection due to the fact that the write needs to
//    be resolved via a trip to the userspace UFFD handler.
// 2. The cost of marking a memory region as write protected.
//
// Model: this process owns the hugepage memory and does the timed stores
// (the "guest"); a handler goroutine on a dedicated OS thread drains the uffd
// and resolves each WP fault with UFFDIO_WRITEPROTECT(unprotect)+wake (no copy).
//
// These are benchmarks, so go test does not run them; they need -bench and,
// because they register a userfaultfd, root:
//
//	sudo -E go test -run '^$' -bench BenchmarkSyncWPFaultLatency -benchtime 1x -v ./packages/orchestrator/pkg/sandbox/uffd/userfaultfd/
//
// Tunables: E2B_WP_PAGES (default 256), E2B_WP_ROUNDS (default 20, multiplied
// by b.N).

import (
	"errors"
	"fmt"
	"math"
	"os"
	"runtime"
	"runtime/debug"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd/testutils"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}

	return def
}

// hugepageMmap allocates a MAP_PRIVATE|ANON 2 MiB-hugepage region, mirroring
// testutils.NewPageMmap but taking testing.TB. Skips (not fails) when the host
// has no hugepages reserved.
func hugepageMmap(tb testing.TB, size uint64) ([]byte, uintptr) {
	tb.Helper()

	pagesize := uint64(header.HugepageSize)
	l := int(math.Ceil(float64(size)/float64(pagesize)) * float64(pagesize))

	b, err := syscall.Mmap(
		-1, 0, l,
		syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_PRIVATE|syscall.MAP_ANONYMOUS|UnixMapHugeTLB|UnixMapHuge2MB,
	)
	if err != nil {
		if errors.Is(err, syscall.ENOMEM) {
			tb.Skipf("skipping: hugepage mmap failed (need %d hugepages reserved): %v", l/int(pagesize), err)
		}
		tb.Fatalf("hugepage mmap: %v", err)
	}
	tb.Cleanup(func() { syscall.Munmap(b) })

	return b, uintptr(unsafe.Pointer(&b[0]))
}

// MAP_HUGETLB / MAP_HUGE_2MB values (avoid importing x/sys/unix just for these;
// they match linux/mman.h). MAP_HUGE_2MB = 21 << MAP_HUGE_SHIFT(26).
const (
	UnixMapHugeTLB = 0x40000
	UnixMapHuge2MB = 21 << 26
)

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := max(int(math.Ceil(p/100*float64(len(sorted))))-1, 0)
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}

	return sorted[idx]
}

type serveResult struct {
	resolved int
	nonWP    int
	err      error
}

// measurement is what the timed store loop reports back to the test goroutine.
type measurement struct {
	latencies []time.Duration
	arms      []time.Duration
	err       error
}

// measurementBudget bounds the whole store loop. A store that faults returns
// only once the handler resolves that fault, so the bound has to be enforced
// from a goroutine that is not the one storing.
const measurementBudget = 120 * time.Second

// wakeBlockedFaults releases every store waiting on a synchronous WP fault in
// the range: unregistering wakes the waiters and their faults retry against an
// unprotected range. A thread inside a page fault cannot be interrupted, not
// by go test's timeout and not by SIGKILL, so whoever stops serving has to
// call this or the store never returns.
func wakeBlockedFaults(fd Fd, start uintptr, size uint64) {
	_ = unregister(fd, start, size)
}

func serveSyncWP(fd Fd, stop int, pagesize uint64, totalFaults int, resolvedSoFar *atomic.Int64) serveResult {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	buf := make([]byte, unsafe.Sizeof(UffdMsg{}))
	pfds := []unix.PollFd{
		{Fd: int32(fd), Events: unix.POLLIN},
		{Fd: int32(stop), Events: unix.POLLIN},
	}
	var res serveResult
	for res.resolved < totalFaults {
		if _, err := unix.Poll(pfds, -1); err != nil {
			if errors.Is(err, syscall.EINTR) {
				continue
			}
			res.err = fmt.Errorf("uffd poll: %w", err)

			return res
		}
		if pfds[1].Revents != 0 {
			return res
		}
		if pfds[0].Revents&(unix.POLLERR|unix.POLLHUP|unix.POLLNVAL) != 0 {
			res.err = fmt.Errorf("uffd poll events: %#x", pfds[0].Revents)

			return res
		}

		n, err := syscall.Read(int(fd), buf)
		if errors.Is(err, syscall.EINTR) || errors.Is(err, syscall.EAGAIN) {
			continue
		}
		if err != nil {
			res.err = fmt.Errorf("uffd read: %w", err)

			return res
		}
		if n == 0 {
			continue
		}

		msg := (*UffdMsg)(unsafe.Pointer(&buf[0]))
		if getMsgEvent(msg) != UFFD_EVENT_PAGEFAULT {
			continue
		}

		arg := getMsgArg(msg)
		pf := (*UffdPagefault)(unsafe.Pointer(&arg[0]))
		if uint64(pf.flags)&uint64(UFFD_PAGEFAULT_FLAG_WP) == 0 {
			res.nonWP++
		}

		addr := getPagefaultAddress(pf) &^ uintptr(pagesize-1)
		// Mode 0 clears WP and wakes the blocked writer.
		if err := fd.writeProtectRange(addr, uintptr(pagesize), uintptr(pagesize), 0); err != nil {
			res.err = fmt.Errorf("unprotect: %w", err)

			return res
		}
		res.resolved++
		resolvedSoFar.Store(int64(res.resolved))
	}

	return res
}

// BenchmarkSyncWPFaultLatency measures synchronous WP fault round-trip latency on
// 2 MiB hugepages and reports the distribution + re-arm cost.
func BenchmarkSyncWPFaultLatency(b *testing.B) {
	if os.Geteuid() != 0 {
		b.Skip("requires root (userfaultfd registration)")
	}

	const pagesize = uint64(header.HugepageSize)
	nPages := envInt("E2B_WP_PAGES", 256)
	// need >=1 warmup + >=1 measured round
	// b.N multiplies the rounds rather than repeating the whole run, so the
	// cold round is discarded once instead of once per iteration.
	nRounds := max(envInt("E2B_WP_ROUNDS", 20), 2) * b.N
	size := pagesize * uint64(nPages)

	mem, memStart := hugepageMmap(b, size)

	// Populate every hugepage (present, no uffd yet) so the measured faults are
	// pure WP faults, not MISSING faults.
	for i := range nPages {
		mem[uint64(i)*pagesize] = 0
	}

	// Create the uffd WITHOUT WP_ASYNC → synchronous WP fault delivery.
	fd, err := newFd(syscall.O_CLOEXEC | syscall.O_NONBLOCK)
	if err != nil {
		b.Fatalf("userfaultfd: %v", err)
	}
	b.Cleanup(func() { fd.close() })

	// UFFDIO_API with features=0 (the crux: no WP_ASYNC). Read back the
	// kernel-supported features for the record.
	api := newUffdioAPI(UFFD_API, 0)
	if _, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd), UFFDIO_API, uintptr(unsafe.Pointer(&api))); errno != 0 {
		b.Fatalf("UFFDIO_API: %v", errno)
	}
	wpAsyncSupported := uint64(api.features)&uint64(UFFD_FEATURE_WP_ASYNC) != 0

	// Register write-protect-only. (Production registers MISSING|WP; we only
	// need WP here since pages are already present.)
	if err := register(fd, memStart, size, UFFDIO_REGISTER_MODE_WP); err != nil {
		b.Fatalf("UFFDIO_REGISTER MODE_WP on hugetlbfs: %v", err)
	}
	b.Cleanup(func() { _ = unregister(fd, memStart, size) })

	totalFaults := nPages * nRounds

	var stop [2]int
	if err := syscall.Pipe2(stop[:], syscall.O_CLOEXEC|syscall.O_NONBLOCK); err != nil {
		b.Fatalf("pipe2: %v", err)
	}

	var resolvedSoFar atomic.Int64
	done := make(chan serveResult, 1)
	handlerExited := make(chan struct{})
	go func() {
		defer close(handlerExited)
		res := serveSyncWP(fd, stop[0], pagesize, totalFaults, &resolvedSoFar)
		if res.err != nil {
			wakeBlockedFaults(fd, memStart, size)
		}
		done <- res
	}()
	b.Cleanup(func() {
		// Closing the pipe wakes poll even when no more faults will arrive.
		// Join before the remaining cleanups close the uffd and unmap memory.
		_ = syscall.Close(stop[1])
		<-handlerExited
		_ = syscall.Close(stop[0])
	})

	// One P each for the fault-blocked writer, the handler and this goroutine
	// (it has to run the deadline branch while the writer holds its P), and
	// stop GC so no STW can block on the fault-stuck writer M.
	if prev := runtime.GOMAXPROCS(0); prev < 3 {
		runtime.GOMAXPROCS(3)
		defer runtime.GOMAXPROCS(prev)
	}
	prevGC := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(prevGC)

	b.ResetTimer()

	// The stores run on their own goroutine so this one stays free to enforce
	// measurementBudget: a store waiting on an unresolved fault never reaches
	// a check of its own.
	measured := make(chan measurement, 1)
	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()

		m := measurement{
			latencies: make([]time.Duration, 0, nPages*(nRounds-1)),
			arms:      make([]time.Duration, 0, nRounds-1),
		}

		for r := range nRounds {
			// Arm: WP the whole range (this is the per-snapshot re-arm cost).
			armStart := time.Now()
			if err := fd.writeProtectRange(memStart, uintptr(size), uintptr(pagesize), UFFDIO_WRITEPROTECT_MODE_WP); err != nil {
				m.err = fmt.Errorf("arm writeProtectRange (round %d): %w", r, err)
				measured <- m

				return
			}
			armDur := time.Since(armStart)

			for i := range nPages {
				off := uint64(i) * pagesize
				start := time.Now()
				mem[off] = byte(r) // store → sync WP fault → handler unprotect+wake → returns
				lat := time.Since(start)

				if r > 0 { // discard round 0 (cold) as warmup
					m.latencies = append(m.latencies, lat)
				}
			}
			if r > 0 {
				m.arms = append(m.arms, armDur)
			}
		}

		measured <- m
	}()

	var m measurement
	select {
	case m = <-measured:
	case <-time.After(measurementBudget):
		resolved := resolvedSoFar.Load()

		wakeBlockedFaults(fd, memStart, size)

		// Join before failing. Failing runs the cleanups, which close the
		// descriptor and unmap mem, and the store goroutine is touching both.
		// It terminates on its own: no store can block on an unregistered
		// range, and the next round's arm fails there.
		<-measured

		b.Fatalf("stores did not finish within %s (%d of %d faults resolved): WP faults are not being delivered, or the handler stopped serving before the last store",
			measurementBudget, resolved, totalFaults)
	}

	b.StopTimer()

	if m.err != nil {
		b.Fatal(m.err)
	}

	latencies, arms := m.latencies, m.arms

	// Join the handler (with a timeout so a non-delivering kernel fails loudly
	// instead of hanging).
	var res serveResult
	select {
	case res = <-done:
	case <-time.After(30 * time.Second):
		b.Fatalf("handler did not resolve %d faults — sync WP not delivering on hugepages", totalFaults)
	}
	if res.err != nil {
		b.Fatalf("handler error after %d/%d faults: %v", res.resolved, totalFaults, res.err)
	}
	if res.resolved != totalFaults {
		b.Fatalf("resolved %d faults, expected %d", res.resolved, totalFaults)
	}
	if res.nonWP != 0 {
		b.Errorf("got %d non-WP pagefaults (expected all WP)", res.nonWP)
	}

	slices.Sort(latencies)
	var sum time.Duration
	for _, l := range latencies {
		sum += l
	}
	mean := sum / time.Duration(len(latencies))
	faultsPerSec := float64(time.Second) / float64(mean)

	var armSum time.Duration
	for _, a := range arms {
		armSum += a
	}
	armMean := armSum / time.Duration(len(arms))

	b.Logf("=== sync WP fault latency (2 MiB hugepages) ===")
	b.Logf("config: %d pages (%d MiB) x %d rounds; %d measured faults (round 0 warmup discarded)",
		nPages, size/(1024*1024), nRounds, len(latencies))
	b.Logf("kernel WP_ASYNC supported: %v; uffd created WITHOUT it (synchronous WP)", wpAsyncSupported)
	b.Logf("per-fault round-trip: p50=%v  p90=%v  p99=%v  max=%v  mean=%v",
		percentile(latencies, 50), percentile(latencies, 90),
		percentile(latencies, 99), percentile(latencies, 100), mean)
	b.Logf("throughput (single writer, lock-step): %.0f faults/sec", faultsPerSec)
	b.Logf("re-arm whole range: mean=%v (%v per hugepage, %d pages)",
		armMean, armMean/time.Duration(nPages), nPages)
	b.Logf("projected tax to re-dirty a working set: 1 GiB=%v, 8 GiB=%v (mean x pages)",
		mean*time.Duration(512), mean*time.Duration(4096))

	b.ReportMetric(float64(mean), "ns/fault")
	b.ReportMetric(float64(percentile(latencies, 99)), "p99-ns/fault")
	b.ReportMetric(float64(armMean)/float64(nPages), "ns/rearmed-page")
}

// BenchmarkAsyncWPWriteLatency is the WP_ASYNC counterpart of BenchmarkSyncWPFaultLatency:
// the current production mechanism. With WP_ASYNC the kernel resolves the write
// fault in-kernel (clears the WP bit, marks the pagemap dirty) with NO userspace
// handler round-trip, so there is no handler here — we just time the stores. The
// dirty set is then read back by scanning /proc/self/pagemap (present && !WP),
// which is the readout cost the async approach pays at snapshot time and that the
// sync/always-sync design avoids. Comparing the two tests gives the per-write tax
// of moving dirty tracking from in-kernel (async) to userspace (sync).
func BenchmarkAsyncWPWriteLatency(b *testing.B) {
	if os.Geteuid() != 0 {
		b.Skip("requires root (userfaultfd registration)")
	}

	const pagesize = uint64(header.HugepageSize)
	nPages := envInt("E2B_WP_PAGES", 256)
	nRounds := max(envInt("E2B_WP_ROUNDS", 20), 2) * b.N
	size := pagesize * uint64(nPages)

	mem, memStart := hugepageMmap(b, size)
	for i := range nPages {
		mem[uint64(i)*pagesize] = 0
	}

	fd, err := newFd(syscall.O_CLOEXEC)
	if err != nil {
		b.Fatalf("userfaultfd: %v", err)
	}
	b.Cleanup(func() { fd.close() })

	// UFFDIO_API WITH WP_ASYNC (production config).
	features := CULong(UFFD_FEATURE_WP_ASYNC)
	if pagesize == header.HugepageSize {
		features |= UFFD_FEATURE_MISSING_HUGETLBFS
	}
	api := newUffdioAPI(UFFD_API, features)
	if _, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd), UFFDIO_API, uintptr(unsafe.Pointer(&api))); errno != 0 {
		b.Fatalf("UFFDIO_API: %v", errno)
	}
	if uint64(api.features)&uint64(UFFD_FEATURE_WP_ASYNC) == 0 {
		b.Skip("kernel does not support UFFD_FEATURE_WP_ASYNC")
	}

	if err := register(fd, memStart, size, UFFDIO_REGISTER_MODE_WP); err != nil {
		b.Fatalf("UFFDIO_REGISTER MODE_WP: %v", err)
	}
	b.Cleanup(func() { _ = unregister(fd, memStart, size) })

	pagemap, err := testutils.NewPagemapReader()
	if err != nil {
		b.Fatalf("pagemap reader: %v", err)
	}
	b.Cleanup(func() { pagemap.Close() })

	if prev := runtime.GOMAXPROCS(0); prev < 2 {
		runtime.GOMAXPROCS(2)
		defer runtime.GOMAXPROCS(prev)
	}
	prevGC := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(prevGC)

	latencies := make([]time.Duration, 0, nPages*(nRounds-1))
	arms := make([]time.Duration, 0, nRounds-1)
	scans := make([]time.Duration, 0, nRounds-1)

	b.ResetTimer()

	for r := range nRounds {
		armStart := time.Now()
		if err := fd.writeProtectRange(memStart, uintptr(size), uintptr(pagesize), UFFDIO_WRITEPROTECT_MODE_WP); err != nil {
			b.Fatalf("arm writeProtectRange (round %d): %v", r, err)
		}
		armDur := time.Since(armStart)

		for i := range nPages {
			off := uint64(i) * pagesize
			start := time.Now()
			mem[off] = byte(r) // in-kernel WP-async clear, no userspace fault, no block
			lat := time.Since(start)
			if r > 0 {
				latencies = append(latencies, lat)
			}
		}

		// Readout: scan pagemap for the dirty set (present && !WP), timed.
		scanStart := time.Now()
		dirty := 0
		for i := range nPages {
			e, rerr := pagemap.ReadEntry(memStart + uintptr(uint64(i)*pagesize))
			if rerr != nil {
				b.Fatalf("pagemap read (round %d page %d): %v", r, i, rerr)
			}
			if e.IsPresent() && !e.IsWriteProtected() {
				dirty++
			}
		}
		scanDur := time.Since(scanStart)

		if dirty != nPages {
			b.Errorf("round %d: pagemap reported %d/%d dirty (expected all written)", r, dirty, nPages)
		}
		if r > 0 {
			arms = append(arms, armDur)
			scans = append(scans, scanDur)
		}
	}

	b.StopTimer()

	slices.Sort(latencies)
	var sum time.Duration
	for _, l := range latencies {
		sum += l
	}
	mean := sum / time.Duration(len(latencies))

	meanOf := func(ds []time.Duration) time.Duration {
		var s time.Duration
		for _, d := range ds {
			s += d
		}

		return s / time.Duration(len(ds))
	}

	b.Logf("=== async WP write latency (2 MiB hugepages, WP_ASYNC = current mechanism) ===")
	b.Logf("config: %d pages (%d MiB) x %d rounds; %d measured writes (round 0 warmup discarded)",
		nPages, size/(1024*1024), nRounds, len(latencies))
	b.Logf("per-write (in-kernel, no handler): p50=%v  p90=%v  p99=%v  max=%v  mean=%v",
		percentile(latencies, 50), percentile(latencies, 90),
		percentile(latencies, 99), percentile(latencies, 100), mean)
	b.Logf("re-arm whole range: mean=%v (%v per hugepage)", meanOf(arms), meanOf(arms)/time.Duration(nPages))
	b.Logf("pagemap dirty-set readout: mean=%v for %d pages (%v per hugepage) — the async-only snapshot cost",
		meanOf(scans), nPages, meanOf(scans)/time.Duration(nPages))

	b.ReportMetric(float64(mean), "ns/write")
	b.ReportMetric(float64(meanOf(scans))/float64(nPages), "ns/scanned-page")
}

// runConcurrentSyncWP runs nWriters writer goroutines (simulating nWriters vCPUs), each on its
// own OS thread, each dirtying a disjoint sub-range of the hugepages with real
// stores that trigger synchronous WP faults. A single reader goroutine drains the
// uffd and fans fault resolution out to `handlers` worker goroutines — mirroring
// the production serve loop (u.wg.SetLimit(maxRequestsInProgress)). Returns every
// writer's per-fault latencies (merged) and the wall time.
//
// Deadlock note: a goroutine blocked in a page fault holds its P (unlike a
// syscall), so the caller MUST set GOMAXPROCS > nWriters or the resolvers starve. The
// blocked writers consume no CPU, so resolvers still run on the physical cores.
func runConcurrentSyncWP(tb testing.TB, mem []byte, memStart uintptr, pagesize uint64, nPages, nRounds, nWriters, handlers int) ([]time.Duration, time.Duration) {
	tb.Helper()

	size := pagesize * uint64(nPages)

	// Non-blocking: the reader polls, then drains until EAGAIN.
	fd, err := newFd(syscall.O_CLOEXEC | syscall.O_NONBLOCK)
	if err != nil {
		tb.Fatalf("userfaultfd: %v", err)
	}
	defer fd.close()

	api := newUffdioAPI(UFFD_API, 0) // features=0 → synchronous WP
	if _, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd), UFFDIO_API, uintptr(unsafe.Pointer(&api))); errno != 0 {
		tb.Fatalf("UFFDIO_API: %v", errno)
	}
	if err := register(fd, memStart, size, UFFDIO_REGISTER_MODE_WP); err != nil {
		tb.Fatalf("register MODE_WP: %v", err)
	}
	defer func() { _ = unregister(fd, memStart, size) }()

	// Handler: one reader draining the uffd (poll-edge-drained), fanning out to
	// `handlers` resolver workers — mirrors the production serve loop.
	workCh := make(chan uintptr, handlers*8)
	var resolvers sync.WaitGroup
	for range handlers {
		resolvers.Go(func() {
			for addr := range workCh {
				_ = fd.writeProtectRange(addr, uintptr(pagesize), uintptr(pagesize), 0) // unprotect + wake
			}
		})
	}

	// stop pipe: signalled once all writers finish — by then every fault is
	// provably resolved (a store can't return until its own fault is), so the
	// reader terminates cleanly without needing to count faults.
	var stop [2]int
	if err := syscall.Pipe2(stop[:], syscall.O_CLOEXEC|syscall.O_NONBLOCK); err != nil {
		tb.Fatalf("pipe2: %v", err)
	}
	defer syscall.Close(stop[0])
	defer syscall.Close(stop[1])

	readerDone := make(chan struct{})
	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		defer close(readerDone)

		buf := make([]byte, unsafe.Sizeof(UffdMsg{}))
		pfds := []unix.PollFd{
			{Fd: int32(fd), Events: unix.POLLIN},
			{Fd: int32(stop[0]), Events: unix.POLLIN},
		}
		for {
			if _, perr := unix.Poll(pfds, -1); perr != nil {
				if errors.Is(perr, syscall.EINTR) {
					continue
				}

				return
			}
			if pfds[1].Revents&unix.POLLIN != 0 {
				return // stop signalled
			}
			for { // drain all ready events
				n, rerr := syscall.Read(int(fd), buf)
				if errors.Is(rerr, syscall.EAGAIN) {
					break
				}
				if errors.Is(rerr, syscall.EINTR) {
					continue
				}
				if rerr != nil || n == 0 {
					break
				}
				msg := (*UffdMsg)(unsafe.Pointer(&buf[0]))
				if getMsgEvent(msg) != UFFD_EVENT_PAGEFAULT {
					continue
				}
				arg := getMsgArg(msg)
				pf := (*UffdPagefault)(unsafe.Pointer(&arg[0]))
				workCh <- getPagefaultAddress(pf) &^ uintptr(pagesize-1)
			}
		}
	}()

	// Writers: nWriters disjoint sub-ranges, each re-arms its own range every round.
	perW := (nPages + nWriters - 1) / nWriters
	results := make([][]time.Duration, nWriters)
	start := make(chan struct{})
	var writers sync.WaitGroup
	for w := range nWriters {
		lo := w * perW
		hi := min(lo+perW, nPages)
		if lo >= hi {
			continue
		}
		writers.Add(1)
		go func(w, lo, hi int) {
			defer writers.Done()
			runtime.LockOSThread()
			defer runtime.UnlockOSThread()

			lats := make([]time.Duration, 0, (hi-lo)*(nRounds-1))
			<-start
			for r := range nRounds {
				// Arm this writer's own sub-range (disjoint from others).
				rangeStart := memStart + uintptr(uint64(lo)*pagesize)
				if err := fd.writeProtectRange(rangeStart, uintptr(uint64(hi-lo)*pagesize), uintptr(pagesize), UFFDIO_WRITEPROTECT_MODE_WP); err != nil {
					tb.Errorf("writer %d arm: %v", w, err)

					return
				}
				for i := lo; i < hi; i++ {
					off := uint64(i) * pagesize
					ts := time.Now()
					mem[off] = byte(r) // sync WP fault → reader → resolver → wake
					if r > 0 {
						lats = append(lats, time.Since(ts))
					}
				}
			}
			results[w] = lats
		}(w, lo, hi)
	}

	wallStart := time.Now()
	close(start)

	// Watchdog: every store blocks until its fault resolves, so all writers
	// finishing proves the handler kept up. Bail loudly instead of hanging.
	writersDone := make(chan struct{})
	go func() { writers.Wait(); close(writersDone) }()
	select {
	case <-writersDone:
	case <-time.After(60 * time.Second):
		_, _ = syscall.Write(stop[1], []byte{1})
		<-readerDone
		close(workCh)

		// The writers are blocked in faults the stopped reader will not serve;
		// leaving them there pins a P each for the rest of the binary.
		wakeBlockedFaults(fd, memStart, size)

		// Join before failing. Failing runs this function's defers and the
		// caller's cleanups, which close the descriptor and unmap mem, while
		// the woken writers still store into mem and can report an arm
		// failure that would land after the benchmark had finished.
		<-writersDone
		resolvers.Wait()

		tb.Fatalf("nWriters=%d: writers did not finish within 60s (handler not keeping up / deadlock)", nWriters)
	}
	wall := time.Since(wallStart)

	// All faults resolved → stop the reader, then drain resolvers.
	if _, err := syscall.Write(stop[1], []byte{1}); err != nil {
		tb.Fatalf("signal stop: %v", err)
	}
	<-readerDone
	close(workCh)
	resolvers.Wait()

	var all []time.Duration
	for _, r := range results {
		all = append(all, r...)
	}

	return all, wall
}

// BenchmarkSyncWPConcurrentLatency sweeps the number of concurrent writers (simulated
// vCPUs) and reports whether the production-style (reader + worker fan-out)
// handler keeps per-fault latency bounded under concurrent load — the
// question the single-writer BenchmarkSyncWPFaultLatency can't answer.
func BenchmarkSyncWPConcurrentLatency(b *testing.B) {
	if os.Geteuid() != 0 {
		b.Skip("requires root (userfaultfd registration)")
	}

	const pagesize = uint64(header.HugepageSize)
	nPages := envInt("E2B_WP_PAGES", 256)
	nRounds := max(envInt("E2B_WP_ROUNDS", 20), 2)
	handlers := envInt("E2B_WP_HANDLERS", runtime.NumCPU())
	size := pagesize * uint64(nPages)

	mem, memStart := hugepageMmap(b, size)
	for i := range nPages {
		mem[uint64(i)*pagesize] = 0
	}

	// Writer-count sweep, capped at nPages and deduped.
	ncpu := runtime.NumCPU()
	candidates := []int{1, 2, 4, ncpu}
	var writerCounts []int
	seen := map[int]bool{}
	for _, w := range candidates {
		if w >= 1 && w <= nPages && !seen[w] {
			seen[w] = true
			writerCounts = append(writerCounts, w)
		}
	}
	slices.Sort(writerCounts)
	maxW := writerCounts[len(writerCounts)-1]

	// GOMAXPROCS must exceed max concurrent fault-blocked writers so resolvers
	// (and the reader) always have a P.
	want := maxW + handlers + 4
	if prev := runtime.GOMAXPROCS(0); prev < want {
		runtime.GOMAXPROCS(want)
		defer runtime.GOMAXPROCS(prev)
	}
	prevGC := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(prevGC)

	b.Logf("config: %d pages x %d rounds, %d handler workers, %d host CPUs; one sub-benchmark per writer count",
		nPages, nRounds, handlers, ncpu)

	for _, W := range writerCounts {
		b.Run(fmt.Sprintf("writers=%d", W), func(b *testing.B) {
			var (
				all  []time.Duration
				wall time.Duration
			)

			for range b.N {
				lats, w := runConcurrentSyncWP(b, mem, memStart, pagesize, nPages, nRounds, W, handlers)
				all = append(all, lats...)
				wall += w
			}

			if len(all) == 0 {
				b.Skip("no faults measured")
			}

			slices.Sort(all)
			b.ReportMetric(float64(percentile(all, 50)), "p50-ns/fault")
			b.ReportMetric(float64(percentile(all, 99)), "p99-ns/fault")
			b.ReportMetric(float64(percentile(all, 100)), "max-ns/fault")
			b.ReportMetric(float64(len(all))/wall.Seconds(), "faults/s")
		})
	}
}
