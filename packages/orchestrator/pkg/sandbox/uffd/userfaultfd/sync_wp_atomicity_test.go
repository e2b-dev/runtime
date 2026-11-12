//go:build linux

package userfaultfd

// Install/arm atomicity tests for synchronous write-protect dirty tracking.
// All tests run on a SYNCHRONOUS uffd (features=0) over a 2 MiB hugetlb page,
// mirroring the sync-WP Firecracker build. They assert kernel behavior the
// sync-WP design relies on (validated on 6.17-gcp) so a kernel or FC change
// that alters the semantics fails loudly here instead of silently invalidating
// design assumptions:
//
//   * CopyDontwakePresence — after UFFDIO_COPY(DONTWAKE) the page IS globally
//     writable (a never-faulted thread writes with no fault) while the faulted
//     thread stays blocked until UFFDIO_WAKE. This is the race window that
//     makes "COPY(DONTWAKE) → WRITEPROTECT → WAKE" non-atomic; and a post-copy
//     UFFDIO_WRITEPROTECT sticks (the next write traps).
//   * MarkerArmThenCopy — a WP marker armed on the NON-present page (hugetlb)
//     is LOST when plain COPY installs it: arm-before-install is not a usable
//     install+arm path.
//   * CopyModeWPOnSyncUffd — UFFDIO_COPY_MODE_WP DOES stick without WP_ASYNC
//     on this private-anon mapping (pagemap wp bit set, host write traps).
//     Note this does not transfer to the shared-memfd KVM config, where guest
//     read faults arrive write-flagged and install unprotected.

import (
	"os"
	"runtime"
	"runtime/debug"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd/testutils"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

// UFFDIO_COPY_MODE_DONTWAKE is part of the stable kernel ABI
// (linux/userfaultfd.h: ((__u64)1<<0)); fd.go only exposes MODE_WP.
const uffdioCopyModeDontwake CULong = 1 << 0

type pfEvent struct {
	addr uintptr
	wp   bool
}

// syncUffdOverHugepage creates a sync uffd (features=0) registered MISSING|WP
// over one fresh hugetlb page, plus a reader goroutine forwarding pagefault
// events. Returns the memory, its base address, the fd, and the event channel.
func syncUffdOverHugepage(t *testing.T) ([]byte, uintptr, Fd, chan pfEvent) {
	t.Helper()
	// These tests assert kernel-version-specific UFFD semantics, validated on
	// the production kernel (6.17-gcp). CI runners ship different kernels
	// where the semantics legitimately differ (observed: a concurrent writer
	// MISSING-faults instead of writing silently after COPY(DONTWAKE)), so
	// they run only where explicitly requested — node-validation runs set
	// this variable.
	if os.Getenv("E2B_UFFD_KERNEL_SEMANTICS") == "" {
		t.Skip("kernel-semantics assertions (prod-kernel behavior); set E2B_UFFD_KERNEL_SEMANTICS=1 to run")
	}
	if os.Geteuid() != 0 {
		t.Skip("requires root (userfaultfd registration)")
	}

	const pagesize = uint64(header.HugepageSize)
	mem, memStart := hugepageMmap(t, pagesize)

	fd, err := newFd(syscall.O_CLOEXEC)
	require.NoError(t, err)
	t.Cleanup(func() { fd.close() })

	api := newUffdioAPI(UFFD_API, 0) // features=0 → synchronous WP
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd), UFFDIO_API, uintptr(unsafe.Pointer(&api)))
	require.Zero(t, errno, "UFFDIO_API")
	require.NoError(t, register(fd, memStart, pagesize, UFFDIO_REGISTER_MODE_MISSING|UFFDIO_REGISTER_MODE_WP))
	t.Cleanup(func() { _ = unregister(fd, memStart, pagesize) })

	events := make(chan pfEvent, 16)
	go func() {
		buf := make([]byte, unsafe.Sizeof(UffdMsg{}))
		for {
			n, rerr := syscall.Read(int(fd), buf)
			if rerr == syscall.EINTR {
				continue
			}
			if rerr != nil || n == 0 {
				close(events)

				return
			}
			msg := (*UffdMsg)(unsafe.Pointer(&buf[0]))
			if getMsgEvent(msg) != UFFD_EVENT_PAGEFAULT {
				continue
			}
			arg := getMsgArg(msg)
			pf := (*UffdPagefault)(unsafe.Pointer(&arg[0]))
			events <- pfEvent{
				addr: getPagefaultAddress(pf),
				wp:   uint64(pf.flags)&uint64(UFFD_PAGEFAULT_FLAG_WP) != 0,
			}
		}
	}()

	// Blocked-in-fault goroutines hold their P; keep spares for the harness.
	if prev := runtime.GOMAXPROCS(0); prev < 6 {
		runtime.GOMAXPROCS(6)
		t.Cleanup(func() { runtime.GOMAXPROCS(prev) })
	}
	prevGC := debug.SetGCPercent(-1)
	t.Cleanup(func() { debug.SetGCPercent(prevGC) })

	return mem, memStart, fd, events
}

func expectEvent(t *testing.T, events chan pfEvent, what string) pfEvent {
	t.Helper()
	select {
	case ev := <-events:
		return ev
	case <-time.After(5 * time.Second):
		require.Failf(t, "timed out", "waiting for %s", what)

		return pfEvent{}
	}
}

func filledPage() []byte {
	b := make([]byte, header.HugepageSize)
	for i := range b {
		b[i] = 0xa5
	}

	return b
}

// TestCopyDontwakePresence checks the atomicity of
// COPY(DONTWAKE) → WRITEPROTECT → WAKE against a never-faulted writer.
//
//nolint:paralleltest // mutates GOMAXPROCS and disables GC
func TestCopyDontwakePresence(t *testing.T) {
	const pagesize = uint64(header.HugepageSize)
	mem, memStart, fd, events := syncUffdOverHugepage(t)

	// Thread A: reads the missing page → MISSING fault, blocks.
	aDone := make(chan struct{})
	var sink byte
	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		sink = mem[0]
		close(aDone)
	}()

	ev := expectEvent(t, events, "thread A's MISSING fault")
	assert.False(t, ev.wp, "A's fault should be MISSING, not WP")

	// Install the page with DONTWAKE (no MODE_WP).
	require.NoError(t, fd.copy(memStart, uintptr(pagesize), filledPage(), uffdioCopyModeDontwake))

	// Thread B: NEVER faulted before; writes the page now.
	bDone := make(chan struct{})
	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		mem[64] = 0x42
		close(bDone)
	}()

	// The page must be globally writable the instant COPY returns: B's write
	// completes with no uffd event even though B never faulted. This is the
	// race window that makes COPY(DONTWAKE) → WRITEPROTECT → WAKE non-atomic,
	// and the reason install+arm must use an atomic mode instead. Kernel
	// behavior, validated on 6.17-gcp; a fault here means the kernel changed
	// the semantics this package's design relies on.
	select {
	case <-bDone:
	case ev := <-events:
		require.Failf(t, "thread B faulted — page not globally writable after COPY(DONTWAKE); "+
			"the COPY→WP race-window assumption no longer holds", "wp=%v", ev.wp)
	case <-time.After(2 * time.Second):
		require.Fail(t, "B neither completed nor faulted within 2s")
	}

	// DONTWAKE must keep the FAULTED thread blocked until the explicit WAKE.
	select {
	case <-aDone:
		require.Fail(t, "thread A proceeded before UFFDIO_WAKE — DONTWAKE did not hold it")
	default:
	}

	// Second half: arm now, wake A, then a later write must trap.
	require.NoError(t, fd.writeProtectRange(memStart, uintptr(pagesize), uintptr(pagesize), UFFDIO_WRITEPROTECT_MODE_WP))
	require.NoError(t, fd.wake(memStart, uintptr(pagesize)))
	select {
	case <-aDone:
	case <-time.After(5 * time.Second):
		require.Fail(t, "A still blocked after WAKE")
	}
	_ = sink

	cDone := make(chan struct{})
	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		mem[128] = 0x7
		close(cDone)
	}()
	ev = expectEvent(t, events, "post-arm write fault")
	assert.True(t, ev.wp, "post-arm fault should be WP")
	require.NoError(t, fd.writeProtectRange(ev.addr&^uintptr(pagesize-1), uintptr(pagesize), uintptr(pagesize), 0))
	require.NoError(t, fd.wake(memStart, uintptr(pagesize)))
	select {
	case <-cDone:
	case <-time.After(5 * time.Second):
		require.Fail(t, "C still blocked after resolve")
	}
}

// TestMarkerArmThenCopy: WP the NON-present hugetlb page (marker),
// then plain COPY(DONTWAKE)+WAKE — does the installed page keep the protection?
//
//nolint:paralleltest // mutates GOMAXPROCS and disables GC
func TestMarkerArmThenCopy(t *testing.T) {
	const pagesize = uint64(header.HugepageSize)
	mem, memStart, fd, events := syncUffdOverHugepage(t)

	// Arm while non-present (hugetlb WP marker — what FC does at restore).
	require.NoError(t, fd.writeProtectRange(memStart, uintptr(pagesize), uintptr(pagesize), UFFDIO_WRITEPROTECT_MODE_WP))

	// Plain COPY (no MODE_WP), no pending fault needed.
	require.NoError(t, fd.copy(memStart, uintptr(pagesize), filledPage(), uffdioCopyModeDontwake))
	require.NoError(t, fd.wake(memStart, uintptr(pagesize)))

	// Read must NOT fault (present) and must see the copied content.
	require.Equal(t, byte(0xa5), mem[0], "copied content visible")

	// The decisive write: trap or sail through?
	wDone := make(chan struct{})
	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		mem[64] = 0x42
		close(wDone)
	}()

	// The WP marker does NOT survive the install: COPY replaces the marker
	// with an unprotected PTE, so the write sails through with no fault.
	// Arm-before-install is therefore not a usable install+arm path. Kernel
	// behavior, validated on 6.17-gcp; a fault here means marker semantics
	// changed and arm-first may have become viable.
	select {
	case ev := <-events:
		require.Failf(t, "marker-armed page trapped the write — WP marker unexpectedly survived COPY", "wp=%v", ev.wp)
	case <-wDone:
	case <-time.After(2 * time.Second):
		require.Fail(t, "writer neither completed nor faulted within 2s")
	}
}

// TestCopyModeWPOnSyncUffd: does COPY_MODE_WP stick without WP_ASYNC?
//
//nolint:paralleltest // mutates GOMAXPROCS and disables GC
func TestCopyModeWPOnSyncUffd(t *testing.T) {
	const pagesize = uint64(header.HugepageSize)
	mem, memStart, fd, events := syncUffdOverHugepage(t)

	// Install WITH MODE_WP (what the production read-fault path requests),
	// no pending fault needed.
	err := fd.copy(memStart, uintptr(pagesize), filledPage(), UFFDIO_COPY_MODE_WP)
	if err != nil {
		// A loud failure here is itself the answer (e.g. EINVAL without WP_ASYNC).
		t.Logf("COPY_MODE_WP returned error on sync uffd: %v", err)
		t.Skip("COPY_MODE_WP not accepted on sync uffd")
	}

	// MODE_WP must install the page present with the uffd-WP bit (pagemap
	// bit 57) already set — that atomicity (no writable instant between
	// install and protection) is the whole point of the mode.
	pagemap, err := testutils.NewPagemapReader()
	require.NoError(t, err)
	defer pagemap.Close()
	entry, err := pagemap.ReadEntry(memStart)
	require.NoError(t, err)
	require.True(t, entry.IsPresent(), "MODE_WP-installed page must be present")
	require.True(t, entry.IsWriteProtected(), "COPY_MODE_WP must set the uffd-wp bit on install")

	require.Equal(t, byte(0xa5), mem[0], "copied content visible (read, no WP fault expected)")

	wDone := make(chan struct{})
	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		mem[64] = 0x42
		close(wDone)
	}()

	// The protection must be enforced: the (host) write to the MODE_WP-installed
	// page traps with a WP fault and completes only after the resolve. Kernel
	// behavior, validated on 6.17-gcp for this private-anon mapping. In the
	// production shared-memfd KVM config, guest READ faults arrive
	// write-flagged and install unprotected, so MODE_WP applies there only to
	// host-side prefetch installs — where the WP bit was confirmed live via a
	// /proc/<fc>/pagemap probe — and runtime UFFDIO_WRITEPROTECT arming of
	// present pages, whose guest-write trapping was confirmed live on the
	// same config (re-arm/double-trap probe and the CoW-window E2E).
	select {
	case ev := <-events:
		assert.True(t, ev.wp, "MODE_WP-installed page must trap with the WP flag")
		require.NoError(t, fd.writeProtectRange(ev.addr&^uintptr(pagesize-1), uintptr(pagesize), uintptr(pagesize), 0))
		require.NoError(t, fd.wake(memStart, uintptr(pagesize)))
		select {
		case <-wDone:
		case <-time.After(5 * time.Second):
			require.Fail(t, "writer still blocked after resolve")
		}
	case <-wDone:
		require.Fail(t, "write sailed through — COPY_MODE_WP did not stick on the sync uffd")
	case <-time.After(2 * time.Second):
		require.Fail(t, "writer neither completed nor faulted within 2s")
	}
}
