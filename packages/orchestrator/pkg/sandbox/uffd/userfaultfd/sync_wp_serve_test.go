//go:build linux

package userfaultfd

// Tests for the synchronous write-protect serve path (resolveWriteProtect):
// a real WP fault is driven end to end through the resolve on a
// write-protected hugetlb page. Reuses hugepageMmap from
// sync_wp_bench_test.go (same package).

import (
	"os"
	"runtime"
	"runtime/debug"
	"sync"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd/testutils"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

// TestSyncWPResolveDirectFault drives one real synchronous WP fault through
// resolveWriteProtect: a writer stores to a write-protected hugepage (and
// blocks), the test reads the WP event and resolves it, and we assert the store
// completed, the page is marked dirty, and the counter advanced.
//
//nolint:paralleltest // mutates GOMAXPROCS and disables GC
func TestSyncWPResolveDirectFault(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("requires root (userfaultfd registration)")
	}

	const pagesize = uint64(header.HugepageSize)
	mem, memStart := hugepageMmap(t, pagesize)
	mem[0] = 0x00 // populate (present, unprotected)

	fd, err := newFd(syscall.O_CLOEXEC)
	require.NoError(t, err)
	t.Cleanup(func() { fd.close() })

	api := newUffdioAPI(UFFD_API, 0) // features=0 → synchronous WP
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd), UFFDIO_API, uintptr(unsafe.Pointer(&api)))
	require.Zero(t, errno, "UFFDIO_API")
	require.NoError(t, register(fd, memStart, pagesize, UFFDIO_REGISTER_MODE_WP))
	t.Cleanup(func() { _ = unregister(fd, memStart, pagesize) })

	u := &Userfaultfd{
		fd:          fd,
		pageSize:    uintptr(pagesize),
		pageTracker: block.NewTracker(),
	}

	// Arm the whole page for write-protection.
	require.NoError(t, fd.writeProtectRange(memStart, uintptr(pagesize), uintptr(pagesize), UFFDIO_WRITEPROTECT_MODE_WP))

	// No STW can wait on the fault-blocked writer thread.
	if prev := runtime.GOMAXPROCS(0); prev < 2 {
		runtime.GOMAXPROCS(2)
		defer runtime.GOMAXPROCS(prev)
	}
	prevGC := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(prevGC)

	var wg sync.WaitGroup
	wg.Go(func() {
		runtime.LockOSThread()
		mem[0] = 0x42 // sync WP fault → blocks until resolved below
	})

	// Read the WP fault event and resolve it. Poll with a deadline instead
	// of a bare blocking read: on a kernel that does not deliver the sync WP
	// fault for this configuration the test must fail with a message, not
	// hang the suite.
	buf := make([]byte, unsafe.Sizeof(UffdMsg{}))
	var addr uintptr
	deadline := time.Now().Add(30 * time.Second)
	for {
		remaining := time.Until(deadline)
		require.Positive(t, remaining, "no WP fault event within 30s — this kernel may not deliver synchronous WP faults for hugetlb")
		pfd := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
		pn, perr := unix.Poll(pfd, int(remaining.Milliseconds()))
		if perr == unix.EINTR {
			continue
		}
		require.NoError(t, perr)
		if pn == 0 {
			continue
		}
		n, rerr := syscall.Read(int(fd), buf)
		if rerr == syscall.EINTR {
			continue
		}
		require.NoError(t, rerr)
		require.NotZero(t, n)
		msg := (*UffdMsg)(unsafe.Pointer(&buf[0]))
		require.Equal(t, CUChar(UFFD_EVENT_PAGEFAULT), getMsgEvent(msg))
		arg := getMsgArg(msg)
		pf := (*UffdPagefault)(unsafe.Pointer(&arg[0]))
		require.NotZero(t, uint64(pf.flags)&uint64(UFFD_PAGEFAULT_FLAG_WP), "expected a WP fault")
		addr = getPagefaultAddress(pf)

		break
	}

	// Model the state a real MODE_WP install leaves behind, so the resolve
	// exercises the Clean→Dirty promotion (the tracker-source write signal).
	u.pageTracker.SetRange(0, 1, block.Clean)

	require.NoError(t, u.resolveWriteProtect(t.Context(), addr, 0, nil, time.Now()))

	wg.Wait() // store unblocked → completed

	assert.Equal(t, byte(0x42), mem[0], "write landed after resolve")
	assert.Equal(t, int64(1), u.wpFaultsResolved.Load(), "one WP fault resolved")
	assert.Equal(t, block.Dirty, u.pageTracker.Get(0), "resolve must promote the page to Dirty")
}

// staleWPTestSetup registers a hugepage for MISSING|WP tracking (the
// production registration; the presence probe's UFFDIO_COPY needs MISSING)
// and returns a handler whose tracker reads Removed for page 0, plus the
// mapped memory and its base address.
func staleWPTestSetup(t *testing.T) (u *Userfaultfd, mem []byte, memStart uintptr) {
	t.Helper()
	if os.Geteuid() != 0 {
		t.Skip("requires root (userfaultfd registration)")
	}

	const pagesize = uint64(header.HugepageSize)
	mem, memStart = hugepageMmap(t, pagesize)

	fd, err := newFd(syscall.O_CLOEXEC)
	require.NoError(t, err)
	t.Cleanup(func() { fd.close() })

	api := newUffdioAPI(UFFD_API, 0) // features=0 → synchronous WP
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd), UFFDIO_API, uintptr(unsafe.Pointer(&api)))
	require.Zero(t, errno, "UFFDIO_API")
	require.NoError(t, register(fd, memStart, pagesize, UFFDIO_REGISTER_MODE_MISSING|UFFDIO_REGISTER_MODE_WP))
	t.Cleanup(func() { _ = unregister(fd, memStart, pagesize) })

	u = &Userfaultfd{
		fd:          fd,
		pageSize:    uintptr(pagesize),
		pageTracker: block.NewTracker(),
	}
	u.pageTracker.SetRange(0, 1, block.Removed)

	return u, mem, memStart
}

// TestSyncWPStaleResolveAbsentPage: the resolve finds tracker=Removed and the
// page genuinely absent (the true-stale case). The presence probe must
// zero-install the page armed, record it Zero, and leave the resolve
// successful — the retried write then WP-faults and promotes normally.
func TestSyncWPStaleResolveAbsentPage(t *testing.T) {
	t.Parallel()

	u, mem, memStart := staleWPTestSetup(t)

	require.NoError(t, u.resolveWriteProtect(t.Context(), memStart, 0, nil, time.Now()))

	assert.Equal(t, block.Zero, u.pageTracker.Get(0), "probe must record the installed zero page")

	pagemap, err := testutils.NewPagemapReader()
	require.NoError(t, err)
	defer pagemap.Close()
	entry, err := pagemap.ReadEntry(memStart)
	require.NoError(t, err)
	assert.True(t, entry.IsPresent(), "probe must have installed the page")
	assert.True(t, entry.IsWriteProtected(), "probe must install ARMED (MODE_WP): the retried write must WP-fault and promote")
	assert.Equal(t, byte(0), mem[0], "installed content must be the zero page REMOVE semantics promise")
}

// TestSyncWPStaleResolvePresentPage: tracker=Removed but the page is present
// and armed — the interleaving where a REMOVE batch that waited on the serve
// lock clobbered a racing reinstall's tracker record. Wake-only here would
// livelock every write to the page; the probe's EEXIST must route the
// resolve to unprotect + MarkWrittenPresent instead.
func TestSyncWPStaleResolvePresentPage(t *testing.T) {
	t.Parallel()

	u, mem, memStart := staleWPTestSetup(t)

	// The "reinstall": present + armed, tracker still Removed (its
	// MarkInstalled was clobbered by the REMOVE batch).
	require.NoError(t, u.fd.copy(memStart, u.pageSize, header.EmptyHugePage[:u.pageSize], UFFDIO_COPY_MODE_WP))

	require.NoError(t, u.resolveWriteProtect(t.Context(), memStart, 0, nil, time.Now()))

	assert.Equal(t, block.Dirty, u.pageTracker.Get(0), "presence-verified resolve must promote through Removed")

	pagemap, err := testutils.NewPagemapReader()
	require.NoError(t, err)
	defer pagemap.Close()
	entry, err := pagemap.ReadEntry(memStart)
	require.NoError(t, err)
	assert.True(t, entry.IsPresent())
	assert.False(t, entry.IsWriteProtected(), "resolve must have cleared the protection: a wake-only path would leave the page armed and livelock the writer")

	mem[0] = 0x42 // must not fault: page is present and unprotected
	assert.Equal(t, byte(0x42), mem[0])
}
