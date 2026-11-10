//go:build linux

package nfsproxy

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/stretchr/testify/require"
	servernfs "github.com/willscott/go-nfs"
	"golang.org/x/sys/unix"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/chrooted"
	nfscfg "github.com/e2b-dev/infra/packages/orchestrator/pkg/nfsproxy/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/nfsproxy/chroot"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"
)

func TestNetworkReleaseDoesNotWaitForOriginalRootClose(t *testing.T) { //nolint:paralleltest // observes an owned syscall and real root descriptors
	f := newConcernFixture(t, nfscfg.Config{}, nil)
	fs := f.mount(t)
	rootHandle := f.proxy.server.Handler.ToHandle(t.Context(), fs, nil)
	releaseOpen := blockConcernRootOpen(t, fs, f.paths[0])

	released := make(chan error, 1)
	go func() { released <- f.sandboxes.NetworkReleased(t.Context(), "127.0.0.1") }()
	require.Eventually(t, func() bool { return f.proxy.retirement.count() == 1 }, time.Second, time.Millisecond)
	select {
	case err := <-released:
		require.NoError(t, err, "logical release must finish without waiting for the backing operation")
	case <-time.After(time.Second):
		t.Fatal("network release waited on the original blocked root operation")
	}
	require.Equal(t, 1, concernRootFDCount(t, f.paths[0]), "logical release must not claim the held root was closed")
	_, _, err := f.proxy.server.Handler.FromHandle(t.Context(), rootHandle)
	var stale *servernfs.NFSStatusError
	require.ErrorAs(t, err, &stale)
	require.Equal(t, servernfs.NFSStatusStale, stale.NFSStatus, "the old root handle must be invalid before logical release succeeds")
	retirement := f.proxy.retirement.retire(t.Context(), f.owner)
	releaseOpen()
	select {
	case <-retirement.done:
		require.NoError(t, retirement.err)
	case <-time.After(time.Second):
		t.Fatal("original root cleanup did not finish after unblocking its syscall")
	}
	require.Zero(t, concernRootFDCount(t, f.paths[0]))
	require.Zero(t, f.proxy.retirement.count())
}

// Both roots begin with real blocked operations so map iteration cannot pick an
// idle root first and hide serial cleanup. Once either starts closing, the
// other operation finishes while the first remains held.
func TestNetworkReleaseClosesIndependentRoots(t *testing.T) { //nolint:paralleltest // observes real blocked syscalls and descriptor lifetimes
	f := newConcernFixture(t, nfscfg.Config{}, nil)
	first := f.mount(t)
	status, second, _ := f.proxy.server.Handler.Mount(t.Context(), f.conn, servernfs.MountRequest{Dirpath: []byte(f.mountPath("second"))})
	require.Equal(t, servernfs.MountStatusOk, status)
	roots := []billy.Filesystem{first, second}
	unblock := []func(){blockConcernRootOpen(t, first, f.paths[0]), blockConcernRootOpen(t, second, f.paths[1])}
	closing := make(chan int, len(roots))
	stop := make(chan struct{})
	var probes sync.WaitGroup
	t.Cleanup(func() {
		close(stop)
		for _, release := range unblock {
			release()
		}
		probes.Wait()
	})
	require.NoError(t, f.sandboxes.NetworkReleased(t.Context(), "127.0.0.1"))
	retirement := f.proxy.retirement.retire(t.Context(), f.owner)
	for i, fs := range roots {
		probes.Go(func() {
			for {
				if _, err := fs.Stat("."); errors.Is(err, chrooted.ErrClosed) {
					closing <- i

					return
				}
				select {
				case <-stop:
					return
				case <-time.After(time.Millisecond):
				}
			}
		})
	}
	var held int
	select {
	case held = <-closing:
	case <-time.After(time.Second):
		t.Fatal("retirement did not close either root to new operations")
	}
	idle := 1 - held
	unblock[idle]()
	require.Eventually(t, func() bool { return concernRootFDCount(t, f.paths[idle]) == 0 }, time.Second, time.Millisecond,
		"an independent root must physically close while its sibling operation remains held")
	require.Equal(t, 1, concernRootFDCount(t, f.paths[held]))
	select {
	case <-retirement.done:
		t.Fatal("retirement completed before the held root was physically closed")
	default:
	}
	unblock[held]()
	select {
	case <-retirement.done:
		require.NoError(t, retirement.err)
	case <-time.After(time.Second):
		t.Fatal("retirement did not join both original root closes")
	}
	require.Zero(t, concernRootFDCount(t, f.paths[held]))
}

func blockConcernRootOpen(t *testing.T, fs billy.Filesystem, root string) func() {
	t.Helper()
	fifo := filepath.Join(root, "blocked-open")
	require.NoError(t, unix.Mkfifo(fifo, 0o600))
	opened := make(chan error, 1)
	thread := make(chan int, 1)
	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		thread <- unix.Gettid()
		file, err := fs.Open("blocked-open")
		if err == nil {
			err = file.Close()
		}
		opened <- err
	}()
	var unblock sync.Once
	releaseOpen := func() {
		unblock.Do(func() {
			file, err := os.OpenFile(fifo, os.O_RDWR|unix.O_NONBLOCK, 0)
			require.NoError(t, err)
			require.NoError(t, <-opened, "the original blocked open and its file close must finish")
			require.NoError(t, file.Close())
		})
	}
	t.Cleanup(releaseOpen)
	// Observe the real openat2 syscall before cancellation; a timer or a
	// goroutine-start signal alone would not establish the held root lock.
	threadPath := filepath.Join("/proc/self/task", strconv.Itoa(<-thread), "syscall")
	require.Eventually(t, func() bool {
		state, err := os.ReadFile(threadPath)
		if err != nil {
			return false
		}
		fields := strings.Fields(string(state))

		return len(fields) > 0 && fields[0] == strconv.Itoa(unix.SYS_OPENAT2)
	}, time.Second, time.Millisecond, "the owned FIFO must hold a real chroot operation")

	return releaseOpen
}

func TestNetworkReleasePreservesCompletedCleanupError(t *testing.T) { //nolint:paralleltest // shares process-wide telemetry with real proxy fixtures
	f := newConcernFixture(t, nfscfg.Config{}, nil)
	originalErr := errors.New("original close failed")
	failed := &mountRetirement{done: make(chan struct{}), err: originalErr}
	close(failed.done)
	f.proxy.retirement.owners[f.owner] = failed
	require.Same(t, failed, f.proxy.retirement.retire(t.Context(), f.owner))
	require.Same(t, failed, f.proxy.retirement.retire(t.Context(), f.owner))
	require.Equal(t, 1, f.proxy.retirement.count(), "a terminal error must stay observable instead of becoming a successful retry")
	sbx, err := f.sandboxes.GetByHostPort("127.0.0.1:1")
	require.NoError(t, err)
	for range 2 {
		require.NoError(t, f.proxy.retirement.OnNetworkRelease(t.Context(), sbx))
		require.ErrorIs(t, failed.err, originalErr, "descriptor cleanup error remains independently owned")
	}
	// This control injects only the already-completed error result. There is
	// no physical root or worker to retire when the fixture releases its route.
	delete(f.proxy.retirement.owners, f.owner)
}

type failedReleaseSubscriber struct{ err error }

func (failedReleaseSubscriber) OnInsert(context.Context, *sandbox.Sandbox)   {}
func (failedReleaseSubscriber) OnStopping(context.Context, *sandbox.Sandbox) {}
func (s failedReleaseSubscriber) OnNetworkRelease(context.Context, *sandbox.Sandbox) error {
	return s.err
}

// Successful reuse needs a real firewall and is covered for both pool versions
// by TestNetworkPoolNativeNFSReleaseBeforeMountDrain. This fixture has no native
// network: a failed subscriber must retain its slot before recycling is attempted.
func TestNetworkPoolRetainsSlotForFailedSubscriber(t *testing.T) { //nolint:paralleltest // real proxy owner with one isolated virtual clock
	synctest.Test(t, func(t *testing.T) {
		entered := make(chan struct{})
		proceed := make(chan struct{})
		var builder *preOpenConcernBuilder
		f := newConcernFixture(t, nfscfg.Config{}, func(inner *chrooted.Builder) chroot.Builder {
			builder = &preOpenConcernBuilder{inner: inner, entered: entered, proceed: proceed, allocated: make(chan *chrooted.Chrooted, 1)}

			return builder
		})
		builder.blocked = f.ids[1]
		var unblock sync.Once
		releaseMount := func() { unblock.Do(func() { close(proceed) }) }
		t.Cleanup(releaseMount)
		first := f.mount(t)
		f.proxy.server.Handler.ToHandle(t.Context(), first, nil)
		mountDone := make(chan servernfs.MountStatus, 1)
		go func() {
			status, _, _ := f.proxy.server.Handler.Mount(t.Context(), f.conn, servernfs.MountRequest{Dirpath: []byte(f.mountPath("second"))})
			mountDone <- status
		}()
		<-entered
		boom := errors.New("another subscriber still owns the slot")
		f.sandboxes.Subscribe(failedReleaseSubscriber{err: boom})
		pool, storage := newEmptyConcernPool(t)
		slot, err := network.NewSlot("nfs-late-return", 1, network.Config{}, network.NewNoopEgressProxy())
		require.NoError(t, err)
		slot.HostIP = net.IPv4(127, 0, 0, 1)
		release := f.sandboxes.NetworkRelease(f.owner.SandboxID, f.owner.LifecycleID)
		notified := make(chan error, 3)
		require.NoError(t, pool.ReturnAsync(t.Context(), slot, func(ctx context.Context, ip string) error {
			err := release(ctx, ip)
			notified <- err

			return err
		}, 0))
		synctest.Wait()
		require.ErrorIs(t, <-notified, boom)
		require.Equal(t, 1, f.proxy.retirement.count())
		require.Zero(t, storage.released.Load())
		require.Equal(t, 1, concernRootFDCount(t, f.paths[0]), "the old descriptor is still owned while the slot is retained")
		releaseMount()
		require.Equal(t, servernfs.MountStatusErrAcces, <-mountDone)
		synctest.Wait()
		require.Zero(t, f.proxy.retirement.count())
		require.Zero(t, concernRootFDCount(t, f.paths[0]))
		require.Zero(t, concernRootFDCount(t, f.paths[1]))
		err = pool.Close(t.Context())
		require.ErrorIs(t, err, network.ErrSlotRetained)
		require.ErrorIs(t, err, boom)
		require.Equal(t, int64(1), storage.released.Load(), "Close must remove even a previously retained slot")
	})
}
