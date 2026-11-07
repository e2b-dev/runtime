//go:build linux

package nbd

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Merovius/nbd/nbdnl"
	"github.com/google/uuid"
	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"golang.org/x/sys/unix"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/nbd/testutils"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

const ioctlTestDeviceSize = 8 << 20

func ioctlTestFlags(t *testing.T, connections int) *featureflags.Client {
	t.Helper()
	td := ldtestdata.DataSource()
	td.Update(td.Flag(featureflags.NBDAttachViaIoctlFlag.Key()).VariationForAll(true))
	td.Update(td.Flag(featureflags.NBDConnectionsPerDevice.Key()).ValueForAll(ldvalue.Int(connections)))
	ff, err := featureflags.NewClientWithDatasource(td)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ff.Close(context.WithoutCancel(t.Context())) })

	return ff
}

func ioctlTestBackend(t *testing.T) block.Device {
	t.Helper()
	zero, err := testutils.NewZeroDevice(ioctlTestDeviceSize, 4096)
	require.NoError(t, err)
	cache, err := block.NewCache(ioctlTestDeviceSize, 4096, os.TempDir()+"/nbd-ioctl-"+uuid.NewString(), false)
	require.NoError(t, err)
	overlay := block.NewOverlay(zero, cache)
	t.Cleanup(func() { _ = overlay.Close() })

	return overlay
}

// openIoctlMount attaches a mount through the ioctl interface; closed reports
// whether the caller closed it already.
func openIoctlMount(t *testing.T, ff *featureflags.Client, unmasked bool) (*DirectPathMount, uint32, *bool) {
	t.Helper()
	if os.Geteuid() != 0 {
		t.Skip("kernel NBD requires root")
	}
	pool := newPartitionedPool(t)
	mnt := NewDirectPathMount(ioctlTestBackend(t), pool, ff, logger.L())

	return openIoctlMountOn(t, mnt, unmasked)
}

func openIoctlMountOn(t *testing.T, mnt *DirectPathMount, unmasked bool) (*DirectPathMount, uint32, *bool) {
	t.Helper()
	mnt.ioctlUnmasked = unmasked
	idx, err := mnt.Open(t.Context())
	require.NoError(t, err)
	closed := new(bool)
	t.Cleanup(func() {
		if *closed {
			return
		}
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 30*time.Second)
		defer cancel()
		assert.NoError(t, mnt.Close(ctx))
	})

	return mnt, idx, closed
}

// singleSlotPool is a pool with one known free device buffered and no feeder,
// so a test can say which device an Open takes and whether its slot came back.
// The slot channel is closed behind that device: an Open that retries fails
// at once in GetDevice instead of waiting there until the go test timeout.
func singleSlotPool(t *testing.T) (*DevicePool, DeviceSlot) {
	t.Helper()
	pool := poolInWindow(t)
	slot, err := pool.getFreeDeviceSlot()
	require.NoError(t, err)
	pool.slots <- *slot
	close(pool.slots)

	return pool, *slot
}

func slotHeld(pool *DevicePool, slot DeviceSlot) bool {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	return pool.usedSlots.Test(uint(slot))
}

func writeReadDevice(t *testing.T, idx uint32, seed byte) {
	t.Helper()
	f, err := os.OpenFile(GetDevicePath(idx), os.O_RDWR|unix.O_DIRECT, 0)
	require.NoError(t, err)
	defer f.Close()
	buf := make([]byte, 64*1024)
	for i := range buf {
		buf[i] = seed + byte(i%7)
	}
	_, err = f.WriteAt(buf, 1<<20)
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	got := make([]byte, len(buf))
	_, err = f.ReadAt(got, 1<<20)
	require.NoError(t, err)
	require.Equal(t, buf, got)
}

func TestIoctlAttachServesIO(t *testing.T) {
	t.Parallel()
	mnt, idx, closed := openIoctlMount(t, ioctlTestFlags(t, 1), false)
	require.NotNil(t, mnt.ioctlSession, "the flag selects the ioctl interface")
	require.True(t, ioctlDeviceStarted(idx))
	writeReadDevice(t, idx, 1)
	got := make([]byte, 4096)
	_, err := mnt.Backend.ReadAt(t.Context(), got, 1<<20)
	require.NoError(t, err)
	require.Equal(t, byte(1), got[0])

	*closed = true
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	require.NoError(t, mnt.Close(ctx))
	require.False(t, ioctlDeviceConnected(idx), "the device reads as free after Close")
	select {
	case <-mnt.ioctlSession.done:
	default:
		t.Fatal("the NBD_DO_IT thread did not return")
	}
	require.True(t, mnt.ioctlSession.disconnectRequested.Load(), "a Close is an end this process asked for, not one the kernel chose")
}

// An attach whose caller goes away while the device is being configured ends
// its session at once and leaves the device free, instead of parking a thread
// nobody owns or waiting out the disconnect bound.
func TestIoctlAttachAbandonedAfterConfigureEndsTheSession(t *testing.T) {
	t.Parallel()
	if os.Geteuid() != 0 {
		t.Skip("kernel NBD requires root")
	}
	pool := poolInWindow(t)
	socks, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	require.NoError(t, err)
	client, server := os.NewFile(uintptr(socks[0]), "client"), os.NewFile(uintptr(socks[1]), "server")
	t.Cleanup(func() { client.Close(); server.Close() })

	var abandoned abandonedSessions
	var elapsed time.Duration
	var idx DeviceSlot
	for {
		idx = freeSlotInWindow(t, pool)
		ctx, cancel := context.WithCancel(t.Context())
		// The hook cancels on a device this attach has claimed; a lost race
		// never reaches it and is retried with the next free slot.
		cfg := ioctlAttachConfig{
			index: idx, socks: []*os.File{client}, size: ioctlTestDeviceSize, blockSize: 4096,
			timeout: time.Minute, flags: uint64(nbdnl.FlagHasFlags), maxDevices: pool.MaxDevices(),
			afterConfigure: func(uint32) { cancel() },
			abandoned:      abandoned.take,
		}
		start := time.Now()
		_, err = attachIoctl(ctx, cfg, logger.L(), false)
		elapsed = time.Since(start)
		cancel()
		if errors.Is(err, errIoctlDeviceTaken) {
			continue
		}
		require.ErrorIs(t, err, context.Canceled)

		break
	}
	require.Less(t, elapsed, 5*time.Second, "the abandoned caller is answered without waiting out the disconnect bound")
	// The caller closes its sockets on the way out, which ends a session
	// that finished configuring after the caller left.
	require.NoError(t, server.Close())
	abandoned.join(t)
	require.Eventually(t, func() bool { return !ioctlDeviceConnected(idx) }, 5*time.Second, 10*time.Millisecond, "the abandoned device is free")
	require.NoError(t, pool.ReleaseDevice(t.Context(), idx, WithInfiniteRetry(), WithTimeout(10*time.Second)))
}

// A caller that leaves before the first NBD_SET_SOCK and closes its socket
// files, as Open does, does not hand the kernel a reused descriptor: the
// session owns duplicates. A fresh socket pair takes the closed numbers, and
// the session must end on its own sockets' closed peers rather than live on
// the new pair.
func TestIoctlAttachCancelledBeforeTheClaimUsesItsOwnSockets(t *testing.T) { //nolint:paralleltest // relies on descriptor numbers being reused, which parallel tests' opens would defeat
	if os.Geteuid() != 0 {
		t.Skip("kernel NBD requires root")
	}
	pool, slot := singleSlotPool(t)
	socks, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	require.NoError(t, err)
	client, server := os.NewFile(uintptr(socks[0]), "client"), os.NewFile(uintptr(socks[1]), "server")
	release := make(chan struct{})
	releaseOnce := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseOnce)
	entered := make(chan struct{})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() {
		<-entered
		cancel()
	}()
	var abandoned abandonedSessions
	sess, err := attachIoctl(ctx, ioctlAttachConfig{
		index: slot, socks: []*os.File{client}, size: ioctlTestDeviceSize, blockSize: 4096, timeout: time.Minute, maxDevices: pool.MaxDevices(),
		beforeConfigure: func(uint32) {
			close(entered)
			<-release
		},
		abandoned: abandoned.take,
	}, logger.L(), false)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, sess)

	// The caller's way out: its files close, and new ones take their numbers.
	require.NoError(t, client.Close())
	require.NoError(t, server.Close())
	fresh, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	require.NoError(t, err)
	t.Cleanup(func() { unix.Close(fresh[0]); unix.Close(fresh[1]) })
	require.Equal(t, socks[0], fresh[0], "the test needs the closed descriptor number reused")

	releaseOnce()
	abandoned.join(t)
	require.Eventually(t, func() bool { return !ioctlDeviceConnected(slot) }, 10*time.Second, 10*time.Millisecond, "the session must end on its own sockets, not live on the fresh pair")
	require.NoError(t, pool.ReleaseDevice(t.Context(), slot, WithInfiniteRetry(), WithTimeout(10*time.Second)))
}

// abandonedSessions collects the sessions an attach left behind and waits
// for their end, so a test releases its device only once it is really free:
// the pool reads a session that has not entered NBD_DO_IT yet as free.
type abandonedSessions struct {
	mu   sync.Mutex
	sess []*ioctlSession
}

func (a *abandonedSessions) take(s *ioctlSession) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.sess = append(a.sess, s)
}

func (a *abandonedSessions) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()

	return len(a.sess)
}

func (a *abandonedSessions) join(t *testing.T) {
	t.Helper()
	a.mu.Lock()
	sess := a.sess
	a.mu.Unlock()
	for _, s := range sess {
		select {
		case <-s.done:
		case <-time.After(disconnectTimeout):
			t.Fatalf("the abandoned session on device %d never ended", s.index)
		}
	}
}

// poolInWindow builds a one-slot pool confined to its own device window and
// closes it on cleanup. Unlike singleSlotPool it pushes no slot: its callers
// claim a fresh in-window device per attempt through freeSlotInWindow.
func poolInWindow(t *testing.T) *DevicePool {
	t.Helper()
	pool, err := NewDevicePool(1)
	require.NoError(t, err)
	releasePadding := claimSlotWindow(t, pool)
	t.Cleanup(func() {
		releasePadding()
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 30*time.Second)
		defer cancel()
		assert.NoError(t, pool.Close(ctx))
	})

	return pool
}

// freeSlotInWindow hands out a free device from the pool's window and marks it
// used, so successive calls give distinct devices: an attach that loses the
// race for one (a parallel test or the smoke-test binary grabs it between the
// sysfs check and NBD_SET_SOCK) retries on the next rather than failing, as
// production Open does with a feeder.
func freeSlotInWindow(t *testing.T, pool *DevicePool) DeviceSlot {
	t.Helper()
	slot, err := pool.getFreeDeviceSlot()
	require.NoError(t, err)
	pool.mu.Lock()
	pool.usedSlots.Set(uint(*slot))
	pool.mu.Unlock()

	return *slot
}

// A caller that goes away while the configuration is still blocked gets its
// answer at once; the session is ended once the configuration finishes. The
// blocking hook runs only on a claimed device, so a lost device race is
// retried with the next free slot rather than mistaken for the cancellation.
func TestIoctlAttachCancelledWhileConfigurationBlocksReturns(t *testing.T) {
	t.Parallel()
	if os.Geteuid() != 0 {
		t.Skip("kernel NBD requires root")
	}
	pool := poolInWindow(t)
	socks, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	require.NoError(t, err)
	client, server := os.NewFile(uintptr(socks[0]), "client"), os.NewFile(uintptr(socks[1]), "server")
	t.Cleanup(func() { client.Close(); server.Close() })
	release := make(chan struct{})
	releaseOnce := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseOnce)

	var abandoned abandonedSessions
	var elapsed time.Duration
	var slot DeviceSlot
	for {
		slot = freeSlotInWindow(t, pool)
		ctx, cancel := context.WithCancel(t.Context())
		entered := make(chan struct{})
		cancelled := make(chan struct{})
		go func() {
			defer close(cancelled)
			select {
			case <-entered:
				cancel()
			case <-ctx.Done():
			}
		}()
		start := time.Now()
		sess, err := attachIoctl(ctx, ioctlAttachConfig{
			index: slot, socks: []*os.File{client}, size: ioctlTestDeviceSize, blockSize: 4096, timeout: time.Minute, maxDevices: pool.MaxDevices(),
			// The hook stands in for a device open or ioctl blocked on the
			// device's lock; it runs only once this attach has claimed the device.
			afterConfigure: func(uint32) {
				close(entered)
				<-release
			},
			abandoned: abandoned.take,
		}, logger.L(), false)
		elapsed = time.Since(start)
		cancel()
		<-cancelled
		if errors.Is(err, errIoctlDeviceTaken) {
			continue
		}
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, sess)

		break
	}
	require.Less(t, elapsed, 3*time.Second, "the attach must answer the cancelled caller while the configuration still blocks")
	// Without the session in hand, join would return at once and the slot
	// would be released while the session still configures its device.
	require.Equal(t, 1, abandoned.count(), "the attach hands over the session it left configuring before it returns")

	releaseOnce()
	// The caller closes its sockets on the way out; that is what ends the
	// session the kernel still holds.
	require.NoError(t, server.Close())
	abandoned.join(t)
	require.Eventually(t, func() bool { return !ioctlDeviceConnected(slot) }, 10*time.Second, 10*time.Millisecond, "the session must be ended once its configuration finishes")
	require.NoError(t, pool.ReleaseDevice(t.Context(), slot, WithInfiniteRetry(), WithTimeout(10*time.Second)))
}

// Open with a caller that goes away while the device is being configured
// reports no device and gives the slot back, which the netlink attach never
// had to do because its connect ignores the context.
func TestIoctlOpenCancelledDuringConfigureReleasesTheSlot(t *testing.T) {
	t.Parallel()
	if os.Geteuid() != 0 {
		t.Skip("kernel NBD requires root")
	}
	// A pool whose feeder stops inside the attach: afterwards the only slots
	// taken in its window are the one buffered and the one under test.
	pool := poolInWindow(t)
	// The feeder claims a slot before it looks at stop, so it is stopped and
	// joined inside the hook, before the unwind releases the slot under test.
	stopAndJoinFeeder := feedSlots(t, pool)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	mnt := NewDirectPathMount(ioctlTestBackend(t), pool, ioctlTestFlags(t, 1), logger.L())
	configured := DeviceSlot(math.MaxUint32)
	mnt.ioctlAfterConfigure = func(deviceIndex uint32) {
		configured = deviceIndex
		stopAndJoinFeeder()
		cancel()
	}
	var abandoned abandonedSessions
	mnt.ioctlAbandoned = abandoned.take
	// A cancelled ioctl attach hands the device to its abandoned session and
	// gives the slot back with Disown; it must not spin-release a device the
	// session still holds (the 30 s release that stranded the slot before).
	released := false
	mnt.releaseDeviceFn = func(ctx context.Context, deviceIndex uint32) error {
		released = true

		return pool.ReleaseDevice(ctx, deviceIndex, WithInfiniteRetry(), WithTimeout(10*time.Second))
	}
	idx, err := mnt.Open(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, released, "a cancelled ioctl attach must disown its slot, not release it")
	require.Equal(t, uint32(math.MaxUint32), idx, "Open must keep reporting that it owns no device")
	require.NotEqual(t, DeviceSlot(math.MaxUint32), configured, "no configured device was observed, so nothing was tested")
	// The session may still be entering NBD_DO_IT when Open returns; it ends
	// on the sockets Open closed on its way out.
	abandoned.join(t)
	require.Eventually(t, func() bool { return !ioctlDeviceConnected(configured) }, 5*time.Second, 10*time.Millisecond, "nbd%d is still connected after a cancelled Open", configured)
	require.Eventually(t, func() bool {
		pool.mu.Lock()
		defer pool.mu.Unlock()

		return !pool.usedSlots.Test(uint(configured))
	}, 5*time.Second, 10*time.Millisecond, "slot %d was never released back to the pool", configured)
	assert.NoError(t, mnt.Close(t.Context()), "closing after a failed Open stays harmless")
}

// A device another session holds refuses a second attach with EBUSY and
// keeps serving: the losing side changes nothing on a device it did not claim.
func TestIoctlAttachLosingTheDeviceLeavesItsOwnerAlone(t *testing.T) {
	t.Parallel()
	mnt, idx, _ := openIoctlMount(t, ioctlTestFlags(t, 1), false)
	socks, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	require.NoError(t, err)
	client, server := os.NewFile(uintptr(socks[0]), "client"), os.NewFile(uintptr(socks[1]), "server")
	t.Cleanup(func() { client.Close(); server.Close() })

	_, err = attachIoctl(t.Context(), ioctlAttachConfig{
		index: idx, socks: []*os.File{client}, size: 2 * ioctlTestDeviceSize, blockSize: 4096,
		timeout: time.Minute, flags: uint64(nbdnl.FlagHasFlags), maxDevices: mnt.devicePool.MaxDevices(),
	}, logger.L(), false)
	require.ErrorIs(t, err, unix.EBUSY)
	require.ErrorIs(t, err, errIoctlDeviceTaken, "a lost race is retryable")
	select {
	case <-mnt.ioctlSession.done:
		t.Fatalf("the owner's session ended: %v", mnt.ioctlSession.exitErr)
	default:
	}
	require.Equal(t, strconv.Itoa(ioctlTestDeviceSize/512), sysfsAttr(t, idx, "size"), "the owner's geometry is untouched")
	writeReadDevice(t, idx, 4)
}

// A device a netlink attach holds refuses the ioctl claim with EINVAL rather
// than EBUSY. Open treats it as a lost race, gives the slot up and takes the
// next device, which is what the netlink attach does on EBUSY, and the
// netlink owner keeps serving. One pool and one slot window serve both.
func TestIoctlOpenRetriesPastANetlinkOwnedDevice(t *testing.T) {
	t.Parallel()
	if os.Geteuid() != 0 {
		t.Skip("kernel NBD requires root")
	}
	pool := poolInWindow(t)
	netlinkFlags, err := featureflags.NewClient("", "", "")
	require.NoError(t, err)
	owner := NewDirectPathMount(ioctlTestBackend(t), pool, netlinkFlags, logger.L())
	// A failed netlink connect retries on a fresh slot. The ioctl Open below
	// still needs the pool when this feeder runs dry.
	stopOwnerFeeder := feedSlotsKeepOpen(t, pool)
	ownerCtx, ownerCancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer ownerCancel()
	ownerIdx, err := owner.Open(ownerCtx)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, owner.Close(context.WithoutCancel(t.Context()))) })
	stopOwnerFeeder()
	require.Nil(t, owner.ioctlSession, "the owner attached through netlink")
	// The slot the owner's feeder left buffered goes back unused, so the
	// owner's device can be offered first below.
	select {
	case spare := <-pool.slots:
		pool.mu.Lock()
		pool.usedSlots.Clear(uint(spare))
		pool.mu.Unlock()
	default:
	}

	// The owner's device is offered again, ahead of what the feeder finds.
	pool.slots <- ownerIdx
	stopFeeder := feedSlots(t, pool)

	mnt := NewDirectPathMount(ioctlTestBackend(t), pool, ioctlTestFlags(t, 1), logger.L())
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	idx, err := mnt.Open(ctx)
	require.NoError(t, err, "the lost race must not fail the attach")
	t.Cleanup(func() { assert.NoError(t, mnt.Close(context.WithoutCancel(t.Context()))) })
	stopFeeder()
	require.NotEqual(t, ownerIdx, idx, "the attach moved to a spare device")
	require.NotNil(t, mnt.ioctlSession)
	require.False(t, slotHeld(pool, ownerIdx), "a lost race must not keep the slot out of the pool")
	// The owner still holds the device through this pool; its release
	// clears the bit again, and nothing offers the device meanwhile.
	pool.mu.Lock()
	pool.usedSlots.Set(uint(ownerIdx))
	pool.mu.Unlock()
	writeReadDevice(t, idx, 5)
	writeReadDevice(t, ownerIdx, 6)
}

func sysfsAttr(t *testing.T, idx uint32, attr string) string {
	t.Helper()
	b, err := os.ReadFile(fmt.Sprintf("%s/nbd%d/%s", sysBlockDir, idx, attr))
	require.NoError(t, err)

	return strings.TrimSpace(string(b))
}

func TestIoctlAttachRejectsInvalidGeometry(t *testing.T) {
	t.Parallel()
	socks, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	require.NoError(t, err)
	client := os.NewFile(uintptr(socks[0]), "client")
	t.Cleanup(func() { client.Close(); unix.Close(socks[1]) })
	_, err = attachIoctl(t.Context(), ioctlAttachConfig{index: 0, socks: []*os.File{client}, size: 4097, blockSize: 4096, maxDevices: 16}, logger.L(), false)
	require.ErrorIs(t, err, errIoctlInvalidGeometry)
	_, err = attachIoctl(t.Context(), ioctlAttachConfig{index: 0, size: 4096, blockSize: 4096, maxDevices: 16}, logger.L(), false)
	require.ErrorIs(t, err, errIoctlInvalidConfig, "no sockets")
	_, err = attachIoctl(t.Context(), ioctlAttachConfig{index: 0, socks: []*os.File{client}, size: 0, blockSize: 4096, maxDevices: 16}, logger.L(), false)
	require.ErrorIs(t, err, errIoctlInvalidConfig, "zero size")
}

// Every SET_SOCK must come from the thread that enters NBD_DO_IT; two
// connections pin that the session runs them on one thread.
func TestIoctlAttachMultipleConnections(t *testing.T) {
	t.Parallel()
	mnt, idx, _ := openIoctlMount(t, ioctlTestFlags(t, 2), false)
	require.Len(t, mnt.socksClient, 2)
	writeReadDevice(t, idx, 2)
}

// The parked thread is signalled the way the runtime and the process do it:
// SIGURG by tgkill and SIGCHLD process-wide. With the mask the device keeps
// serving; without it the driver ends the session on the first signal.
func TestIoctlSessionSurvivesSignals(t *testing.T) {
	t.Parallel()
	t.Run("masked", func(t *testing.T) {
		t.Parallel()
		mnt, idx, _ := openIoctlMount(t, ioctlTestFlags(t, 1), false)
		tid := mnt.ioctlSession.tid
		require.NotZero(t, tid)
		for range 200 {
			require.NoError(t, unix.Tgkill(os.Getpid(), tid, unix.SIGURG))
			require.NoError(t, unix.Tgkill(os.Getpid(), tid, unix.SIGCHLD))
		}
		time.Sleep(50 * time.Millisecond)
		select {
		case <-mnt.ioctlSession.done:
			t.Fatalf("the session ended under signals: %v", mnt.ioctlSession.exitErr)
		default:
		}
		writeReadDevice(t, idx, 3)
	})
	t.Run("unmasked control", func(t *testing.T) {
		t.Parallel()
		mnt, _, _ := openIoctlMount(t, ioctlTestFlags(t, 1), true)
		require.NoError(t, unix.Tgkill(os.Getpid(), mnt.ioctlSession.tid, unix.SIGURG))
		select {
		case <-mnt.ioctlSession.done:
			require.ErrorIs(t, mnt.ioctlSession.exitErr, unix.EBUSY, "the restarted NBD_DO_IT finds the pid set")
		case <-time.After(5 * time.Second):
			t.Fatal("an unmasked thread should lose its session to one signal")
		}
	})
}

// A backend that dies ends the session from the kernel's side; Close still
// tears the mount down, frees the device and gives the slot back.
func TestIoctlSessionEndsWhenTheServerCloses(t *testing.T) {
	t.Parallel()
	if os.Geteuid() != 0 {
		t.Skip("kernel NBD requires root")
	}
	pool, slot := singleSlotPool(t)
	mnt, idx, closed := openIoctlMountOn(t, NewDirectPathMount(ioctlTestBackend(t), pool, ioctlTestFlags(t, 1), logger.L()), false)
	require.Equal(t, slot, idx)
	for _, s := range mnt.socksServer {
		require.NoError(t, s.Close())
	}
	select {
	case <-mnt.ioctlSession.done:
	case <-time.After(10 * time.Second):
		t.Fatal("NBD_DO_IT did not return after the server side closed")
	}
	require.True(t, mnt.ioctlSession.endedByKernel.Load())

	*closed = true
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	err := mnt.Close(ctx)
	// The flush fails on a dead device; the device and its slot must still be released.
	if err != nil {
		require.NotContains(t, err.Error(), "releasing", "close after server death: %v", err)
	}
	require.False(t, ioctlDeviceConnected(idx))
	require.False(t, slotHeld(pool, slot), "the slot of the dead device goes back to the pool")
}

func TestIoctlAttachRejectsDeadconnTimeout(t *testing.T) {
	t.Parallel()
	if os.Geteuid() != 0 {
		t.Skip("kernel NBD requires root")
	}
	pool, slot := singleSlotPool(t)
	mnt := NewDirectPathMount(ioctlTestBackend(t), pool, ioctlTestFlags(t, 1), logger.L(), WithDeadconnTimeout(time.Second))
	start := time.Now()
	_, err := mnt.Open(t.Context())
	require.ErrorIs(t, err, errIoctlDeadconnTimeout)
	require.Less(t, time.Since(start), 5*time.Second, "a refused configuration is final, not retried")
	require.Eventually(t, func() bool { return !slotHeld(pool, slot) }, 5*time.Second, 10*time.Millisecond,
		"the slot the refused attach took goes back to the pool")
	require.False(t, ioctlDeviceConnected(slot))
}

// attachedSession is a session on a known device with a socket pair nobody
// serves; closing server ends it from the kernel's side.
func attachedSession(t *testing.T, cfg ioctlAttachConfig) (*ioctlSession, *os.File) {
	t.Helper()
	socks, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	require.NoError(t, err)
	client, server := os.NewFile(uintptr(socks[0]), "client"), os.NewFile(uintptr(socks[1]), "server")
	t.Cleanup(func() { client.Close(); server.Close() })
	cfg.socks = []*os.File{client}
	if cfg.size == 0 {
		cfg.size, cfg.blockSize = ioctlTestDeviceSize, 4096
	}
	if cfg.timeout == 0 {
		cfg.timeout = time.Minute
	}
	cfg.flags = uint64(nbdnl.FlagHasFlags)
	sess, err := attachIoctl(t.Context(), cfg, logger.L(), false)
	require.NoError(t, err)

	return sess, server
}

// A disconnect request the driver never answers does not hold the caller past
// its bound: the socket shutdown ends the session anyway.
func TestIoctlDisconnectBoundHoldsWhenTheRequestHangs(t *testing.T) {
	t.Parallel()
	if os.Geteuid() != 0 {
		t.Skip("kernel NBD requires root")
	}
	pool, slot := singleSlotPool(t)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	sess, _ := attachedSession(t, ioctlAttachConfig{
		index: slot, maxDevices: pool.MaxDevices(),
		disconnectIoctl: func(int) error {
			<-release

			return nil
		},
	})
	start := time.Now()
	err := sess.disconnect(t.Context(), 3*time.Second)
	require.Less(t, time.Since(start), 10*time.Second, "disconnect must return within its bound")
	// The hung request keeps its duplicate descriptor, so the device reads as
	// connected until it answers; the bound reports that rather than waiting.
	require.Error(t, err)
	select {
	case <-sess.done:
	case <-time.After(5 * time.Second):
		t.Fatal("the socket shutdown must end NBD_DO_IT although the request never answered")
	}
	release <- struct{}{}
	require.Eventually(t, func() bool { return !ioctlDeviceConnected(slot) }, 10*time.Second, 10*time.Millisecond)
	require.NoError(t, pool.ReleaseDevice(t.Context(), slot, WithInfiniteRetry(), WithTimeout(10*time.Second)))
}

// A descriptor close that stalls in the kernel does not block a disconnect
// caller on the session's lock; the caller gets its bound.
func TestIoctlDisconnectBoundHoldsWhenTheDescriptorCloseHangs(t *testing.T) {
	t.Parallel()
	if os.Geteuid() != 0 {
		t.Skip("kernel NBD requires root")
	}
	pool, slot := singleSlotPool(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	sess, server := attachedSession(t, ioctlAttachConfig{
		index: slot, maxDevices: pool.MaxDevices(),
		closeCtl: func(fd int) error {
			close(entered)
			<-release

			return unix.Close(fd)
		},
	})
	require.NoError(t, server.Close())
	<-entered
	start := time.Now()
	err := sess.disconnect(t.Context(), 2*time.Second)
	require.Less(t, time.Since(start), 10*time.Second, "disconnect must return within its bound")
	require.Error(t, err, "the session cannot finish while its close hangs")
	release <- struct{}{}
	<-sess.done
	require.Eventually(t, func() bool { return !ioctlDeviceConnected(slot) }, 10*time.Second, 10*time.Millisecond)
	require.NoError(t, pool.ReleaseDevice(t.Context(), slot, WithInfiniteRetry(), WithTimeout(10*time.Second)))
}

// closeWithHungSession opens a mount through the ioctl interface, with before
// setting the hooks that hold part of its teardown and after running on the
// attached mount, closes it under a two-second disconnect bound and asserts
// that Close returned with the slot still claimed.
func closeWithHungSession(t *testing.T, pool *DevicePool, slot DeviceSlot, before, after func(mnt *DirectPathMount)) *DirectPathMount {
	t.Helper()
	mnt := NewDirectPathMount(ioctlTestBackend(t), pool, ioctlTestFlags(t, 1), logger.L())
	mnt.deferredReleaseDone = make(chan error, 1)
	if before != nil {
		before(mnt)
	}
	mnt.disconnectDeviceFn = func(ctx context.Context, _ uint32) error {
		return mnt.ioctlSession.disconnect(ctx, 2*time.Second)
	}
	idx, err := mnt.Open(t.Context())
	require.NoError(t, err)
	require.Equal(t, slot, idx)
	if after != nil {
		after(mnt)
	}

	start := time.Now()
	err = mnt.Close(t.Context())
	require.Less(t, time.Since(start), 15*time.Second, "Close must return while the session's teardown hangs")
	require.ErrorContains(t, err, "slot release deferred")
	require.True(t, slotHeld(pool, slot), "a slot whose device is still in use must stay claimed")

	return mnt
}

// holdDeviceDescriptor takes a private duplicate of the session's device
// descriptor, as a disconnect request does, and returns its close. The
// device reads as in use until that duplicate is closed, whatever the
// session itself does.
func holdDeviceDescriptor(t *testing.T, sess *ioctlSession) func() {
	t.Helper()
	sess.ctlMu.Lock()
	defer sess.ctlMu.Unlock()
	require.GreaterOrEqual(t, sess.ctl, 0, "the session must still hold its descriptor")
	fd, err := unix.FcntlInt(uintptr(sess.ctl), unix.F_DUPFD_CLOEXEC, 0)
	require.NoError(t, err)
	release := sync.OnceFunc(func() { _ = unix.Close(fd) })
	t.Cleanup(release)

	return release
}

// A Close whose ioctl disconnect runs out of time returns instead of waiting
// on the slot: the device reads as in use until the session's hung close
// finishes, and the slot comes back once it does.
func TestCloseReturnsWhileTheSessionCloseHangs(t *testing.T) {
	t.Parallel()
	if os.Geteuid() != 0 {
		t.Skip("kernel NBD requires root")
	}
	pool, slot := singleSlotPool(t)
	release := make(chan struct{})
	releaseOnce := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseOnce)
	mnt := closeWithHungSession(t, pool, slot, func(mnt *DirectPathMount) {
		mnt.ioctlCloseCtl = func(fd int) error {
			<-release

			return unix.Close(fd)
		}
	}, nil)
	select {
	case <-mnt.ioctlSession.done:
		t.Fatal("the held close must keep the session's cleanup from finishing")
	default:
	}

	releaseOnce()
	require.Eventually(t, func() bool { return !slotHeld(pool, slot) }, 10*time.Second, 10*time.Millisecond, "slot %d was never released", slot)
	require.Eventually(t, func() bool { return !ioctlDeviceConnected(slot) }, 10*time.Second, 10*time.Millisecond)
	require.NoError(t, <-mnt.deferredReleaseDone)
}

// A disconnect request that hangs keeps its private duplicate of the device
// open after the session's own cleanup has finished, so the device still
// reads as in use once done is closed; Close returns all the same. The
// duplicate is taken here, as the request takes it, so the scenario does not
// depend on the request reaching the descriptor before the cleanup does.
func TestCloseReturnsWhileADisconnectDuplicateIsHeld(t *testing.T) {
	t.Parallel()
	if os.Geteuid() != 0 {
		t.Skip("kernel NBD requires root")
	}
	pool, slot := singleSlotPool(t)
	var release func()
	mnt := closeWithHungSession(t, pool, slot, nil, func(mnt *DirectPathMount) {
		release = holdDeviceDescriptor(t, mnt.ioctlSession)
	})
	select {
	case <-mnt.ioctlSession.done:
	case <-time.After(5 * time.Second):
		t.Fatal("the socket shutdown must end the session although a duplicate is held")
	}
	require.True(t, ioctlDeviceConnected(slot), "the held duplicate must keep the device in use")

	release()
	require.Eventually(t, func() bool { return !slotHeld(pool, slot) }, 10*time.Second, 10*time.Millisecond, "slot %d was never released", slot)
	require.Eventually(t, func() bool { return !ioctlDeviceConnected(slot) }, 10*time.Second, 10*time.Millisecond)
	require.NoError(t, <-mnt.deferredReleaseDone)
}

// A deferred release that never finds the device free gives up at its
// deadline and leaves the slot claimed, so a hang cannot hold a goroutine
// for longer than the pool's own close would wait.
func TestDeferredReleaseGivesUpAtItsDeadline(t *testing.T) {
	t.Parallel()
	if os.Geteuid() != 0 {
		t.Skip("kernel NBD requires root")
	}
	pool, slot := singleSlotPool(t)
	mnt := closeWithHungSession(t, pool, slot, func(mnt *DirectPathMount) {
		mnt.deferredReleaseTimeout = time.Second
	}, func(mnt *DirectPathMount) {
		holdDeviceDescriptor(t, mnt.ioctlSession)
	})
	select {
	case err := <-mnt.deferredReleaseDone:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(10 * time.Second):
		t.Fatal("the deferred release must give up at its deadline")
	}
	require.True(t, slotHeld(pool, slot), "a slot whose device never freed must stay claimed")
	require.True(t, ioctlDeviceConnected(slot))
}

// A session whose NBD_DO_IT returned before the device read as started is
// reported as ended at once, without waiting for its cleanup or the start
// deadline.
func TestIoctlAttachReportsAnExitBeforeTheDeviceStarts(t *testing.T) {
	t.Parallel()
	if os.Geteuid() != 0 {
		t.Skip("kernel NBD requires root")
	}
	pool, slot := singleSlotPool(t)
	socks, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	require.NoError(t, err)
	client, server := os.NewFile(uintptr(socks[0]), "client"), os.NewFile(uintptr(socks[1]), "server")
	t.Cleanup(func() { client.Close(); server.Close() })
	release := make(chan struct{})
	releaseOnce := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseOnce)
	start := time.Now()
	sess, err := attachIoctl(t.Context(), ioctlAttachConfig{
		index: slot, socks: []*os.File{client}, size: ioctlTestDeviceSize, blockSize: 4096, timeout: time.Minute, maxDevices: pool.MaxDevices(),
		started:        func(uint32) bool { return false },
		afterConfigure: func(uint32) { _ = server.Close() },
		closeCtl: func(fd int) error {
			<-release

			return unix.Close(fd)
		},
	}, logger.L(), false)
	require.ErrorIs(t, err, errIoctlSessionEnded)
	require.Nil(t, sess)
	require.Less(t, time.Since(start), 5*time.Second, "the exit must be reported before the start deadline")

	releaseOnce()
	require.Eventually(t, func() bool { return !ioctlDeviceConnected(slot) }, 10*time.Second, 10*time.Millisecond)
	require.NoError(t, pool.ReleaseDevice(t.Context(), slot, WithInfiniteRetry(), WithTimeout(10*time.Second)))
}

// A session the kernel ends is classified when NBD_DO_IT returns, so a Close
// that arrives while its cleanup is still running cannot relabel it.
func TestIoctlKernelEndedSessionIsClassifiedBeforeCleanup(t *testing.T) {
	t.Parallel()
	if os.Geteuid() != 0 {
		t.Skip("kernel NBD requires root")
	}
	pool, slot := singleSlotPool(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	sess, server := attachedSession(t, ioctlAttachConfig{
		index: slot, maxDevices: pool.MaxDevices(),
		closeCtl: func(fd int) error {
			close(entered)
			<-release

			return unix.Close(fd)
		},
	})
	require.NoError(t, server.Close())
	<-entered
	sess.expectEnd()
	close(release)
	<-sess.done
	require.True(t, sess.endedByKernel.Load(), "the late Close must not hide a session the kernel ended")
	require.NoError(t, pool.ReleaseDevice(t.Context(), slot, WithInfiniteRetry(), WithTimeout(10*time.Second)))
}

// A caller that goes away while the connected device is being opened still
// gets no device, on either interface: the open can wait on the device. The
// pool is fed from its window until the device is attached: a netlink attach
// loses its device to any transient opener (udev probes the window's devices)
// and retries with the next slot, which a single pushed slot could not serve.
func TestOpenCancelledDuringDeviceOpenReturnsNoDevice(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		flags func(t *testing.T) *featureflags.Client
	}{
		{"netlink", func(t *testing.T) *featureflags.Client {
			t.Helper()
			ff, err := featureflags.NewClient("", "", "")
			require.NoError(t, err)

			return ff
		}},
		{"ioctl", func(t *testing.T) *featureflags.Client {
			t.Helper()

			return ioctlTestFlags(t, 1)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if os.Geteuid() != 0 {
				t.Skip("kernel NBD requires root")
			}
			pool := poolInWindow(t)
			stopAndJoinFeeder := feedSlots(t, pool)

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			mnt := NewDirectPathMount(ioctlTestBackend(t), pool, tc.flags(t), logger.L())
			// The device is this mount's from here on, so the feeder stops
			// before the unwind under test releases the slot.
			attached := DeviceSlot(math.MaxUint32)
			claim := func(deviceIndex uint32) {
				attached = deviceIndex
				stopAndJoinFeeder()
			}
			mnt.afterConnect = claim
			mnt.ioctlAfterConfigure = claim
			mnt.openDeviceFn = func(path string) (*os.File, error) {
				cancel()

				return os.Open(path)
			}
			idx, err := mnt.Open(ctx)
			require.ErrorIs(t, err, context.Canceled)
			require.Equal(t, uint32(math.MaxUint32), idx, "Open must keep reporting that it owns no device")
			require.NotEqual(t, DeviceSlot(math.MaxUint32), attached, "no attach was observed, so nothing was tested")
			require.Eventually(t, func() bool { return !slotHeld(pool, attached) }, 10*time.Second, 10*time.Millisecond, "slot %d was never released", attached)
			connected, err := isDeviceConnectedIn(sysBlockDir, attached)
			require.NoError(t, err)
			require.False(t, connected)
			assert.NoError(t, mnt.Close(t.Context()), "closing after a failed Open stays harmless")
		})
	}
}

// A session the kernel ends while the connected device is being opened is
// not handed back, whether its cleanup has finished or is still held.
func TestIoctlOpenRefusesASessionEndedDuringTheDeviceOpen(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		holdCleanup bool
	}{
		{name: "cleanup completed"},
		{name: "cleanup held", holdCleanup: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if os.Geteuid() != 0 {
				t.Skip("kernel NBD requires root")
			}
			pool, slot := singleSlotPool(t)
			mnt := NewDirectPathMount(ioctlTestBackend(t), pool, ioctlTestFlags(t, 1), logger.L())
			release := make(chan struct{})
			releaseOnce := sync.OnceFunc(func() { close(release) })
			t.Cleanup(releaseOnce)
			if tc.holdCleanup {
				// The hold lifts while Open is still unwinding: its release
				// of the device waits for the cleanup to finish.
				mnt.ioctlCloseCtl = func(fd int) error {
					time.AfterFunc(3*time.Second, releaseOnce)
					<-release

					return unix.Close(fd)
				}
			}
			mnt.openDeviceFn = func(path string) (*os.File, error) {
				f, err := os.Open(path)
				if err != nil {
					return nil, err
				}
				// The device stays open, so its attributes outlive the session.
				for _, s := range mnt.socksServer {
					_ = s.Close()
				}
				<-mnt.ioctlSession.exited
				if !tc.holdCleanup {
					<-mnt.ioctlSession.done
				}

				return f, nil
			}
			idx, err := mnt.Open(t.Context())
			require.ErrorIs(t, err, errIoctlSessionEnded)
			require.Equal(t, uint32(math.MaxUint32), idx, "Open must keep reporting that it owns no device")
			require.Eventually(t, func() bool { return !slotHeld(pool, slot) }, 10*time.Second, 10*time.Millisecond, "slot %d was never released", slot)
			require.Eventually(t, func() bool { return !ioctlDeviceConnected(slot) }, 10*time.Second, 10*time.Millisecond)
		})
	}
}

// A configuration that cannot attach is refused once, with the slot given back.
func TestIoctlOpenRejectsInvalidConfigWithoutRetry(t *testing.T) {
	t.Parallel()
	if os.Geteuid() != 0 {
		t.Skip("kernel NBD requires root")
	}
	pool, slot := singleSlotPool(t)
	mnt := NewDirectPathMount(ioctlTestBackend(t), pool, ioctlTestFlags(t, 0), logger.L())
	start := time.Now()
	_, err := mnt.Open(t.Context())
	require.ErrorIs(t, err, errIoctlInvalidConfig)
	require.Less(t, time.Since(start), 5*time.Second)
	require.Eventually(t, func() bool { return !slotHeld(pool, slot) }, 5*time.Second, 10*time.Millisecond)
}

// A configuration the driver refuses after the claim took leaves the device
// free, and is final.
func TestIoctlAttachPartialConfigurationFreesTheDevice(t *testing.T) {
	t.Parallel()
	if os.Geteuid() != 0 {
		t.Skip("kernel NBD requires root")
	}
	pool, slot := singleSlotPool(t)
	socks, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	require.NoError(t, err)
	client, server := os.NewFile(uintptr(socks[0]), "client"), os.NewFile(uintptr(socks[1]), "server")
	t.Cleanup(func() { client.Close(); server.Close() })
	// The driver takes block sizes that are powers of two only.
	_, err = attachIoctl(t.Context(), ioctlAttachConfig{
		index: slot, socks: []*os.File{client}, size: 3000 * 1024, blockSize: 3000,
		timeout: time.Minute, flags: uint64(nbdnl.FlagHasFlags), maxDevices: pool.MaxDevices(),
	}, logger.L(), false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid argument")
	require.False(t, ioctlDeviceConnected(slot), "a refused configuration leaves the device free")
	require.NoError(t, pool.ReleaseDevice(t.Context(), slot))
}

// The thread limit is raised once, by one thread per device, on top of
// whatever the process had: the parked threads are additive to the rest.
func TestEnsureThreadLimit(t *testing.T) { //nolint:paralleltest // process-wide thread limit and guard
	restore := debug.SetMaxThreads(threadLimitProbe)
	debug.SetMaxThreads(restore)
	t.Cleanup(func() {
		debug.SetMaxThreads(restore)
		threadLimitMu.Lock()
		threadLimitRaised = false
		threadLimitMu.Unlock()
	})
	for _, tc := range []struct {
		previous int
		devices  uint
	}{
		{previous: 10000, devices: 256},
		{previous: 10000, devices: 4096},
		{previous: 20000, devices: 9000},
	} {
		threadLimitMu.Lock()
		threadLimitRaised = false
		threadLimitMu.Unlock()
		debug.SetMaxThreads(tc.previous)
		ensureThreadLimit(t.Context(), logger.L(), tc.devices)
		want := tc.previous + int(tc.devices)
		require.Equal(t, want, debug.SetMaxThreads(want), "previous %d, devices %d", tc.previous, tc.devices)
		// A second call leaves the raised limit alone.
		ensureThreadLimit(t.Context(), logger.L(), tc.devices)
		require.Equal(t, want, debug.SetMaxThreads(want))
	}
	require.Greater(t, threadLimitProbe, 1<<20, "the probe limit must sit above anything a process reaches")
}

// A disowned slot counts as released: it was counted as acquired when it was
// handed out, and the dashboard reads the difference as leaked slots.
func TestDisownCountsARelease(t *testing.T) { //nolint:paralleltest // reads a process-wide counter that parallel tests move
	pool, err := NewDevicePool(1)
	if errors.Is(err, ErrNBDModuleNotLoaded) {
		t.Skip("NBD module not loaded")
	}
	require.NoError(t, err)
	before := slotsReleasedCount(t)
	pool.mu.Lock()
	pool.usedSlots.Set(3)
	pool.mu.Unlock()
	pool.Disown(t.Context(), 3)
	require.False(t, slotHeld(pool, 3))
	require.Equal(t, before+1, slotsReleasedCount(t))
}

// slotsReleasedCount sums the pool's released counter.
func slotsReleasedCount(t *testing.T) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, testMetricReader.Collect(t.Context(), &rm))
	var total int64
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != "orchestrator.nbd.slots_pool.released" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.Truef(t, ok, "released must be a counter, got %T", m.Data)
			for _, dp := range sum.DataPoints {
				total += dp.Value
			}
		}
	}

	return total
}
