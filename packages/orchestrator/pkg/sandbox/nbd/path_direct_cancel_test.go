//go:build linux

package nbd

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/nbd/testutils"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

func withAfterConnect(f func(deviceIndex uint32)) MountOption {
	return func(m *DirectPathMount) { m.afterConnect = f }
}

// feedSlots supplies pool with free device slots, standing in for Populate. Populate cannot be
// used here: it would re-acquire the very slot the test asserts was released. But the pool
// still has to be refillable, because Open's connect-retry loop takes a fresh slot per
// attempt - a pool seeded with exactly one slot deadlocks in GetDevice the first time a
// concurrent test connects the device this one was handed.
//
// The returned stop ends the feeder and waits for it; it is safe to call more than once and
// also runs on cleanup. A feeder that finds no free device, or has fed maxFeedAttempts slots,
// closes the pool, so a machine with nothing free fails the test with ErrClosed in seconds
// instead of hanging until the go test timeout.
func feedSlots(t *testing.T, pool *DevicePool) (stop func()) {
	t.Helper()

	return startFeeder(t, pool, true)
}

// feedSlotsKeepOpen is feedSlots for a pool the test uses again after this feeder: running
// dry ends the feeder but leaves the pool open, so the caller bounds its Open with a deadline.
func feedSlotsKeepOpen(t *testing.T, pool *DevicePool) (stop func()) {
	t.Helper()

	return startFeeder(t, pool, false)
}

const maxFeedAttempts = 16

func startFeeder(t *testing.T, pool *DevicePool, closeWhenDry bool) func() {
	t.Helper()

	stopCh := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range maxFeedAttempts {
			slot, err := pool.getFreeDeviceSlot()
			if err != nil {
				break
			}

			select {
			case pool.slots <- *slot:
			case <-stopCh:
				// The slot was claimed but never offered, so nothing else frees it.
				pool.mu.Lock()
				pool.usedSlots.Clear(uint(*slot))
				pool.mu.Unlock()

				return
			}
		}

		if closeWhenDry {
			pool.doneOnce.Do(func() { close(pool.done) })
		}
	}()

	stop := sync.OnceFunc(func() {
		close(stopCh)
		<-done
	})
	t.Cleanup(stop)

	return stop
}

// A cancellation observed after nbdnl.Connect has succeeded must not strand the device. The
// kernel has /dev/nbdX wired to our socket pairs by then and the pool slot is checked out, but
// Open reports the "no device" sentinel, which is also how Close decides there is nothing to
// disconnect or release - so the slot would be lost for the life of the process and the device
// left connected.
func TestPathDirect_OpenCancelledAfterConnect(t *testing.T) {
	t.Parallel()

	featureFlags, err := featureflags.NewClient("", "", "")
	require.NoError(t, err)
	testOpenCancelledAfterConnect(t, featureFlags)
}

// The ioctl attach has no wait loop of its own, so its post-connect
// cancellation goes through the same unwind.
func TestPathDirect_OpenCancelledAfterConnectViaIoctl(t *testing.T) {
	t.Parallel()

	testOpenCancelledAfterConnect(t, ioctlTestFlags(t, 1))
}

func testOpenCancelledAfterConnect(t *testing.T, featureFlags *featureflags.Client) {
	t.Helper()

	if os.Geteuid() != 0 {
		t.Skip("the nbd requires root privileges to run")
	}

	const size = 10 * 1024 * 1024
	const blockSize = header.RootfsBlockSize

	source, err := testutils.NewZeroDevice(size, blockSize)
	require.NoError(t, err, "failed to create zero device")

	cachePath := filepath.Join(os.TempDir(), fmt.Sprintf("test-open-cancel.cache-%s", uuid.New().String()))
	t.Cleanup(func() { os.RemoveAll(cachePath) })

	cache, err := block.NewCache(size, blockSize, cachePath, false)
	require.NoError(t, err, "failed to create cache")

	overlay := block.NewOverlay(source, cache)
	t.Cleanup(func() { overlay.Close() })

	// Confine the pool to its own slot window: contention with parallel
	// tests' pools otherwise burns the feeder's attempts on devices another
	// pool connects first, and an exhausted feeder surfaces as ErrClosed in
	// place of the cancellation under test.
	pool := poolInWindow(t)

	// Stopped and joined before the unwind under test releases the slot: a
	// feeder that is between its checks would otherwise take the slot back
	// and the assertion below would read its claim as a leak.
	stopAndJoinFeeder := feedSlots(t, pool)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	// The hook runs on this goroutine, inside Open, so the cancellation is guaranteed to land
	// in the post-connect window rather than racing it. Stop the feeder in the same breath:
	// from here on nothing else may take the slot whose release is under test, and nothing
	// can, because the device is connected and so reads as in-use.
	connected := DeviceSlot(math.MaxUint32)
	mnt := NewDirectPathMount(overlay, pool, featureFlags, logger.L(),
		withAfterConnect(func(deviceIndex uint32) {
			connected = deviceIndex
			stopAndJoinFeeder()
			cancel()
		}),
	)

	idx, err := mnt.Open(ctx)

	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, uint32(math.MaxUint32), idx, "Open must keep reporting that it owns no device")
	require.NotEqual(t, DeviceSlot(math.MaxUint32), connected, "no connect was observed, so nothing was tested")

	stillConnected, err := isDeviceConnectedIn(sysBlockDir, connected)
	require.NoError(t, err)
	assert.False(t, stillConnected, "nbd%d is still connected after a cancelled Open", connected)

	pool.mu.Lock()
	used := pool.usedSlots.Test(uint(connected))
	pool.mu.Unlock()
	assert.False(t, used, "slot %d was never released back to the pool", connected)

	// Callers close on any Open error, and that must stay harmless.
	assert.NoError(t, mnt.Close(t.Context()))
}
