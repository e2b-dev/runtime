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

// feedSlotsUntil supplies pool with free device slots until stop is closed, standing in for
// Populate. Populate cannot be used here: it would re-acquire the very slot the test asserts
// was released. But the pool still has to be refillable, because Open's connect-retry loop
// takes a fresh slot per attempt - a pool seeded with exactly one slot deadlocks in GetDevice
// the first time a concurrent test connects the device this one was handed.
//
// It stops on its own after maxAttempts so a machine with nothing free fails the test in
// seconds instead of hanging until the go test timeout: closing done makes GetDevice return
// ErrClosed, which surfaces as an Open error the assertions below report.
func feedSlotsUntil(pool *DevicePool, stop <-chan struct{}) {
	const maxAttempts = 16

	go func() {
		for range maxAttempts {
			slot, err := pool.getFreeDeviceSlot()
			if err != nil {
				break
			}

			select {
			case pool.slots <- *slot:
			case <-stop:
				return
			}
		}

		pool.doneOnce.Do(func() { close(pool.done) })
	}()
}

// A cancellation observed after nbdnl.Connect has succeeded must not strand the device. The
// kernel has /dev/nbdX wired to our socket pairs by then and the pool slot is checked out, but
// Open reports the "no device" sentinel, which is also how Close decides there is nothing to
// disconnect or release - so the slot would be lost for the life of the process and the device
// left connected.
func TestPathDirect_OpenCancelledAfterConnect(t *testing.T) {
	t.Parallel()

	if os.Geteuid() != 0 {
		t.Skip("the nbd requires root privileges to run")
	}

	featureFlags, err := featureflags.NewClient()
	require.NoError(t, err)

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

	pool, err := NewDevicePool(1)
	require.NoError(t, err, "failed to create device pool")

	// Confine the pool to its own slot window: contention with parallel
	// tests' pools otherwise burns the feeder's attempts on devices another
	// pool connects first, and an exhausted feeder surfaces as ErrClosed in
	// place of the cancellation under test.
	claimSlotWindow(t, pool)

	stop := make(chan struct{})
	stopFeeder := sync.OnceFunc(func() { close(stop) })
	t.Cleanup(stopFeeder)
	feedSlotsUntil(pool, stop)

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
			stopFeeder()
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
