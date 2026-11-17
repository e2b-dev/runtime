//go:build linux

package nbd

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/bits-and-blooms/bitset"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

// unreachableSlot is an index no NBD device can have, so isDeviceFree always
// fails to read /sys/block/nbd<idx>/size and release() keeps returning an
// error. That drives the retry loop deterministically, whether or not the nbd
// module is loaded on the machine running the test.
const unreachableSlot = DeviceSlot(1 << 20)

func retryingPool() *DevicePool {
	return &DevicePool{
		done:      make(chan struct{}),
		usedSlots: bitset.New(16),
		slots:     make(chan DeviceSlot, 1),
	}
}

// A cancelled context must abort the release retry loop while it is backing
// off, not only when it happens to be at the top of the loop. Otherwise every
// stuck device adds a full backoff interval to orchestrator shutdown.
func TestReleaseDeviceAbortsBackoffOnContextCancel(t *testing.T) {
	t.Parallel()

	pool := retryingPool()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	// Cancel once the first failed attempt has entered the backoff.
	time.AfterFunc(releaseRetryDelay/10, cancel)

	start := time.Now()
	err := pool.ReleaseDevice(ctx, unreachableSlot, WithInfiniteRetry())
	elapsed := time.Since(start)

	require.ErrorIs(t, err, context.Canceled)
	assert.Less(t, elapsed, releaseRetryDelay,
		"ReleaseDevice slept through the cancellation instead of aborting the backoff")
}

// The same applies to the deadline installed by WithTimeout, which Close relies
// on to bound how long a single stuck device can hold up the pool.
func TestReleaseDeviceAbortsBackoffOnTimeout(t *testing.T) {
	t.Parallel()

	pool := retryingPool()

	start := time.Now()
	err := pool.ReleaseDevice(t.Context(), unreachableSlot,
		WithInfiniteRetry(),
		WithTimeout(releaseRetryDelay/10),
	)
	elapsed := time.Since(start)

	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, elapsed, releaseRetryDelay,
		"ReleaseDevice slept past its own deadline")
}

// Without infinite retry the first failure is returned immediately, so the
// backoff change must not alter that path.
func TestReleaseDeviceReturnsFirstErrorWithoutInfiniteRetry(t *testing.T) {
	t.Parallel()

	pool := retryingPool()

	err := pool.ReleaseDevice(t.Context(), unreachableSlot)

	require.Error(t, err)
	assert.NotErrorIs(t, err, context.Canceled)
}

// A slot whose device an ioctl session still holds stays claimed until the
// session lets go of it, then returns to the pool. Once the pool is closing,
// the slot is left to Close, which releases every claimed slot it saw: a
// clear behind its back would count the release twice.
func TestDisownAfterWaitsForTheSession(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		closeFirst  bool
		wantClaimed bool
	}{
		{name: "session lets go", wantClaimed: false},
		{name: "pool closes first", closeFirst: true, wantClaimed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				pool := retryingPool()
				pool.usedSlots.Set(3)
				deviceReleased := make(chan struct{})
				pool.disownAfter(t.Context(), 3, deviceReleased, logger.NewNopLogger())
				synctest.Wait()
				require.True(t, slotHeld(pool, 3), "the slot must stay claimed while the session holds its device")

				if tc.closeFirst {
					pool.mu.Lock()
					pool.doneOnce.Do(func() { close(pool.done) })
					pool.mu.Unlock()
				}
				close(deviceReleased)
				synctest.Wait()
				assert.Equal(t, tc.wantClaimed, slotHeld(pool, 3))
			})
		})
	}
}

// A session that holds its device past the threshold is reported while the
// slot is still claimed, and again once the slot returns.
func TestDisownAfterWarnsWhileTheSessionHoldsTheDevice(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		core, logs := observer.New(zap.WarnLevel)
		pool := retryingPool()
		pool.usedSlots.Set(3)
		deviceReleased := make(chan struct{})
		pool.disownAfter(t.Context(), 3, deviceReleased, logger.NewTracedLoggerFromCore(core))

		time.Sleep(slotHoldWarnThreshold - time.Second)
		synctest.Wait()
		assert.Zero(t, logs.Len(), "a session within the threshold must not be reported")

		time.Sleep(time.Second)
		synctest.Wait()
		require.Equal(t, 1, logs.Len(), "a session past the threshold must be reported while it holds the device")
		assert.Equal(t, uint32(3), logs.All()[0].ContextMap()["device_index"])
		assert.True(t, slotHeld(pool, 3), "the report must not give the slot up")

		close(deviceReleased)
		synctest.Wait()
		assert.False(t, slotHeld(pool, 3))
		assert.Equal(t, 2, logs.Len(), "the slot's return after a report must be logged too")
	})
}
