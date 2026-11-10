//go:build linux

package nbd

import (
	"context"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Every parallel test builds a private DevicePool over the machine-global
// /dev/nbd namespace, and free-ness is check-then-act against /sys/block: two
// pools can pick the same device, the loser's connect fails, and its retries
// drain slots until tests fail on devices they never touched. Each test pool
// therefore works a disjoint slot window, pre-marking the bits outside the
// window as used so the pool never scans foreign slots.
//
// testWindowSize bounds one test's appetite: an Open needs one slot per
// connect attempt and the feeder holds only a few ready.
const testWindowSize = 8

// Window indices are recycled once the pool that worked them is torn down:
// concurrent windows are then bounded by test parallelism rather than by the
// package's test count, so repeated in-process passes (-count) fit the same
// namespace.
var (
	windowMu    sync.Mutex
	freeWindows []uint
	nextWindow  uint
)

// acquireWindow hands out a window index below capacity, waiting for a
// recycled one when every window is held: -parallel may exceed the number of
// windows that fit (one on a 16-device built-in driver, five on 64), and a
// transiently full namespace must queue behind running tests rather than
// fail. The deadline turns a window held past its test's cleanup into a
// diagnosis instead of a package-wide hang.
func acquireWindow(t *testing.T, capacity uint) uint {
	t.Helper()

	const acquireTimeout = 5 * time.Minute
	deadline := time.Now().Add(acquireTimeout)

	for {
		windowMu.Lock()
		if n := len(freeWindows); n > 0 {
			w := freeWindows[n-1]
			freeWindows = freeWindows[:n-1]
			windowMu.Unlock()

			return w
		}
		if nextWindow < capacity {
			w := nextWindow
			nextWindow++
			windowMu.Unlock()

			return w
		}
		windowMu.Unlock()

		if time.Now().After(deadline) {
			t.Fatalf("no slot window freed in %s (capacity %d); a test is holding its window past cleanup",
				acquireTimeout, capacity)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func releaseWindow(w uint) {
	windowMu.Lock()
	defer windowMu.Unlock()

	freeWindows = append(freeWindows, w)
}

// claimSlotWindow confines pool to its own slot window. The returned release
// must run before pool.Close, which would otherwise read every padding bit as
// a claimed device and try to release slots owned by other tests.
func claimSlotWindow(t *testing.T, pool *DevicePool) (release func()) {
	t.Helper()

	// usedSlots.Len is fixed at construction: every pool reads the same
	// nbds_max, so capacity is a process-wide constant.
	total := pool.usedSlots.Len()
	// The windows sit above the smoke test binary's pool — go test runs the
	// two binaries concurrently. That pool populates from device 0 upward and
	// reaches past its size: Populate claims one in-flight slot beyond the
	// buffered ones, and each checked-out device holds its slot while the
	// feeder refills above it, so the base is the pool size plus one in
	// flight plus one checked out, aligned up to the window size: 72 for the
	// default pool of 64, 8 for the two slots the unit-test action sets,
	// which fits a built-in driver's 16 devices. On larger namespaces the
	// base also scales with nbds_max (96 on a 256-device module) so the
	// smoke test keeps room to grow.
	base := max(total*3/8, alignUp(smokeTestPoolSize(t)+2, testWindowSize))
	var capacity uint
	if total > base {
		capacity = (total - base) / testWindowSize
	}
	require.NotZero(t, capacity,
		"the device namespace cannot fit one test window above the smoke test's pool; raise nbds_max or lower NBD_POOL_SIZE")

	window := acquireWindow(t, capacity)
	// Registered after a successful acquire and before the caller's pool
	// cleanup, so only in-range indices circulate and LIFO order recycles the
	// window only after the pool that worked it is fully closed.
	t.Cleanup(func() { releaseWindow(window) })

	lo := base + window*testWindowSize
	hi := lo + testWindowSize

	pool.mu.Lock()
	defer pool.mu.Unlock()

	for slot := range total {
		if slot < lo || slot >= hi {
			pool.usedSlots.Set(slot)
		}
	}

	return func() {
		pool.mu.Lock()
		defer pool.mu.Unlock()

		for slot := range total {
			if slot < lo || slot >= hi {
				pool.usedSlots.Clear(slot)
			}
		}
	}
}

// smokeTestPoolSize is the NBD_POOL_SIZE the smoke test binary builds its
// pool with (cfg.Config.NBDPoolSize, default 64). Both binaries read the same
// environment, so the windows start where that pool ends.
func smokeTestPoolSize(t *testing.T) uint {
	t.Helper()

	value := os.Getenv("NBD_POOL_SIZE")
	if value == "" {
		return 64
	}

	size, err := strconv.ParseUint(value, 10, 32)
	require.NoError(t, err, "NBD_POOL_SIZE must be a positive integer")
	require.NotZero(t, size, "NBD_POOL_SIZE must be a positive integer")

	return uint(size)
}

func alignUp(n, to uint) uint {
	return (n + to - 1) / to * to
}

// newPartitionedPool builds a windowed pool with a running feeder and tears
// both down on cleanup. Callers register their own cleanups afterwards, so
// whatever uses the pool is torn down before the pool is.
func newPartitionedPool(t *testing.T) *DevicePool {
	t.Helper()

	// A small buffer keeps the feeder lazy: it claims only a few window slots
	// ahead and blocks on the channel rather than hoarding the whole window.
	pool, err := NewDevicePool(2)
	require.NoError(t, err, "failed to create device pool")

	releasePadding := claimSlotWindow(t, pool)

	poolCtx, poolCancel := context.WithCancel(t.Context())
	poolClosed := make(chan struct{})

	go func() {
		pool.Populate(poolCtx)
		close(poolClosed)
	}()

	t.Cleanup(func() {
		poolCancel()
		<-poolClosed

		releasePadding()

		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 30*time.Second)
		defer cancel()

		if err := pool.Close(ctx); err != nil {
			t.Logf("failed to close device pool: %v", err)
		}
	})

	return pool
}
