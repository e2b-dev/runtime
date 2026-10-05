//go:build linux

package nbd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bits-and-blooms/bitset"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// GetDevice must report the caller's own cancellation, not the pool state:
// when both are ready, a bare select picks pseudo-randomly and a cancelled
// Open sees ErrClosed instead of context.Canceled.
func TestGetDevicePrefersCallerCancellation(t *testing.T) {
	t.Parallel()

	pool := retryingPool()
	pool.doneOnce.Do(func() { close(pool.done) })

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	// Repeat to catch the pseudo-random pick; one call passes half the time.
	for range 100 {
		_, err := pool.GetDevice(ctx)
		require.ErrorIs(t, err, context.Canceled)
	}
}

// A slots channel closed by Populate's exit must read as a closed pool, not
// as a successful acquisition of slot 0.
func TestGetDeviceClosedFeedIsNotASlot(t *testing.T) {
	t.Parallel()

	pool := retryingPool()
	close(pool.slots)

	_, err := pool.GetDevice(t.Context())
	require.ErrorIs(t, err, ErrClosed)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	for range 100 {
		_, err := pool.GetDevice(ctx)
		require.ErrorIs(t, err, context.Canceled)
	}
}

// One stuck device must not consume Close's budget for every device queued
// behind it: the free ones release, and only the stuck one reports an error.
func TestCloseBoundsEachReleaseIndependently(t *testing.T) {
	t.Parallel()

	blockDir := t.TempDir()

	// Slot 0 reads as in use (a pid file marks a connected device), so its
	// release retries until the deadline. Slots 1-4 read as free.
	require.NoError(t, os.MkdirAll(filepath.Join(blockDir, "nbd0"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(blockDir, "nbd0", "pid"), []byte("1\n"), 0o644))

	for slot := 1; slot < 5; slot++ {
		dir := filepath.Join(blockDir, fmt.Sprintf("nbd%d", slot))
		require.NoError(t, os.MkdirAll(dir, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(dir, "size"), []byte("0\n"), 0o644))
	}

	pool := &DevicePool{
		done:        make(chan struct{}),
		usedSlots:   bitset.New(8),
		slots:       make(chan DeviceSlot, 1),
		sysBlockDir: blockDir,
	}
	for slot := range uint(5) {
		pool.usedSlots.Set(slot)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()

	err := pool.Close(ctx)

	require.ErrorContains(t, err, "failed to release device 0")
	require.ErrorIs(t, err, context.DeadlineExceeded)
	for slot := 1; slot < 5; slot++ {
		assert.NotContains(t, err.Error(), fmt.Sprintf("failed to release device %d", slot),
			"a free device must not fail on the budget the stuck device spent")
	}

	pool.mu.Lock()
	defer pool.mu.Unlock()
	assert.True(t, pool.usedSlots.Test(0), "the stuck device stays claimed")
	for slot := uint(1); slot < 5; slot++ {
		assert.False(t, pool.usedSlots.Test(slot), "free device %d must be released", slot)
	}
}
