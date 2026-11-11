//go:build linux

package network

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/nftables"
	"github.com/google/nftables/expr"
	"github.com/stretchr/testify/require"
)

// slowReleaseStorage makes each teardown take one second on the test clock.
func slowReleaseStorage() *fakeStorage {
	return &fakeStorage{releaseFn: func(*Slot) error {
		time.Sleep(time.Second)

		return nil
	}}
}

// poolWithReturnsInFlight has n slots on their way back, each inside its
// return delay.
func poolWithReturnsInFlight(t *testing.T, storage *fakeStorage, n int) *Pool {
	t.Helper()

	pool := NewPool(2, n, storage, Config{})
	close(pool.newSlots)
	for i := range n {
		require.NoError(t, pool.ReturnAsync(t.Context(), newTestSlot(i+1), noopRelease, time.Hour))
	}
	synctest.Wait()

	return pool
}

// Many sandboxes ending just before a shutdown must not use up the rest of
// it: a Close with a teardown deadline tears their slots down itself, one at
// a time, and leaves the ones the deadline does not reach.
func TestPool_BoundedCloseTearsDownReturnsInFlight(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		deadline time.Duration
		tornDown int64
	}{
		{"passes during the teardown", 3500 * time.Millisecond, 4},
		{"already passed", -time.Second, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				storage := slowReleaseStorage()
				pool := poolWithReturnsInFlight(t, storage, 6)

				err := pool.Close(WithTeardownDeadline(t.Context(), time.Now().Add(tc.deadline)))

				require.ErrorIs(t, err, ErrSlotsLeft)
				require.Equal(t, tc.tornDown, storage.released.Load())
			})
		})
	}
}

// Without a teardown deadline nothing changes: every return tears its own
// slot down, all at once.
func TestPool_CloseWithoutADeadlineLeavesTeardownToTheReturns(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		storage := slowReleaseStorage()
		pool := poolWithReturnsInFlight(t, storage, 6)

		start := time.Now()
		// Close error ignored: tearing down a slot without a network may fail.
		_ = pool.Close(t.Context())

		require.Equal(t, time.Second, time.Since(start))
		require.Equal(t, int64(6), storage.released.Load())
	})
}

// A return that arrives after a Close with a teardown deadline has finished
// has nobody to leave its slot to.
func TestPool_ReturnAfterABoundedCloseTearsItsSlotDown(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		storage := &fakeStorage{}
		pool := NewPool(2, 1, storage, Config{})
		close(pool.newSlots)
		require.NoError(t, pool.Close(WithTeardownDeadline(t.Context(), time.Now().Add(time.Hour))))

		// Return error ignored: tearing down a slot without a network may fail.
		_ = pool.ReturnAsync(t.Context(), newTestSlot(1), noopRelease, 0)

		require.Equal(t, int64(1), storage.released.Load())
	})
}

// A slot left at the teardown deadline still has its firewall closed: a return
// cut short by the shutdown read no counters, and the next start's reclaim
// deletes the namespace that holds them.
func TestPool_BoundedCloseClosesTheFirewallOfASlotItLeaves(t *testing.T) { //nolint:paralleltest // mutates the caller's netns via LockOSThread + netns.Set; cannot run in parallel
	slot := newFirewallTestSlot(t, "left-at-the-teardown-deadline")
	require.NotNil(t, slot.Firewall)
	storage := &fakeStorage{}
	pool := NewPool(2, 1, storage, Config{})
	close(pool.newSlots)
	require.NoError(t, pool.ReturnAsync(t.Context(), slot, noopRelease, time.Hour))

	err := pool.Close(WithTeardownDeadline(t.Context(), time.Now().Add(-time.Second)))

	require.ErrorIs(t, err, ErrSlotsLeft)
	require.Zero(t, storage.released.Load())
	require.Nil(t, slot.Firewall)
}

// A firewall that fails to close at hand-over is a failed cleanup like any
// other: Close reports it, so the stop is not taken for a clean one.
func TestPool_BoundedCloseReportsAFirewallThatFailedToClose(t *testing.T) { //nolint:paralleltest // mutates the caller's netns via LockOSThread + netns.Set; cannot run in parallel
	slot := newFirewallTestSlot(t, "firewall-close-fails-at-hand-over")
	// A rule with a verdict the kernel does not know fails the batch that
	// deletes the firewall's table.
	fw := slot.Firewall
	fw.conn.AddRule(&nftables.Rule{Table: fw.table, Chain: fw.filterChain, Exprs: []expr.Any{&expr.Verdict{Kind: expr.VerdictKind(1 << 20)}}})
	storage := &fakeStorage{}
	pool := NewPool(2, 1, storage, Config{})
	close(pool.newSlots)
	require.NoError(t, pool.ReturnAsync(t.Context(), slot, noopRelease, time.Hour))

	err := pool.Close(WithTeardownDeadline(t.Context(), time.Now().Add(time.Hour)))

	require.Error(t, err)
	require.Equal(t, int64(1), storage.released.Load(), "Close still tears the slot down")
}
