//go:build linux

package v2

import (
	"context"
	"errors"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"
)

// slowReleaseStorage makes each teardown take one second on the test clock.
func slowReleaseStorage() *failingStorage {
	return &failingStorage{releaseFn: func(*network.Slot) error {
		time.Sleep(time.Second)

		return nil
	}}
}

func noopRelease(context.Context, string) error { return nil }

// returnTestSlot is the n-th of the fixed slot indexes these tests use, below
// the ones the teardown deadline test takes at the top of the slot range:
// these slots never get a network, and reserved indexes are not handed back.
func returnTestSlot(t *testing.T, n int) *network.Slot {
	t.Helper()

	return makeTestSlot(t, network.GetVrtSlotsSize()-18-n)
}

// poolWithReturnsInFlight has n slots on their way back, each inside its
// return delay. The slots are the n fixed ones from first on.
func poolWithReturnsInFlight(t *testing.T, storage *failingStorage, first, n int) *V2Pool {
	t.Helper()

	pool := NewV2Pool(storage, testConfig(), nil, nil)
	close(pool.newSlots)
	for i := range n {
		require.NoError(t, pool.ReturnAsync(t.Context(), returnTestSlot(t, first+i), noopRelease, time.Hour))
	}
	synctest.Wait()

	return pool
}

// Many sandboxes ending just before a shutdown must not use up the rest of
// it: a Close with a teardown deadline tears their slots down itself, a few
// at a time, and leaves the ones the deadline does not reach.
func TestV2Pool_BoundedCloseTearsDownReturnsInFlight(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		first    int
		slots    int
		deadline time.Duration
		tornDown int64
	}{
		// Two rounds of teardowns start before the deadline; the third does not.
		{"passes during the teardown", 19, 3 * handedTeardownWorkers, 1500 * time.Millisecond, 2 * handedTeardownWorkers},
		{"already passed", 6, 6, -time.Second, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				storage := slowReleaseStorage()
				pool := poolWithReturnsInFlight(t, storage, tc.first, tc.slots)

				err := pool.Close(network.WithTeardownDeadline(t.Context(), time.Now().Add(tc.deadline)))

				require.ErrorIs(t, err, network.ErrSlotsLeft)
				require.Equal(t, tc.tornDown, storage.released.Load())
			})
		})
	}
}

// Close tears several slots down at once and still reports every teardown
// that failed, not only the first.
func TestV2Pool_BoundedCloseReportsEveryFailedTeardown(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		errRelease := errors.New("release failed")
		storage := &failingStorage{releaseFn: func(*network.Slot) error { return errRelease }}
		pool := poolWithReturnsInFlight(t, storage, 0, 3)

		err := pool.Close(network.WithTeardownDeadline(t.Context(), time.Now().Add(time.Hour)))

		require.Error(t, err)
		require.Equal(t, 3, strings.Count(err.Error(), errRelease.Error()))
	})
}

// Without a teardown deadline nothing changes: every return tears its own
// slot down, all at once.
func TestV2Pool_CloseWithoutADeadlineLeavesTeardownToTheReturns(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		storage := slowReleaseStorage()
		pool := poolWithReturnsInFlight(t, storage, 12, 6)

		start := time.Now()
		// Close error ignored: tearing down a slot without a network may fail.
		_ = pool.Close(t.Context())

		require.Equal(t, time.Second, time.Since(start))
		require.Equal(t, int64(6), storage.released.Load())
	})
}

// A return that arrives after a Close with a teardown deadline has finished
// has nobody to leave its slot to.
func TestV2Pool_ReturnAfterABoundedCloseTearsItsSlotDown(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		storage := &failingStorage{}
		pool := NewV2Pool(storage, testConfig(), nil, nil)
		close(pool.newSlots)
		require.NoError(t, pool.Close(network.WithTeardownDeadline(t.Context(), time.Now().Add(time.Hour))))

		// Return error ignored: tearing down a slot without a network may fail.
		_ = pool.ReturnAsync(t.Context(), returnTestSlot(t, 18), noopRelease, 0)

		require.Equal(t, int64(1), storage.released.Load())
	})
}

// A slot left at the teardown deadline still has its firewall closed: a return
// cut short by the shutdown read no counters, and the next start's reclaim
// deletes the namespace that holds them.
func TestV2Pool_BoundedCloseClosesTheFirewallOfASlotItLeaves(t *testing.T) { //nolint:paralleltest // mutates host netns state: singleton nftables table "v2-host-firewall", named netns, veth links
	skipIfNotLinuxRoot(t)

	hf := newTestHostFirewall(t, testConfig())
	observer, err := NewVethObserver()
	require.NoError(t, err)
	defer observer.Close()
	slot := makeTestSlot(t, reserveNSTestIdx(t))
	sv2 := NewSlotV2(slot)
	require.NoError(t, CreateNetworkV2(t.Context(), slot, sv2, hf, observer))
	t.Cleanup(func() { _ = RemoveNetworkV2(context.WithoutCancel(t.Context()), slot, sv2, hf, observer) })
	require.NotNil(t, slot.Firewall)

	// The pool gets no host firewall of its own, so its Close leaves the
	// test's one open for the cleanup above.
	storage := &failingStorage{}
	pool := NewV2Pool(storage, testConfig(), nil, nil)
	close(pool.newSlots)
	require.NoError(t, pool.ReturnAsync(t.Context(), slot, noopRelease, time.Hour))

	err = pool.Close(network.WithTeardownDeadline(t.Context(), time.Now().Add(-time.Second)))

	require.ErrorIs(t, err, network.ErrSlotsLeft)
	require.Zero(t, storage.released.Load())
	require.Nil(t, slot.Firewall)
}
