//go:build linux

package network

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
)

type fakeStorage struct {
	released  atomic.Int64
	releaseFn func(*Slot) error
}

func (f *fakeStorage) Acquire(_ context.Context) (*Slot, error) {
	return nil, context.Canceled
}

func (f *fakeStorage) Release(s *Slot) error {
	f.released.Add(1)
	if f.releaseFn != nil {
		return f.releaseFn(s)
	}

	return nil
}

// testSlotIdxOffset keeps test slot indices outside the range any real
// pool would allocate (vrtSlotsSize == 32766) so cleanup()'s namespace
// teardown can't collide with namespaces another test binary is actively
// using — e.g. the smoketest package creating ns-2, ns-3, …
const testSlotIdxOffset = 1 << 30

// newTestSlot is a slot without a network: the pool refuses to hand it out
// or reuse it, so a return of it always takes the cleanup path. A test whose
// return must reach the reuse queue, or whose Get must succeed, builds a real
// slot with newFirewallTestSlot.
func newTestSlot(idx int) *Slot {
	return &Slot{Idx: idx + testSlotIdxOffset, egressProxy: NoopEgressProxy{}}
}

// noopRelease satisfies returnSlot's ReleaseNotify parameter without doing
// anything. Tests cover the cleanup path and don't care about the
// network-release notification.
func noopRelease(context.Context, string) error { return nil }

func poolTestConfig() Config {
	var config Config
	config.NetworkVersion = 1

	return config
}

func emptySandboxNetworkConfig() *orchestrator.SandboxNetworkConfig {
	return new(orchestrator.SandboxNetworkConfig)
}

// TestReturn_NoPanicDuringClose races Return against Close to guard
// against regressions of the send-on-closed-channel panic.
func TestReturn_NoPanicDuringClose(t *testing.T) {
	t.Parallel()

	const runs = 20
	const workers = 32
	const iters = 50

	for run := range runs {
		storage := &fakeStorage{}
		pool := NewPool(2, workers*iters, storage, Config{})
		close(pool.newSlots)

		var wg sync.WaitGroup
		start := make(chan struct{})

		for w := range workers {
			wg.Go(func() {
				defer func() {
					if r := recover(); r != nil {
						t.Errorf("Return panicked (run=%d worker=%d): %v", run, w, r)
					}
				}()

				<-start

				for i := range iters {
					_ = pool.returnSlot(t.Context(), newTestSlot(w*iters+i+1), noopRelease, 0)
				}
			})
		}

		close(start)
		_ = pool.Close(t.Context())

		wg.Wait()
	}
}

func TestClose_Idempotent(t *testing.T) {
	t.Parallel()

	pool := NewPool(2, 4, &fakeStorage{}, Config{})
	close(pool.newSlots)

	require.NoError(t, pool.Close(t.Context()))
	require.NoError(t, pool.Close(t.Context()))
}

func TestReturn_AfterCloseCleansUpLocally(t *testing.T) { //nolint:paralleltest // mutates the caller's netns via LockOSThread + netns.Set; cannot run in parallel
	slot := newFirewallTestSlot(t, "pool-return-after-close")
	storage := &fakeStorage{}
	pool := NewPool(2, 4, storage, slot.config)
	close(pool.newSlots)

	require.NoError(t, pool.Close(t.Context()))

	before := storage.released.Load()
	err := pool.returnSlot(t.Context(), slot, noopRelease, 0)
	after := storage.released.Load()

	assert.Equal(t, int64(1), after-before, "Return after Close must invoke Storage.Release via cleanup")
	require.ErrorIs(t, err, ErrClosed)
}

func TestReturnAsync_DoesNotBlockOnReturnDelay(t *testing.T) {
	t.Parallel()

	storage := &fakeStorage{}
	pool := NewPool(2, 4, storage, Config{})
	close(pool.newSlots)

	done := make(chan error, 1)
	go func() {
		done <- pool.ReturnAsync(t.Context(), newTestSlot(1), noopRelease, time.Hour)
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("ReturnAsync blocked on the return delay")
	}

	// Close short-circuits the hour-long delay into the cleanup path and
	// must not return before the slot is released. The synthetic slot has no
	// real network configuration, so teardown errors are not the leak signal.
	if err := pool.Close(t.Context()); err != nil {
		t.Logf("synthetic slot network cleanup: %v", err)
	}
	assert.Equal(t, int64(1), storage.released.Load(), "Close returned before the in-flight return released the slot")
}

// Every slot handed to ReturnAsync must be released by the time Close
// returns, whether cleaned up in flight or drained from reusedSlots.
func TestClose_WaitsForInFlightAsyncReturns(t *testing.T) {
	t.Parallel()

	const slots = 8

	storage := &fakeStorage{}
	pool := NewPool(2, slots, storage, Config{})
	close(pool.newSlots)

	for i := range slots {
		require.NoError(t, pool.ReturnAsync(t.Context(), newTestSlot(i+1), noopRelease, 10*time.Millisecond))
	}

	// Close error ignored: cleanup()'s netlink/iptables teardown may fail
	// in the test environment; the release count is the leak signal.
	_ = pool.Close(t.Context())

	assert.Equal(t, int64(slots), storage.released.Load(), "Close returned before all in-flight returns released their slots")
}

// After Close, ReturnAsync cannot register with Close's wait, so it must
// clean the slot up inline before returning.
func TestReturnAsync_AfterCloseCleansUpSynchronously(t *testing.T) {
	t.Parallel()

	storage := &fakeStorage{}
	pool := NewPool(2, 4, storage, Config{})
	close(pool.newSlots)

	require.NoError(t, pool.Close(t.Context()))

	err := pool.ReturnAsync(t.Context(), newTestSlot(1), noopRelease, time.Hour)
	require.ErrorIs(t, err, ErrClosed)
	assert.Equal(t, int64(1), storage.released.Load(), "ReturnAsync after Close must release the slot before returning")
}

func TestReturn_AfterClose_CleanupFailure_PreservesErrClosed(t *testing.T) { //nolint:paralleltest // mutates the caller's netns via LockOSThread + netns.Set; cannot run in parallel
	slot := newFirewallTestSlot(t, "pool-return-cleanup-failure")
	boom := errors.New("boom")
	storage := &fakeStorage{releaseFn: func(_ *Slot) error { return boom }}
	pool := NewPool(2, 4, storage, slot.config)
	close(pool.newSlots)

	require.NoError(t, pool.Close(t.Context()))

	err := pool.returnSlot(t.Context(), slot, noopRelease, 0)
	require.ErrorIs(t, err, ErrClosed)
	require.ErrorIs(t, err, boom)
}

// A failed release notification keeps the slot allocated and out of reuse on
// the normal and canceled paths. Close physically removes every retained
// slot, and a return after Close removes it immediately without reuse.
func TestReturn_FailedReleaseRetainsSlot(t *testing.T) {
	t.Parallel()

	boom := errors.New("boom")
	failRelease := func(ctx context.Context, _ string) error {
		assert.NoError(t, ctx.Err(), "caller cancellation must not skip lifecycle cleanup")

		return boom
	}
	storage := &fakeStorage{}
	pool := NewPool(2, 4, storage, Config{})
	close(pool.newSlots)
	canceled, cancel := context.WithCancel(t.Context())
	cancel()

	require.ErrorIs(t, pool.returnSlot(t.Context(), newTestSlot(1), failRelease, 0), boom)
	err := pool.returnSlot(canceled, newTestSlot(2), failRelease, time.Hour)
	require.ErrorIs(t, err, ErrSlotRetained)
	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, pool.ReturnAsync(t.Context(), newTestSlot(3), failRelease, time.Hour))
	require.ErrorIs(t, pool.Close(t.Context()), ErrSlotRetained)
	err = pool.returnSlot(t.Context(), newTestSlot(4), failRelease, time.Hour)
	require.NotErrorIs(t, err, ErrSlotRetained)
	require.ErrorIs(t, err, ErrClosed)

	assert.Equal(t, int64(4), storage.released.Load(), "shutdown must physically remove all four slots, including earlier failed returns")
	assert.Empty(t, pool.reusedSlots)
}

type poolGetResult struct {
	slot *Slot
	err  error
}

func TestPool_GetPrefersInitiallyReusedSlot(t *testing.T) { //nolint:paralleltest // mutates the caller's netns via LockOSThread + netns.Set; cannot run in parallel
	reused := newFirewallTestSlot(t, "pool-get-reused")
	newSlot := newFirewallTestSlot(t, "pool-get-new")
	pool := NewPool(NewSlotsPoolSize, ReusedSlotsPoolSize, &fakeStorage{}, reused.config)
	pool.reusedSlots <- reused
	reusableSlotsAvailableCounter.Add(t.Context(), 1)
	pool.newSlots <- newSlot
	newSlotsAvailableCounter.Add(t.Context(), 1)

	got, err := pool.Get(t.Context(), &orchestrator.SandboxNetworkConfig{}, sandboxtypes.EgressClassSandbox)
	require.NoError(t, err)
	require.Same(t, reused, got)
	assert.Empty(t, pool.reusedSlots)
	assert.Len(t, pool.newSlots, 1)
}

func TestPool_GetMakesProgressWithOnlyNewSlot(t *testing.T) { //nolint:paralleltest // mutates the caller's netns via LockOSThread + netns.Set; cannot run in parallel
	newSlot := newFirewallTestSlot(t, "pool-get-only-new")
	pool := NewPool(NewSlotsPoolSize, ReusedSlotsPoolSize, &fakeStorage{}, newSlot.config)
	pool.newSlots <- newSlot
	newSlotsAvailableCounter.Add(t.Context(), 1)

	got, err := pool.Get(t.Context(), &orchestrator.SandboxNetworkConfig{}, sandboxtypes.EgressClassSandbox)
	require.NoError(t, err)
	require.Same(t, newSlot, got)
	assert.Empty(t, pool.newSlots)
	assert.Empty(t, pool.reusedSlots)
}

func TestPool_GetReturnsErrClosedWhenNewSlotsAreClosed(t *testing.T) {
	t.Parallel()

	pool := NewPool(NewSlotsPoolSize, ReusedSlotsPoolSize, &fakeStorage{}, Config{NetworkVersion: 1})
	close(pool.newSlots)

	got, err := pool.Get(t.Context(), &orchestrator.SandboxNetworkConfig{}, sandboxtypes.EgressClassSandbox)
	require.Nil(t, got)
	require.ErrorIs(t, err, ErrClosed)
}

func TestPool_GetReturnsContextErrorWhenWaiting(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		pool := NewPool(NewSlotsPoolSize, ReusedSlotsPoolSize, &fakeStorage{}, Config{NetworkVersion: 1})
		ctx, cancel := context.WithCancel(t.Context())
		result := make(chan poolGetResult, 1)
		go func() {
			slot, err := pool.Get(ctx, &orchestrator.SandboxNetworkConfig{}, sandboxtypes.EgressClassSandbox)
			result <- poolGetResult{slot: slot, err: err}
		}()
		synctest.Wait()
		cancel()
		synctest.Wait()

		got := <-result
		require.Nil(t, got.slot)
		require.ErrorIs(t, got.err, context.Canceled)
	})
}

func TestPool_CloseUnblocksGetWaitingForSlot(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		pool := NewPool(NewSlotsPoolSize, ReusedSlotsPoolSize, &fakeStorage{}, Config{NetworkVersion: 1})
		result := make(chan poolGetResult, 1)
		go func() {
			slot, err := pool.Get(t.Context(), &orchestrator.SandboxNetworkConfig{}, sandboxtypes.EgressClassSandbox)
			result <- poolGetResult{slot: slot, err: err}
		}()
		synctest.Wait()

		closeDone := make(chan error, 1)
		go func() { closeDone <- pool.Close(t.Context()) }()
		synctest.Wait()
		got := <-result
		require.Nil(t, got.slot)
		require.ErrorIs(t, got.err, ErrClosed)

		close(pool.newSlots)
		synctest.Wait()
		require.NoError(t, <-closeDone)
	})
}

func TestPool_CloseCanceledBeforePublicationLeavesPoolOpen(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		pool := NewPool(NewSlotsPoolSize, ReusedSlotsPoolSize, &fakeStorage{}, Config{NetworkVersion: 1})
		defer close(pool.newSlots)

		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		closeDone := make(chan error, 1)
		go func() { closeDone <- pool.Close(ctx) }()
		synctest.Wait()

		select {
		case err := <-closeDone:
			require.ErrorIs(t, err, context.Canceled)
		default:
			t.Fatal("Close did not return the cancellation before publishing closed")
		}

		select {
		case <-pool.done:
			t.Fatal("canceled Close published closed")
		default:
		}

		pool.closeMu.RLock()
		closed := pool.closed
		pool.closeMu.RUnlock()
		assert.False(t, closed)
	})
}

// Once closed is published, Get rejects admission before selecting any slot.
// This checks the admission gate; final-claim races are exercised below.
func TestPool_GetRejectsAdmissionWhenClosed(t *testing.T) {
	t.Parallel()

	storage := new(fakeStorage)
	pool := NewPool(NewSlotsPoolSize, ReusedSlotsPoolSize, storage, poolTestConfig())
	close(pool.newSlots)

	returned := newTestSlot(71)
	pool.reusedSlots <- returned
	reusableSlotsAvailableCounter.Add(t.Context(), 1)

	pool.closeMu.Lock()
	pool.closed = true
	pool.closeMu.Unlock()

	got, err := pool.Get(t.Context(), emptySandboxNetworkConfig(), sandboxtypes.EgressClassSandbox)
	assert.Nil(t, got, "a closed pool must not hand out its reusable slot")
	assert.Len(t, pool.reusedSlots, 1, "the slot remains pool-owned until Close drains it")

	_ = pool.Close(t.Context())
	assert.Equal(t, int64(1), storage.released.Load(), "Close releases the retained slot exactly once")
	require.ErrorIs(t, err, ErrClosed)
}

// A Get tracked before Close can receive a reusable slot immediately before
// Close owns admission. Its final claim must reject and clean the slot, and
// Close must join that admission-owned cleanup before returning.
func TestPool_CloseWinsFinalClaimAndJoinsCleanup(t *testing.T) { //nolint:paralleltest // native slot setup mutates the caller's netns
	returned := newFirewallTestSlot(t, "pool-close-final-claim")

	synctest.Test(t, func(t *testing.T) {
		cleanupStarted := make(chan struct{})
		cleanupAllowed := make(chan struct{})
		storage := new(fakeStorage)
		storage.releaseFn = func(*Slot) error {
			close(cleanupStarted)
			<-cleanupAllowed

			return nil
		}
		pool := NewPool(NewSlotsPoolSize, 0, storage, returned.config)

		getDone := make(chan poolGetResult, 1)
		go func() {
			slot, err := pool.Get(t.Context(), emptySandboxNetworkConfig(), sandboxtypes.EgressClassSandbox)
			getDone <- poolGetResult{slot: slot, err: err}
		}()
		synctest.Wait()

		// Hold the close writer lock while the waiting Get receives the slot.
		// Publishing the exact Close state after that receive forces the final
		// claim to observe closed when the lock is released.
		pool.closeMu.Lock()
		reusableSlotsAvailableCounter.Add(t.Context(), 1)
		pool.reusedSlots <- returned
		pool.closed = true
		pool.doneOnce.Do(func() { close(pool.done) })
		close(pool.newSlots)
		pool.closeMu.Unlock()

		closeDone := make(chan error, 1)
		go func() { closeDone <- pool.Close(t.Context()) }()
		synctest.Wait()

		select {
		case <-cleanupStarted:
		default:
			t.Error("post-close claim did not start slot cleanup")
		}
		assert.Empty(t, getDone, "Get returned before rejected-slot cleanup finished")
		assert.Empty(t, closeDone, "Close returned before admission-owned cleanup finished")

		close(cleanupAllowed)
		synctest.Wait()

		got := <-getDone
		require.Nil(t, got.slot)
		require.ErrorIs(t, got.err, ErrClosed)
		require.NoError(t, <-closeDone)
		assert.Equal(t, int64(1), storage.released.Load(), "the rejected slot is released exactly once")
	})
}

// Close preserves an unresolved error of each kind without collecting every
// later return failure. Individual callers still receive their own diagnostic.
func TestPool_CloseRetainsBoundedUnresolvedErrors(t *testing.T) {
	t.Parallel()

	for _, notification := range []bool{true, false} {
		name := "teardown"
		if notification {
			name = "notification"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			storage := &fakeStorage{}
			pool := NewPool(2, 4, storage, Config{})
			close(pool.newSlots)
			first := errors.New("first unresolved failure")
			later := errors.New("later unresolved failure")
			for i, failure := range []error{first, later} {
				released := func(context.Context, string) error { return nil }
				if notification {
					released = func(context.Context, string) error { return failure }
				} else {
					storage.releaseFn = func(*Slot) error { return failure }
				}
				err := pool.returnSlot(t.Context(), newTestSlot(i+1), released, 0)
				require.ErrorIs(t, err, failure, "each return must report its own failure")
			}
			err := pool.Close(t.Context())
			require.ErrorIs(t, err, first, "shutdown must preserve an unresolved failure")
			require.NotErrorIs(t, err, later, "shutdown must not collect a lifetime history")
			require.Equal(t, int64(2), storage.released.Load(), "all slots must reach physical cleanup")
		})
	}
}
