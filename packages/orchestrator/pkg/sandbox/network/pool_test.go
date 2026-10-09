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

func newTestSlot(idx int) *Slot {
	return &Slot{Idx: idx + testSlotIdxOffset, egressProxy: NoopEgressProxy{}}
}

// noopRelease satisfies returnSlot's ReleaseNotify parameter without doing
// anything. Tests cover the cleanup path and don't care about the
// network-release notification.
func noopRelease(context.Context, string) error { return nil }

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

func TestReturn_AfterCloseCleansUpLocally(t *testing.T) {
	t.Parallel()

	storage := &fakeStorage{}
	pool := NewPool(2, 4, storage, Config{})
	close(pool.newSlots)

	require.NoError(t, pool.Close(t.Context()))

	before := storage.released.Load()
	err := pool.returnSlot(t.Context(), newTestSlot(1), noopRelease, 0)
	after := storage.released.Load()

	assert.Equal(t, int64(1), after-before, "Return after Close must invoke Storage.Release via cleanup")
	require.ErrorIs(t, err, ErrClosed)
}

func TestReturnAsync_DoesNotBlockOnReturnDelay(t *testing.T) {
	t.Parallel()

	storage := &fakeStorage{}
	pool := NewPool(2, 4, storage, Config{})
	close(pool.newSlots)

	done := make(chan struct{})
	go func() {
		defer close(done)

		_ = pool.ReturnAsync(t.Context(), newTestSlot(1), noopRelease, time.Hour)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("ReturnAsync blocked on the return delay")
	}

	// Close short-circuits the hour-long delay into the cleanup path and
	// must not return before the slot is released.
	require.NoError(t, pool.Close(t.Context()))
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

func TestReturn_AfterClose_CleanupFailure_PreservesErrClosed(t *testing.T) {
	t.Parallel()

	boom := errors.New("boom")
	storage := &fakeStorage{releaseFn: func(_ *Slot) error { return boom }}
	pool := NewPool(2, 4, storage, Config{})
	close(pool.newSlots)

	require.NoError(t, pool.Close(t.Context()))

	err := pool.returnSlot(t.Context(), newTestSlot(1), noopRelease, 0)
	require.ErrorIs(t, err, ErrClosed)
	require.ErrorIs(t, err, boom)
}

// A failed release notification keeps the slot allocated and out of reuse on
// the normal, canceled and closed paths. An async return still finishes, so
// Close does not wait on it.
func TestReturn_FailedReleaseRetainsSlot(t *testing.T) {
	t.Parallel()

	boom := errors.New("boom")
	failRelease := func(context.Context, string) error { return boom }
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
	require.NoError(t, pool.Close(t.Context()))
	err = pool.returnSlot(t.Context(), newTestSlot(4), failRelease, time.Hour)
	require.ErrorIs(t, err, ErrSlotRetained)
	require.ErrorIs(t, err, ErrClosed)

	assert.Zero(t, storage.released.Load(), "a retained slot must not be released for reuse")
	assert.Empty(t, pool.reusedSlots)
}

type poolGetResult struct {
	slot *Slot
	err  error
}

func TestPool_GetPrefersInitiallyReusedSlot(t *testing.T) {
	t.Parallel()

	pool := NewPool(NewSlotsPoolSize, ReusedSlotsPoolSize, &fakeStorage{}, Config{NetworkVersion: 1})
	reused := newTestSlot(51)
	newSlot := newTestSlot(52)
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

func TestPool_GetMakesProgressWithOnlyNewSlot(t *testing.T) {
	t.Parallel()

	pool := NewPool(NewSlotsPoolSize, ReusedSlotsPoolSize, &fakeStorage{}, Config{NetworkVersion: 1})
	newSlot := newTestSlot(61)
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
