//go:build linux

package v2

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
)

type blockingReleaseStorage struct {
	once     sync.Once
	released atomic.Int64
	started  chan struct{}
	allowed  chan struct{}
}

func (s *blockingReleaseStorage) Acquire(context.Context) (*network.Slot, error) {
	return nil, context.Canceled
}

func (s *blockingReleaseStorage) Release(*network.Slot) error {
	s.once.Do(func() { close(s.started) })
	<-s.allowed
	s.released.Add(1)

	return nil
}

func TestV2Pool_TakePrefersReusedSlot(t *testing.T) {
	t.Parallel()

	pool := NewV2Pool(&failingStorage{}, testConfig(), nil, nil)
	reused := &network.Slot{Idx: 1}
	pool.reusedSlots <- reused
	reusableSlotsAvailableCounter.Add(t.Context(), 1)
	pool.newSlots <- &network.Slot{Idx: 2}
	newSlotsAvailableCounter.Add(t.Context(), 1)

	got, err := pool.take(t.Context())
	require.NoError(t, err)
	require.Same(t, reused, got)
	assert.Len(t, pool.newSlots, 1)
}

func TestV2Pool_TakeReceivesSlotReturnedWhileWaiting(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		pool := NewV2Pool(&failingStorage{}, testConfig(), nil, nil)
		type result struct {
			slot *network.Slot
			err  error
		}
		done := make(chan result, 1)
		go func() {
			slot, err := pool.take(t.Context())
			done <- result{slot, err}
		}()
		// Both queues are empty, so take is past its fast path and waiting.
		synctest.Wait()

		returned := &network.Slot{Idx: 3}
		pool.reusedSlots <- returned
		reusableSlotsAvailableCounter.Add(t.Context(), 1)
		synctest.Wait()

		select {
		case got := <-done:
			require.NoError(t, got.err)
			require.Same(t, returned, got.slot)
		default:
			t.Fatal("a waiting take must receive a slot returned to the reuse queue")
		}
	})
}

func TestV2Pool_TakeReturnsErrClosedWhenNewSlotsAreClosed(t *testing.T) {
	t.Parallel()

	pool := NewV2Pool(&failingStorage{}, testConfig(), nil, nil)
	close(pool.newSlots)

	got, err := pool.take(t.Context())
	require.Nil(t, got)
	require.ErrorIs(t, err, network.ErrClosed)
}

func TestV2Pool_CloseUnblocksGetWaitingForSlot(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		pool := NewV2Pool(&failingStorage{}, testConfig(), nil, nil)
		type result struct {
			slot *network.Slot
			err  error
		}
		getDone := make(chan result, 1)
		go func() {
			slot, err := pool.Get(t.Context(), new(orchestrator.SandboxNetworkConfig), sandboxtypes.EgressClassSandbox)
			getDone <- result{slot: slot, err: err}
		}()
		synctest.Wait()

		closeDone := make(chan error, 1)
		go func() { closeDone <- pool.Close(t.Context()) }()
		synctest.Wait()

		got := <-getDone
		require.Nil(t, got.slot)
		require.ErrorIs(t, got.err, network.ErrClosed)

		close(pool.newSlots)
		synctest.Wait()
		require.NoError(t, <-closeDone)
	})
}

func TestV2Pool_CloseCanceledBeforePublicationLeavesPoolOpen(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		pool := NewV2Pool(&failingStorage{}, testConfig(), nil, nil)
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
func TestV2Pool_GetRejectsAdmissionWhenClosed(t *testing.T) {
	t.Parallel()

	storage := new(failingStorage)
	pool := NewV2Pool(storage, testConfig(), nil, nil)
	close(pool.newSlots)

	returned := makeTestSlot(t, reserveNSTestIdx(t))
	pool.reusedSlots <- returned
	reusableSlotsAvailableCounter.Add(t.Context(), 1)

	pool.closeMu.Lock()
	pool.closed = true
	pool.closeMu.Unlock()

	got, err := pool.Get(t.Context(), new(orchestrator.SandboxNetworkConfig), sandboxtypes.EgressClassSandbox)
	assert.Nil(t, got, "a closed pool must not hand out its reusable slot")
	assert.Len(t, pool.reusedSlots, 1, "the slot remains pool-owned until Close drains it")

	_ = pool.Close(t.Context())
	assert.Equal(t, int64(1), storage.released.Load(), "Close releases the retained slot exactly once")
	require.ErrorIs(t, err, network.ErrClosed)
}

// A Get tracked before Close can receive a reusable slot immediately before
// Close owns admission. Its final claim must reject and clean the slot, and
// Close must join that admission-owned cleanup before returning.
func TestV2Pool_CloseWinsFinalClaimAndJoinsCleanup(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		cleanupStarted := make(chan struct{})
		cleanupAllowed := make(chan struct{})
		storage := new(blockingReleaseStorage)
		storage.started = cleanupStarted
		storage.allowed = cleanupAllowed
		pool := NewV2Pool(storage, testConfig(), nil, nil)
		pool.reusedSlots = make(chan *network.Slot)

		type result struct {
			slot *network.Slot
			err  error
		}
		getDone := make(chan result, 1)
		go func() {
			slot, err := pool.Get(t.Context(), new(orchestrator.SandboxNetworkConfig), sandboxtypes.EgressClassSandbox)
			getDone <- result{slot: slot, err: err}
		}()
		synctest.Wait()

		// Hold the close writer lock while the waiting Get receives the slot.
		// Publishing the exact Close state after that receive forces the final
		// claim to observe closed when the lock is released.
		pool.closeMu.Lock()
		returned := makeTestSlot(t, reserveNSTestIdx(t))
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
		require.ErrorIs(t, got.err, network.ErrClosed)
		require.NoError(t, <-closeDone)
		assert.Equal(t, int64(1), storage.released.Load(), "the rejected slot is released exactly once")
	})
}
