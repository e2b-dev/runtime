//go:build linux

package v2

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"
)

type failingStorage struct {
	attempts  atomic.Int64
	released  atomic.Int64
	releaseFn func(*network.Slot) error
	// failures > 0 parks every attempt after that many until its context ends.
	failures int64
	mu       sync.Mutex
	at       []time.Time
}

func (s *failingStorage) Acquire(ctx context.Context) (*network.Slot, error) {
	n := s.attempts.Add(1)
	s.mu.Lock()
	s.at = append(s.at, time.Now())
	s.mu.Unlock()
	if s.failures > 0 && n > s.failures {
		<-ctx.Done()

		return nil, ctx.Err()
	}

	return nil, errors.New("persistent acquire failure")
}

func (s *failingStorage) Release(slot *network.Slot) error {
	s.released.Add(1)
	if s.releaseFn != nil {
		return s.releaseFn(slot)
	}

	return nil
}

func newRetryTestPool(t *testing.T, storage network.Storage, delay time.Duration) (*V2Pool, *sdkmetric.ManualReader) {
	t.Helper()

	reader := sdkmetric.NewManualReader()
	metrics, err := NewMetrics(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	require.NoError(t, err)

	return NewV2Pool(storage, testConfig(), nil, nil, WithPoolMetrics(metrics), WithPopulateRetryDelay(delay)), reader
}

func TestV2Pool_PopulateBacksOffAfterCreationFailure(t *testing.T) {
	t.Parallel()

	const delay = 50 * time.Millisecond
	storage := &failingStorage{failures: 3}
	pool, reader := newRetryTestPool(t, storage, delay)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		pool.Populate(ctx)
		close(done)
	}()

	// Populate reaches the parked attempt only after it has handled every failure.
	require.Eventually(t, func() bool { return storage.attempts.Load() == storage.failures+1 }, 5*time.Second, time.Millisecond)
	cancel()
	requireReceive(t, done)

	for i := 1; i < len(storage.at); i++ {
		require.GreaterOrEqual(t, storage.at[i].Sub(storage.at[i-1]), delay, "attempt %d did not wait out the retry delay", i+1)
	}

	values := collectCounterValues(t, reader)
	require.Equal(t, storage.failures, values[counterKey(metricCreationFailures, "stage", string(slotCreationStageAcquire))])
}

func TestV2Pool_PopulateCancellationExitsBackoffPromptly(t *testing.T) {
	t.Parallel()

	storage := &failingStorage{}
	pool, _ := newRetryTestPool(t, storage, time.Hour)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { pool.Populate(ctx); close(done) }()
	require.Eventually(t, func() bool { return storage.attempts.Load() == 1 }, time.Second, time.Millisecond)
	cancel()
	requireReceive(t, done)
}

func TestV2Pool_CloseExitsBackoffPromptly(t *testing.T) {
	t.Parallel()

	storage := &failingStorage{}
	pool, _ := newRetryTestPool(t, storage, time.Hour)
	done := make(chan struct{})
	go func() { pool.Populate(t.Context()); close(done) }()
	require.Eventually(t, func() bool { return storage.attempts.Load() == 1 }, time.Second, time.Millisecond)
	require.NoError(t, pool.Close(t.Context()))
	requireReceive(t, done)
}

func requireReceive(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Populate did not exit promptly")
	}
}

// A failed release notification keeps the slot allocated and out of reuse on
// the normal, canceled and closed paths. An async return still finishes, so
// Close does not wait on it.
func TestV2Pool_FailedReleaseRetainsSlot(t *testing.T) {
	t.Parallel()

	boom := errors.New("boom")
	failRelease := func(context.Context, string) error { return boom }
	storage := &failingStorage{}
	pool, _ := newRetryTestPool(t, storage, time.Hour)
	close(pool.newSlots)
	canceled, cancel := context.WithCancel(t.Context())
	cancel()

	require.ErrorIs(t, pool.returnSlot(t.Context(), makeTestSlot(t, reserveNSTestIdx(t)), failRelease, 0), boom)
	err := pool.returnSlot(canceled, makeTestSlot(t, reserveNSTestIdx(t)), failRelease, time.Hour)
	require.ErrorIs(t, err, network.ErrSlotRetained)
	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, pool.ReturnAsync(t.Context(), makeTestSlot(t, reserveNSTestIdx(t)), failRelease, time.Hour))
	require.ErrorIs(t, pool.Close(t.Context()), network.ErrSlotRetained)
	err = pool.returnSlot(t.Context(), makeTestSlot(t, reserveNSTestIdx(t)), failRelease, time.Hour)
	require.NotErrorIs(t, err, network.ErrSlotRetained)
	require.ErrorIs(t, err, network.ErrClosed)

	require.Equal(t, int64(4), storage.released.Load(), "shutdown must physically remove all four slots, including earlier failed returns")
	require.Empty(t, pool.reusedSlots)
}

// Close preserves an unresolved error of each kind without collecting every
// later return failure. Individual callers still receive their own diagnostic.
func TestV2Pool_CloseRetainsBoundedUnresolvedErrors(t *testing.T) {
	t.Parallel()

	for _, notification := range []bool{true, false} {
		name := "teardown"
		if notification {
			name = "notification"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			storage := &failingStorage{}
			pool, _ := newRetryTestPool(t, storage, time.Hour)
			close(pool.newSlots)
			first := errors.New("first unresolved failure")
			later := errors.New("later unresolved failure")
			for _, failure := range []error{first, later} {
				released := func(context.Context, string) error { return nil }
				if notification {
					released = func(context.Context, string) error { return failure }
				} else {
					storage.releaseFn = func(*network.Slot) error { return failure }
				}
				err := pool.returnSlot(t.Context(), makeTestSlot(t, reserveNSTestIdx(t)), released, 0)
				require.ErrorIs(t, err, failure, "each return must report its own failure")
			}
			err := pool.Close(t.Context())
			require.ErrorIs(t, err, first, "shutdown must preserve an unresolved failure")
			require.NotErrorIs(t, err, later, "shutdown must not collect a lifetime history")
			require.Equal(t, int64(2), storage.released.Load(), "all slots must reach physical cleanup")
		})
	}
}
