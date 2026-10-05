//go:build linux

package v2

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"
)

type failingStorage struct {
	attempts atomic.Int64
	released atomic.Int64
}

func (s *failingStorage) Acquire(context.Context) (*network.Slot, error) {
	s.attempts.Add(1)

	return nil, errors.New("persistent acquire failure")
}

func (s *failingStorage) Release(*network.Slot) error {
	s.released.Add(1)

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

	storage := &failingStorage{}
	pool, reader := newRetryTestPool(t, storage, 50*time.Millisecond)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		pool.Populate(ctx)
		close(done)
	}()

	require.Eventually(t, func() bool { return storage.attempts.Load() >= 1 }, time.Second, time.Millisecond)
	time.Sleep(120 * time.Millisecond)
	cancel()
	requireReceive(t, done)
	require.LessOrEqual(t, storage.attempts.Load(), int64(3))

	values := collectCounterValues(t, reader)
	require.Equal(t, storage.attempts.Load(), values[counterKey(metricCreationFailures, "stage", string(slotCreationStageAcquire))])
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

	require.ErrorIs(t, pool.returnSlot(t.Context(), &network.Slot{Idx: 1}, failRelease, 0), boom)
	err := pool.returnSlot(canceled, &network.Slot{Idx: 2}, failRelease, time.Hour)
	require.ErrorIs(t, err, network.ErrSlotRetained)
	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, pool.ReturnAsync(t.Context(), &network.Slot{Idx: 3}, failRelease, time.Hour))
	require.NoError(t, pool.Close(t.Context()))
	err = pool.returnSlot(t.Context(), &network.Slot{Idx: 4}, failRelease, time.Hour)
	require.ErrorIs(t, err, network.ErrSlotRetained)
	require.ErrorIs(t, err, network.ErrClosed)

	require.Zero(t, storage.released.Load(), "a retained slot must not be released for reuse")
	require.Empty(t, pool.reusedSlots)
}
