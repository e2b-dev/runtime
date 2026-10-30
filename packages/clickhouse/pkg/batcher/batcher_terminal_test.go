package batcher

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
)

// blockFirstFlush returns a BatcherFunc that, on its first call only, signals
// entered and then blocks until release is closed; every later flush returns
// immediately. This lets a test pin the processor on a full queue without the
// Func re-blocking on subsequent batches.
func blockFirstFlush[T any](entered chan<- struct{}, release <-chan struct{}) func(context.Context, []T) error {
	var once sync.Once

	return func(context.Context, []T) error {
		once.Do(func() {
			close(entered)
			<-release
		})

		return nil
	}
}

// PushWithContext blocks while the queue is full and succeeds once the
// processor drains a slot, instead of dropping like the non-blocking Push.
func TestBatcherPushWithContextBlocksThenSucceeds(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	entered := make(chan struct{})
	b, err := NewBatcher(blockFirstFlush[int](entered, release),
		BatcherOptions{MaxDelay: time.Hour, MaxBatchSize: 1, QueueSize: 1})
	require.NoError(t, err)
	require.NoError(t, b.Start(t.Context()))
	defer b.Stop()

	// First item is picked up by the processor and blocks it in Func.
	require.NoError(t, b.Push(1))
	<-entered
	// Queue (cap 1) now fills.
	require.NoError(t, b.Push(2))
	require.ErrorIs(t, b.Push(3), ErrBatcherQueueFull, "non-blocking Push drops on a full queue")

	// A bounded-blocking push on the full queue succeeds once Func returns and
	// the processor pulls the queued item, freeing a slot.
	done := make(chan error, 1)
	go func() { done <- b.PushWithContext(t.Context(), 4) }()
	time.Sleep(20 * time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("PushWithContext returned early (%v); it should have blocked on the full queue", err)
	default:
	}
	close(release)
	require.NoError(t, <-done, "PushWithContext should succeed after a slot frees up")
}

// PushWithContext drops (and does not block forever) when its context expires
// while the queue stays full.
func TestBatcherPushWithContextTimesOut(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	entered := make(chan struct{})
	b, err := NewBatcher(blockFirstFlush[int](entered, release),
		BatcherOptions{MaxDelay: time.Hour, MaxBatchSize: 1, QueueSize: 1})
	require.NoError(t, err)
	require.NoError(t, b.Start(t.Context()))
	defer func() { close(release); b.Stop() }()

	require.NoError(t, b.Push(1))
	<-entered
	require.NoError(t, b.Push(2)) // queue full

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, b.PushWithContext(ctx, 3), ErrBatcherQueueFull,
		"a terminal push whose deadline elapses on a stuck queue is dropped, not blocked forever")
}

func TestBatcherPushWithContextNotStarted(t *testing.T) {
	t.Parallel()
	b, err := NewBatcher[int](func(context.Context, []int) error { return nil }, BatcherOptions{})
	require.NoError(t, err)
	require.ErrorIs(t, b.PushWithContext(t.Context(), 1), ErrBatcherNotStarted)
}

// StopWithContext bounds the final drain: when the drain ctx is already done, a
// persistently failing flush is not retried forever — Stop returns promptly and
// the goroutine is not leaked.
func TestBatcherStopWithContextBoundsDrainRetry(t *testing.T) {
	t.Parallel()
	source := ldtestdata.DataSource()
	// Many retries with a long backoff would hang for minutes if unbounded.
	source.Update(source.Flag(featureflags.ClickhouseBatcherFlushMaxRetries.Key()).ValueForAll(ldvalue.Int(1_000_000)))
	source.Update(source.Flag(featureflags.ClickhouseBatcherMaxBatchSize.Key()).ValueForAll(ldvalue.Int(1)))
	ff, err := featureflags.NewClientWithDatasource(source)
	require.NoError(t, err)
	defer ff.Close(context.WithoutCancel(t.Context()))

	var calls atomic.Int32
	b, err := NewBatcher(func(_ context.Context, _ []int) error {
		calls.Add(1)

		return errors.New("clickhouse down")
	}, BatcherOptions{Name: "drain-test", FeatureFlags: ff, MaxDelay: time.Hour, MaxBatchSize: 1, QueueSize: 4})
	require.NoError(t, err)
	require.NoError(t, b.Start(t.Context()))

	require.NoError(t, b.Push(1))
	time.Sleep(20 * time.Millisecond) // let the processor pick it up and start failing

	ctx, cancel := context.WithCancel(t.Context())
	cancel() // drain budget already exhausted

	done := make(chan error, 1)
	go func() { done <- b.StopWithContext(ctx) }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("StopWithContext did not return; drain retried past its cancelled ctx")
	}
}

// A transient flush failure is retried (per the flag) and the batch is
// delivered rather than dropped on the first error.
func TestBatcherFlushRetriesThenSucceeds(t *testing.T) {
	t.Parallel()
	source := ldtestdata.DataSource()
	source.Update(source.Flag(featureflags.ClickhouseBatcherFlushMaxRetries.Key()).ValueForAll(ldvalue.Int(3)))
	source.Update(source.Flag(featureflags.ClickhouseBatcherMaxBatchSize.Key()).ValueForAll(ldvalue.Int(1)))
	ff, err := featureflags.NewClientWithDatasource(source)
	require.NoError(t, err)
	defer ff.Close(context.WithoutCancel(t.Context()))

	var calls atomic.Int32
	delivered := make(chan int, 1)
	b, err := NewBatcher(func(_ context.Context, batch []int) error {
		if calls.Add(1) < 3 {
			return errors.New("transient clickhouse error")
		}
		delivered <- len(batch)

		return nil
	}, BatcherOptions{Name: "retry-test", FeatureFlags: ff, MaxDelay: time.Hour, MaxBatchSize: 1, QueueSize: 4})
	require.NoError(t, err)
	require.NoError(t, b.Start(t.Context()))
	defer b.Stop()

	require.NoError(t, b.Push(1))
	select {
	case n := <-delivered:
		require.Equal(t, 1, n)
	case <-time.After(5 * time.Second):
		t.Fatal("batch was not delivered within timeout; retry did not recover the transient failure")
	}
	require.GreaterOrEqual(t, calls.Load(), int32(3), "expected the flush to be retried until success")
}

// With retries disabled (no flag client), a flush failure drops the batch after
// a single attempt — the legacy behavior is preserved.
func TestBatcherFlushNoRetryByDefault(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	handled := make(chan error, 1)
	b, err := NewBatcher(func(_ context.Context, _ []int) error {
		calls.Add(1)

		return errors.New("boom")
	}, BatcherOptions{
		MaxDelay: time.Hour, MaxBatchSize: 1, QueueSize: 4,
		ErrorHandler: func(err error) { handled <- err },
	})
	require.NoError(t, err)
	require.NoError(t, b.Start(t.Context()))
	defer b.Stop()

	require.NoError(t, b.Push(1))
	select {
	case <-handled:
	case <-time.After(5 * time.Second):
		t.Fatal("ErrorHandler was not called")
	}
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, int32(1), calls.Load(), "static batcher must not retry (legacy drop-on-first-error)")
}
