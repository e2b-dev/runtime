package batcher

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
)

func TestBatcherStartStop(t *testing.T) {
	t.Parallel()
	b, err := NewBatcher[int](func(context.Context, []int) error { return nil }, BatcherOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for range 100 {
		if err := b.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := b.Stop(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestBatcherPushNotStarted(t *testing.T) {
	t.Parallel()
	b, err := NewBatcher[int](func(context.Context, []int) error { return nil }, BatcherOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Push(123); !errors.Is(err, ErrBatcherNotStarted) {
		t.Fatalf("expected ErrBatcherNotStarted, got %v", err)
	}
}

func TestBatcherStopNotStarted(t *testing.T) {
	t.Parallel()
	b, err := NewBatcher[int](func(context.Context, []int) error { return nil }, BatcherOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Stop(); !errors.Is(err, ErrBatcherNotStarted) {
		t.Fatalf("expected ErrBatcherNotStarted, got %v", err)
	}
}

func TestBatcherDoubleStop(t *testing.T) {
	t.Parallel()
	b, err := NewBatcher[int](func(context.Context, []int) error { return nil }, BatcherOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := b.Stop(); err != nil {
		t.Fatal(err)
	}
	if err := b.Stop(); !errors.Is(err, ErrBatcherNotStarted) {
		t.Fatalf("expected ErrBatcherNotStarted, got %v", err)
	}
}

func TestBatcherDoubleStart(t *testing.T) {
	t.Parallel()
	b, err := NewBatcher[int](func(context.Context, []int) error { return nil }, BatcherOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := b.Start(t.Context()); !errors.Is(err, ErrBatcherAlreadyStarted) {
		t.Fatalf("expected ErrBatcherAlreadyStarted, got %v", err)
	}
}

func TestBatcherPushStop(t *testing.T) {
	t.Parallel()
	n := 0
	b, err := NewBatcher[int](func(_ context.Context, batch []int) error {
		n += len(batch)

		return nil
	}, BatcherOptions{MaxDelay: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	for i := range 10 {
		if err := b.Push(i); err != nil {
			t.Fatalf("cannot add item %d to batch: %v", i, err)
		}
	}
	if err := b.Stop(); err != nil {
		t.Fatal(err)
	}

	if n != 10 {
		t.Fatalf("Unexpected n=%d. Expected 10", n)
	}
}

func TestBatcherPushMaxBatchSize(t *testing.T) {
	t.Parallel()
	testBatcherPushMaxBatchSize(t, 1, 100)
	testBatcherPushMaxBatchSize(t, 10, 100)
	testBatcherPushMaxBatchSize(t, 100, 100)
	testBatcherPushMaxBatchSize(t, 101, 100)
	testBatcherPushMaxBatchSize(t, 1003, 15)
	testBatcherPushMaxBatchSize(t, 1033, 17)
}

func TestBatcherPushMaxDelay(t *testing.T) {
	t.Parallel()
	testBatcherPushMaxDelay(t, 100, time.Millisecond)
	testBatcherPushMaxDelay(t, 205, 10*time.Millisecond)
	testBatcherPushMaxDelay(t, 313, 100*time.Millisecond)
}

func TestBatcherConcurrentPush(t *testing.T) {
	t.Parallel()
	s := uint32(0)
	b, err := NewBatcher[uint32](func(_ context.Context, batch []uint32) error {
		for _, v := range batch {
			atomic.AddUint32(&s, v)
		}

		return nil
	}, BatcherOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	ss := uint32(0)
	for range 10 {
		wg.Go(func() {
			for i := range 100 {
				b.Push(uint32(i))
				time.Sleep(time.Millisecond)
				atomic.AddUint32(&ss, uint32(i))
			}
		})
	}
	wg.Wait()
	if err := b.Stop(); err != nil {
		t.Fatal(err)
	}
	if s != ss {
		t.Fatalf("Unepxected sum %d. Expecting %d", s, ss)
	}
}

func TestBatcherQueueSize(t *testing.T) {
	t.Parallel()
	ch := make(chan struct{})
	entered := make(chan struct{}, 10)
	completed := make(chan struct{}, 10)
	n := 0
	b, err := NewBatcher(func(_ context.Context, batch []int) error {
		entered <- struct{}{}
		<-ch
		n += len(batch)
		completed <- struct{}{}

		return nil
	}, BatcherOptions{MaxDelay: time.Hour, MaxBatchSize: 3, QueueSize: 10})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	for i := range 3 {
		if err := b.Push(i); err != nil {
			t.Fatalf("cannot add item %d to batch: %v", i, err)
		}
	}
	<-entered
	for i := range 10 {
		if err := b.Push(i); err != nil {
			t.Fatalf("cannot add item %d to batch: %v", i, err)
		}
	}
	if b.QueueLen() != b.QueueSize {
		t.Fatalf("Unexpected queue size %d. Expecting %d", b.QueueLen(), b.QueueSize)
	}

	// Queue is full: Push must return immediately with ErrBatcherQueueFull.
	for range 10 {
		if err := b.Push(123); !errors.Is(err, ErrBatcherQueueFull) {
			t.Fatalf("expected ErrBatcherQueueFull on full queue, got %v", err)
		}
	}

	close(ch)
	// Four completed batches leave at most one queued item.
	for range 4 {
		<-completed
	}
	for i := range 5 {
		if err := b.Push(i); err != nil {
			t.Fatalf("cannot add item %d to batch: %v", i, err)
		}
	}
	if err := b.Stop(); err != nil {
		t.Fatal(err)
	}

	if n != 18 {
		t.Fatalf("Unexpected number of items passed to batcher func: %d. Expected 18", n)
	}
}

func testBatcherPushMaxDelay(t *testing.T, itemsCount int, maxDelay time.Duration) {
	t.Helper()

	startedAt := time.Now()
	lastTime := startedAt
	n := 0
	nn := 0
	b, err := NewBatcher[int](func(_ context.Context, batch []int) error {
		if time.Since(lastTime) > maxDelay+10*time.Millisecond {
			t.Fatalf("Unexpected delay between batches: %s. Expected no more than %s. itemsCount=%d",
				time.Since(lastTime), maxDelay, itemsCount)
		}
		lastTime = time.Now()
		nn += len(batch)
		n++

		return nil
	}, BatcherOptions{
		MaxDelay:     maxDelay,
		MaxBatchSize: 100500,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	for i := range itemsCount {
		if err := b.Push(i); err != nil {
			t.Fatalf("cannot add item %d to batch: %v", i, err)
		}
		time.Sleep(time.Millisecond)
	}
	if err := b.Stop(); err != nil {
		t.Fatal(err)
	}

	expectedN := int(time.Since(startedAt)/maxDelay) + 2
	if n > expectedN {
		t.Fatalf("Unexpected number of batch func calls: %d. Expected no more than %d. itemsCount=%d, maxDelay=%s",
			n, expectedN, itemsCount, maxDelay)
	}
	if itemsCount != nn {
		t.Fatalf("Unexpected number of items passed to batcher func: %d. Expected %d. maxDelay=%s", nn, itemsCount, maxDelay)
	}
}

func testBatcherPushMaxBatchSize(t *testing.T, itemsCount, batchSize int) {
	t.Helper()

	n := 0
	nn := 0
	b, err := NewBatcher[int](func(_ context.Context, batch []int) error {
		if len(batch) > batchSize {
			t.Fatalf("Unexpected batch size=%d. Must not exceed %d. itemsCount=%d", len(batch), batchSize, itemsCount)
		}
		if len(batch) == 0 {
			t.Fatalf("Empty batch. itemsCount=%d, batchSize=%d", itemsCount, batchSize)
		}
		nn += len(batch)
		n++

		return nil
	}, BatcherOptions{
		MaxDelay:     time.Hour,
		MaxBatchSize: batchSize,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	for i := range itemsCount {
		if err := b.Push(i); err != nil {
			t.Fatalf("cannot add item %d to batch: %v", i, err)
		}
	}
	if err := b.Stop(); err != nil {
		t.Fatal(err)
	}

	expectedN := (itemsCount + batchSize - 1) / batchSize
	if n != expectedN {
		t.Fatalf("Unexpected number of batcher func calls: %d. Expected %d. itemsCount=%d, batchSize=%d",
			n, expectedN, itemsCount, batchSize)
	}
	if nn != itemsCount {
		t.Fatalf("Unexpected number of items in all batches: %d. Expected %d. batchSize=%d", nn, itemsCount, batchSize)
	}
}

func TestBatcherRefreshesRunningLimits(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		source := ldtestdata.DataSource()
		set := func(flag featureflags.IntFlag, value int) {
			source.Update(source.Flag(flag.Key()).ValueForAll(ldvalue.Int(value)))
		}
		set(featureflags.ClickhouseBatcherMaxBatchSize, 3)
		set(featureflags.ClickhouseBatcherMaxDelay, 3600000)
		ff, err := featureflags.NewClientWithDatasource(source)
		require.NoError(t, err)
		defer ff.Close(context.WithoutCancel(t.Context()))
		batches := make(chan int, 8)
		b, err := NewBatcher(func(_ context.Context, items []int) error {
			batches <- len(items)

			return nil
		}, BatcherOptions{Name: "live", FeatureFlags: ff, QueueSize: 7})
		require.NoError(t, err)
		require.NoError(t, b.Start(t.Context()))
		defer b.Stop()
		require.NoError(t, b.Push(1))
		require.NoError(t, b.Push(2))
		synctest.Wait()
		require.Empty(t, batches)
		set(featureflags.ClickhouseBatcherMaxBatchSize, 2)
		set(featureflags.ClickhouseBatcherQueueSize, 1)
		time.Sleep(31 * time.Second)
		synctest.Wait()
		require.Len(t, batches, 1)
		require.Equal(t, 2, <-batches, "lowering the limit must flush the existing batch without another Push")
		require.Equal(t, 7, b.QueueSize, "queue capacity stays at its startup option")
		set(featureflags.ClickhouseBatcherMaxDelay, 1000)
		require.NoError(t, b.Push(3))
		time.Sleep(31 * time.Second)
		synctest.Wait()
		require.Len(t, batches, 1)
		require.Equal(t, 1, <-batches, "new delay must flush a partial batch without restart")
		set(featureflags.ClickhouseBatcherMaxBatchSize, 0)
		set(featureflags.ClickhouseBatcherMaxDelay, math.MaxInt64/int(time.Millisecond)+1)
		time.Sleep(31 * time.Second)
		require.NoError(t, b.Push(4))
		require.NoError(t, b.Push(5))
		synctest.Wait()
		require.Len(t, batches, 1)
		require.Equal(t, 2, <-batches, "invalid updates retain the last valid size")
		require.NoError(t, b.Push(6))
		time.Sleep(2 * time.Second)
		synctest.Wait()
		require.Len(t, batches, 1)
		require.Equal(t, 1, <-batches, "overflowing updates retain the last valid delay")
	})
}

func TestBatcherDelayRefreshKeepsBatchAge(t *testing.T) {
	t.Parallel()
	for _, queuedAt := range []time.Duration{5 * time.Second, 25 * time.Second} {
		t.Run(queuedAt.String(), func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				source := ldtestdata.DataSource()
				source.Update(source.Flag(featureflags.ClickhouseBatcherMaxDelay.Key()).ValueForAll(ldvalue.Int(3600000)))
				ff, err := featureflags.NewClientWithDatasource(source)
				require.NoError(t, err)
				defer ff.Close(context.WithoutCancel(t.Context()))
				flushed := make(chan struct{}, 1)
				b, err := NewBatcher(func(context.Context, []int) error {
					flushed <- struct{}{}

					return nil
				}, BatcherOptions{FeatureFlags: ff})
				require.NoError(t, err)
				require.NoError(t, b.Start(t.Context()))
				defer b.Stop()
				time.Sleep(queuedAt)
				require.NoError(t, b.Push(1))
				synctest.Wait()
				source.Update(source.Flag(featureflags.ClickhouseBatcherMaxDelay.Key()).ValueForAll(ldvalue.Int(10000)))
				time.Sleep(30*time.Second - queuedAt)
				synctest.Wait()
				remaining := queuedAt + 10*time.Second - 30*time.Second
				if remaining > 0 {
					require.Empty(t, flushed)
					time.Sleep(remaining - time.Nanosecond)
					synctest.Wait()
					require.Empty(t, flushed)
					time.Sleep(time.Nanosecond)
					synctest.Wait()
				}
				require.Len(t, flushed, 1, "refresh must honor elapsed batch age")
			})
		})
	}
}
