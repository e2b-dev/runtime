package batcher

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
)

func TestBatcherStartStop(t *testing.T) {
	t.Parallel()
	b, err := NewBatcher[int](func(context.Context, []int) error { return nil }, BatcherOptions{})
	require.NoError(t, err)
	for range 100 {
		require.NoError(t, b.Start(t.Context()))
		require.NoError(t, b.Stop())
	}
}

func TestBatcherPushNotStarted(t *testing.T) {
	t.Parallel()
	b, err := NewBatcher[int](func(context.Context, []int) error { return nil }, BatcherOptions{})
	require.NoError(t, err)
	require.ErrorIs(t, b.Push(123), ErrBatcherNotStarted)
}

func TestBatcherStopNotStarted(t *testing.T) {
	t.Parallel()
	b, err := NewBatcher[int](func(context.Context, []int) error { return nil }, BatcherOptions{})
	require.NoError(t, err)
	require.ErrorIs(t, b.Stop(), ErrBatcherNotStarted)
}

func TestBatcherDoubleStop(t *testing.T) {
	t.Parallel()
	b, err := NewBatcher[int](func(context.Context, []int) error { return nil }, BatcherOptions{})
	require.NoError(t, err)
	require.NoError(t, b.Start(t.Context()))
	require.NoError(t, b.Stop())
	require.ErrorIs(t, b.Stop(), ErrBatcherNotStarted)
}

func TestBatcherDoubleStart(t *testing.T) {
	t.Parallel()
	b, err := NewBatcher[int](func(context.Context, []int) error { return nil }, BatcherOptions{})
	require.NoError(t, err)
	require.NoError(t, b.Start(t.Context()))
	require.ErrorIs(t, b.Start(t.Context()), ErrBatcherAlreadyStarted)
}

func TestBatcherPushStop(t *testing.T) {
	t.Parallel()
	n := 0
	b, err := NewBatcher[int](func(_ context.Context, batch []int) error {
		n += len(batch)

		return nil
	}, BatcherOptions{MaxDelay: time.Hour})
	require.NoError(t, err)
	require.NoError(t, b.Start(t.Context()))
	for i := range 10 {
		require.NoError(t, b.Push(i), "cannot add item %d to batch", i)
	}
	require.NoError(t, b.Stop())

	require.Equal(t, 10, n)
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
	synctest.Test(t, func(t *testing.T) {
		testBatcherPushMaxDelay(t, 100, time.Millisecond)
		testBatcherPushMaxDelay(t, 205, 10*time.Millisecond)
		testBatcherPushMaxDelay(t, 313, 100*time.Millisecond)
	})
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
	require.NoError(t, err)
	require.NoError(t, b.Start(t.Context()))
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
	require.NoError(t, b.Stop())
	require.Equal(t, ss, s)
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
	require.NoError(t, err)
	require.NoError(t, b.Start(t.Context()))
	for i := range 3 {
		require.NoError(t, b.Push(i), "cannot add item %d to batch", i)
	}
	<-entered
	for i := range 10 {
		require.NoError(t, b.Push(i), "cannot add item %d to batch", i)
	}
	require.Equal(t, b.QueueSize, b.QueueLen())

	// Queue is full: Push must return immediately with ErrBatcherQueueFull.
	for range 10 {
		require.ErrorIs(t, b.Push(123), ErrBatcherQueueFull)
	}

	close(ch)
	// Four completed batches leave at most one queued item.
	for range 4 {
		<-completed
	}
	for i := range 5 {
		require.NoError(t, b.Push(i), "cannot add item %d to batch", i)
	}
	require.NoError(t, b.Stop())

	require.Equal(t, 18, n, "number of items passed to batcher func")
}

func testBatcherPushMaxDelay(t *testing.T, itemsCount int, maxDelay time.Duration) {
	t.Helper()

	startedAt := time.Now()
	pushedAt := make([]time.Time, itemsCount)
	n := 0
	nn := 0
	b, err := NewBatcher[int](func(_ context.Context, batch []int) error {
		// Func runs on the batcher goroutine, where require would exit it and leave Stop blocked.
		if !assert.NotEmpty(t, batch, "itemsCount=%d", itemsCount) {
			return nil
		}
		assert.LessOrEqual(t, time.Since(pushedAt[batch[0]]), maxDelay,
			"batch flushed too long after its first item. itemsCount=%d", itemsCount)
		nn += len(batch)
		n++

		return nil
	}, BatcherOptions{
		MaxDelay:     maxDelay,
		MaxBatchSize: 100500,
	})
	require.NoError(t, err)
	require.NoError(t, b.Start(t.Context()))
	defer b.Stop()
	for i := range itemsCount {
		pushedAt[i] = time.Now()
		require.NoError(t, b.Push(i), "cannot add item %d to batch", i)
		time.Sleep(time.Millisecond)
	}
	require.NoError(t, b.Stop())

	expectedN := int(time.Since(startedAt)/maxDelay) + 2
	require.LessOrEqual(t, n, expectedN, "number of batch func calls. itemsCount=%d, maxDelay=%s", itemsCount, maxDelay)
	require.Equal(t, itemsCount, nn, "number of items passed to batcher func. maxDelay=%s", maxDelay)
}

func testBatcherPushMaxBatchSize(t *testing.T, itemsCount, batchSize int) {
	t.Helper()

	n := 0
	nn := 0
	b, err := NewBatcher[int](func(_ context.Context, batch []int) error {
		assert.LessOrEqual(t, len(batch), batchSize, "batch size. itemsCount=%d", itemsCount)
		assert.NotEmpty(t, batch, "itemsCount=%d, batchSize=%d", itemsCount, batchSize)
		nn += len(batch)
		n++

		return nil
	}, BatcherOptions{
		MaxDelay:     time.Hour,
		MaxBatchSize: batchSize,
	})
	require.NoError(t, err)
	require.NoError(t, b.Start(t.Context()))
	for i := range itemsCount {
		require.NoError(t, b.Push(i), "cannot add item %d to batch", i)
	}
	require.NoError(t, b.Stop())

	expectedN := (itemsCount + batchSize - 1) / batchSize
	require.Equal(t, expectedN, n, "number of batcher func calls. itemsCount=%d, batchSize=%d", itemsCount, batchSize)
	require.Equal(t, itemsCount, nn, "number of items in all batches. batchSize=%d", batchSize)
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
