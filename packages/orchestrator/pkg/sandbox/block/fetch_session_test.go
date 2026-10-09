//go:build linux

package block

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

const fetchSessionBlockSize int64 = 4096

func makeTestCacheForSession(t *testing.T, numBlocks int64) *Cache {
	t.Helper()

	size := fetchSessionBlockSize * numBlocks
	c, err := NewCache(size, fetchSessionBlockSize, filepath.Join(t.TempDir(), "cache"), false)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	return c
}

type cancelBetweenCheckAndWaitContext struct {
	done       chan struct{}
	errSampled chan struct{}
	releaseErr chan struct{}

	errCalls   atomic.Int32
	cancelOnce sync.Once
	afterMu    sync.Mutex
	after      func()
}

func newCancelBetweenCheckAndWaitContext() *cancelBetweenCheckAndWaitContext {
	ctx := new(cancelBetweenCheckAndWaitContext)
	ctx.done = make(chan struct{})
	ctx.errSampled = make(chan struct{})
	ctx.releaseErr = make(chan struct{})

	return ctx
}

func (c *cancelBetweenCheckAndWaitContext) Deadline() (time.Time, bool) { return time.Time{}, false }
func (c *cancelBetweenCheckAndWaitContext) Done() <-chan struct{}       { return c.done }
func (c *cancelBetweenCheckAndWaitContext) Value(any) any               { return nil }

func (c *cancelBetweenCheckAndWaitContext) Err() error {
	if c.errCalls.Add(1) == 1 {
		close(c.errSampled)
		<-c.releaseErr

		return nil
	}

	select {
	case <-c.done:
		return context.Canceled
	default:
		return nil
	}
}

func (c *cancelBetweenCheckAndWaitContext) AfterFunc(f func()) func() bool {
	c.afterMu.Lock()
	c.after = f
	c.afterMu.Unlock()

	return func() bool {
		c.afterMu.Lock()
		defer c.afterMu.Unlock()
		if c.after == nil {
			return false
		}
		c.after = nil

		return true
	}
}

func (c *cancelBetweenCheckAndWaitContext) cancel() {
	c.cancelOnce.Do(func() {
		close(c.done)

		c.afterMu.Lock()
		f := c.after
		c.after = nil
		c.afterMu.Unlock()
		if f == nil {
			return
		}

		go f()
	})
}

// Mutex contention is not durably blocking for synctest. The channel token
// mirrors the same critical section while mu remains the session's state lock.
type fetchSessionTestMutex struct {
	mu    *sync.Mutex
	token chan struct{}
}

func (m *fetchSessionTestMutex) Lock() {
	m.token <- struct{}{}
	m.mu.Lock()
}

func (m *fetchSessionTestMutex) Unlock() {
	m.mu.Unlock()
	<-m.token
}

func TestFetchSession_FastPath(t *testing.T) {
	t.Parallel()

	const blockSize = fetchSessionBlockSize

	cache := makeTestCacheForSession(t, 4)
	s := newFetchSession(0, 4*blockSize, cache)
	s.bytesReady.Store(4 * blockSize)

	// All blocks already covered — must return immediately via atomic fast path.
	start := time.Now()
	require.NoError(t, s.registerAndWait(t.Context(), 0))
	require.NoError(t, s.registerAndWait(t.Context(), 3*blockSize))
	require.Less(t, time.Since(start), time.Millisecond)
}

func TestFetchSession_ProgressiveAdvance(t *testing.T) {
	t.Parallel()

	const blockSize = fetchSessionBlockSize
	const numBlocks = 512 // 2 MB total

	cache := makeTestCacheForSession(t, numBlocks)
	s := newFetchSession(0, int64(numBlocks)*blockSize, cache)

	var returned atomic.Int64

	type result struct {
		blockIdx int
		err      error
	}
	ch := make(chan result, numBlocks)

	for i := range numBlocks {
		go func(idx int) {
			err := s.registerAndWait(t.Context(), int64(idx)*blockSize)
			returned.Add(1)
			ch <- result{idx, err}
		}(i)
	}

	// bytesReady is 0 — no waiter can have returned yet.
	time.Sleep(time.Millisecond)
	require.Equal(t, int64(0), returned.Load())

	for covered := 2; covered <= numBlocks; covered += 2 {
		s.advance(int64(covered) * blockSize)

		got := [2]int{}
		for j := range 2 {
			r := <-ch
			require.NoError(t, r.err, "block %d", r.blockIdx)
			got[j] = r.blockIdx
		}

		if got[0] > got[1] {
			got[0], got[1] = got[1], got[0]
		}

		require.Equal(t, [2]int{covered - 2, covered - 1}, got,
			"advance to %d should unblock exactly blocks %d and %d", covered, covered-2, covered-1)
		require.Equal(t, int64(covered), returned.Load())
	}
}

func TestFetchSession_SetDoneUnblocksAll(t *testing.T) {
	t.Parallel()

	const blockSize = fetchSessionBlockSize
	const numBlocks = 64

	cache := makeTestCacheForSession(t, numBlocks)
	s := newFetchSession(0, int64(numBlocks)*blockSize, cache)

	var returned atomic.Int64
	ch := make(chan error, numBlocks)

	for i := range numBlocks {
		go func(idx int) {
			err := s.registerAndWait(t.Context(), int64(idx)*blockSize)
			returned.Add(1)
			ch <- err
		}(i)
	}

	require.Equal(t, int64(0), returned.Load())

	s.setDone()

	for range numBlocks {
		require.NoError(t, <-ch)
	}

	require.Equal(t, int64(numBlocks), returned.Load())
}

func TestFetchSession_FailPropagatesError(t *testing.T) {
	t.Parallel()

	const blockSize = fetchSessionBlockSize

	cache := makeTestCacheForSession(t, 4)
	s := newFetchSession(0, 4*blockSize, cache)

	var returned atomic.Int64
	ch := make(chan error, 1)

	go func() {
		err := s.registerAndWait(t.Context(), 0)
		returned.Add(1)
		ch <- err
	}()

	require.Equal(t, int64(0), returned.Load())

	sentinel := errors.New("storage unavailable")
	s.fail(sentinel)

	require.ErrorIs(t, <-ch, sentinel)
	require.Equal(t, int64(1), returned.Load())
}

func TestFetchSession_ContextCancellation(t *testing.T) {
	t.Parallel()

	const blockSize = fetchSessionBlockSize

	cache := makeTestCacheForSession(t, 4)
	s := newFetchSession(0, 4*blockSize, cache)

	ctx, cancel := context.WithCancel(t.Context())

	var returned atomic.Int64
	ch := make(chan error, 1)

	go func() {
		err := s.registerAndWait(ctx, 0)
		returned.Add(1)
		ch <- err
	}()

	require.Equal(t, int64(0), returned.Load())

	cancel()

	require.ErrorIs(t, <-ch, context.Canceled)
	require.Equal(t, int64(1), returned.Load())
}

func TestFetchSession_CancelBetweenCheckAndWait(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		cache := makeTestCacheForSession(t, 4)
		s := newFetchSession(0, 4*fetchSessionBlockSize, cache)
		s.cond.L = &fetchSessionTestMutex{mu: &s.mu, token: make(chan struct{}, 1)}
		ctx := newCancelBetweenCheckAndWaitContext()
		result := make(chan error, 1)

		go func() { result <- s.registerAndWait(ctx, 0) }()

		<-ctx.errSampled
		ctx.cancel()
		// Err still holds the condition lock. An unlocked callback must finish
		// its real Broadcast before this returns; a locked callback cannot yet
		// take the token. No Cond.Wait has registered at this point.
		synctest.Wait()
		close(ctx.releaseErr)
		synctest.Wait()

		select {
		case err := <-result:
			require.ErrorIs(t, err, context.Canceled)
		default:
			// Settle the intentionally stranded pre-fix waiter so the test
			// leaves no goroutine or cache ownership behind after reporting RED.
			s.fail(context.Canceled)
			synctest.Wait()
			require.ErrorIs(t, <-result, context.Canceled)
			t.Fatal("context cancellation wakeup was lost between Err and Cond.Wait")
		}
	})
}

func TestFetchSession_TerminatedButCachedByPriorSession(t *testing.T) {
	t.Parallel()

	const blockSize = fetchSessionBlockSize

	cache := makeTestCacheForSession(t, 4)
	s := newFetchSession(0, 4*blockSize, cache)

	// Mark block 0 as cached externally (simulates a prior fetch session).
	cache.setIsCached(0, blockSize)

	// Fail this session — but block 0 is already in the cache.
	s.fail(errors.New("some error"))

	err := s.registerAndWait(t.Context(), 0)
	require.NoError(t, err)
}

func TestFetchSession_TerminatedNoErrorBlockNotCached(t *testing.T) {
	t.Parallel()

	const blockSize = fetchSessionBlockSize

	cache := makeTestCacheForSession(t, 4)
	s := newFetchSession(0, 4*blockSize, cache)

	// Manually put in a terminal state without setting bytesReady = chunkLen.
	// (setDone always sets bytesReady, so this is a defensive-code path.)
	s.mu.Lock()
	s.done = true
	s.mu.Unlock()
	s.cond.Broadcast()

	err := s.registerAndWait(t.Context(), 2*blockSize)
	require.Error(t, err)
	require.Contains(t, err.Error(), "terminated without error but block")
}

func TestFetchSession_FailIfRunning_NoOpAfterSetDone(t *testing.T) {
	t.Parallel()

	const blockSize = fetchSessionBlockSize

	cache := makeTestCacheForSession(t, 4)
	s := newFetchSession(0, 4*blockSize, cache)

	s.setDone()
	s.failIfRunning(errors.New("should be ignored"))

	// setDone set bytesReady = chunkLen, so all blocks are covered.
	require.NoError(t, s.registerAndWait(t.Context(), 0))
	require.NoError(t, s.registerAndWait(t.Context(), 3*blockSize))
	require.NoError(t, s.fetchErr)
}

func TestFetchSession_FailIfRunning_BeforeDone(t *testing.T) {
	t.Parallel()

	const blockSize = fetchSessionBlockSize

	cache := makeTestCacheForSession(t, 4)
	s := newFetchSession(0, 4*blockSize, cache)

	sentinel := errors.New("panic recovery")
	s.failIfRunning(sentinel)

	err := s.registerAndWait(t.Context(), 0)
	require.ErrorIs(t, err, sentinel)
}

func TestFetchSession_NonZeroChunkOffset(t *testing.T) {
	t.Parallel()

	const blockSize = fetchSessionBlockSize

	cache := makeTestCacheForSession(t, 8)

	chunkOff := 2 * blockSize            // chunk starts at block 2
	chunkLen := blockSize + 1000         // 1.24 blocks — not aligned, exercises min clamp
	lastBlockOff := chunkOff + blockSize // block 3
	s := newFetchSession(chunkOff, chunkLen, cache)

	var returned atomic.Int64
	ch := make(chan error, 2)

	go func() {
		err := s.registerAndWait(t.Context(), chunkOff) // block 2
		returned.Add(1)
		ch <- err
	}()

	go func() {
		// Block 3 extends past chunkOff+chunkLen, so endByte is clamped to chunkLen.
		// relEnd = lastBlockOff + blockSize - chunkOff = 2*blockSize = 8192
		// endByte = min(8192, 5096) = 5096 (chunkLen)
		err := s.registerAndWait(t.Context(), lastBlockOff)
		returned.Add(1)
		ch <- err
	}()

	require.Equal(t, int64(0), returned.Load())

	// Advance covers block 2 only.
	// relEnd for block 2 = chunkOff + blockSize - chunkOff = blockSize = 4096
	// endByte = min(4096, 5096) = 4096
	s.advance(blockSize)

	require.NoError(t, <-ch)
	require.Equal(t, int64(1), returned.Load())

	// Advance to chunkLen — enough for the partial last block.
	s.advance(chunkLen)

	require.NoError(t, <-ch)
	require.Equal(t, int64(2), returned.Load())
}

func TestFetchSession_ConcurrentWaitersAndCancel(t *testing.T) {
	t.Parallel()

	const blockSize = fetchSessionBlockSize

	cache := makeTestCacheForSession(t, 8)
	s := newFetchSession(0, 8*blockSize, cache)

	ctx, cancel := context.WithCancel(t.Context())

	var returned atomic.Int64

	var wg sync.WaitGroup

	// 4 waiters with cancellable context, 4 with background context.
	cancelErrs := make([]error, 4)
	bgErrs := make([]error, 4)

	for i := range 4 {
		wg.Add(2)

		go func(idx int) {
			defer wg.Done()
			cancelErrs[idx] = s.registerAndWait(ctx, int64(idx)*blockSize)
			returned.Add(1)
		}(i)

		go func(idx int) {
			defer wg.Done()
			bgErrs[idx] = s.registerAndWait(t.Context(), int64(idx+4)*blockSize)
			returned.Add(1)
		}(i)
	}

	require.Equal(t, int64(0), returned.Load())

	// Cancel the first group.
	cancel()

	// Complete the session for the second group.
	s.setDone()

	wg.Wait()

	require.Equal(t, int64(8), returned.Load())

	for i, err := range cancelErrs {
		// May have been cancelled OR completed — both are OK since setDone races with cancel.
		if err != nil {
			require.ErrorIs(t, err, context.Canceled, "cancel waiter %d", i)
		}
	}

	for i, err := range bgErrs {
		require.NoError(t, err, "bg waiter %d", i)
	}
}
