//go:build linux

package block

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

type fetchSession struct {
	// chunk is what we are fetching, can be >= 1 block. chunkOff/chunkLen are absolute offsets in U-space.
	chunkOff int64
	chunkLen int64
	cache    *Cache

	mu   sync.Mutex
	cond sync.Cond // broadcast on progress; lazily initialized with mu

	fetchErr error
	done     bool // true once terminated (success or error)

	// source is the backend that served the fetch; set by runFetch.
	source atomic.Int32

	// bytesReady is the byte count (from chunkOff) up to which all blocks
	// are fully written and marked cached. Atomic so registerAndWait can
	// do a lock-free fast-path check: bytesReady only increases.
	bytesReady atomic.Int64
}

func (s *fetchSession) setSource(src storage.Source) {
	s.source.Store(int32(src))
}

func (s *fetchSession) Source() storage.Source {
	return storage.Source(s.source.Load())
}

// contains reports whether the session covers the byte range [off, off+length).
func (s *fetchSession) contains(off, length int64) bool {
	return s.chunkOff <= off && s.chunkOff+s.chunkLen >= off+length
}

// terminated reports whether the session reached a terminal state.
// Must be called with mu held.
func (s *fetchSession) terminated() bool {
	return s.done
}

func newFetchSession(chunkOff, chunkLen int64, cache *Cache) *fetchSession {
	s := &fetchSession{
		chunkOff: chunkOff,
		chunkLen: chunkLen,
		cache:    cache,
	}
	s.cond.L = &s.mu

	return s
}

// registerAndWait blocks until the block at blockOff is cached, the session
// terminates, or ctx is cancelled. Each caller requests exactly one block.
func (s *fetchSession) registerAndWait(ctx context.Context, blockOff int64) error {
	blockSize := s.cache.blockSize

	if blockOff%blockSize != 0 {
		return fmt.Errorf("blockOff %d is not aligned to block size %d", blockOff, blockSize)
	}

	if blockOff < s.chunkOff || blockOff >= s.chunkOff+s.chunkLen {
		return fmt.Errorf("blockOff %d is outside session range [%d, %d)", blockOff, s.chunkOff, s.chunkOff+s.chunkLen)
	}

	// endByte is the byte offset (relative to chunkOff) that must be ready
	// for our block to be fully written.
	relEnd := blockOff + blockSize - s.chunkOff
	endByte := min(relEnd, s.chunkLen)

	// Lock-free fast path: bytesReady only increases, so >= endByte
	// guarantees data is available.
	if s.bytesReady.Load() >= endByte {
		return nil
	}

	// Serialize the cancellation wake with the predicate check and Cond.Wait
	// registration. Cond.Wait atomically unlocks mu only after the waiter is
	// registered, so the callback cannot spend the sole cancellation Broadcast
	// in the check-to-wait gap.
	stop := context.AfterFunc(ctx, func() {
		s.cond.L.Lock()
		defer s.cond.L.Unlock()
		s.cond.Broadcast()
	})
	defer stop()

	s.cond.L.Lock()
	defer s.cond.L.Unlock()

	for {
		if s.bytesReady.Load() >= endByte {
			return nil
		}

		// Terminal but block not covered — only happens on error.
		// setDone sets bytesReady=chunkLen, so terminated() with bytesReady < endByte
		// means fetchErr != nil. Check cache in case a prior session already fetched this block.
		if s.terminated() {
			// isCached reads an atomic bitset — safe to call under mu.
			if s.cache.isCached(blockOff, blockSize) {
				return nil
			}

			if s.fetchErr == nil {
				return fmt.Errorf("fetch session terminated without error but block %d not cached (bytesReady=%d, endByte=%d)",
					blockOff/blockSize, s.bytesReady.Load(), endByte)
			}

			return fmt.Errorf("fetch failed: %w", s.fetchErr)
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}

		s.cond.Wait()
	}
}

// advance updates progress and wakes all waiters.
func (s *fetchSession) advance(bytesReady int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.bytesReady.Store(bytesReady)
	s.cond.Broadcast()
}

// setDone marks the session as successfully completed and wakes all waiters.
func (s *fetchSession) setDone() {
	s.mu.Lock()
	s.bytesReady.Store(s.chunkLen)
	s.done = true
	s.mu.Unlock()

	s.cond.Broadcast()
}

// fail records the error unconditionally and wakes all waiters.
func (s *fetchSession) fail(err error) {
	s.mu.Lock()
	s.fetchErr = err
	s.done = true
	s.mu.Unlock()

	s.cond.Broadcast()
}

// failIfRunning records the error only if the session has not already
// terminated — used in panic recovery and safety-net defers to avoid
// overriding a successful completion. Always broadcasts to ensure no
// waiter blocks forever.
func (s *fetchSession) failIfRunning(err error) {
	s.mu.Lock()
	if !s.terminated() {
		s.fetchErr = err
		s.done = true
	}
	s.mu.Unlock()

	s.cond.Broadcast()
}
