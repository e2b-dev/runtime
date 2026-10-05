package utils

import (
	"context"
	"fmt"
	"sync"
)

type Semaphore interface {
	Acquire(ctx context.Context, n int64) error
	TryAcquire(n int64) bool
	Release(n int64)
}

type AdjustableSemaphore struct {
	mu   sync.Mutex
	cond *sync.Cond

	used  int64
	limit int64
}

func NewAdjustableSemaphore(limit int64) (*AdjustableSemaphore, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("NewAdjustableSemaphore: limit must be > 0, got: %d", limit)
	}

	as := &AdjustableSemaphore{limit: limit}
	as.cond = sync.NewCond(&as.mu)

	return as, nil
}

func (s *AdjustableSemaphore) Acquire(ctx context.Context, n int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if n <= 0 {
		return fmt.Errorf("acquiring less than or equal to 0 elements is not supported, got: %d", n)
	}

	// Hold the lock so cancellation cannot broadcast between ctx.Err and Wait.
	stop := context.AfterFunc(ctx, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.cond.Broadcast()
	})
	defer stop() // ensure we don’t leak the callback

	for s.used+n > s.limit {
		if err := ctx.Err(); err != nil { // ctx already cancelled?
			return err
		}

		// Wait for change in semaphore state
		s.cond.Wait()
	}

	s.used += n

	return nil
}

func (s *AdjustableSemaphore) TryAcquire(n int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if n <= 0 {
		return false
	}

	if s.used+n > s.limit {
		return false
	}

	s.used += n

	return true
}

func (s *AdjustableSemaphore) SetLimit(limit int64) error {
	if limit <= 0 {
		return fmt.Errorf("SetLimit: limit must be > 0, got: %d", limit)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if limit == s.limit {
		return nil // no change
	}

	s.limit = limit
	s.cond.Broadcast()

	return nil
}

func (s *AdjustableSemaphore) Release(n int64) {
	if n <= 0 {
		panic("Release: n must be > 0")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.used < n {
		panic("Release: cannot release more than acquired")
	}

	s.used -= n
	s.cond.Broadcast()
}
