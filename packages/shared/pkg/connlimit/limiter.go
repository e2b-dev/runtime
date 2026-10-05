package connlimit

import (
	"sync/atomic"

	"github.com/e2b-dev/infra/packages/shared/pkg/smap"
)

// ConnectionLimiter tracks and limits per-key concurrent connections.
type ConnectionLimiter struct {
	connections *smap.Map[*atomic.Int64]
}

// NewConnectionLimiter creates a new ConnectionLimiter.
func NewConnectionLimiter() *ConnectionLimiter {
	return &ConnectionLimiter{
		connections: smap.New[*atomic.Int64](),
	}
}

// TryAcquire attempts to acquire a connection slot for a key.
// Returns (current count after increment, true) if successful, or (current count, false) if limit exceeded.
// If maxLimit is negative, no limit is enforced. If maxLimit is 0, all connections are blocked.
func (l *ConnectionLimiter) TryAcquire(key string, maxLimit int) (int64, bool) {
	if maxLimit == 0 {
		return l.Count(key), false
	}

	var count int64
	var acquired bool
	// Admission and idle-counter deletion must hold the same shard lock.
	l.connections.Upsert(key, &atomic.Int64{}, func(exists bool, counter, newCounter *atomic.Int64) *atomic.Int64 {
		if !exists {
			counter = newCounter
		}
		count = counter.Load()
		if maxLimit < 0 || count < int64(maxLimit) {
			count = counter.Add(1)
			acquired = true
		}

		return counter
	})

	return count, acquired
}

// Release decrements the connection count for a key.
func (l *ConnectionLimiter) Release(key string) {
	l.connections.RemoveCb(key, func(_ string, counter *atomic.Int64, exists bool) bool {
		if !exists {
			return false
		}
		if counter.Load() > 0 {
			counter.Add(-1)
		}

		return counter.Load() == 0
	})
}

// Remove removes a key entry entirely. Call when the key is no longer needed.
func (l *ConnectionLimiter) Remove(key string) {
	l.connections.Remove(key)
}

// Count returns the current connection count for a key.
func (l *ConnectionLimiter) Count(key string) int64 {
	if counter, ok := l.connections.Get(key); ok {
		return counter.Load()
	}

	return 0
}
