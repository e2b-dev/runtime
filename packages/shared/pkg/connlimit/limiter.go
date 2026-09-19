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

func (l *ConnectionLimiter) getCounter(key string) *atomic.Int64 {
	return l.connections.Upsert(key, &atomic.Int64{}, func(exists bool, valueInMap, newValue *atomic.Int64) *atomic.Int64 {
		if exists && valueInMap.Load() >= 0 {
			return valueInMap
		}

		return newValue
	})
}

// TryAcquire attempts to acquire a connection slot for a key.
// Returns (current count after increment, true) if successful, or (current count, false) if limit exceeded.
// If maxLimit is negative, no limit is enforced. If maxLimit is 0, all connections are blocked.
func (l *ConnectionLimiter) TryAcquire(key string, maxLimit int) (int64, bool) {
	if maxLimit == 0 {
		return l.Count(key), false
	}

	for {
		counter := l.getCounter(key)
		current := counter.Load()
		if current < 0 {
			continue
		}
		if maxLimit >= 0 && current >= int64(maxLimit) {
			return current, false
		}
		if counter.CompareAndSwap(current, current+1) {
			return current + 1, true
		}
	}
}

// Release decrements the connection count for a key.
func (l *ConnectionLimiter) Release(key string) {
	counter, ok := l.connections.Get(key)
	if !ok {
		return
	}

	for {
		current := counter.Load()
		if current <= 0 {
			return
		}
		if counter.CompareAndSwap(current, current-1) {
			if current-1 == 0 {
				if counter.CompareAndSwap(0, -1) {
					l.connections.RemoveCb(key, func(k string, v *atomic.Int64, exists bool) bool {
						return exists && v == counter
					})
				}
			}

			return
		}
	}
}

// Remove removes a key entry entirely. Call when the key is no longer needed.
func (l *ConnectionLimiter) Remove(key string) {
	l.connections.Remove(key)
}

// Count returns the current connection count for a key.
func (l *ConnectionLimiter) Count(key string) int64 {
	if counter, ok := l.connections.Get(key); ok {
		if count := counter.Load(); count > 0 {
			return count
		}
	}

	return 0
}
