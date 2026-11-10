package connlimit

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConnectionLimiter_TryAcquire(t *testing.T) {
	t.Parallel()

	t.Run("acquires up to limit", func(t *testing.T) {
		t.Parallel()
		limiter := NewConnectionLimiter()

		count, ok := limiter.TryAcquire("sandbox1", 3)
		assert.True(t, ok)
		assert.Equal(t, int64(1), count)

		count, ok = limiter.TryAcquire("sandbox1", 3)
		assert.True(t, ok)
		assert.Equal(t, int64(2), count)

		count, ok = limiter.TryAcquire("sandbox1", 3)
		assert.True(t, ok)
		assert.Equal(t, int64(3), count)
	})

	t.Run("rejects when at limit", func(t *testing.T) {
		t.Parallel()
		limiter := NewConnectionLimiter()

		for range 5 {
			limiter.TryAcquire("sandbox1", 5)
		}

		count, ok := limiter.TryAcquire("sandbox1", 5)
		assert.False(t, ok)
		assert.Equal(t, int64(5), count)
	})

	t.Run("zero limit blocks all connections", func(t *testing.T) {
		t.Parallel()
		limiter := NewConnectionLimiter()

		count, ok := limiter.TryAcquire("sandbox1", 0)
		assert.False(t, ok)
		assert.Equal(t, int64(0), count)
		assert.Equal(t, int64(0), limiter.Count("sandbox1"))
	})

	t.Run("negative limit means no limit", func(t *testing.T) {
		t.Parallel()
		limiter := NewConnectionLimiter()

		for range 100 {
			_, ok := limiter.TryAcquire("sandbox1", -1)
			assert.True(t, ok)
		}
		assert.Equal(t, int64(100), limiter.Count("sandbox1"))
	})

	t.Run("separate sandboxes have separate limits", func(t *testing.T) {
		t.Parallel()
		limiter := NewConnectionLimiter()

		for range 3 {
			limiter.TryAcquire("sandbox1", 3)
		}

		count, ok := limiter.TryAcquire("sandbox2", 3)
		assert.True(t, ok)
		assert.Equal(t, int64(1), count)
	})
}

func TestConnectionLimiter_Release(t *testing.T) {
	t.Parallel()

	t.Run("release allows new acquire", func(t *testing.T) {
		t.Parallel()
		limiter := NewConnectionLimiter()

		for range 3 {
			limiter.TryAcquire("sandbox1", 3)
		}

		_, ok := limiter.TryAcquire("sandbox1", 3)
		assert.False(t, ok)

		limiter.Release("sandbox1")

		_, ok = limiter.TryAcquire("sandbox1", 3)
		assert.True(t, ok)
	})

	t.Run("release on unknown sandbox is safe", func(t *testing.T) {
		t.Parallel()
		limiter := NewConnectionLimiter()
		limiter.Release("unknown")
	})

	t.Run("double release does not go negative", func(t *testing.T) {
		t.Parallel()
		limiter := NewConnectionLimiter()

		limiter.TryAcquire("sandbox1", 10)
		assert.Equal(t, int64(1), limiter.Count("sandbox1"))

		limiter.Release("sandbox1")
		assert.Equal(t, int64(0), limiter.Count("sandbox1"))

		limiter.Release("sandbox1")
		assert.Equal(t, int64(0), limiter.Count("sandbox1"))

		limiter.Release("sandbox1")
		assert.Equal(t, int64(0), limiter.Count("sandbox1"))
	})
}

func TestConnectionLimiter_Remove(t *testing.T) {
	t.Parallel()

	t.Run("remove clears sandbox entry", func(t *testing.T) {
		t.Parallel()
		limiter := NewConnectionLimiter()

		limiter.TryAcquire("sandbox1", 10)
		limiter.TryAcquire("sandbox1", 10)
		assert.Equal(t, int64(2), limiter.Count("sandbox1"))

		limiter.Remove("sandbox1")
		assert.Equal(t, int64(0), limiter.Count("sandbox1"))
	})

	t.Run("remove on unknown sandbox is safe", func(t *testing.T) {
		t.Parallel()
		limiter := NewConnectionLimiter()
		limiter.Remove("unknown")
	})

	t.Run("acquire after remove starts fresh", func(t *testing.T) {
		t.Parallel()
		limiter := NewConnectionLimiter()

		for range 5 {
			limiter.TryAcquire("sandbox1", 5)
		}
		assert.Equal(t, int64(5), limiter.Count("sandbox1"))

		limiter.Remove("sandbox1")

		count, ok := limiter.TryAcquire("sandbox1", 5)
		assert.True(t, ok)
		assert.Equal(t, int64(1), count)
	})
}

func TestConnectionLimiter_Count(t *testing.T) {
	t.Parallel()

	limiter := NewConnectionLimiter()
	assert.Equal(t, int64(0), limiter.Count("sandbox1"))

	limiter.TryAcquire("sandbox1", 10)
	assert.Equal(t, int64(1), limiter.Count("sandbox1"))

	limiter.TryAcquire("sandbox1", 10)
	assert.Equal(t, int64(2), limiter.Count("sandbox1"))

	limiter.Release("sandbox1")
	assert.Equal(t, int64(1), limiter.Count("sandbox1"))
}

func TestConnectionLimiter_Concurrent(t *testing.T) {
	t.Parallel()

	t.Run("concurrent acquires respect limit", func(t *testing.T) {
		t.Parallel()
		limiter := NewConnectionLimiter()
		const limit = 50
		const goroutines = 100

		var wg sync.WaitGroup
		acquired := make(chan bool, goroutines)

		for range goroutines {
			wg.Go(func() {
				_, ok := limiter.TryAcquire("sandbox1", limit)
				acquired <- ok
			})
		}

		wg.Wait()
		close(acquired)

		successCount := 0
		for ok := range acquired {
			if ok {
				successCount++
			}
		}

		assert.Equal(t, limit, successCount)
		assert.Equal(t, int64(limit), limiter.Count("sandbox1"))
	})

	t.Run("concurrent acquire and release", func(t *testing.T) {
		t.Parallel()
		limiter := NewConnectionLimiter()
		const limit = 10
		const iterations = 1000

		var wg sync.WaitGroup

		for range iterations {
			wg.Go(func() {
				if _, ok := limiter.TryAcquire("sandbox1", limit); ok {
					limiter.Release("sandbox1")
				}
			})
		}

		wg.Wait()
		assert.Zero(t, limiter.Count("sandbox1"))
		assert.Zero(t, limiter.connections.Count(), "last release removes idle counters")
	})
}

func TestConnectionLimiter_LateAdmissionAfterRemoval(t *testing.T) {
	t.Parallel()
	limiter := NewConnectionLimiter()
	limiter.TryAcquire("lifecycle", 1)
	limiter.Remove("lifecycle")
	_, acquired := limiter.TryAcquire("lifecycle", 1)
	assert.True(t, acquired)
	limiter.Release("lifecycle")
	assert.Zero(t, limiter.connections.Count())
	limiter.TryAcquire("blocked", 0)
	assert.Zero(t, limiter.connections.Count())
}
