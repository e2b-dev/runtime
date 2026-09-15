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

	t.Run("zero limit blocks all connections without retaining map entry", func(t *testing.T) {
		t.Parallel()
		limiter := NewConnectionLimiter()

		count, ok := limiter.TryAcquire("sandbox1", 0)
		assert.False(t, ok)
		assert.Equal(t, int64(0), count)
		assert.Equal(t, int64(0), limiter.Count("sandbox1"))
		assert.Equal(t, 0, limiter.connections.Count())
	})

	t.Run("zero limit across multiple keys leaves zero retained map entries", func(t *testing.T) {
		t.Parallel()
		limiter := NewConnectionLimiter()

		for i := range 100 {
			key := "blocked-key-" + string(rune(i))
			count, ok := limiter.TryAcquire(key, 0)
			assert.False(t, ok)
			assert.Equal(t, int64(0), count)
		}

		assert.Equal(t, 0, limiter.connections.Count())
	})

	t.Run("zero limit on existing connection reports current count without modifying", func(t *testing.T) {
		t.Parallel()
		limiter := NewConnectionLimiter()

		count, ok := limiter.TryAcquire("sandbox1", 5)
		assert.True(t, ok)
		assert.Equal(t, int64(1), count)

		count, ok = limiter.TryAcquire("sandbox1", 0)
		assert.False(t, ok)
		assert.Equal(t, int64(1), count)
		assert.Equal(t, 1, limiter.connections.Count())

		limiter.Release("sandbox1")
		assert.Equal(t, 0, limiter.connections.Count())
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

	t.Run("release removes map entry when count reaches zero", func(t *testing.T) {
		t.Parallel()
		limiter := NewConnectionLimiter()

		_, ok := limiter.TryAcquire("sandbox1", 5)
		assert.True(t, ok)
		assert.Equal(t, 1, limiter.connections.Count())

		limiter.Release("sandbox1")
		assert.Equal(t, int64(0), limiter.Count("sandbox1"))
		assert.Equal(t, 0, limiter.connections.Count())
	})

	t.Run("acquiring after zero count cleanup starts fresh", func(t *testing.T) {
		t.Parallel()
		limiter := NewConnectionLimiter()

		limiter.TryAcquire("sandbox1", 5)
		limiter.Release("sandbox1")
		assert.Equal(t, 0, limiter.connections.Count())

		count, ok := limiter.TryAcquire("sandbox1", 5)
		assert.True(t, ok)
		assert.Equal(t, int64(1), count)
		assert.Equal(t, 1, limiter.connections.Count())
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
	})

	t.Run("concurrent acquire during release does not orphan connections", func(t *testing.T) {
		t.Parallel()
		limiter := NewConnectionLimiter()
		const limit = 100
		const goroutines = 50
		const iterations = 500

		var wg sync.WaitGroup
		for range goroutines {
			wg.Go(func() {
				for range iterations {
					if _, ok := limiter.TryAcquire("sandbox-race", limit); ok {
						limiter.Release("sandbox-race")
					}
				}
			})
		}

		wg.Wait()

		assert.Equal(t, int64(0), limiter.Count("sandbox-race"))
		assert.Equal(t, 0, limiter.connections.Count())
	})
}
