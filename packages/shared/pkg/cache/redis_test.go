package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

type testValue struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

func newTestRedisCache(t *testing.T, redisClient redis.UniversalClient) *RedisCache[testValue] {
	t.Helper()

	return NewRedisCache[testValue](RedisConfig[testValue]{
		TTL:         30 * time.Second,
		RedisClient: redisClient,
		RedisPrefix: fmt.Sprintf("test:%s", t.Name()),
	})
}

func TestRedisNoPTTL_MatchesRedisBehavior(t *testing.T) {
	t.Parallel()
	redisClient := redis_utils.SetupInstance(t)

	// Set a key with no expiration
	err := redisClient.Set(t.Context(), "test:no-ttl", "value", 0).Err()
	require.NoError(t, err)

	pttl := redisClient.PTTL(t.Context(), "test:no-ttl").Val()
	assert.Equal(t, redisNoPTTL, pttl, "redisNoPTTL constant should match PTTL result for a key with no expiration")
}

func TestRedisCache_RedisHit(t *testing.T) {
	t.Parallel()
	redisClient := redis_utils.SetupInstance(t)
	rc := newTestRedisCache(t, redisClient)
	defer rc.Close(t.Context())

	key := "key1"
	expected := testValue{ID: "2", Name: "Bob"}

	// Store directly in Redis
	data, err := json.Marshal(expected)
	require.NoError(t, err)
	err = redisClient.Set(t.Context(), rc.RedisKey(key), data, 30*time.Second).Err()
	require.NoError(t, err)

	callbackCalled := false
	result, err := rc.GetOrSet(t.Context(), key, func(_ context.Context, _ string) (testValue, error) {
		callbackCalled = true

		return testValue{}, nil
	})

	require.NoError(t, err)
	assert.Equal(t, expected, result)
	assert.False(t, callbackCalled, "callback should not be called on Redis hit")
}

func TestRedisCache_CallbackFallback(t *testing.T) {
	t.Parallel()
	redisClient := redis_utils.SetupInstance(t)
	rc := newTestRedisCache(t, redisClient)
	defer rc.Close(t.Context())

	key := "key1"
	expected := testValue{ID: "3", Name: "Charlie"}

	result, err := rc.GetOrSet(t.Context(), key, func(_ context.Context, _ string) (testValue, error) {
		return expected, nil
	})

	require.NoError(t, err)
	assert.Equal(t, expected, result)

	// Verify Redis was populated
	data, err := redisClient.Get(t.Context(), rc.RedisKey(key)).Bytes()
	require.NoError(t, err)

	var redisValue testValue
	err = json.Unmarshal(data, &redisValue)
	require.NoError(t, err)
	assert.Equal(t, expected, redisValue)
}

func TestRedisCache_RedisErrorFallthrough(t *testing.T) {
	t.Parallel()
	// Use a client pointing to a non-existent Redis
	badClient := redis.NewClient(&redis.Options{
		Addr:        "127.0.0.1:1", // invalid port, use IP to avoid DNS lookup
		DialTimeout: 100 * time.Millisecond,
	})
	defer badClient.Close()

	rc := NewRedisCache[testValue](RedisConfig[testValue]{
		TTL:          30 * time.Second,
		RedisClient:  badClient,
		RedisPrefix:  "test:bad",
		RedisTimeout: 200 * time.Millisecond,
	})
	defer rc.Close(t.Context())

	expected := testValue{ID: "4", Name: "Diana"}

	result, err := rc.GetOrSet(t.Context(), "key1", func(_ context.Context, _ string) (testValue, error) {
		return expected, nil
	})

	require.NoError(t, err)
	assert.Equal(t, expected, result)
}

func TestRedisCache_Delete(t *testing.T) {
	t.Parallel()
	redisClient := redis_utils.SetupInstance(t)
	rc := newTestRedisCache(t, redisClient)
	defer rc.Close(t.Context())

	key := "key1"
	value := testValue{ID: "5", Name: "Eve"}

	// Populate Redis
	rc.Set(t.Context(), key, value)

	// Verify populated
	_, err := redisClient.Get(t.Context(), rc.RedisKey(key)).Bytes()
	require.NoError(t, err)

	// Delete
	rc.Delete(t.Context(), key)

	// Verify cleared
	_, err = redisClient.Get(t.Context(), rc.RedisKey(key)).Result()
	assert.ErrorIs(t, err, redis.Nil)
}

func TestRedisCache_TryDeleteReportsAFailedEviction(t *testing.T) {
	t.Parallel()
	redisClient := redis_utils.SetupInstance(t)
	rc := newTestRedisCache(t, redisClient)
	defer rc.Close(t.Context())

	rc.Set(t.Context(), "key1", testValue{ID: "5", Name: "Eve"})
	require.NoError(t, rc.TryDelete(t.Context(), "key1"))

	unreachable := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: -1})
	t.Cleanup(func() { _ = unreachable.Close() })
	broken := NewRedisCache[testValue](RedisConfig[testValue]{
		TTL: 30 * time.Second, RedisClient: unreachable, RedisPrefix: t.Name(),
		RedisTimeout: 200 * time.Millisecond, LockTTL: RedisLockOff,
	})
	defer broken.Close(t.Context())

	require.Error(t, broken.TryDelete(t.Context(), "key1"))
}

// A writer (GetOrSet backfill) holds the per-key lock across its SET. Delete
// must wait for that lock — even past the 5s acquire timeout used by writers —
// so the DEL is ordered after the in-flight write and stale data cannot be
// repopulated for a full TTL (e.g. a revoked API key resurrected into the auth
// cache).
func TestRedisCache_DeleteWaitsForInflightWriter(t *testing.T) {
	t.Parallel()
	redisClient := redis_utils.SetupInstance(t)
	rc := newTestRedisCache(t, redisClient)
	defer rc.Close(t.Context())

	key := "key1"
	callbackStarted := make(chan struct{})
	writerDone := make(chan struct{})

	var eg errgroup.Group
	eg.Go(func() error {
		defer close(writerDone)
		_, err := rc.GetOrSet(t.Context(), key, func(_ context.Context, _ string) (testValue, error) {
			close(callbackStarted)
			// Hold the write lock longer than the acquire timeout writers use,
			// simulating a callback stalled on a slow backing store.
			time.Sleep(acquireLockTimeout + time.Second)

			return testValue{ID: "9", Name: "stale"}, nil
		})

		return err
	})

	<-callbackStarted
	rc.Delete(t.Context(), key)

	// Delete must have waited for the writer's lock, so by the time it
	// returns the writer's SET has already happened and been removed.
	select {
	case <-writerDone:
	default:
		t.Fatal("Delete returned while the writer still held the lock; the writer's SET could repopulate stale data")
	}

	require.NoError(t, eg.Wait())

	_, err := redisClient.Get(t.Context(), rc.RedisKey(key)).Result()
	assert.ErrorIs(t, err, redis.Nil, "the DEL must be ordered after the in-flight writer's SET")
}

func TestRedisCache_SetWritesRedis(t *testing.T) {
	t.Parallel()
	redisClient := redis_utils.SetupInstance(t)
	rc := newTestRedisCache(t, redisClient)
	defer rc.Close(t.Context())

	key := "key1"
	value := testValue{ID: "7", Name: "Grace"}

	rc.Set(t.Context(), key, value)

	// Verify Redis
	data, err := redisClient.Get(t.Context(), rc.RedisKey(key)).Bytes()
	require.NoError(t, err)

	var redisValue testValue
	err = json.Unmarshal(data, &redisValue)
	require.NoError(t, err)
	assert.Equal(t, value, redisValue)
}

func TestRedisCache_Singleflight(t *testing.T) {
	t.Parallel()
	redisClient := redis_utils.SetupInstance(t)
	rc := newTestRedisCache(t, redisClient)
	defer rc.Close(t.Context())

	var callCount atomic.Int32
	expected := testValue{ID: "8", Name: "Hank"}
	callback := func(_ context.Context, _ string) (testValue, error) { //nolint:unparam
		time.Sleep(100 * time.Millisecond)
		callCount.Add(1)

		return expected, nil
	}

	var wg errgroup.Group
	results := make([]testValue, 10)
	for i := range 10 {
		wg.Go(func() error {
			val, err := rc.GetOrSet(t.Context(), "key1", callback)
			if err != nil {
				return err
			}
			results[i] = val

			return nil
		})
	}

	err := wg.Wait()
	require.NoError(t, err)

	// All should return the same value
	for i, result := range results {
		assert.Equal(t, expected, result, "result %d mismatch", i)
	}

	// Callback should only be called once due to singleflight
	assert.Equal(t, int32(1), callCount.Load())
}

func TestRedisCache_RedisRefresh_TriggeredWhenStale(t *testing.T) {
	t.Parallel()
	redisClient := redis_utils.SetupInstance(t)

	redisTTL := 10 * time.Second
	refreshInterval := 100 * time.Millisecond

	rc := NewRedisCache[testValue](RedisConfig[testValue]{
		TTL:             redisTTL,
		RefreshInterval: refreshInterval,
		RefreshTimeout:  5 * time.Second,
		RedisClient:     redisClient,
		RedisPrefix:     fmt.Sprintf("test:%s", t.Name()),
	})
	defer rc.Close(t.Context())

	key := "key1"
	staleValue := testValue{ID: "stale", Name: "StaleData"}
	freshValue := testValue{ID: "fresh", Name: "FreshData"}

	// Populate Redis with a value that has a short remaining TTL (simulating age)
	data, err := json.Marshal(staleValue)
	require.NoError(t, err)
	// Set with a TTL such that age = redisTTL - remainingTTL > refreshInterval
	remainingTTL := redisTTL - refreshInterval - 50*time.Millisecond
	err = redisClient.Set(t.Context(), rc.RedisKey(key), data, remainingTTL).Err()
	require.NoError(t, err)

	var callCount atomic.Int32
	result, err := rc.GetOrSet(t.Context(), key, func(_ context.Context, _ string) (testValue, error) {
		callCount.Add(1)

		return freshValue, nil
	})

	require.NoError(t, err)
	// Should return the stale value immediately
	assert.Equal(t, staleValue, result)

	// Wait for background refresh to complete
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		redisData, err := redisClient.Get(t.Context(), rc.RedisKey(key)).Bytes()
		if !assert.NoError(c, err) {
			return
		}
		var redisValue testValue
		err = json.Unmarshal(redisData, &redisValue)
		assert.NoError(c, err)
		assert.Equal(c, freshValue, redisValue)
	}, 2*time.Second, 50*time.Millisecond)

	assert.GreaterOrEqual(t, callCount.Load(), int32(1))
}

func TestRedisCache_RedisRefresh_UpdatesRedis(t *testing.T) {
	t.Parallel()
	redisClient := redis_utils.SetupInstance(t)

	redisTTL := 10 * time.Second
	refreshInterval := 100 * time.Millisecond

	rc := NewRedisCache[testValue](RedisConfig[testValue]{
		TTL:             redisTTL,
		RefreshInterval: refreshInterval,
		RefreshTimeout:  5 * time.Second,
		RedisClient:     redisClient,
		RedisPrefix:     fmt.Sprintf("test:%s", t.Name()),
	})
	defer rc.Close(t.Context())

	key := "key1"
	staleValue := testValue{ID: "stale", Name: "StaleData"}
	freshValue := testValue{ID: "fresh", Name: "FreshData"}

	// Populate Redis with a stale entry
	data, err := json.Marshal(staleValue)
	require.NoError(t, err)
	remainingTTL := redisTTL - refreshInterval - 100*time.Millisecond
	err = redisClient.Set(t.Context(), rc.RedisKey(key), data, remainingTTL).Err()
	require.NoError(t, err)

	_, err = rc.GetOrSet(t.Context(), key, func(_ context.Context, _ string) (testValue, error) {
		return freshValue, nil
	})
	require.NoError(t, err)

	// Wait for background refresh
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		redisData, err := redisClient.Get(t.Context(), rc.RedisKey(key)).Bytes()
		if !assert.NoError(c, err) {
			return
		}
		var redisValue testValue
		err = json.Unmarshal(redisData, &redisValue)
		assert.NoError(c, err)
		assert.Equal(c, freshValue, redisValue)
	}, 2*time.Second, 50*time.Millisecond)
}

func TestRedisCache_RedisRefresh_ErrorKeepsStaleValue(t *testing.T) {
	t.Parallel()
	redisClient := redis_utils.SetupInstance(t)

	redisTTL := 10 * time.Second
	refreshInterval := 100 * time.Millisecond

	rc := NewRedisCache[testValue](RedisConfig[testValue]{
		TTL:             redisTTL,
		RefreshInterval: refreshInterval,
		RefreshTimeout:  5 * time.Second,
		RedisClient:     redisClient,
		RedisPrefix:     fmt.Sprintf("test:%s", t.Name()),
	})
	defer rc.Close(t.Context())

	key := "key1"
	staleValue := testValue{ID: "stale", Name: "StaleData"}

	// Populate Redis with a stale entry
	data, err := json.Marshal(staleValue)
	require.NoError(t, err)
	remainingTTL := redisTTL - refreshInterval - 50*time.Millisecond
	err = redisClient.Set(t.Context(), rc.RedisKey(key), data, remainingTTL).Err()
	require.NoError(t, err)

	var callCount atomic.Int32
	result, err := rc.GetOrSet(t.Context(), key, func(_ context.Context, _ string) (testValue, error) {
		callCount.Add(1)

		return testValue{}, errors.New("database unavailable")
	})

	require.NoError(t, err)
	assert.Equal(t, staleValue, result)

	// Wait for background refresh attempt to complete
	time.Sleep(200 * time.Millisecond)

	// Redis should still have the stale value (not deleted)
	redisData, err := redisClient.Get(t.Context(), rc.RedisKey(key)).Bytes()
	require.NoError(t, err)
	var redisValue testValue
	err = json.Unmarshal(redisData, &redisValue)
	require.NoError(t, err)
	assert.Equal(t, staleValue, redisValue)

	assert.GreaterOrEqual(t, callCount.Load(), int32(1))
}

func TestRedisCache_RedisRefresh_Disabled(t *testing.T) {
	t.Parallel()
	redisClient := redis_utils.SetupInstance(t)

	// No RedisRefreshInterval set (defaults to 0)
	rc := newTestRedisCache(t, redisClient)
	defer rc.Close(t.Context())

	key := "key1"
	expected := testValue{ID: "no-refresh", Name: "NoRefresh"}

	// Store in Redis
	data, err := json.Marshal(expected)
	require.NoError(t, err)
	err = redisClient.Set(t.Context(), rc.RedisKey(key), data, 30*time.Second).Err()
	require.NoError(t, err)

	var callCount atomic.Int32
	result, err := rc.GetOrSet(t.Context(), key, func(_ context.Context, _ string) (testValue, error) {
		callCount.Add(1)

		return testValue{ID: "different", Name: "Different"}, nil
	})

	require.NoError(t, err)
	assert.Equal(t, expected, result)

	// Wait to make sure no background refresh happens
	time.Sleep(200 * time.Millisecond)

	// Callback should not have been called (Redis hit, no refresh)
	assert.Equal(t, int32(0), callCount.Load())
}

func TestRedisCache_DeleteByPrefix(t *testing.T) {
	t.Parallel()
	redisClient := redis_utils.SetupInstance(t)
	rc := newTestRedisCache(t, redisClient)
	defer rc.Close(t.Context())

	// Populate keys with a shared prefix and one without
	rc.Set(t.Context(), "team1:sandbox-a", testValue{ID: "a", Name: "A"})
	rc.Set(t.Context(), "team1:sandbox-b", testValue{ID: "b", Name: "B"})
	rc.Set(t.Context(), "team2:sandbox-c", testValue{ID: "c", Name: "C"})

	// Delete by prefix
	deleted := rc.DeleteByPrefix(t.Context(), "team1:")

	// Should have deleted exactly the two team1 keys
	assert.Len(t, deleted, 2)
	assert.ElementsMatch(t, []string{"team1:sandbox-a", "team1:sandbox-b"}, deleted)

	// Verify team1 keys are gone
	_, err := redisClient.Get(t.Context(), rc.RedisKey("team1:sandbox-a")).Result()
	require.ErrorIs(t, err, redis.Nil)
	_, err = redisClient.Get(t.Context(), rc.RedisKey("team1:sandbox-b")).Result()
	require.ErrorIs(t, err, redis.Nil)

	// Verify team2 key still exists
	_, err = redisClient.Get(t.Context(), rc.RedisKey("team2:sandbox-c")).Result()
	assert.NoError(t, err)
}

func TestRedisCache_DeleteByPrefix_NoMatches(t *testing.T) {
	t.Parallel()
	redisClient := redis_utils.SetupInstance(t)
	rc := newTestRedisCache(t, redisClient)
	defer rc.Close(t.Context())

	rc.Set(t.Context(), "key1", testValue{ID: "1", Name: "One"})

	deleted := rc.DeleteByPrefix(t.Context(), "nonexistent:")

	assert.Empty(t, deleted)

	// Original key untouched
	_, err := redisClient.Get(t.Context(), rc.RedisKey("key1")).Result()
	assert.NoError(t, err)
}

func newTestRedisCacheWithLock(t *testing.T, redisClient redis.UniversalClient) *RedisCache[testValue] {
	t.Helper()

	return NewRedisCache(RedisConfig[testValue]{
		TTL:         30 * time.Second,
		RedisClient: redisClient,
		RedisPrefix: fmt.Sprintf("test:%s", t.Name()),
	})
}

func TestRedisCache_Lock_PreventsParallelCallbacks(t *testing.T) {
	t.Parallel()
	redisClient := redis_utils.SetupInstance(t)

	// Two independent cache instances (simulating two service instances) sharing the same Redis
	rc1 := newTestRedisCacheWithLock(t, redisClient)
	defer rc1.Close(t.Context())
	rc2 := newTestRedisCacheWithLock(t, redisClient)
	defer rc2.Close(t.Context())

	var callCount atomic.Int32
	expected := testValue{ID: "locked", Name: "LockedValue"}
	callback := func(_ context.Context, _ string) (testValue, error) { //nolint:unparam
		callCount.Add(1)
		// Simulate slow data fetch so the second instance has time to acquire (and wait for) the lock
		time.Sleep(200 * time.Millisecond)

		return expected, nil
	}

	var wg errgroup.Group
	var result1, result2 testValue
	wg.Go(func() error {
		v, err := rc1.GetOrSet(t.Context(), "key1", callback)
		result1 = v

		return err
	})
	wg.Go(func() error {
		// Small delay to ensure rc1 acquires the lock first
		time.Sleep(20 * time.Millisecond)
		v, err := rc2.GetOrSet(t.Context(), "key1", callback)
		result2 = v

		return err
	})

	err := wg.Wait()
	require.NoError(t, err)

	assert.Equal(t, expected, result1)
	assert.Equal(t, expected, result2)
	// Callback should only be called once — the second instance should find the value
	// in Redis after acquiring the lock (triple-check).
	assert.Equal(t, int32(1), callCount.Load())
}

func TestRedisCache_Lock_GracefulDegradation(t *testing.T) {
	t.Parallel()
	// Use a client pointing to a non-existent Redis — lock acquisition will fail
	badClient := redis.NewClient(&redis.Options{
		Addr:        "localhost:1",
		DialTimeout: 100 * time.Millisecond,
	})
	defer badClient.Close()

	rc := NewRedisCache(RedisConfig[testValue]{
		TTL:          30 * time.Second,
		RedisClient:  badClient,
		RedisPrefix:  "test:lock-degrade",
		RedisTimeout: 200 * time.Millisecond,
	})
	defer rc.Close(t.Context())

	expected := testValue{ID: "degraded", Name: "DegradedValue"}

	// Even though locking is enabled and Redis is unavailable, the callback should still execute
	result, err := rc.GetOrSet(t.Context(), "key1", func(_ context.Context, _ string) (testValue, error) {
		return expected, nil
	})

	require.NoError(t, err)
	assert.Equal(t, expected, result)
}

func TestRedisCache_Lock_Disabled(t *testing.T) {
	t.Parallel()
	redisClient := redis_utils.SetupInstance(t)

	rc := NewRedisCache(RedisConfig[testValue]{
		TTL:         30 * time.Second,
		RedisClient: redisClient,
		RedisPrefix: fmt.Sprintf("test:%s", t.Name()),
		LockTTL:     RedisLockOff,
	})
	defer rc.Close(t.Context())

	_, isNoop := rc.locker.(redis_utils.NoopLocker)
	assert.True(t, isNoop, "locker should be NoopLocker when LockTTL is RedisLockOff")

	expected := testValue{ID: "no-lock", Name: "NoLock"}

	result, err := rc.GetOrSet(t.Context(), "key1", func(_ context.Context, _ string) (testValue, error) {
		return expected, nil
	})

	require.NoError(t, err)
	assert.Equal(t, expected, result)
}
