package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/bsm/redislock"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

const (
	RedisRefreshIntervalOff = 0
	RedisLockOff            = -1
	RedisDefaultTimeout     = 2 * time.Second

	// redisNoPTTL is the value returned by go-redis PTTL when a key has no expiration.
	// Redis returns the integer -1; go-redis converts it to -1 * time.Nanosecond.
	redisNoPTTL = -1 * time.Nanosecond

	redisScanCount = 100

	defaultLockRetryInterval = 50 * time.Millisecond
	acquireLockTimeout       = 5 * time.Second
)

// RedisConfig holds the configuration for a RedisCache.
type RedisConfig[V any] struct {
	RedisClient redis.UniversalClient
	TTL         time.Duration
	// RefreshInterval triggers a background refresh of a Redis entry
	// when its age exceeds this duration. 0 = no background refresh.
	RefreshInterval time.Duration
	// RefreshTimeout is the timeout for the callback during Redis refresh.
	// Defaults to 30s.
	RefreshTimeout time.Duration
	RedisTimeout   time.Duration // default 2s
	RedisPrefix    string        // e.g. "template:build"
	// ExtractKeyFunc is an optional function to extract a key from the value.
	// When set, the key used for Redis storage is derived from the fetched value
	// rather than the original lookup key.
	ExtractKeyFunc ExtractKeyFunc[V]
	// LockTTL controls distributed locking for cache updates.
	// Lock is acquired before calling the data callback to prevent thundering herd across instances.
	// TTL is auto-computed as RefreshTimeout + 2*RedisTimeout
	LockTTL time.Duration
	// LockRetryInterval is the interval between lock acquisition retries.
	// Defaults to 50ms.
	LockRetryInterval time.Duration
}

// RedisCache is a generic two-tier cache: Redis + user callback.
type RedisCache[V any] struct {
	config       RedisConfig[V]
	fetchGroup   singleflight.Group
	redisRefresh singleflight.Group
	locker       redis_utils.Locker
}

// NewRedisCache creates a new RedisCache with the given configuration.
func NewRedisCache[V any](config RedisConfig[V]) *RedisCache[V] {
	if config.RedisTimeout == 0 {
		config.RedisTimeout = RedisDefaultTimeout
	}

	if config.RefreshTimeout == 0 {
		config.RefreshTimeout = 30 * time.Second
	}

	var locker redis_utils.Locker = redis_utils.NoopLocker{}
	if config.LockTTL != RedisLockOff {
		if config.LockRetryInterval == 0 {
			config.LockRetryInterval = defaultLockRetryInterval
		}

		config.LockTTL = config.RefreshTimeout + 2*config.RedisTimeout

		locker = &redis_utils.RedisLocker{Client: redislock.New(config.RedisClient)}
	}

	rc := &RedisCache[V]{
		config: config,
		locker: locker,
	}

	return rc
}

// GetOrSet retrieves a value from Redis, falling back to dataCallback on miss.
// On callback hit, the value is backfilled into Redis.
// Singleflight deduplicates concurrent misses for the same key.
func (rc *RedisCache[V]) GetOrSet(ctx context.Context, key string, dataCallback DataCallback[V]) (V, error) {
	// Fast path: try Redis outside singleflight
	value, remainingTTL, err := rc.getFromRedis(ctx, key)
	if err == nil {
		// Check if eligible for refresh and refresh if necessary
		rc.handleRefresh(ctx, key, remainingTTL, dataCallback)

		return value, nil
	}

	// Cache miss: deduplicate concurrent fetches
	type result struct {
		value V
		err   error
	}

	r, _, _ := rc.fetchGroup.Do(key, func() (any, error) {
		ctx := context.WithoutCancel(ctx)

		// Acquire distributed lock if enabled
		lock, lockErr := rc.acquireLock(ctx, key, redislock.LinearBackoff(rc.config.LockRetryInterval), acquireLockTimeout)
		defer rc.releaseLock(ctx, lock, key)
		// We want to get the results even without the lock to prevent failing all the waiting requests

		// Double-check Redis (another goroutine may have populated it)
		if v, _, redisErr := rc.getFromRedis(ctx, key); redisErr == nil {
			return result{value: v}, nil
		}

		// Call the data callback with rc.config.RefreshTimeout timeout
		callbackCtx, cancel := context.WithTimeout(ctx, rc.config.RefreshTimeout)
		defer cancel()
		v, cbErr := dataCallback(callbackCtx, key)
		if cbErr != nil {
			return result{err: cbErr}, nil
		}

		// Backfill into Redis
		storeKey := key
		if rc.config.ExtractKeyFunc != nil {
			storeKey = rc.config.ExtractKeyFunc(v)
		}

		if lockErr != nil {
			logger.L().Warn(ctx, "RedisCache: failed to acquire refresh lock, skipping update in redis",
				zap.String("key", key),
				zap.Error(lockErr))

			return result{value: v}, nil
		}

		rc.setInRedis(ctx, storeKey, v)

		return result{value: v}, nil
	})

	res := r.(result)

	return res.value, res.err
}

// Set stores a value in Redis.
func (rc *RedisCache[V]) Set(ctx context.Context, key string, value V) {
	rc.setInRedis(ctx, key, value)
}

// Delete removes a value from Redis.
//
// Writers (the GetOrSet backfill and the background refresh) hold the per-key
// lock across their SET, so Delete waits for that lock to guarantee the DEL
// is ordered after any in-flight write; without this, a writer that read the
// backing store just before the caller's mutation could repopulate the entry
// with stale data for a full TTL after a fire-and-forget delete.
//
// The guarantee comes with constraints the caller must respect:
//   - The wait is bounded by LockTTL (RefreshTimeout + 2*RedisTimeout) plus a
//     margin, and by ctx — pass a context that survives at least that long
//     (and detached from request cancellation when the delete must not be
//     skipped), otherwise Delete degrades to the best-effort behavior below.
//   - If the lock still cannot be obtained (Redis/lock-service errors, or the
//     ctx expiring), Delete falls back to a best-effort DEL and a concurrent
//     writer may repopulate the entry with stale data until its TTL expires.
//   - Healthy writers always finish inside the wait window: their data
//     callback is capped at RefreshTimeout, which is strictly less than the
//     lock TTL the wait is derived from.
func (rc *RedisCache[V]) Delete(ctx context.Context, key string) {
	if err := rc.TryDelete(ctx, key); err != nil {
		logger.L().Warn(ctx, "RedisCache: Redis DEL error",
			zap.String("key", rc.RedisKey(key)),
			zap.Error(err))
	}
}

// TryDelete is Delete for callers whose correctness depends on the entry being
// gone: a failed DEL is returned so the caller can retry instead of reporting a
// stale entry as evicted.
func (rc *RedisCache[V]) TryDelete(ctx context.Context, key string) error {
	// Wait past a wedged writer's lock auto-expiry (LockTTL) so lock
	// acquisition can only fail on Redis/lock-service errors or ctx expiry,
	// not on writer contention.
	lock, err := rc.acquireLock(ctx, key, redislock.LinearBackoff(rc.config.LockRetryInterval), rc.config.LockTTL+acquireLockTimeout)
	if err != nil {
		logger.L().Warn(ctx, "RedisCache - Delete: failed to acquire lock, deleting best-effort; a concurrent writer may repopulate stale data",
			zap.String("key", key),
			zap.Error(err))
		// Continue without the lock to remove the stale data
		// In that case it's just a best effort, the data may get repopulated with stale data
	}
	defer rc.releaseLock(ctx, lock, key)

	ctx, cancel := context.WithTimeout(ctx, rc.config.RedisTimeout)
	defer cancel()

	if err := rc.config.RedisClient.Del(ctx, rc.RedisKey(key)).Err(); err != nil {
		return fmt.Errorf("delete %s: %w", rc.RedisKey(key), err)
	}

	return nil
}

// DeleteByPrefix removes all keys matching the given prefix from Redis.
// Uses SCAN (not KEYS) to avoid blocking Redis on large keyspaces.
// Keys are collected first, then deleted after SCAN completes so that
// the keyspace is not mutated during cursor iteration.
// Returns the list of deleted cache keys (without the Redis prefix).
func (rc *RedisCache[V]) DeleteByPrefix(ctx context.Context, prefix string) []string {
	redisPattern := fmt.Sprintf("%s:%s*", rc.config.RedisPrefix, prefix)

	// Phase 1: collect all matching keys without mutating the keyspace.
	allKeys := rc.collectKeys(ctx, prefix)
	if len(allKeys) == 0 {
		return nil
	}

	// Phase 2: delete all collected keys.
	pipeCtx, cancel := context.WithTimeout(ctx, rc.config.RedisTimeout)
	defer cancel()
	pipe := rc.config.RedisClient.Pipeline()
	for _, redisKey := range allKeys {
		pipe.Del(pipeCtx, redisKey)
	}

	if _, err := pipe.Exec(pipeCtx); err != nil {
		logger.L().Warn(ctx, "RedisCache: pipeline DEL error",
			zap.String("pattern", redisPattern),
			zap.Error(err))

		return nil
	}

	deleted := make([]string, 0, len(allKeys))
	for _, redisKey := range allKeys {
		deleted = append(deleted, redisKey[len(rc.config.RedisPrefix)+1:])
	}

	return deleted
}

func (rc *RedisCache[V]) collectKeys(ctx context.Context, prefix string) []string {
	var allKeys []string
	var cursor uint64
	for {
		scanCtx, cancel := context.WithTimeout(ctx, rc.config.RedisTimeout)
		keys, nextCursor, err := rc.config.RedisClient.Scan(scanCtx, cursor, fmt.Sprintf("%s:%s*", rc.config.RedisPrefix, prefix), redisScanCount).Result()
		cancel()
		if err != nil {
			logger.L().Warn(ctx, "RedisCache: SCAN error",
				zap.String("pattern", fmt.Sprintf("%s:%s*", rc.config.RedisPrefix, prefix)),
				zap.Error(err))

			break
		}

		allKeys = append(allKeys, keys...)

		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}

	return allKeys
}

// Close is a no-op (no background goroutines to stop).
func (rc *RedisCache[V]) Close(_ context.Context) error {
	return nil
}

// RedisClient returns the underlying Redis client for custom operations (e.g. Lua scripts).
func (rc *RedisCache[V]) RedisClient() redis.UniversalClient {
	return rc.config.RedisClient
}

// RedisKey returns the full Redis key for a given cache key.
func (rc *RedisCache[V]) RedisKey(key string) string {
	return fmt.Sprintf("%s:%s", rc.config.RedisPrefix, key)
}

// getFromRedis fetches both the value and remaining TTL from Redis using a pipeline.
func (rc *RedisCache[V]) getFromRedis(ctx context.Context, key string) (V, time.Duration, error) {
	var zero V

	ctx, cancel := context.WithTimeout(ctx, rc.config.RedisTimeout)
	defer cancel()

	redisKey := rc.RedisKey(key)

	pipe := rc.config.RedisClient.Pipeline()
	getCmd := pipe.Get(ctx, redisKey)
	ttlCmd := pipe.PTTL(ctx, redisKey)

	_, err := pipe.Exec(ctx)
	if err != nil {
		return zero, 0, err
	}

	data, err := getCmd.Bytes()
	if err != nil {
		return zero, 0, err
	}

	var value V
	if err := json.Unmarshal(data, &value); err != nil {
		return zero, 0, fmt.Errorf("redis cache: failed to unmarshal value: %w", err)
	}

	remainingTTL := ttlCmd.Val()

	return value, remainingTTL, nil
}

// refreshRedis refreshes a Redis entry in the background by calling the data callback.
func (rc *RedisCache[V]) refreshRedis(ctx context.Context, key string, dataCallback DataCallback[V]) {
	rc.redisRefresh.Do(key, func() (any, error) {
		// Acquire lock without retry — if another instance is refreshing, skip.
		lock, lockErr := rc.acquireLock(ctx, key, redislock.NoRetry(), acquireLockTimeout)
		if errors.Is(lockErr, redislock.ErrNotObtained) {
			logger.L().Debug(ctx, "RedisCache: skipping refresh, lock held by another instance",
				zap.String("key", key))

			return nil, nil
		}
		if lockErr != nil {
			logger.L().Warn(ctx, "RedisCache: failed to acquire refresh lock, skipping",
				zap.String("key", key),
				zap.Error(lockErr))

			return nil, nil
		}
		defer rc.releaseLock(context.WithoutCancel(ctx), lock, key)

		ctx, cancel := context.WithTimeout(ctx, rc.config.RefreshTimeout)
		defer cancel()

		value, err := dataCallback(ctx, key)
		if err != nil {
			logger.L().Warn(ctx, "RedisCache: Redis background refresh failed",
				zap.String("key", key),
				zap.Error(err))

			return nil, nil
		}

		storeKey := key
		if rc.config.ExtractKeyFunc != nil {
			storeKey = rc.config.ExtractKeyFunc(value)
		}

		rc.setInRedis(ctx, storeKey, value)

		return nil, nil
	})
}

// releaseLock releases a distributed lock. Logs on failure but does not return an error.
func (rc *RedisCache[V]) releaseLock(ctx context.Context, lock redis_utils.Lock, key string) {
	ctx, cancel := context.WithTimeout(ctx, rc.config.RedisTimeout)
	defer cancel()

	if err := lock.Release(ctx); err != nil {
		logger.L().Warn(ctx, "RedisCache: failed to release lock",
			zap.String("key", key),
			zap.Error(err))
	}
}

// acquireLock attempts to acquire a distributed lock for the given key,
// retrying per the strategy until maxWait or ctx expires, whichever is first.
// Always returns a non-nil Lock (NoopLock on failure) so callers can defer Release unconditionally.
func (rc *RedisCache[V]) acquireLock(ctx context.Context, key string, retry redislock.RetryStrategy, maxWait time.Duration) (redis_utils.Lock, error) {
	ctx, cancel := context.WithTimeout(ctx, maxWait)
	defer cancel()

	lockKey := redis_utils.GetLockKey(rc.RedisKey(key))

	lock, err := rc.locker.Obtain(ctx, lockKey, rc.config.LockTTL, &redislock.Options{
		RetryStrategy: retry,
	})
	if err != nil {
		return redis_utils.NoopLock{}, err
	}

	return lock, nil
}

func (rc *RedisCache[V]) setInRedis(ctx context.Context, key string, value V) {
	ctx, cancel := context.WithTimeout(ctx, rc.config.RedisTimeout)
	defer cancel()

	data, err := json.Marshal(value)
	if err != nil {
		logger.L().Warn(ctx, "RedisCache: failed to marshal value for Redis",
			zap.String("key", key),
			zap.Error(err))

		return
	}

	redisKey := rc.RedisKey(key)
	if err := rc.config.RedisClient.Set(ctx, redisKey, data, rc.config.TTL).Err(); err != nil {
		logger.L().Warn(ctx, "RedisCache: Redis SET error",
			zap.String("key", redisKey),
			zap.Error(err))
	}
}

// handleRefresh checks if the key should be refreshed in the background and refreshes it if necessary
func (rc *RedisCache[V]) handleRefresh(ctx context.Context, key string, remainingTTL time.Duration, dataCallback DataCallback[V]) {
	// check if key isn't persistent, this shouldn't really happen as it should be set with an expiration time
	if remainingTTL == redisNoPTTL {
		logger.L().Warn(ctx, "redis key unexpectedly persistent", zap.String("key", key))

		return
	}

	// Check if key is ready for to be refreshed in the background
	age := rc.config.TTL - remainingTTL
	if rc.config.RefreshInterval != RedisRefreshIntervalOff && age > rc.config.RefreshInterval {
		go rc.refreshRedis(context.WithoutCancel(ctx), key, dataCallback)
	}
}
