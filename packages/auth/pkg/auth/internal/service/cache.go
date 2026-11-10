package service

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/e2b-dev/infra/packages/auth/pkg/types"
	"github.com/e2b-dev/infra/packages/shared/pkg/cache"
)

const (
	authInfoExpiration = 5 * time.Minute
	refreshInterval    = 1 * time.Minute
	refreshTimeout     = 30 * time.Second
	// invalidateTimeout must exceed the cache's write-lock wait so an
	// invalidation can outwait any in-flight cache writer instead of degrading
	// to a best-effort delete that a stale write could overwrite. The wait is
	// bounded by the lock TTL (refreshTimeout + 2x the 2s default Redis
	// timeout = 34s) plus the 5s lock-acquire margin; 45s adds headroom for
	// the DEL itself.
	invalidateTimeout = 45 * time.Second

	authCacheRedisPrefix = "auth:team"
)

// authCache is a Redis-backed TTL cache for team authentication data.
type authCache struct {
	cache *cache.RedisCache[*types.Team]
}

// newAuthCache creates a new Redis-backed authCache with default TTL and refresh settings.
func newAuthCache(redisClient redis.UniversalClient) *authCache {
	return &authCache{
		cache: cache.NewRedisCache(cache.RedisConfig[*types.Team]{
			RedisClient:     redisClient,
			TTL:             authInfoExpiration,
			RefreshInterval: refreshInterval,
			RefreshTimeout:  refreshTimeout,
			RedisPrefix:     authCacheRedisPrefix,
		}),
	}
}

// GetOrSet returns the cached value for the key, or calls dataCallback to populate it.
func (c *authCache) GetOrSet(ctx context.Context, key string, dataCallback func(ctx context.Context, key string) (*types.Team, error)) (*types.Team, error) {
	return c.cache.GetOrSet(ctx, key, dataCallback)
}

// Invalidate removes a single entry from the cache by key.
func (c *authCache) Invalidate(ctx context.Context, key string) {
	c.cache.Delete(ctx, key)
}

// TryInvalidate removes a single entry and reports a failed eviction, for
// callers that must retry rather than leave the stale entry serving.
func (c *authCache) TryInvalidate(ctx context.Context, key string) error {
	return c.cache.TryDelete(ctx, key)
}

// Close is a no-op for the Redis-backed cache (no background goroutines).
func (c *authCache) Close(ctx context.Context) error {
	return c.cache.Close(ctx)
}
