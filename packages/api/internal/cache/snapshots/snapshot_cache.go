package snapshotcache

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"

	sqlcdb "github.com/e2b-dev/infra/packages/api/internal/db/client"
	"github.com/e2b-dev/infra/packages/api/internal/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/cache"
	"github.com/e2b-dev/infra/packages/shared/pkg/postgres/dberrors"
)

const (
	snapshotCacheTTL             = 5 * time.Minute
	snapshotCacheRefreshInterval = 1 * time.Minute

	// v2 carries the build's envd_unresponsive mark; a replica without it
	// must not refill an entry a replica with it reads.
	snapshotCacheKeyPrefix = "snapshot:last:v2"

	// SnapshotInfoFormat marks an entry written with the build's max_ram_mb and
	// start_memory_mb. The key stays shared with older replicas so their
	// invalidations still reach it; an entry without the mark is refetched.
	SnapshotInfoFormat = 1
)

var tracer = otel.Tracer("github.com/e2b-dev/infra/packages/api/internal/cache/snapshots")

// SnapshotInfo holds cached snapshot with build and alias information.
type SnapshotInfo struct {
	Aliases  []string         `json:"aliases"`
	Names    []string         `json:"names"`
	Snapshot queries.Snapshot `json:"snapshot"`
	EnvBuild queries.EnvBuild `json:"env_build"`
	NotFound bool             `json:"not_found,omitempty"`
	Format   int              `json:"format,omitempty"`
}

var errNotFoundSentinel = &SnapshotInfo{NotFound: true, Format: SnapshotInfoFormat}

var ErrSnapshotNotFound = errors.New("snapshot not found")

type SnapshotCache struct {
	cache *cache.RedisCache[*SnapshotInfo]
	db    *sqlcdb.Client
}

func NewSnapshotCache(db *sqlcdb.Client, redisClient redis.UniversalClient) *SnapshotCache {
	rc := cache.NewRedisCache(cache.RedisConfig[*SnapshotInfo]{
		TTL:             snapshotCacheTTL,
		RefreshInterval: snapshotCacheRefreshInterval,
		RedisClient:     redisClient,
		RedisPrefix:     snapshotCacheKeyPrefix,
	})

	return &SnapshotCache{
		cache: rc,
		db:    db,
	}
}

// Get returns the last snapshot for a sandbox, using cache with DB fallback.
func (c *SnapshotCache) Get(ctx context.Context, sandboxID string) (*SnapshotInfo, error) {
	ctx, span := tracer.Start(ctx, "get last snapshot")
	defer span.End()

	// An entry below the current format was written by an older replica,
	// without the fields this one reads, and is refilled under the cache lock.
	info, err := c.cache.GetOrSetFresh(ctx, sandboxID, c.fetchFromDB, func(info *SnapshotInfo) bool {
		return info.Format < SnapshotInfoFormat
	})
	if err != nil {
		return nil, err
	}

	if info.NotFound {
		return nil, ErrSnapshotNotFound
	}

	return info, nil
}

func (c *SnapshotCache) fetchFromDB(ctx context.Context, sandboxID string) (*SnapshotInfo, error) {
	ctx, span := tracer.Start(ctx, "fetch last snapshot from DB")
	defer span.End()

	row, err := c.db.GetLastSnapshot(ctx, sandboxID)
	if err != nil {
		if dberrors.IsNotFoundError(err) {
			return errNotFoundSentinel, nil
		}

		return nil, fmt.Errorf("fetching last snapshot: %w", err)
	}

	return &SnapshotInfo{
		Aliases:  row.Aliases,
		Names:    row.Names,
		Snapshot: row.Snapshot,
		EnvBuild: row.EnvBuild,
		Format:   SnapshotInfoFormat,
	}, nil
}

// Invalidate removes the cached snapshot for a sandbox.
func (c *SnapshotCache) Invalidate(ctx context.Context, sandboxID string) {
	c.cache.Delete(ctx, sandboxID)
}

func (c *SnapshotCache) Close(ctx context.Context) error {
	return c.cache.Close(ctx)
}
