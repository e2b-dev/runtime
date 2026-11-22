package templatecache

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	sqlcdb "github.com/e2b-dev/infra/packages/api/internal/db/client"
	"github.com/e2b-dev/infra/packages/shared/pkg/cache"
	"github.com/e2b-dev/infra/packages/shared/pkg/clusters"
	"github.com/e2b-dev/infra/packages/shared/pkg/postgres/dberrors"
)

const (
	metadataCacheTTL             = 5 * time.Minute
	metadataCacheRefreshInterval = time.Minute

	metadataCacheKeyPrefix = "template:metadata"
)

// TemplateMetadata holds mutable template metadata (public flag, cluster assignment).
type TemplateMetadata struct {
	TemplateID string    `json:"template_id"`
	TeamID     uuid.UUID `json:"team_id"`
	Public     bool      `json:"public"`
	ClusterID  uuid.UUID `json:"cluster_id"`
}

// TemplateMetadataCache caches mutable template metadata by templateID.
type TemplateMetadataCache struct {
	cache *cache.RedisCache[*TemplateMetadata]
	db    *sqlcdb.Client
}

func NewTemplateMetadataCache(db *sqlcdb.Client, redisClient redis.UniversalClient) *TemplateMetadataCache {
	rc := cache.NewRedisCache[*TemplateMetadata](cache.RedisConfig[*TemplateMetadata]{
		TTL:             metadataCacheTTL,
		RefreshInterval: metadataCacheRefreshInterval,
		RedisClient:     redisClient,
		RedisPrefix:     metadataCacheKeyPrefix,
	})

	return &TemplateMetadataCache{
		cache: rc,
		db:    db,
	}
}

// Get returns the metadata for a template, fetching from DB on cache miss.
func (c *TemplateMetadataCache) Get(ctx context.Context, templateID string) (*TemplateMetadata, error) {
	ctx, span := tracer.Start(ctx, "get template metadata")
	defer span.End()

	metadata, err := c.cache.GetOrSet(ctx, templateID, c.fetchFromDB)
	if err != nil {
		return nil, err
	}

	return metadata, nil
}

func (c *TemplateMetadataCache) fetchFromDB(ctx context.Context, templateID string) (*TemplateMetadata, error) {
	ctx, span := tracer.Start(ctx, "fetch template metadata from DB")
	defer span.End()

	result, err := c.db.GetTemplateById(ctx, templateID)
	if err != nil {
		if dberrors.IsNotFoundError(err) {
			return nil, ErrTemplateNotFound
		}

		return nil, fmt.Errorf("fetching template metadata: %w", err)
	}

	return &TemplateMetadata{
		TemplateID: result.ID,
		TeamID:     result.TeamID,
		Public:     result.Public,
		ClusterID:  clusters.WithClusterFallback(result.ClusterID),
	}, nil
}

// Invalidate removes the cached metadata for a template.
func (c *TemplateMetadataCache) Invalidate(ctx context.Context, templateID string) {
	c.cache.Delete(ctx, templateID)
}

func (c *TemplateMetadataCache) Close(ctx context.Context) error {
	return c.cache.Close(ctx)
}
