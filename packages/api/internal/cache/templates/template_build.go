package templatecache

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	sqlcdb "github.com/e2b-dev/infra/packages/db/client"
	"github.com/e2b-dev/infra/packages/db/pkg/dberrors"
	"github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/cache"
	"github.com/e2b-dev/infra/packages/shared/pkg/clusters"
)

const (
	buildCacheTTL             = 5 * time.Minute
	buildCacheRefreshInterval = 1 * time.Minute

	buildCacheKeyPrefix = "template:build"
)

type TemplateBuildInfo struct {
	TeamID      uuid.UUID              `json:"team_id"`
	TemplateID  string                 `json:"template_id"`
	BuildStatus types.BuildStatusGroup `json:"build_status"`
	Reason      types.BuildReason      `json:"reason"`

	ClusterID uuid.UUID `json:"cluster_id"`
	NodeID    *string   `json:"node_id,omitempty"`
}

var ErrTemplateBuildInfoNotFound = errors.New("template build info not found")

type TemplatesBuildCache struct {
	cache *cache.RedisCache[TemplateBuildInfo]
	db    *sqlcdb.Client
}

func NewTemplateBuildCache(db *sqlcdb.Client, redisClient redis.UniversalClient) *TemplatesBuildCache {
	rc := cache.NewRedisCache[TemplateBuildInfo](cache.RedisConfig[TemplateBuildInfo]{
		TTL:             buildCacheTTL,
		RefreshInterval: buildCacheRefreshInterval,
		RedisClient:     redisClient,
		RedisPrefix:     buildCacheKeyPrefix,
	})

	return &TemplatesBuildCache{
		cache: rc,
		db:    db,
	}
}

func (c *TemplatesBuildCache) Invalidate(ctx context.Context, buildID uuid.UUID) {
	c.cache.Delete(ctx, buildID.String())
}

func (c *TemplatesBuildCache) Get(ctx context.Context, buildID uuid.UUID, templateID string) (TemplateBuildInfo, error) {
	return c.cache.GetOrSet(ctx, buildID.String(), c.fetchFromDB(templateID, buildID))
}

func (c *TemplatesBuildCache) Close(ctx context.Context) error {
	return c.cache.Close(ctx)
}

// fetchFromDB returns a callback that fetches the build from the database.
func (c *TemplatesBuildCache) fetchFromDB(templateID string, buildID uuid.UUID) func(context.Context, string) (TemplateBuildInfo, error) {
	return func(ctx context.Context, _ string) (TemplateBuildInfo, error) {
		result, err := c.db.GetTemplateBuildWithTemplate(ctx, queries.GetTemplateBuildWithTemplateParams{
			TemplateID: templateID,
			BuildID:    buildID,
		})
		if err != nil {
			if dberrors.IsNotFoundError(err) {
				return TemplateBuildInfo{}, ErrTemplateBuildInfoNotFound
			}

			return TemplateBuildInfo{}, fmt.Errorf("failed to get template build '%s': %w", buildID, err)
		}

		return TemplateBuildInfo{
			TeamID:      result.ActiveEnv.TeamID,
			TemplateID:  result.ActiveEnv.ID,
			BuildStatus: result.EnvBuild.StatusGroup,
			Reason:      result.EnvBuild.Reason,
			ClusterID:   clusters.WithClusterFallback(result.ActiveEnv.ClusterID),
			NodeID:      result.EnvBuild.ClusterNodeID,
		}, nil
	}
}
