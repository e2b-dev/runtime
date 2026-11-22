package templatecache

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	sqlcdb "github.com/e2b-dev/infra/packages/api/internal/db/client"
	"github.com/e2b-dev/infra/packages/api/internal/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/cache"
	"github.com/e2b-dev/infra/packages/shared/pkg/clusters"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
	"github.com/e2b-dev/infra/packages/shared/pkg/postgres/dberrors"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	sharedUtils "github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

const (
	templateCacheTTL             = 5 * time.Minute
	templateCacheRefreshInterval = 1 * time.Minute

	templateCacheKeyPrefix = "template:info"

	// templateInfoFormat marks an entry written with the build's max_ram_mb and
	// start_memory_mb. The key stays shared with older replicas so their
	// invalidations, privacy changes included, still reach it; an entry
	// without the mark is refetched.
	templateInfoFormat = 1
)

var tracer = otel.Tracer("github.com/e2b-dev/infra/packages/api/internal/cache/templates")

func buildCacheKey(templateID, tag string) string {
	// Wrap templateID in {} so it becomes a Redis hash tag — all keys for the
	// same template land on the same hash slot in Redis Cluster, enabling
	// pipelined prefix deletion in InvalidateAllTags.
	return fmt.Sprintf("{%s}:%s", templateID, tag)
}

// TemplateInfo holds cached template with build information
type TemplateInfo struct {
	Template  *api.Template     `json:"template"`
	TeamID    uuid.UUID         `json:"team_id"`
	ClusterID uuid.UUID         `json:"cluster_id"`
	Build     *queries.EnvBuild `json:"build"`
	Tag       string            `json:"tag"`
	Format    int               `json:"format,omitempty"`
}

// TemplateCache caches template+build by templateID:tag.
// This is a simple lookup layer - resolution happens in AliasCache.
type TemplateCache struct {
	cache         *cache.RedisCache[*TemplateInfo]
	db            *sqlcdb.Client
	aliasCache    *AliasCache
	metadataCache *TemplateMetadataCache
}

func NewTemplateCache(db *sqlcdb.Client, redisClient redis.UniversalClient) *TemplateCache {
	redisCache := cache.NewRedisCache[*TemplateInfo](cache.RedisConfig[*TemplateInfo]{
		TTL:             templateCacheTTL,
		RefreshInterval: templateCacheRefreshInterval,
		RedisClient:     redisClient,
		RedisPrefix:     templateCacheKeyPrefix,

		ExtractKeyFunc: func(value *TemplateInfo) string {
			return buildCacheKey(value.Template.TemplateID, value.Tag)
		},
	})

	return &TemplateCache{
		cache:         redisCache,
		db:            db,
		aliasCache:    NewAliasCache(db, redisClient),
		metadataCache: NewTemplateMetadataCache(db, redisClient),
	}
}

// ResolveAlias resolves an identifier to AliasInfo (templateID, teamID).
// The identifier is "namespace/alias" or just "alias" (already validated by id.ParseName).
// namespaceFallback is used for bare aliases (no explicit namespace).
func (c *TemplateCache) ResolveAlias(ctx context.Context, identifier string, namespaceFallback string) (*AliasInfo, error) {
	return c.aliasCache.Resolve(ctx, identifier, namespaceFallback)
}

// GetByID looks up template info by direct template ID only (no alias resolution).
func (c *TemplateCache) GetByID(ctx context.Context, templateID string) (*AliasInfo, error) {
	return c.aliasCache.LookupByID(ctx, templateID)
}

// ResolveAliasWithMetadata chains alias resolution with metadata lookup.
func (c *TemplateCache) ResolveAliasWithMetadata(ctx context.Context, identifier string, namespaceFallback string) (*AliasInfo, *TemplateMetadata, error) {
	aliasInfo, err := c.ResolveAlias(ctx, identifier, namespaceFallback)
	if err != nil {
		return nil, nil, err
	}

	metadata, err := c.metadataCache.Get(ctx, aliasInfo.TemplateID)
	if err != nil {
		return nil, nil, err
	}

	return aliasInfo, metadata, nil
}

func (c *TemplateCache) GetMetadata(ctx context.Context, templateID string) (*TemplateMetadata, error) {
	return c.metadataCache.Get(ctx, templateID)
}

// Get fetches a template with build by templateID and tag.
// Does NOT do alias resolution - callers should use ResolveAlias first.
// Performs access control and cluster checks.
func (c *TemplateCache) Get(ctx context.Context, templateID string, tag *string, teamID uuid.UUID, clusterID uuid.UUID) (*api.Template, *queries.EnvBuild, error) {
	ctx, span := tracer.Start(ctx, "get template", trace.WithAttributes(
		telemetry.WithTemplateID(templateID),
		telemetry.WithTeamID(teamID.String()),
		telemetry.WithClusterID(clusterID),
		attribute.String("tag", sharedUtils.DerefOrDefault(tag, id.DefaultTag)),
	))
	defer span.End()

	// Step 1: Get template with build by ID and tag
	templateInfo, err := c.getByID(ctx, templateID, tag)
	if err != nil {
		return nil, nil, err
	}

	// Step 2: Access control check
	if templateInfo.TeamID != teamID && !templateInfo.Template.Public {
		return nil, nil, fmt.Errorf("%w: team '%s' cannot access template '%s'", ErrAccessDenied, teamID, templateID)
	}

	// Step 3: Cluster check
	if templateInfo.ClusterID != clusterID {
		return nil, nil, fmt.Errorf("%w: template '%s' not in cluster '%s'", ErrClusterMismatch, templateID, clusterID)
	}

	return templateInfo.Template, templateInfo.Build, nil
}

// getByID fetches template+build by templateID and tag
func (c *TemplateCache) getByID(ctx context.Context, templateID string, tag *string) (*TemplateInfo, error) {
	tagValue := id.DefaultTag
	if tag != nil {
		tagValue = *tag
	}
	cacheKey := buildCacheKey(templateID, tagValue)

	fetch := c.fetchTemplateWithBuild(templateID, tag)
	// An entry below the current format was written by an older replica,
	// without the fields this one reads, and is refilled under the cache lock.
	info, err := c.cache.GetOrSetFresh(ctx, cacheKey, fetch, func(info *TemplateInfo) bool {
		return info.Format < templateInfoFormat
	})
	if err != nil {
		return nil, err
	}

	return info, nil
}

func (c *TemplateCache) fetchTemplateWithBuild(templateID string, tag *string) func(context.Context, string) (*TemplateInfo, error) {
	return func(ctx context.Context, _ string) (*TemplateInfo, error) {
		ctx, span := tracer.Start(ctx, "fetch template with build")
		defer span.End()

		result, err := c.db.GetTemplateWithBuildByTag(ctx, queries.GetTemplateWithBuildByTagParams{
			TemplateID: templateID,
			Tag:        tag,
		})
		if err != nil {
			if dberrors.IsNotFoundError(err) {
				return nil, templateTagNotFoundError{
					Tag: sharedUtils.DerefOrDefault(tag, id.DefaultTag),
				}
			}

			return nil, fmt.Errorf("fetching template with build: %w", err)
		}

		build := &result.EnvBuild
		template := result.ActiveEnv
		clusterID := clusters.WithClusterFallback(template.ClusterID)

		tagValue := sharedUtils.DerefOrDefault(tag, id.DefaultTag)

		return &TemplateInfo{
			Template: &api.Template{
				TemplateID: template.ID,
				BuildID:    build.ID.String(),
				Public:     template.Public,
				Aliases:    result.Aliases,
				Names:      result.Names,
			},
			TeamID:    template.TeamID,
			ClusterID: clusterID,
			Build:     build,
			Tag:       tagValue,
			Format:    templateInfoFormat,
		}, nil
	}
}

func (c *TemplateCache) Invalidate(ctx context.Context, templateID string, tag *string) {
	tagValue := id.DefaultTag
	if tag != nil {
		tagValue = *tag
	}
	cacheKey := buildCacheKey(templateID, tagValue)
	c.cache.Delete(ctx, cacheKey)
}

// InvalidateAllTags invalidates the cache for the given templateID across all tags
func (c *TemplateCache) InvalidateAllTags(ctx context.Context, templateID string) []string {
	pattern := buildCacheKey(templateID, "")
	keys := c.cache.DeleteByPrefix(ctx, pattern)

	c.metadataCache.Invalidate(ctx, templateID)

	return keys
}

// InvalidateAliasesByTemplateID invalidates alias cache entries for the given keys.
// aliasKeys are cache-key-formatted strings as returned by DeleteTemplate.
func (c *TemplateCache) InvalidateAliasesByTemplateID(ctx context.Context, templateID string, aliasKeys []string) {
	c.aliasCache.InvalidateAliasesByTemplateID(ctx, templateID, aliasKeys)
}

// InvalidateAlias invalidates the alias cache entry
func (c *TemplateCache) InvalidateAlias(ctx context.Context, namespace *string, alias string) {
	c.aliasCache.Invalidate(ctx, namespace, alias)
}

func (c *TemplateCache) Close(ctx context.Context) error {
	return errors.Join(c.aliasCache.Close(ctx), c.metadataCache.Close(ctx), c.cache.Close(ctx))
}
