package templatecache

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/db/pkg/testutils"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

// TestTemplateMetadataCache_Get tests that metadata cache returns correct data
func TestTemplateMetadataCache_Get(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	redis := redis_utils.SetupInstance(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	templateID := testutils.CreateTestTemplate(t, db, teamID)

	cache := NewTemplateMetadataCache(db.SqlcClient, redis)
	defer cache.Close(ctx)

	metadata, err := cache.Get(ctx, templateID)
	require.NoError(t, err)
	require.NotNil(t, metadata)
	assert.Equal(t, templateID, metadata.TemplateID)
	assert.Equal(t, teamID, metadata.TeamID)
	assert.Equal(t, consts.LocalClusterID, metadata.ClusterID)
}

// TestTemplateMetadataCache_Get_NotFound tests that non-existent template IDs return error
func TestTemplateMetadataCache_Get_NotFound(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	redis := redis_utils.SetupInstance(t)
	ctx := t.Context()

	cache := NewTemplateMetadataCache(db.SqlcClient, redis)
	defer cache.Close(ctx)

	metadata, err := cache.Get(ctx, "non-existent-id")
	require.ErrorIs(t, err, ErrTemplateNotFound)
	require.Nil(t, metadata)
}

// TestTemplateMetadataCache_Invalidate tests that invalidation clears the cached entry
func TestTemplateMetadataCache_Invalidate(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	redis := redis_utils.SetupInstance(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	templateID := testutils.CreateTestTemplate(t, db, teamID)

	cache := NewTemplateMetadataCache(db.SqlcClient, redis)
	defer cache.Close(ctx)

	// Populate cache
	_, err := cache.Get(ctx, templateID)
	require.NoError(t, err)

	// Verify Redis entry exists
	redisKey := cache.cache.RedisKey(templateID)
	exists, err := redis.Exists(ctx, redisKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), exists, "metadata should be cached")

	// Invalidate
	cache.Invalidate(ctx, templateID)

	// Verify Redis entry is gone
	exists, err = redis.Exists(ctx, redisKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), exists, "metadata should be deleted after invalidation")

	// Re-fetch should still work (fresh from DB)
	metadata, err := cache.Get(ctx, templateID)
	require.NoError(t, err)
	require.NotNil(t, metadata)
	assert.Equal(t, templateID, metadata.TemplateID)
}
