package templatecache

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	"github.com/e2b-dev/infra/packages/db/pkg/types"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

// TestRedisTemplatesBuildCache_Get_CacheHit tests that Get returns from Redis cache when available.
func TestRedisTemplatesBuildCache_Get_CacheHit(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	redisClient := redis_utils.SetupInstance(t)
	ctx := t.Context()

	c := NewTemplateBuildCache(db.SqlcClient, redisClient)

	buildID := uuid.New()
	info := TemplateBuildInfo{
		TeamID:      uuid.New(),
		TemplateID:  "test-template",
		BuildStatus: types.BuildStatusGroupInProgress,
		ClusterID:   uuid.New(),
	}

	// Store in Redis via the cache's Set
	c.cache.Set(ctx, buildID.String(), info)

	// Get should return from Redis without hitting DB
	result, err := c.Get(ctx, buildID, "test-template")
	require.NoError(t, err)
	assert.Equal(t, info.TeamID, result.TeamID)
	assert.Equal(t, info.TemplateID, result.TemplateID)
	assert.Equal(t, info.BuildStatus, result.BuildStatus)
}

// TestRedisTemplatesBuildCache_Get_L2Hit tests that storing in Redis works and Get retrieves from Redis.
func TestRedisTemplatesBuildCache_Get_L2Hit(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	redisClient := redis_utils.SetupInstance(t)
	ctx := t.Context()

	c := NewTemplateBuildCache(db.SqlcClient, redisClient)

	buildID := uuid.New()
	info := TemplateBuildInfo{
		TeamID:      uuid.New(),
		TemplateID:  "test-template",
		BuildStatus: types.BuildStatusGroupInProgress,
		ClusterID:   uuid.New(),
	}

	// Store directly in Redis
	buildJSON, err := json.Marshal(info)
	require.NoError(t, err)

	buildKey := c.cache.RedisKey(buildID.String())
	err = redisClient.Set(ctx, buildKey, buildJSON, buildCacheTTL).Err()
	require.NoError(t, err)

	// Get should return from Redis
	result, err := c.Get(ctx, buildID, "test-template")
	require.NoError(t, err)
	assert.Equal(t, info.TeamID, result.TeamID)
	assert.Equal(t, info.TemplateID, result.TemplateID)
}

// TestRedisTemplatesBuildCache_Get_DBFallback tests that Get falls back to DB when cache misses.
func TestRedisTemplatesBuildCache_Get_DBFallback(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	redisClient := redis_utils.SetupInstance(t)
	ctx := t.Context()

	// Create test data in DB
	teamID := testutils.CreateTestTeam(t, db)
	templateID := testutils.CreateTestTemplate(t, db, teamID)
	buildID := testutils.CreateTestBuild(t, ctx, db, templateID, types.BuildStatusGroupInProgress)
	testutils.CreateTestBuildAssignment(t, ctx, db, templateID, buildID, "default")

	c := NewTemplateBuildCache(db.SqlcClient, redisClient)
	defer c.Close(t.Context())

	// Get should fall back to DB
	result, err := c.Get(ctx, buildID, templateID)
	require.NoError(t, err)
	assert.Equal(t, teamID, result.TeamID)
	assert.Equal(t, templateID, result.TemplateID)
}

// TestRedisTemplatesBuildCache_Get_NotFound tests that Get returns error when build not found.
func TestRedisTemplatesBuildCache_Get_NotFound(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	redisClient := redis_utils.SetupInstance(t)
	ctx := t.Context()

	c := NewTemplateBuildCache(db.SqlcClient, redisClient)
	defer c.Close(t.Context())

	// Try to get non-existent build
	_, err := c.Get(ctx, uuid.New(), "non-existent-template")
	require.Error(t, err)
	require.ErrorIs(t, err, ErrTemplateBuildInfoNotFound)
}

// TestRedisTemplatesBuildCache_Invalidate tests that Invalidate deletes the key from Redis.
func TestRedisTemplatesBuildCache_Invalidate(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	redisClient := redis_utils.SetupInstance(t)
	ctx := t.Context()

	c := NewTemplateBuildCache(db.SqlcClient, redisClient)
	defer c.Close(t.Context())

	buildID := uuid.New()
	info := TemplateBuildInfo{
		TeamID:      uuid.New(),
		TemplateID:  "test-template",
		BuildStatus: types.BuildStatusGroupInProgress,
		ClusterID:   uuid.New(),
	}

	// Store in Redis via the cache
	c.cache.Set(ctx, buildID.String(), info)

	buildKey := c.cache.RedisKey(buildID.String())

	// Verify key exists
	exists, err := redisClient.Exists(ctx, buildKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), exists)

	// Invalidate via SetStatus
	c.Invalidate(ctx, buildID)

	// Redis key should be gone
	exists, err = redisClient.Exists(ctx, buildKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), exists)
}
