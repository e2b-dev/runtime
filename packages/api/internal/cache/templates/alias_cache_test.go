package templatecache

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/db/pkg/testutils"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

// TestAliasCacheResolve_BareAliasInTeamNamespace tests that a bare alias
// is found when it exists in the team's namespace
func TestAliasCacheResolve_BareAliasInTeamNamespace(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	redis := redis_utils.SetupInstance(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	teamSlug := testutils.GetTeamSlug(t, ctx, db, teamID)
	templateID := testutils.CreateTestTemplate(t, db, teamID)

	testutils.CreateTestTemplateAliasWithName(t, db, templateID, "my-alias", &teamSlug)

	cache := NewAliasCache(db.SqlcClient, redis)
	defer cache.Close(ctx)

	// Bare alias should resolve via team namespace fallback
	info, err := cache.Resolve(ctx, "my-alias", teamSlug)
	require.NoError(t, err)
	require.NotNil(t, info)
	assert.Equal(t, templateID, info.TemplateID)
	assert.Equal(t, teamID, info.TeamID)
}

// TestAliasCacheResolve_BareAliasFallbackToNullNamespace tests that a bare alias
// falls back to NULL namespace (promoted templates) when not found in team namespace
func TestAliasCacheResolve_BareAliasFallbackToNullNamespace(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	redis := redis_utils.SetupInstance(t)
	ctx := t.Context()

	// Create requesting team (has no aliases)
	requestingTeamID := testutils.CreateTestTeam(t, db)
	requestingTeamSlug := testutils.GetTeamSlug(t, ctx, db, requestingTeamID)

	// Create promoted template owned by another team
	promotedTeamID := testutils.CreateTestTeam(t, db)
	promotedTemplateID := testutils.CreateTestTemplate(t, db, promotedTeamID)

	// Create alias with NULL namespace (promoted)
	testutils.CreateTestTemplateAliasWithName(t, db, promotedTemplateID, "base", nil)

	cache := NewAliasCache(db.SqlcClient, redis)
	defer cache.Close(ctx)

	// Bare alias "base" should:
	// 1. Try requesting team's namespace -> not found
	// 2. Fall back to NULL namespace -> found
	info, err := cache.Resolve(ctx, "base", requestingTeamSlug)
	require.NoError(t, err)
	require.NotNil(t, info)
	assert.Equal(t, promotedTemplateID, info.TemplateID)
	assert.Equal(t, promotedTeamID, info.TeamID)
}

// TestAliasCacheResolve_ExplicitNamespaceNoFallback tests that an explicit namespace
// does NOT fall back to NULL namespace
func TestAliasCacheResolve_ExplicitNamespaceNoFallback(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	redis := redis_utils.SetupInstance(t)
	ctx := t.Context()

	// Create team
	teamID := testutils.CreateTestTeam(t, db)
	teamSlug := testutils.GetTeamSlug(t, ctx, db, teamID)
	templateID := testutils.CreateTestTemplate(t, db, teamID)

	// Create alias only in NULL namespace
	testutils.CreateTestTemplateAliasWithName(t, db, templateID, "only-promoted", nil)

	cache := NewAliasCache(db.SqlcClient, redis)
	defer cache.Close(ctx)

	// Explicit namespace lookup should NOT fall back to NULL
	info, err := cache.Resolve(ctx, teamSlug+"/only-promoted", teamSlug)
	require.ErrorIs(t, err, ErrTemplateNotFound)
	require.Nil(t, info)
}

// TestAliasCacheResolve_ExplicitNamespaceNoIDFallback tests that an explicit namespace
// does NOT fall back to template ID lookup, even if the alias token matches a real template ID.
// This prevents "team-x/<templateID>" from resolving when no alias exists.
func TestAliasCacheResolve_ExplicitNamespaceNoIDFallback(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	redis := redis_utils.SetupInstance(t)
	ctx := t.Context()

	// Create team and template
	teamID := testutils.CreateTestTeam(t, db)
	teamSlug := testutils.GetTeamSlug(t, ctx, db, teamID)
	templateID := testutils.CreateTestTemplate(t, db, teamID)

	cache := NewAliasCache(db.SqlcClient, redis)
	defer cache.Close(ctx)

	// Request "team-slug/<templateID>" - no alias exists with this name in team namespace.
	// Even though templateID is a valid template ID, it should NOT resolve because
	// explicit namespace lookups should not fall back to ID lookup.
	info, err := cache.Resolve(ctx, teamSlug+"/"+templateID, teamSlug)
	require.ErrorIs(t, err, ErrTemplateNotFound)
	require.Nil(t, info)

	// But bare templateID should still resolve via ID fallback
	info, err = cache.Resolve(ctx, templateID, teamSlug)
	require.NoError(t, err)
	require.NotNil(t, info)
	assert.Equal(t, templateID, info.TemplateID)
}

// TestAliasCacheResolve_TeamOverridesPromoted tests that team's alias
// takes precedence over promoted template with same name.
func TestAliasCacheResolve_TeamOverridesPromoted(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	redis := redis_utils.SetupInstance(t)
	ctx := t.Context()

	// Create team and its template
	teamID := testutils.CreateTestTeam(t, db)
	teamSlug := testutils.GetTeamSlug(t, ctx, db, teamID)
	teamTemplateID := testutils.CreateTestTemplate(t, db, teamID)

	// Create promoted template
	promotedTeamID := testutils.CreateTestTeam(t, db)
	promotedTemplateID := testutils.CreateTestTemplate(t, db, promotedTeamID)

	// Both have alias "shared-name"
	testutils.CreateTestTemplateAliasWithName(t, db, promotedTemplateID, "shared-name", nil)
	testutils.CreateTestTemplateAliasWithName(t, db, teamTemplateID, "shared-name", &teamSlug)

	cache := NewAliasCache(db.SqlcClient, redis)
	defer cache.Close(ctx)

	// Team's alias should take precedence
	info, err := cache.Resolve(ctx, "shared-name", teamSlug)
	require.NoError(t, err)
	require.NotNil(t, info)
	assert.Equal(t, teamTemplateID, info.TemplateID, "Team's alias should override promoted")
}

// TestAliasCacheResolve_DirectTemplateID tests that direct template IDs work
func TestAliasCacheResolve_DirectTemplateID(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	redis := redis_utils.SetupInstance(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	teamSlug := testutils.GetTeamSlug(t, ctx, db, teamID)
	templateID := testutils.CreateTestTemplate(t, db, teamID)

	cache := NewAliasCache(db.SqlcClient, redis)
	defer cache.Close(ctx)

	// Direct template ID should resolve
	info, err := cache.Resolve(ctx, templateID, teamSlug)
	require.NoError(t, err)
	require.NotNil(t, info)
	assert.Equal(t, templateID, info.TemplateID)
}

// TestAliasCacheResolve_NotFound tests that non-existent aliases return 404
func TestAliasCacheResolve_NotFound(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	redis := redis_utils.SetupInstance(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	teamSlug := testutils.GetTeamSlug(t, ctx, db, teamID)

	cache := NewAliasCache(db.SqlcClient, redis)
	defer cache.Close(ctx)

	info, err := cache.Resolve(ctx, "non-existent", teamSlug)
	require.ErrorIs(t, err, ErrTemplateNotFound)
	require.Nil(t, info)

	var notFoundErr templateNotFoundError
	require.ErrorAs(t, err, &notFoundErr)
	assert.Equal(t, "non-existent", notFoundErr.Identifier)
}

func TestAliasCacheResolve_ExplicitNamespaceNotFoundUsesRequestedIdentifier(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	redis := redis_utils.SetupInstance(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	teamSlug := testutils.GetTeamSlug(t, ctx, db, teamID)
	identifier := teamSlug + "/missing"

	cache := NewAliasCache(db.SqlcClient, redis)
	defer cache.Close(ctx)

	info, err := cache.Resolve(ctx, identifier, teamSlug)
	require.ErrorIs(t, err, ErrTemplateNotFound)
	require.Nil(t, info)

	var notFoundErr templateNotFoundError
	require.ErrorAs(t, err, &notFoundErr)
	assert.Equal(t, identifier, notFoundErr.Identifier)
}

// TestAliasCacheLookupByID tests direct template ID lookup
func TestAliasCacheLookupByID(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	redis := redis_utils.SetupInstance(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	templateID := testutils.CreateTestTemplate(t, db, teamID)

	cache := NewAliasCache(db.SqlcClient, redis)
	defer cache.Close(ctx)

	info, err := cache.LookupByID(ctx, templateID)
	require.NoError(t, err)
	require.NotNil(t, info)
	assert.Equal(t, templateID, info.TemplateID)
	assert.Equal(t, teamID, info.TeamID)
}

// TestAliasCacheLookupByID_NotFound tests that non-existent template IDs return 404
func TestAliasCacheLookupByID_NotFound(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	redis := redis_utils.SetupInstance(t)
	ctx := t.Context()

	cache := NewAliasCache(db.SqlcClient, redis)
	defer cache.Close(ctx)

	info, err := cache.LookupByID(ctx, "non-existent-id")
	require.ErrorIs(t, err, ErrTemplateNotFound)
	require.Nil(t, info)
}

// TestAliasCacheLookupByID_UsesCache tests that LookupByID uses cached entries from Resolve
func TestAliasCacheLookupByID_UsesCache(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	redis := redis_utils.SetupInstance(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	teamSlug := testutils.GetTeamSlug(t, ctx, db, teamID)
	templateID := testutils.CreateTestTemplate(t, db, teamID)

	testutils.CreateTestTemplateAliasWithName(t, db, templateID, "cached-alias", &teamSlug)

	cache := NewAliasCache(db.SqlcClient, redis)
	defer cache.Close(ctx)

	// First, resolve by alias (this caches by both alias and template ID)
	_, err := cache.Resolve(ctx, "cached-alias", teamSlug)
	require.NoError(t, err)

	// Verify the template ID key was backfilled into Redis by the alias resolve
	idKey := cache.cache.RedisKey(templateID)
	exists, err := redis.Exists(ctx, idKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), exists, "template ID key should be cached after alias resolve")

	// Lookup by ID should return correct data from cache
	info2, err := cache.LookupByID(ctx, templateID)
	require.NoError(t, err)
	require.NotNil(t, info2)
	assert.Equal(t, templateID, info2.TemplateID)
	assert.Equal(t, teamID, info2.TeamID)
}

// TestAliasCache_InvalidateAliasesByTemplateID tests that InvalidateAliasesByTemplateID
// deletes alias cache entries and the template-ID-keyed entry from Redis.
func TestAliasCache_InvalidateAliasesByTemplateID(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	redis := redis_utils.SetupInstance(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	teamSlug := testutils.GetTeamSlug(t, ctx, db, teamID)
	templateID := testutils.CreateTestTemplate(t, db, teamID)

	// Create two aliases: one namespaced, one bare (NULL namespace)
	testutils.CreateTestTemplateAliasWithName(t, db, templateID, "alias-a", &teamSlug)
	testutils.CreateTestTemplateAliasWithName(t, db, templateID, "alias-b", nil)

	cache := NewAliasCache(db.SqlcClient, redis)
	defer cache.Close(ctx)

	// Resolve both aliases to populate the cache
	info, err := cache.Resolve(ctx, teamSlug+"/alias-a", teamSlug)
	require.NoError(t, err)
	assert.Equal(t, templateID, info.TemplateID)

	info, err = cache.Resolve(ctx, "alias-b", teamSlug)
	require.NoError(t, err)
	assert.Equal(t, templateID, info.TemplateID)

	// Verify all three Redis keys exist: namespaced alias, bare alias, and template ID
	namespacedKey := cache.cache.RedisKey(buildAliasKey(&teamSlug, "alias-a"))
	bareKey := cache.cache.RedisKey(buildAliasKey(nil, "alias-b"))
	idKey := cache.cache.RedisKey(templateID)

	for _, key := range []string{namespacedKey, bareKey, idKey} {
		exists, err := redis.Exists(ctx, key).Result()
		require.NoError(t, err)
		assert.Equal(t, int64(1), exists, "key %s should exist before invalidation", key)
	}

	// Invalidate by template ID with the alias keys
	cache.InvalidateAliasesByTemplateID(ctx, templateID, []string{
		buildAliasKey(&teamSlug, "alias-a"),
		buildAliasKey(nil, "alias-b"),
	})

	// Assert all three keys are deleted
	for _, key := range []string{namespacedKey, bareKey, idKey} {
		exists, err := redis.Exists(ctx, key).Result()
		require.NoError(t, err)
		assert.Equal(t, int64(0), exists, "key %s should be deleted after invalidation", key)
	}
}

// TestAliasCache_InvalidateAliasesByTemplateID_EmptyKeys tests that
// InvalidateAliasesByTemplateID deletes the template-ID-keyed entry even
// when no alias keys are provided.
func TestAliasCache_InvalidateAliasesByTemplateID_EmptyKeys(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	redis := redis_utils.SetupInstance(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	teamSlug := testutils.GetTeamSlug(t, ctx, db, teamID)
	templateID := testutils.CreateTestTemplate(t, db, teamID)

	cache := NewAliasCache(db.SqlcClient, redis)
	defer cache.Close(ctx)

	// Resolve by template ID directly to populate the cache
	info, err := cache.Resolve(ctx, templateID, teamSlug)
	require.NoError(t, err)
	assert.Equal(t, templateID, info.TemplateID)

	// Verify the template ID key exists in Redis
	idKey := cache.cache.RedisKey(templateID)
	exists, err := redis.Exists(ctx, idKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), exists, "template ID key should exist before invalidation")

	// Invalidate with no alias keys
	cache.InvalidateAliasesByTemplateID(ctx, templateID, nil)

	// Assert the template ID key is deleted
	exists, err = redis.Exists(ctx, idKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), exists, "template ID key should be deleted after invalidation")
}

// TestAliasCacheResolve_NegativeCaching tests that not-found results are cached (tombstones)
func TestAliasCacheResolve_NegativeCaching(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	redis := redis_utils.SetupInstance(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	teamSlug := testutils.GetTeamSlug(t, ctx, db, teamID)

	cache := NewAliasCache(db.SqlcClient, redis)
	defer cache.Close(ctx)

	// First lookup - not found, should cache tombstone
	info, err := cache.Resolve(ctx, "non-existent-alias", teamSlug)
	require.ErrorIs(t, err, ErrTemplateNotFound)
	require.Nil(t, info)

	// Create the template and alias AFTER the first lookup
	templateID := testutils.CreateTestTemplate(t, db, teamID)
	testutils.CreateTestTemplateAliasWithName(t, db, templateID, "non-existent-alias", &teamSlug)

	// Second lookup should still return not found (cached tombstone)
	info, err = cache.Resolve(ctx, "non-existent-alias", teamSlug)
	require.ErrorIs(t, err, ErrTemplateNotFound)
	require.Nil(t, info)
}

// TestAliasCacheResolve_NegativeCachingFallback tests that tombstones are cached
// for intermediate lookups during fallback resolution
func TestAliasCacheResolve_NegativeCachingFallback(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	redis := redis_utils.SetupInstance(t)
	ctx := t.Context()

	// Create requesting team
	requestingTeamID := testutils.CreateTestTeam(t, db)
	requestingTeamSlug := testutils.GetTeamSlug(t, ctx, db, requestingTeamID)

	// Create promoted template with NULL namespace alias
	promotedTeamID := testutils.CreateTestTeam(t, db)
	promotedTemplateID := testutils.CreateTestTemplate(t, db, promotedTeamID)
	testutils.CreateTestTemplateAliasWithName(t, db, promotedTemplateID, "promoted-alias", nil)

	cache := NewAliasCache(db.SqlcClient, redis)
	defer cache.Close(ctx)

	// Resolve bare alias - tries team namespace first (not found, caches tombstone),
	// then falls back to NULL namespace (found)
	info, err := cache.Resolve(ctx, "promoted-alias", requestingTeamSlug)
	require.NoError(t, err)
	require.NotNil(t, info)
	assert.Equal(t, promotedTemplateID, info.TemplateID)

	// Verify tombstone was cached for team namespace lookup
	tombstoneKey := cache.cache.RedisKey(buildAliasKey(&requestingTeamSlug, "promoted-alias"))
	exists, err := redis.Exists(ctx, tombstoneKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), exists, "tombstone should be cached for team namespace miss")

	// Verify positive result was cached for NULL namespace lookup
	positiveKey := cache.cache.RedisKey(buildAliasKey(nil, "promoted-alias"))
	exists, err = redis.Exists(ctx, positiveKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), exists, "positive result should be cached for NULL namespace hit")
}

func TestAliasCacheResolve_IDLookupDoesNotLeakNamespace(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	redis := redis_utils.SetupInstance(t)
	ctx := t.Context()

	ownerTeamID := testutils.CreateTestTeam(t, db)
	ownerSlug := testutils.GetTeamSlug(t, ctx, db, ownerTeamID)
	templateID := testutils.CreateTestTemplate(t, db, ownerTeamID)
	testutils.CreateTestTemplateAliasWithName(t, db, templateID, "shared-name", &ownerSlug)

	cache := NewAliasCache(db.SqlcClient, redis)
	defer cache.Close(ctx)

	info, err := cache.Resolve(ctx, "shared-name", ownerSlug)
	require.NoError(t, err)
	require.NotNil(t, info)
	assert.Equal(t, ownerSlug+"/shared-name", info.MatchedIdentifier)

	byID, err := cache.LookupByID(ctx, templateID)
	require.NoError(t, err)
	require.NotNil(t, byID)
	assert.Equal(t, templateID, byID.MatchedIdentifier, "direct-ID entries must not carry another team's namespace")
}

func TestAliasCacheResolve_PopulatesNamespace(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	redis := redis_utils.SetupInstance(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	teamSlug := testutils.GetTeamSlug(t, ctx, db, teamID)
	templateID := testutils.CreateTestTemplate(t, db, teamID)
	testutils.CreateTestTemplateAliasWithName(t, db, templateID, "ns-alias", &teamSlug)

	cache := NewAliasCache(db.SqlcClient, redis)
	defer cache.Close(ctx)

	info, err := cache.Resolve(ctx, "ns-alias", teamSlug)
	require.NoError(t, err)
	require.NotNil(t, info)
	assert.Equal(t, teamSlug+"/ns-alias", info.MatchedIdentifier)
}

func TestAliasCacheResolve_PopulatesMatchedIdentifierFromLookupKey(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	redis := redis_utils.SetupInstance(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	teamSlug := testutils.GetTeamSlug(t, ctx, db, teamID)
	templateID := testutils.CreateTestTemplate(t, db, teamID)
	testutils.CreateTestTemplateAliasWithName(t, db, templateID, "cached-alias", &teamSlug)

	cache := NewAliasCache(db.SqlcClient, redis)
	defer cache.Close(ctx)

	legacyCachedValue, err := json.Marshal(&AliasInfo{
		TemplateID: templateID,
		TeamID:     teamID,
	})
	require.NoError(t, err)

	err = redis.Set(ctx, cache.cache.RedisKey(buildAliasKey(&teamSlug, "cached-alias")), legacyCachedValue, aliasCacheTTL).Err()
	require.NoError(t, err)

	info, err := cache.Resolve(ctx, "cached-alias", teamSlug)
	require.NoError(t, err)
	require.NotNil(t, info)
	assert.Equal(t, teamSlug+"/cached-alias", info.MatchedIdentifier)
}
