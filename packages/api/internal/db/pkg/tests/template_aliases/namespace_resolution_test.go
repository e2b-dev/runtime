package aliases

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/db/pkg/testutils"
	"github.com/e2b-dev/infra/packages/api/internal/db/queries"
)

func TestGetTemplateByAlias_MatchesNamespace(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	templateID := testutils.CreateTestTemplate(t, db, teamID)
	teamSlug := testutils.GetTeamSlug(t, ctx, db, teamID)

	testutils.CreateTestTemplateAliasWithName(t, db, templateID, "my-template", &teamSlug)

	result, err := db.SqlcClient.GetTemplateByAlias(ctx, queries.GetTemplateByAliasParams{
		Alias:     "my-template",
		Namespace: &teamSlug,
	})
	require.NoError(t, err)
	assert.Equal(t, templateID, result.ID)
}

func TestGetTemplateByAlias_MatchesNullNamespace(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	templateID := testutils.CreateTestTemplate(t, db, teamID)

	testutils.CreateTestTemplateAliasWithName(t, db, templateID, "base-template", nil)

	result, err := db.SqlcClient.GetTemplateByAlias(ctx, queries.GetTemplateByAliasParams{
		Alias:     "base-template",
		Namespace: nil,
	})
	require.NoError(t, err)
	assert.Equal(t, templateID, result.ID)
}

func TestGetTemplateByAlias_NotFound(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	templateID := testutils.CreateTestTemplate(t, db, teamID)
	teamSlug := testutils.GetTeamSlug(t, ctx, db, teamID)

	testutils.CreateTestTemplateAliasWithName(t, db, templateID, "team-scoped", &teamSlug)

	// Different namespace should not find it
	otherNamespace := "other-team"
	_, err := db.SqlcClient.GetTemplateByAlias(ctx, queries.GetTemplateByAliasParams{
		Alias:     "team-scoped",
		Namespace: &otherNamespace,
	})
	require.Error(t, err)

	// Non-existent alias should not find it
	_, err = db.SqlcClient.GetTemplateByAlias(ctx, queries.GetTemplateByAliasParams{
		Alias:     "non-existent",
		Namespace: &teamSlug,
	})
	require.Error(t, err)
}

func TestGetTemplateById(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	templateID := testutils.CreateTestTemplate(t, db, teamID)

	result, err := db.SqlcClient.GetTemplateById(ctx, templateID)
	require.NoError(t, err)
	assert.Equal(t, templateID, result.ID)
}

func TestTwoTeamsCanHaveSameAliasName(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	// Create two teams
	teamAID := testutils.CreateTestTeam(t, db)
	teamBID := testutils.CreateTestTeam(t, db)
	teamASlug := testutils.GetTeamSlug(t, ctx, db, teamAID)
	teamBSlug := testutils.GetTeamSlug(t, ctx, db, teamBID)

	// Create templates for each team
	templateAID := testutils.CreateTestTemplate(t, db, teamAID)
	templateBID := testutils.CreateTestTemplate(t, db, teamBID)

	// Both teams create an alias with the same name "my-template"
	testutils.CreateTestTemplateAliasWithName(t, db, templateAID, "my-template", &teamASlug)
	testutils.CreateTestTemplateAliasWithName(t, db, templateBID, "my-template", &teamBSlug)

	// Verify team A's alias resolves to team A's template
	resultA, err := db.SqlcClient.GetTemplateByAlias(ctx, queries.GetTemplateByAliasParams{
		Alias:     "my-template",
		Namespace: &teamASlug,
	})
	require.NoError(t, err)
	assert.Equal(t, templateAID, resultA.ID)

	// Verify team B's alias resolves to team B's template
	resultB, err := db.SqlcClient.GetTemplateByAlias(ctx, queries.GetTemplateByAliasParams{
		Alias:     "my-template",
		Namespace: &teamBSlug,
	})
	require.NoError(t, err)
	assert.Equal(t, templateBID, resultB.ID)
}

func TestCheckAliasExistsInNamespace_FindsInSameNamespace(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	templateID := testutils.CreateTestTemplate(t, db, teamID)
	teamSlug := testutils.GetTeamSlug(t, ctx, db, teamID)

	testutils.CreateTestTemplateAliasWithName(t, db, templateID, "my-alias", &teamSlug)

	// Should find alias in team's namespace
	result, err := db.SqlcClient.CheckAliasExistsInNamespace(ctx, queries.CheckAliasExistsInNamespaceParams{
		Alias:     "my-alias",
		Namespace: &teamSlug,
	})
	require.NoError(t, err)
	assert.Equal(t, templateID, result.EnvID)
	assert.Equal(t, &teamSlug, result.Namespace)
}

func TestCheckAliasExistsInNamespace_NotFoundInDifferentNamespace(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	templateID := testutils.CreateTestTemplate(t, db, teamID)
	teamSlug := testutils.GetTeamSlug(t, ctx, db, teamID)

	testutils.CreateTestTemplateAliasWithName(t, db, templateID, "team-only-alias", &teamSlug)

	// Should NOT find alias in a different namespace
	otherNamespace := "other-team-slug"
	_, err := db.SqlcClient.CheckAliasExistsInNamespace(ctx, queries.CheckAliasExistsInNamespaceParams{
		Alias:     "team-only-alias",
		Namespace: &otherNamespace,
	})
	require.Error(t, err)
}

func TestCheckAliasExistsInNamespace_NullNamespaceForPromotedTemplates(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	templateID := testutils.CreateTestTemplate(t, db, teamID)

	// Create alias with NULL namespace (promoted/public template)
	testutils.CreateTestTemplateAliasWithName(t, db, templateID, "base", nil)

	// Should find alias with NULL namespace
	result, err := db.SqlcClient.CheckAliasExistsInNamespace(ctx, queries.CheckAliasExistsInNamespaceParams{
		Alias:     "base",
		Namespace: nil,
	})
	require.NoError(t, err)
	assert.Equal(t, templateID, result.EnvID)
	assert.Nil(t, result.Namespace)
}
