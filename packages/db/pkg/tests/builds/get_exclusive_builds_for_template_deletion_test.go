package builds

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	"github.com/e2b-dev/infra/packages/db/pkg/types"
)

func TestGetExclusiveBuildsForTemplateDeletion_ExclusiveBuild(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	// Create a template with a build assigned only to it
	teamID := testutils.CreateTestTeam(t, db)
	templateID := testutils.CreateTestTemplate(t, db, teamID)
	buildID := testutils.CreateTestBuild(t, ctx, db, templateID, types.BuildStatusGroupReady)
	testutils.CreateTestBuildAssignment(t, ctx, db, templateID, buildID, "default")

	// Execute query
	results, err := db.SqlcClient.GetExclusiveBuildsForTemplateDeletion(ctx, templateID)
	require.NoError(t, err)

	// Build should be returned since it's only assigned to this template
	assert.Len(t, results, 1, "Should return 1 exclusive build")
	assert.Equal(t, buildID, results[0].BuildID, "Build ID should match")
}

func TestGetExclusiveBuildsForTemplateDeletion_SharedBuild(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	// Create two templates
	teamID := testutils.CreateTestTeam(t, db)
	template1ID := testutils.CreateTestTemplate(t, db, teamID)
	template2ID := testutils.CreateTestTemplate(t, db, teamID)

	// Create a build assigned to template1
	buildID := testutils.CreateTestBuild(t, ctx, db, template1ID, types.BuildStatusGroupReady)
	testutils.CreateTestBuildAssignment(t, ctx, db, template1ID, buildID, "default")

	// Also assign the same build to template2 (shared build)
	testutils.CreateTestBuildAssignment(t, ctx, db, template2ID, buildID, "default")

	// Execute query for template1
	results, err := db.SqlcClient.GetExclusiveBuildsForTemplateDeletion(ctx, template1ID)
	require.NoError(t, err)

	// Build should NOT be returned since it's shared with another template
	assert.Empty(t, results, "Should not return shared builds")
}

func TestGetExclusiveBuildsForTemplateDeletion_MixedBuilds(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	// Create two templates
	teamID := testutils.CreateTestTeam(t, db)
	template1ID := testutils.CreateTestTemplate(t, db, teamID)
	template2ID := testutils.CreateTestTemplate(t, db, teamID)

	// Create an exclusive build for template1
	exclusiveBuildID := testutils.CreateTestBuild(t, ctx, db, template1ID, types.BuildStatusGroupReady)
	testutils.CreateTestBuildAssignment(t, ctx, db, template1ID, exclusiveBuildID, "default")

	// Create a shared build (assigned to both templates)
	sharedBuildID := testutils.CreateTestBuild(t, ctx, db, template1ID, types.BuildStatusGroupReady)
	testutils.CreateTestBuildAssignment(t, ctx, db, template1ID, sharedBuildID, "default")
	testutils.CreateTestBuildAssignment(t, ctx, db, template2ID, sharedBuildID, "default")

	// Execute query for template1
	results, err := db.SqlcClient.GetExclusiveBuildsForTemplateDeletion(ctx, template1ID)
	require.NoError(t, err)

	// Only the exclusive build should be returned
	assert.Len(t, results, 1, "Should return only 1 exclusive build")
	assert.Equal(t, exclusiveBuildID, results[0].BuildID, "Should return the exclusive build")
}

func TestGetExclusiveBuildsForTemplateDeletion_NoBuilds(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	// Create template without any builds
	teamID := testutils.CreateTestTeam(t, db)
	templateID := testutils.CreateTestTemplate(t, db, teamID)

	// Execute query
	results, err := db.SqlcClient.GetExclusiveBuildsForTemplateDeletion(ctx, templateID)
	require.NoError(t, err)

	// Should return empty results (no builds to delete)
	assert.Empty(t, results, "Should return empty results for template with no builds")
}

func TestGetExclusiveBuildsForTemplateDeletion_MultipleTagsSameTemplate(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	// Create template with a build assigned with multiple tags
	teamID := testutils.CreateTestTeam(t, db)
	templateID := testutils.CreateTestTemplate(t, db, teamID)
	buildID := testutils.CreateTestBuild(t, ctx, db, templateID, types.BuildStatusGroupReady)

	// Assign same build with multiple tags to same template
	testutils.CreateTestBuildAssignment(t, ctx, db, templateID, buildID, "default")
	testutils.CreateTestBuildAssignment(t, ctx, db, templateID, buildID, "v1")
	testutils.CreateTestBuildAssignment(t, ctx, db, templateID, buildID, "latest")

	// Execute query
	results, err := db.SqlcClient.GetExclusiveBuildsForTemplateDeletion(ctx, templateID)
	require.NoError(t, err)

	// Should return the build only once (DISTINCT)
	assert.Len(t, results, 1, "Should return build only once despite multiple tag assignments")
	assert.Equal(t, buildID, results[0].BuildID, "Build ID should match")
}

func TestGetExclusiveBuildsForTemplateDeletion_SharedBuildAcrossTeams(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	// Create two templates belonging to different teams
	team1ID := testutils.CreateTestTeam(t, db)
	team2ID := testutils.CreateTestTeam(t, db)
	template1ID := testutils.CreateTestTemplate(t, db, team1ID)
	template2ID := testutils.CreateTestTemplate(t, db, team2ID)

	// Create a build assigned to template1
	buildID := testutils.CreateTestBuild(t, ctx, db, template1ID, types.BuildStatusGroupReady)
	testutils.CreateTestBuildAssignment(t, ctx, db, template1ID, buildID, "default")

	// Also assign the same build to template2 (shared build across teams)
	testutils.CreateTestBuildAssignment(t, ctx, db, template2ID, buildID, "default")

	// Execute query for template1
	results, err := db.SqlcClient.GetExclusiveBuildsForTemplateDeletion(ctx, template1ID)
	require.NoError(t, err)

	// Build should NOT be returned since it's shared with another template (even in a different team)
	assert.Empty(t, results, "Should not return builds shared across teams")
}

func TestGetExclusiveBuildsForTemplateDeletion_MixedBuildsAcrossTeams(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	// Create two templates belonging to different teams
	team1ID := testutils.CreateTestTeam(t, db)
	team2ID := testutils.CreateTestTeam(t, db)
	template1ID := testutils.CreateTestTemplate(t, db, team1ID)
	template2ID := testutils.CreateTestTemplate(t, db, team2ID)

	// Create an exclusive build for template1
	exclusiveBuildID := testutils.CreateTestBuild(t, ctx, db, template1ID, types.BuildStatusGroupReady)
	testutils.CreateTestBuildAssignment(t, ctx, db, template1ID, exclusiveBuildID, "default")

	// Create a shared build (assigned to both templates from different teams)
	sharedBuildID := testutils.CreateTestBuild(t, ctx, db, template1ID, types.BuildStatusGroupReady)
	testutils.CreateTestBuildAssignment(t, ctx, db, template1ID, sharedBuildID, "default")
	testutils.CreateTestBuildAssignment(t, ctx, db, template2ID, sharedBuildID, "default")

	// Execute query for template1
	results, err := db.SqlcClient.GetExclusiveBuildsForTemplateDeletion(ctx, template1ID)
	require.NoError(t, err)

	// Only the exclusive build should be returned
	assert.Len(t, results, 1, "Should return only 1 exclusive build")
	assert.Equal(t, exclusiveBuildID, results[0].BuildID, "Should return the exclusive build, not the one shared across teams")
}
