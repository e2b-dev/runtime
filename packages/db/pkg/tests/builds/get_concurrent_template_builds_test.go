package builds

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	"github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/db/queries"
)

func TestGetConcurrentTemplateBuilds_ReturnsBuildWithSameTag(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	templateID := testutils.CreateTestTemplate(t, db, teamID)

	buildA := testutils.CreateTestBuild(t, ctx, db, templateID, types.BuildStatusGroupInProgress)
	testutils.CreateTestBuildAssignment(t, ctx, db, templateID, buildA, "v1")

	buildB := testutils.CreateTestBuild(t, ctx, db, templateID, types.BuildStatusGroupInProgress)
	testutils.CreateTestBuildAssignment(t, ctx, db, templateID, buildB, "v1")

	results, err := db.SqlcClient.GetConcurrentTemplateBuilds(ctx, queries.GetConcurrentTemplateBuildsParams{
		TemplateID:     templateID,
		CurrentBuildID: buildA,
	})
	require.NoError(t, err)

	assert.Len(t, results, 1, "Should return the other build with the same tag")
	assert.Equal(t, buildB, results[0].ID)
}

func TestGetConcurrentTemplateBuilds_DoesNotReturnBuildWithDifferentTag(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	templateID := testutils.CreateTestTemplate(t, db, teamID)

	buildA := testutils.CreateTestBuild(t, ctx, db, templateID, types.BuildStatusGroupInProgress)
	testutils.CreateTestBuildAssignment(t, ctx, db, templateID, buildA, "v1")

	buildB := testutils.CreateTestBuild(t, ctx, db, templateID, types.BuildStatusGroupInProgress)
	testutils.CreateTestBuildAssignment(t, ctx, db, templateID, buildB, "v2")

	results, err := db.SqlcClient.GetConcurrentTemplateBuilds(ctx, queries.GetConcurrentTemplateBuildsParams{
		TemplateID:     templateID,
		CurrentBuildID: buildA,
	})
	require.NoError(t, err)

	assert.Empty(t, results, "Should not return builds with non-overlapping tags")
}

func TestGetConcurrentTemplateBuilds_ReturnsBuildsWithOverlappingTags(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	templateID := testutils.CreateTestTemplate(t, db, teamID)

	// Build A has tags: v1, v2
	buildA := testutils.CreateTestBuild(t, ctx, db, templateID, types.BuildStatusGroupInProgress)
	testutils.CreateTestBuildAssignment(t, ctx, db, templateID, buildA, "v1")
	testutils.CreateTestBuildAssignment(t, ctx, db, templateID, buildA, "v2")

	// Build B has tags: v2, v3 (overlaps on v2)
	buildB := testutils.CreateTestBuild(t, ctx, db, templateID, types.BuildStatusGroupInProgress)
	testutils.CreateTestBuildAssignment(t, ctx, db, templateID, buildB, "v2")
	testutils.CreateTestBuildAssignment(t, ctx, db, templateID, buildB, "v3")

	// Build C has tags: v3 only (no overlap with A)
	buildC := testutils.CreateTestBuild(t, ctx, db, templateID, types.BuildStatusGroupInProgress)
	testutils.CreateTestBuildAssignment(t, ctx, db, templateID, buildC, "v3")

	results, err := db.SqlcClient.GetConcurrentTemplateBuilds(ctx, queries.GetConcurrentTemplateBuildsParams{
		TemplateID:     templateID,
		CurrentBuildID: buildA,
	})
	require.NoError(t, err)

	assert.Len(t, results, 1, "Should return only the build with overlapping tags")
	assert.Equal(t, buildB, results[0].ID, "Build B overlaps on tag v2")
}

func TestGetConcurrentTemplateBuilds_DoesNotReturnBuildsFromOtherTemplates(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	template1 := testutils.CreateTestTemplate(t, db, teamID)
	template2 := testutils.CreateTestTemplate(t, db, teamID)

	buildA := testutils.CreateTestBuild(t, ctx, db, template1, types.BuildStatusGroupInProgress)
	testutils.CreateTestBuildAssignment(t, ctx, db, template1, buildA, "v1")

	// Same tag, different template
	buildB := testutils.CreateTestBuild(t, ctx, db, template2, types.BuildStatusGroupInProgress)
	testutils.CreateTestBuildAssignment(t, ctx, db, template2, buildB, "v1")

	results, err := db.SqlcClient.GetConcurrentTemplateBuilds(ctx, queries.GetConcurrentTemplateBuildsParams{
		TemplateID:     template1,
		CurrentBuildID: buildA,
	})
	require.NoError(t, err)

	assert.Empty(t, results, "Should not return builds from other templates")
}

func TestGetConcurrentTemplateBuilds_OnlyReturnsPendingAndInProgressBuilds(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	templateID := testutils.CreateTestTemplate(t, db, teamID)

	currentBuild := testutils.CreateTestBuild(t, ctx, db, templateID, types.BuildStatusGroupInProgress)
	testutils.CreateTestBuildAssignment(t, ctx, db, templateID, currentBuild, "v1")

	pendingBuild := testutils.CreateTestBuild(t, ctx, db, templateID, types.BuildStatusGroupPending)
	testutils.CreateTestBuildAssignment(t, ctx, db, templateID, pendingBuild, "v1")

	inProgressBuild := testutils.CreateTestBuild(t, ctx, db, templateID, types.BuildStatusGroupInProgress)
	testutils.CreateTestBuildAssignment(t, ctx, db, templateID, inProgressBuild, "v1")

	readyBuild := testutils.CreateTestBuild(t, ctx, db, templateID, types.BuildStatusGroupReady)
	testutils.CreateTestBuildAssignment(t, ctx, db, templateID, readyBuild, "v1")

	failedBuild := testutils.CreateTestBuild(t, ctx, db, templateID, types.BuildStatusGroupFailed)
	testutils.CreateTestBuildAssignment(t, ctx, db, templateID, failedBuild, "v1")

	results, err := db.SqlcClient.GetConcurrentTemplateBuilds(ctx, queries.GetConcurrentTemplateBuildsParams{
		TemplateID:     templateID,
		CurrentBuildID: currentBuild,
	})
	require.NoError(t, err)

	resultIDs := make(map[string]bool)
	for _, r := range results {
		resultIDs[r.ID.String()] = true
	}

	assert.True(t, resultIDs[pendingBuild.String()], "Pending build should be returned")
	assert.True(t, resultIDs[inProgressBuild.String()], "In-progress build should be returned")
	assert.False(t, resultIDs[readyBuild.String()], "Ready build should NOT be returned")
	assert.False(t, resultIDs[failedBuild.String()], "Failed build should NOT be returned")
	assert.Len(t, results, 2)
}

func TestGetConcurrentTemplateBuilds_NoDuplicatesWithMultipleOverlappingTags(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	templateID := testutils.CreateTestTemplate(t, db, teamID)

	// Both builds share two tags — should still return only one result
	buildA := testutils.CreateTestBuild(t, ctx, db, templateID, types.BuildStatusGroupInProgress)
	testutils.CreateTestBuildAssignment(t, ctx, db, templateID, buildA, "v1")
	testutils.CreateTestBuildAssignment(t, ctx, db, templateID, buildA, "v2")

	buildB := testutils.CreateTestBuild(t, ctx, db, templateID, types.BuildStatusGroupInProgress)
	testutils.CreateTestBuildAssignment(t, ctx, db, templateID, buildB, "v1")
	testutils.CreateTestBuildAssignment(t, ctx, db, templateID, buildB, "v2")

	results, err := db.SqlcClient.GetConcurrentTemplateBuilds(ctx, queries.GetConcurrentTemplateBuildsParams{
		TemplateID:     templateID,
		CurrentBuildID: buildA,
	})
	require.NoError(t, err)

	assert.Len(t, results, 1, "Should return build B exactly once despite multiple overlapping tags")
	assert.Equal(t, buildB, results[0].ID)
}
