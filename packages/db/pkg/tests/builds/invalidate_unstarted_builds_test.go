package builds

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	"github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/db/queries"
)

func TestInvalidateUnstartedTemplateBuilds_InvalidatesWaitingBuilds(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	// Create template with a waiting build
	teamID := testutils.CreateTestTeam(t, db)
	templateID := testutils.CreateTestTemplate(t, db, teamID)
	buildID := testutils.CreateTestBuild(t, ctx, db, templateID, types.BuildStatusGroupPending)
	testutils.CreateTestBuildAssignment(t, ctx, db, templateID, buildID, "default")

	// Invalidate waiting builds for default tag
	err := db.SqlcClient.InvalidateUnstartedTemplateBuilds(ctx, queries.InvalidateUnstartedTemplateBuildsParams{
		TemplateID: templateID,
		Tags:       []string{"default"},
		Reason:     types.BuildReason{Message: "Test invalidation"},
	})
	require.NoError(t, err)

	// Verify build status changed to failed
	status := testutils.GetBuildStatusGroup(t, ctx, db, buildID)
	assert.Equal(t, types.BuildStatusGroupFailed, status, "Waiting build should be invalidated to failed")
}

func TestInvalidateUnstartedTemplateBuilds_OnlyAffectsSpecificTag(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	// Create template with builds assigned to different tags
	teamID := testutils.CreateTestTeam(t, db)
	templateID := testutils.CreateTestTemplate(t, db, teamID)

	// Build with 'default' tag
	defaultBuildID := testutils.CreateTestBuild(t, ctx, db, templateID, types.BuildStatusGroupPending)
	testutils.CreateTestBuildAssignment(t, ctx, db, templateID, defaultBuildID, "default")

	// Build with 'v1' tag only (delete trigger-created 'default' assignment)
	v1BuildID := testutils.CreateTestBuild(t, ctx, db, templateID, types.BuildStatusGroupPending)
	testutils.DeleteTriggerBuildAssignment(t, ctx, db, templateID, v1BuildID, "default")
	testutils.CreateTestBuildAssignment(t, ctx, db, templateID, v1BuildID, "v1")

	// Invalidate only 'default' tag builds
	err := db.SqlcClient.InvalidateUnstartedTemplateBuilds(ctx, queries.InvalidateUnstartedTemplateBuildsParams{
		TemplateID: templateID,
		Tags:       []string{"default"},
		Reason:     types.BuildReason{Message: "Test invalidation"},
	})
	require.NoError(t, err)

	// Verify only default build was invalidated
	defaultStatus := testutils.GetBuildStatusGroup(t, ctx, db, defaultBuildID)
	assert.Equal(t, types.BuildStatusGroupFailed, defaultStatus, "Default tag build should be invalidated")

	v1Status := testutils.GetBuildStatusGroup(t, ctx, db, v1BuildID)
	assert.Equal(t, types.BuildStatusGroupPending, v1Status, "v1 tag build should NOT be affected")
}

func TestInvalidateUnstartedTemplateBuilds_DoesNotAffectOtherTemplates(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	// Create two templates with waiting builds
	teamID := testutils.CreateTestTeam(t, db)
	template1ID := testutils.CreateTestTemplate(t, db, teamID)
	template2ID := testutils.CreateTestTemplate(t, db, teamID)

	build1ID := testutils.CreateTestBuild(t, ctx, db, template1ID, types.BuildStatusGroupPending)
	testutils.CreateTestBuildAssignment(t, ctx, db, template1ID, build1ID, "default")

	build2ID := testutils.CreateTestBuild(t, ctx, db, template2ID, types.BuildStatusGroupPending)
	testutils.CreateTestBuildAssignment(t, ctx, db, template2ID, build2ID, "default")

	// Invalidate only template1's builds
	err := db.SqlcClient.InvalidateUnstartedTemplateBuilds(ctx, queries.InvalidateUnstartedTemplateBuildsParams{
		TemplateID: template1ID,
		Tags:       []string{"default"},
		Reason:     types.BuildReason{Message: "Test invalidation"},
	})
	require.NoError(t, err)

	// Verify only template1's build was invalidated
	status1 := testutils.GetBuildStatusGroup(t, ctx, db, build1ID)
	assert.Equal(t, types.BuildStatusGroupFailed, status1, "Template1's build should be invalidated")

	status2 := testutils.GetBuildStatusGroup(t, ctx, db, build2ID)
	assert.Equal(t, types.BuildStatusGroupPending, status2, "Template2's build should NOT be affected")
}

func TestInvalidateUnstartedTemplateBuilds_DoesNotAffectNonWaitingBuilds(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	// Create template with builds in various states
	teamID := testutils.CreateTestTeam(t, db)
	templateID := testutils.CreateTestTemplate(t, db, teamID)

	waitingBuildID := testutils.CreateTestBuild(t, ctx, db, templateID, types.BuildStatusGroupPending)
	testutils.CreateTestBuildAssignment(t, ctx, db, templateID, waitingBuildID, "default")

	buildingBuildID := testutils.CreateTestBuild(t, ctx, db, templateID, types.BuildStatusGroupInProgress)
	testutils.CreateTestBuildAssignment(t, ctx, db, templateID, buildingBuildID, "default")

	uploadedBuildID := testutils.CreateTestBuild(t, ctx, db, templateID, types.BuildStatusGroupReady)
	testutils.CreateTestBuildAssignment(t, ctx, db, templateID, uploadedBuildID, "default")

	// Invalidate waiting builds
	err := db.SqlcClient.InvalidateUnstartedTemplateBuilds(ctx, queries.InvalidateUnstartedTemplateBuildsParams{
		TemplateID: templateID,
		Tags:       []string{"default"},
		Reason:     types.BuildReason{Message: "Test invalidation"},
	})
	require.NoError(t, err)

	// Verify only waiting build was invalidated
	waitingStatus := testutils.GetBuildStatusGroup(t, ctx, db, waitingBuildID)
	assert.Equal(t, types.BuildStatusGroupFailed, waitingStatus, "Waiting build should be invalidated")

	buildingStatus := testutils.GetBuildStatusGroup(t, ctx, db, buildingBuildID)
	assert.Equal(t, types.BuildStatusGroupInProgress, buildingStatus, "Building build should NOT be affected")

	uploadedStatus := testutils.GetBuildStatusGroup(t, ctx, db, uploadedBuildID)
	assert.Equal(t, types.BuildStatusGroupReady, uploadedStatus, "Uploaded build should NOT be affected")
}

func TestInvalidateUnstartedTemplateBuilds_MultipleWaitingBuilds(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	// Create template with multiple waiting builds
	teamID := testutils.CreateTestTeam(t, db)
	templateID := testutils.CreateTestTemplate(t, db, teamID)

	build1ID := testutils.CreateTestBuild(t, ctx, db, templateID, types.BuildStatusGroupPending)
	testutils.CreateTestBuildAssignment(t, ctx, db, templateID, build1ID, "default")

	build2ID := testutils.CreateTestBuild(t, ctx, db, templateID, types.BuildStatusGroupPending)
	testutils.CreateTestBuildAssignment(t, ctx, db, templateID, build2ID, "default")

	build3ID := testutils.CreateTestBuild(t, ctx, db, templateID, types.BuildStatusGroupPending)
	testutils.CreateTestBuildAssignment(t, ctx, db, templateID, build3ID, "default")

	// Invalidate all waiting builds
	err := db.SqlcClient.InvalidateUnstartedTemplateBuilds(ctx, queries.InvalidateUnstartedTemplateBuildsParams{
		TemplateID: templateID,
		Tags:       []string{"default"},
		Reason:     types.BuildReason{Message: "Test invalidation"},
	})
	require.NoError(t, err)

	// Verify all waiting builds were invalidated
	assert.Equal(t, types.BuildStatusGroupFailed, testutils.GetBuildStatusGroup(t, ctx, db, build1ID), "Build 1 should be invalidated")
	assert.Equal(t, types.BuildStatusGroupFailed, testutils.GetBuildStatusGroup(t, ctx, db, build2ID), "Build 2 should be invalidated")
	assert.Equal(t, types.BuildStatusGroupFailed, testutils.GetBuildStatusGroup(t, ctx, db, build3ID), "Build 3 should be invalidated")
}

func TestInvalidateUnstartedTemplateBuilds_MultipleTagsInSingleCall(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	// Create template with builds assigned to different tags
	teamID := testutils.CreateTestTeam(t, db)
	templateID := testutils.CreateTestTemplate(t, db, teamID)

	// Build with 'default' tag
	defaultBuildID := testutils.CreateTestBuild(t, ctx, db, templateID, types.BuildStatusGroupPending)
	testutils.CreateTestBuildAssignment(t, ctx, db, templateID, defaultBuildID, "default")

	// Build with 'v1' tag only (delete trigger-created 'default' assignment)
	v1BuildID := testutils.CreateTestBuild(t, ctx, db, templateID, types.BuildStatusGroupPending)
	testutils.DeleteTriggerBuildAssignment(t, ctx, db, templateID, v1BuildID, "default")
	testutils.CreateTestBuildAssignment(t, ctx, db, templateID, v1BuildID, "v1")

	// Build with 'v2' tag only (delete trigger-created 'default' assignment)
	v2BuildID := testutils.CreateTestBuild(t, ctx, db, templateID, types.BuildStatusGroupPending)
	testutils.DeleteTriggerBuildAssignment(t, ctx, db, templateID, v2BuildID, "default")
	testutils.CreateTestBuildAssignment(t, ctx, db, templateID, v2BuildID, "v2")

	// Build with 'stable' tag only (should NOT be affected) - delete trigger-created 'default' assignment
	stableBuildID := testutils.CreateTestBuild(t, ctx, db, templateID, types.BuildStatusGroupPending)
	testutils.DeleteTriggerBuildAssignment(t, ctx, db, templateID, stableBuildID, "default")
	testutils.CreateTestBuildAssignment(t, ctx, db, templateID, stableBuildID, "stable")

	// Invalidate 'default', 'v1', and 'v2' tags in a single call
	err := db.SqlcClient.InvalidateUnstartedTemplateBuilds(ctx, queries.InvalidateUnstartedTemplateBuildsParams{
		TemplateID: templateID,
		Tags:       []string{"default", "v1", "v2"},
		Reason:     types.BuildReason{Message: "Test batch invalidation"},
	})
	require.NoError(t, err)

	// Verify the specified tags were invalidated
	assert.Equal(t, types.BuildStatusGroupFailed, testutils.GetBuildStatusGroup(t, ctx, db, defaultBuildID), "Default tag build should be invalidated")
	assert.Equal(t, types.BuildStatusGroupFailed, testutils.GetBuildStatusGroup(t, ctx, db, v1BuildID), "v1 tag build should be invalidated")
	assert.Equal(t, types.BuildStatusGroupFailed, testutils.GetBuildStatusGroup(t, ctx, db, v2BuildID), "v2 tag build should be invalidated")

	// Verify the 'stable' tag was NOT affected
	assert.Equal(t, types.BuildStatusGroupPending, testutils.GetBuildStatusGroup(t, ctx, db, stableBuildID), "stable tag build should NOT be affected")
}
