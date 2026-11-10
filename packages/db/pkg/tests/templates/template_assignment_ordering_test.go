package templates

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	"github.com/e2b-dev/infra/packages/db/queries"
)

// TestGetTemplateWithBuildByTag_AssignmentOrderDifferentFromBuildOrder verifies that
// when assignment created_at order differs from build created_at order, the assignment
// order takes precedence.
func TestGetTemplateWithBuildByTag_AssignmentOrderDifferentFromBuildOrder(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	// Create team and template
	teamID := testutils.CreateTestTeam(t, db)
	templateID := testutils.CreateTestTemplate(t, db, teamID)

	// Create builds: build1 (older), build2 (newer)
	build1ID := testutils.CreateTestBuild(t, ctx, db, templateID, "uploaded")
	time.Sleep(10 * time.Millisecond)
	build2ID := testutils.CreateTestBuild(t, ctx, db, templateID, "uploaded")

	// Assign in REVERSE order: build2 first, then build1
	// Even though build2 was created later, build1 has the latest assignment
	testutils.CreateTestBuildAssignment(t, ctx, db, templateID, build2ID, "default")
	time.Sleep(10 * time.Millisecond)
	testutils.CreateTestBuildAssignment(t, ctx, db, templateID, build1ID, "default")

	// GetTemplateWithBuildByTag should return build1 (latest assignment)
	result, err := db.SqlcClient.GetTemplateWithBuildByTag(ctx, queries.GetTemplateWithBuildByTagParams{
		TemplateID: templateID,
		Tag:        nil, // defaults to 'default'
	})
	require.NoError(t, err)

	assert.Equal(t, build1ID, result.EnvBuild.ID,
		"GetTemplateWithBuildByTag should use assignment order, not build creation order")
}

// TestGetTemplateWithBuildByTag_AssignmentOrderSameAsBuildOrder verifies that the query
// returns the build from the most recent assignment for a specific tag.
func TestGetTemplateWithBuildByTag_AssignmentOrderSameAsBuildOrder(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	// Create team and template
	teamID := testutils.CreateTestTeam(t, db)
	templateID := testutils.CreateTestTemplate(t, db, teamID)

	// Create two builds
	build1ID := testutils.CreateTestBuild(t, ctx, db, templateID, "uploaded")
	build2ID := testutils.CreateTestBuild(t, ctx, db, templateID, "uploaded")

	// Assign both to 'default' tag - build2 should be latest
	testutils.CreateTestBuildAssignment(t, ctx, db, templateID, build1ID, "default")
	time.Sleep(10 * time.Millisecond)
	testutils.CreateTestBuildAssignment(t, ctx, db, templateID, build2ID, "default")

	// GetTemplateWithBuildByTag should return build2 (latest assignment)
	result, err := db.SqlcClient.GetTemplateWithBuildByTag(ctx, queries.GetTemplateWithBuildByTagParams{
		TemplateID: templateID,

		Tag: nil,
	})
	require.NoError(t, err)

	assert.Equal(t, build2ID, result.EnvBuild.ID,
		"GetTemplateWithBuildByTag should return the build from the latest assignment")
}

// TestGetTemplateWithBuildByTag_OnlyReturnsUploadedBuilds verifies that only builds
// with status IN ('success', 'uploaded') are returned, even if a non-ready build has a more recent assignment.
func TestGetTemplateWithBuildByTag_OnlyReturnsUploadedBuilds(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	// Create team and template
	teamID := testutils.CreateTestTeam(t, db)
	templateID := testutils.CreateTestTemplate(t, db, teamID)

	// Create an uploaded build and a waiting build
	uploadedBuildID := testutils.CreateTestBuild(t, ctx, db, templateID, "uploaded")
	waitingBuildID := testutils.CreateTestBuild(t, ctx, db, templateID, "waiting")

	// Assign uploaded first, then waiting (latest)
	testutils.CreateTestBuildAssignment(t, ctx, db, templateID, uploadedBuildID, "default")
	time.Sleep(10 * time.Millisecond)
	testutils.CreateTestBuildAssignment(t, ctx, db, templateID, waitingBuildID, "default")

	// GetTemplateWithBuildByTag should return the uploaded build, not waiting
	result, err := db.SqlcClient.GetTemplateWithBuildByTag(ctx, queries.GetTemplateWithBuildByTagParams{
		TemplateID: templateID,

		Tag: nil,
	})
	require.NoError(t, err)

	assert.Equal(t, uploadedBuildID, result.EnvBuild.ID,
		"GetTemplateWithBuildByTag should only return builds with status IN ('success', 'uploaded')")
}

// TestGetTemplateWithBuildByTag_SpecificTag verifies that the query returns
// the correct build for a specific non-default tag.
func TestGetTemplateWithBuildByTag_SpecificTag(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	// Create team and template
	teamID := testutils.CreateTestTeam(t, db)
	templateID := testutils.CreateTestTemplate(t, db, teamID)

	// Create two builds
	defaultBuildID := testutils.CreateTestBuild(t, ctx, db, templateID, "uploaded")
	v1BuildID := testutils.CreateTestBuild(t, ctx, db, templateID, "uploaded")

	// Assign to different tags
	testutils.CreateTestBuildAssignment(t, ctx, db, templateID, defaultBuildID, "default")
	testutils.CreateTestBuildAssignment(t, ctx, db, templateID, v1BuildID, "v1")

	// Query for 'v1' tag
	v1Tag := "v1"
	result, err := db.SqlcClient.GetTemplateWithBuildByTag(ctx, queries.GetTemplateWithBuildByTagParams{
		TemplateID: templateID,

		Tag: &v1Tag,
	})
	require.NoError(t, err)

	assert.Equal(t, v1BuildID, result.EnvBuild.ID,
		"GetTemplateWithBuildByTag should return the build for the specified tag")
}

// TestGetTeamTemplates_AssignmentOrderDifferentFromBuildOrder verifies that
// GetTeamTemplates uses assignment order for determining the latest build.
func TestGetTeamTemplates_AssignmentOrderDifferentFromBuildOrder(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	// Create team and template
	teamID := testutils.CreateTestTeam(t, db)
	templateID := testutils.CreateTestTemplate(t, db, teamID)

	// Create builds: build1 (older), build2 (newer)
	build1ID := testutils.CreateTestBuild(t, ctx, db, templateID, "uploaded")
	time.Sleep(10 * time.Millisecond)
	build2ID := testutils.CreateTestBuild(t, ctx, db, templateID, "uploaded")

	// Assign in REVERSE order: build2 first, then build1 (latest assignment)
	testutils.CreateTestBuildAssignment(t, ctx, db, templateID, build2ID, "default")
	time.Sleep(10 * time.Millisecond)
	testutils.CreateTestBuildAssignment(t, ctx, db, templateID, build1ID, "default")

	// GetTeamTemplates should return build1 (latest assignment)
	results, err := db.SqlcClient.GetTeamTemplates(ctx, queries.GetTeamTemplatesParams{TeamID: teamID})
	require.NoError(t, err)
	require.Len(t, results, 1)

	assert.Equal(t, build1ID, results[0].BuildID,
		"GetTeamTemplates should use assignment order, not build creation order")
}
