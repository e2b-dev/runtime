package queries_test

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
)

// TestGetExclusiveBuildsForTemplateDeletion_ExclusiveBuild verifies that a build
// assigned only to the target template is returned by the query.
func TestGetExclusiveBuildsForTemplateDeletion_ExclusiveBuild(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	templateID := testutils.CreateTestTemplate(t, db, teamID)
	buildID := testutils.CreateTestBuild(t, ctx, db, templateID, "success")
	testutils.CreateTestBuildAssignment(t, ctx, db, templateID, buildID, "latest")

	rows, err := db.SqlcClient.GetExclusiveBuildsForTemplateDeletion(ctx, templateID)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, buildID, rows[0].BuildID)
	assert.Equal(t, "test-node", *rows[0].ClusterNodeID)
}

// TestGetExclusiveBuildsForTemplateDeletion_SharedBuildExcluded verifies that a
// build assigned to multiple templates is NOT returned (it would break the other template).
func TestGetExclusiveBuildsForTemplateDeletion_SharedBuildExcluded(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	templateA := testutils.CreateTestTemplate(t, db, teamID)
	templateB := testutils.CreateTestTemplate(t, db, teamID)

	// Shared build: assigned to both templates
	sharedBuildID := testutils.CreateTestBuild(t, ctx, db, templateA, "success")
	testutils.CreateTestBuildAssignment(t, ctx, db, templateA, sharedBuildID, "latest")
	testutils.CreateTestBuildAssignment(t, ctx, db, templateB, sharedBuildID, "latest")

	// Exclusive build: only on templateA
	exclusiveBuildID := testutils.CreateTestBuild(t, ctx, db, templateA, "success")
	testutils.CreateTestBuildAssignment(t, ctx, db, templateA, exclusiveBuildID, "v2")

	rows, err := db.SqlcClient.GetExclusiveBuildsForTemplateDeletion(ctx, templateA)
	require.NoError(t, err)
	require.Len(t, rows, 1, "only the exclusive build should be returned")
	assert.Equal(t, exclusiveBuildID, rows[0].BuildID)
}

// TestGetExclusiveBuildsForTemplateDeletion_WorksAfterSoftDelete verifies that
// the query still returns builds after the template has been soft-deleted, because
// it joins the raw envs table rather than the active_envs view.
func TestGetExclusiveBuildsForTemplateDeletion_WorksAfterSoftDelete(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	templateID := testutils.CreateTestTemplate(t, db, teamID)
	buildID := testutils.CreateTestBuild(t, ctx, db, templateID, "success")
	testutils.CreateTestBuildAssignment(t, ctx, db, templateID, buildID, "latest")

	// Soft-delete the template
	err := db.SqlcClient.TestsRawSQL(ctx,
		`UPDATE public.envs SET deleted_at = NOW() WHERE id = $1`,
		templateID,
	)
	require.NoError(t, err, "soft-delete template")

	// The query must still work after soft-delete
	rows, err := db.SqlcClient.GetExclusiveBuildsForTemplateDeletion(ctx, templateID)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, buildID, rows[0].BuildID)
}

// TestGetExclusiveBuildsForTemplateDeletion_ReturnsClusterID verifies that
// cluster_id from the envs table is included in the result.
func TestGetExclusiveBuildsForTemplateDeletion_ReturnsClusterID(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	templateID := testutils.CreateTestTemplate(t, db, teamID)

	// Insert a cluster record first (FK constraint) then link the template to it
	clusterID := uuid.New()
	err := db.SqlcClient.TestsRawSQL(ctx,
		`INSERT INTO public.clusters (id, endpoint, endpoint_tls, token, name) VALUES ($1, 'https://cluster.local', false, 'tok', 'test-cluster')`,
		clusterID,
	)
	require.NoError(t, err, "insert cluster")

	err = db.SqlcClient.TestsRawSQL(ctx,
		`UPDATE public.envs SET cluster_id = $1 WHERE id = $2`,
		clusterID, templateID,
	)
	require.NoError(t, err, "set cluster_id")

	buildID := testutils.CreateTestBuild(t, ctx, db, templateID, "success")
	testutils.CreateTestBuildAssignment(t, ctx, db, templateID, buildID, "latest")

	rows, err := db.SqlcClient.GetExclusiveBuildsForTemplateDeletion(ctx, templateID)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.NotNil(t, rows[0].ClusterID)
	assert.Equal(t, clusterID, *rows[0].ClusterID)
}

// TestGetExclusiveBuildsForTemplateDeletion_NoBuilds verifies that templates
// with no builds return an empty result without error.
func TestGetExclusiveBuildsForTemplateDeletion_NoBuilds(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	templateID := testutils.CreateTestTemplate(t, db, teamID)

	rows, err := db.SqlcClient.GetExclusiveBuildsForTemplateDeletion(ctx, templateID)
	require.NoError(t, err)
	assert.Empty(t, rows)
}

// TestGetExclusiveBuildsForTemplateDeletion_MultipleExclusiveBuilds verifies
// that all exclusive builds for a template are returned, not just the first.
func TestGetExclusiveBuildsForTemplateDeletion_MultipleExclusiveBuilds(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	templateID := testutils.CreateTestTemplate(t, db, teamID)

	buildIDs := make([]uuid.UUID, 3)
	for i := range buildIDs {
		buildID := testutils.CreateTestBuild(t, ctx, db, templateID, "success")
		testutils.CreateTestBuildAssignment(t, ctx, db, templateID, buildID, uuid.New().String())
		buildIDs[i] = buildID
	}

	rows, err := db.SqlcClient.GetExclusiveBuildsForTemplateDeletion(ctx, templateID)
	require.NoError(t, err)
	assert.Len(t, rows, 3)
}
