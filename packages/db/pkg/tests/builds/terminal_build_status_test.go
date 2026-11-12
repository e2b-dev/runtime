package builds

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	"github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/db/queries"
)

// Several API instances poll the same build, so two of them can record a
// terminal outcome for it. The first one recorded stands: a build failed for
// running out of time has its artifacts deleted on the node, so a later write
// flipping it back to ready would publish a template whose layers are gone.

func buildReason(t *testing.T, ctx context.Context, db *testutils.Database, buildID uuid.UUID) string {
	t.Helper()

	var message string

	err := db.SqlcClient.TestsRawSQLQuery(ctx,
		"SELECT reason->>'message' FROM public.env_builds WHERE id = $1",
		func(rows pgx.Rows) error {
			if !rows.Next() {
				return nil
			}

			return rows.Scan(&message)
		},
		buildID,
	)
	require.NoError(t, err, "Failed to query build reason")

	return message
}

func failBuild(t *testing.T, ctx context.Context, db *testutils.Database, buildID uuid.UUID, message string) bool {
	t.Helper()

	now := time.Now()

	recorded, err := db.SqlcClient.FailTemplateBuildAndDeactivate(ctx, queries.FailTemplateBuildAndDeactivateParams{
		StatusGroup: types.BuildStatusGroupFailed,
		FinishedAt:  &now,
		Reason:      types.BuildReason{Message: message},
		BuildID:     buildID,
	})
	require.NoError(t, err)

	return recorded
}

func finishBuild(t *testing.T, ctx context.Context, db *testutils.Database, buildID uuid.UUID) {
	t.Helper()

	totalDisk := int64(1024)
	envdVersion := "v1.0.0"

	err := db.SqlcClient.FinishTemplateBuild(ctx, queries.FinishTemplateBuildParams{
		BuildID:            buildID,
		StatusGroup:        types.BuildStatusGroupReady,
		TotalDiskSizeMb:    &totalDisk,
		EnvdVersion:        &envdVersion,
		KernelVersion:      seededKernel,
		FirecrackerVersion: seededFC,
	})
	require.NoError(t, err)
}

func TestFailTemplateBuildAndDeactivate_RecordsTheFirstTerminalOutcome(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	templateID := testutils.CreateTestTemplate(t, db, teamID)
	buildID := testutils.CreateTestBuild(t, ctx, db, templateID, "building")

	assert.True(t, failBuild(t, ctx, db, buildID, "build status polling timed out"),
		"the write that ends an in-progress build reports that it did")
	assert.Equal(t, types.BuildStatusGroupFailed, testutils.GetBuildStatusGroup(t, ctx, db, buildID))

	assert.False(t, failBuild(t, ctx, db, buildID, "cancelled by admin"),
		"a build already over must report the loss so the caller does not act on it")
	assert.Equal(t, "build status polling timed out", buildReason(t, ctx, db, buildID),
		"the reason of the write that ended the build stands")
}

func TestFailTemplateBuildAndDeactivate_LeavesAFinishedBuildReady(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	templateID := testutils.CreateTestTemplate(t, db, teamID)
	buildID := testutils.CreateTestBuild(t, ctx, db, templateID, "building")

	finishBuild(t, ctx, db, buildID)

	assert.False(t, failBuild(t, ctx, db, buildID, "build status polling timed out"),
		"a poller whose deadline fires after a peer recorded success must not overwrite it")
	assert.Equal(t, types.BuildStatusGroupReady, testutils.GetBuildStatusGroup(t, ctx, db, buildID))
}

func TestFinishTemplateBuild_LeavesAFailedBuildFailed(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	templateID := testutils.CreateTestTemplate(t, db, teamID)
	buildID := testutils.CreateTestBuild(t, ctx, db, templateID, "building")

	failBuild(t, ctx, db, buildID, "build status polling timed out")

	finishBuild(t, ctx, db, buildID)

	assert.Equal(t, types.BuildStatusGroupFailed, testutils.GetBuildStatusGroup(t, ctx, db, buildID),
		"a failed build has had its artifacts deleted, so it must not come back as ready")
}

func TestFailTemplateBuildAndDeactivate_ReleasesTheConcurrencySlotWhenItWins(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	templateID := testutils.CreateTestTemplate(t, db, teamID)
	buildID := testutils.CreateTestBuild(t, ctx, db, templateID, "building")

	err := db.SqlcClient.CreateActiveTemplateBuild(ctx, queries.CreateActiveTemplateBuildParams{
		BuildID:    buildID,
		TeamID:     teamID,
		TemplateID: templateID,
		Tags:       []string{"latest"},
	})
	require.NoError(t, err)

	require.True(t, failBuild(t, ctx, db, buildID, "build status polling timed out"))

	count, err := db.SqlcClient.GetInProgressTemplateBuildsByTeam(ctx, queries.GetInProgressTemplateBuildsByTeamParams{
		TeamID:            teamID,
		ExcludeTemplateID: uuid.NewString(),
		ExcludeTags:       []string{"none"},
	})
	require.NoError(t, err)
	assert.Equal(t, int64(0), count, "ending the build releases the team's concurrency slot")
}
