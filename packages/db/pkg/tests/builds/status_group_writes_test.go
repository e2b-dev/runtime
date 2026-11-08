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

// trg_compute_status_group overwrites status_group from status, so only with
// it disabled does a test show that every writer sets the group itself.
func TestBuildWritersSetStatusGroupThemselves(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	db := testutils.SetupDatabase(t)
	require.NoError(t, db.SqlcClient.TestsRawSQL(ctx,
		`ALTER TABLE public.env_builds DISABLE TRIGGER trg_compute_status_group`))

	teamID := testutils.CreateTestTeam(t, db)
	templateID := testutils.CreateTestTemplate(t, db, teamID)
	createBuild := func(t *testing.T) uuid.UUID {
		t.Helper()

		buildID := uuid.New()
		require.NoError(t, db.SqlcClient.CreateTemplateBuild(ctx, queries.CreateTemplateBuildParams{
			BuildID:            buildID,
			TemplateID:         templateID,
			StatusGroup:        types.BuildStatusGroupPending,
			RamMb:              512,
			Vcpu:               1,
			KernelVersion:      seededKernel,
			FirecrackerVersion: seededFC,
			FreeDiskSizeMb:     512,
		}))

		return buildID
	}

	type write struct {
		writer  string
		buildID uuid.UUID
		want    types.BuildStatusGroup
	}
	var writes []write

	writes = append(writes, write{"CreateTemplateBuild", createBuild(t), types.BuildStatusGroupPending})

	for _, group := range []types.BuildStatusGroup{
		types.BuildStatusGroupPending,
		types.BuildStatusGroupInProgress,
		types.BuildStatusGroupReady,
		types.BuildStatusGroupFailed,
	} {
		buildID := createBuild(t)
		require.NoError(t, db.SqlcClient.UpdateEnvBuildStatus(ctx, queries.UpdateEnvBuildStatusParams{
			StatusGroup: group,
			FinishedAt:  new(time.Now()),
			BuildID:     buildID,
		}))
		writes = append(writes, write{"UpdateEnvBuildStatus " + string(group), buildID, group})
	}

	failed := createBuild(t)
	require.True(t, failBuild(t, ctx, db, failed, "failed"))
	writes = append(writes, write{"FailTemplateBuildAndDeactivate", failed, types.BuildStatusGroupFailed})

	finished := createBuild(t)
	finishBuild(t, ctx, db, finished)
	writes = append(writes, write{"FinishTemplateBuild", finished, types.BuildStatusGroupReady})

	invalidated := createBuild(t)
	testutils.CreateTestBuildAssignment(t, ctx, db, templateID, invalidated, "unstarted")
	require.NoError(t, db.SqlcClient.InvalidateUnstartedTemplateBuilds(ctx, queries.InvalidateUnstartedTemplateBuildsParams{
		TemplateID: templateID,
		Tags:       []string{"unstarted"},
	}))
	writes = append(writes, write{"InvalidateUnstartedTemplateBuilds", invalidated, types.BuildStatusGroupFailed})

	snapshot := testutils.UpsertTestSnapshotWithStatus(t, ctx, db, "snap-"+uuid.NewString(), "sbx-"+uuid.NewString(),
		teamID, templateID, types.BuildStatusGroupInProgress)
	writes = append(writes, write{"UpsertSnapshot", snapshot.BuildID, types.BuildStatusGroupInProgress})

	for _, w := range writes {
		assert.Equal(t, w.want, readStatusGroup(t, ctx, db, w.buildID), w.writer)
	}

	// The status each writer stores alongside must give back the same group
	// through the trigger, or the trigger would rewrite it on the next update.
	require.NoError(t, db.SqlcClient.TestsRawSQL(ctx,
		`ALTER TABLE public.env_builds ENABLE TRIGGER trg_compute_status_group`))
	for _, w := range writes {
		require.NoError(t, db.SqlcClient.TestsRawSQL(ctx,
			`UPDATE public.env_builds SET status = status WHERE id = $1`, w.buildID))
		assert.Equal(t, w.want, readStatusGroup(t, ctx, db, w.buildID), w.writer+" through the trigger")
	}
}

func readStatusGroup(t *testing.T, ctx context.Context, db *testutils.Database, buildID uuid.UUID) types.BuildStatusGroup {
	t.Helper()

	var group types.BuildStatusGroup
	require.NoError(t, db.SqlcClient.TestsRawSQLQuery(ctx,
		"SELECT status_group FROM public.env_builds WHERE id = $1",
		func(rows pgx.Rows) error {
			require.True(t, rows.Next())

			return rows.Scan(&group)
		},
		buildID,
	))

	return group
}
