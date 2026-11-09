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

// Pods that predate status_group write only status, newer ones only
// status_group; while both run, the trigger keeps each derived from the other.
func TestStatusGroupTriggerKeepsBothColumnsInStep(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	db := testutils.SetupDatabase(t)
	teamID := testutils.CreateTestTeam(t, db)
	templateID := testutils.CreateTestTemplate(t, db, teamID)

	filled := map[types.BuildStatusGroup]string{}
	for _, group := range []types.BuildStatusGroup{
		types.BuildStatusGroupPending,
		types.BuildStatusGroupInProgress,
		types.BuildStatusGroupReady,
		types.BuildStatusGroupFailed,
	} {
		grouped := uuid.New()
		require.NoError(t, db.SqlcClient.CreateTemplateBuild(ctx, queries.CreateTemplateBuildParams{
			BuildID:            grouped,
			TemplateID:         templateID,
			StatusGroup:        group,
			RamMb:              512,
			Vcpu:               1,
			KernelVersion:      seededKernel,
			FirecrackerVersion: seededFC,
			FreeDiskSizeMb:     512,
		}))
		status := readStatus(t, ctx, db, grouped)
		require.NotEmpty(t, status, "status filled for %s", group)
		filled[group] = status

		legacy := testutils.CreateTestBuild(t, ctx, db, templateID, status)
		assert.Equal(t, group, readStatusGroup(t, ctx, db, legacy), "an older pod writing %q", status)
	}

	buildID := testutils.CreateTestBuild(t, ctx, db, templateID, string(types.BuildStatusBuilding))
	require.Equal(t, types.BuildStatusGroupInProgress, readStatusGroup(t, ctx, db, buildID))

	require.NoError(t, db.SqlcClient.UpdateEnvBuildStatus(ctx, queries.UpdateEnvBuildStatusParams{
		StatusGroup: types.BuildStatusGroupReady,
		FinishedAt:  new(time.Now()),
		BuildID:     buildID,
	}))
	assert.Equal(t, filled[types.BuildStatusGroupReady], readStatus(t, ctx, db, buildID), "a group-only update fills status")

	require.NoError(t, db.SqlcClient.TestsRawSQL(ctx,
		`UPDATE public.env_builds SET status = $2 WHERE id = $1`, buildID, string(types.BuildStatusFailed)))
	assert.Equal(t, types.BuildStatusGroupFailed, readStatusGroup(t, ctx, db, buildID), "a status update derives the group")
}

func readStatus(t *testing.T, ctx context.Context, db *testutils.Database, buildID uuid.UUID) string {
	t.Helper()

	var status string
	require.NoError(t, db.SqlcClient.TestsRawSQLQuery(ctx,
		"SELECT status FROM public.env_builds WHERE id = $1",
		func(rows pgx.Rows) error {
			require.True(t, rows.Next())

			return rows.Scan(&status)
		},
		buildID,
	))

	return status
}
