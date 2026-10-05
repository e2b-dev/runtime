package templates

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	dashboardqueries "github.com/e2b-dev/infra/packages/db/pkg/dashboard/queries"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
)

// createReadyBuildWithAssignment creates a ready build with the given
// resources and a default-tag assignment created at the given offset from now.
func createReadyBuildWithAssignment(t *testing.T, ctx context.Context, db *testutils.Database, templateID string, vcpu, ramMb int64, assignmentAge time.Duration) uuid.UUID {
	t.Helper()
	buildID := uuid.New()

	err := db.SqlcClient.TestsRawSQL(ctx,
		`INSERT INTO public.env_builds
		(id, env_id, status, vcpu, ram_mb, free_disk_size_mb, kernel_version, firecracker_version, created_at, updated_at)
		VALUES ($1, $2, 'uploaded', $3, $4, 512, '6.1.0', '1.4.0', NOW(), NOW())`,
		buildID, templateID, vcpu, ramMb,
	)
	require.NoError(t, err, "Failed to create ready build")

	err = db.SqlcClient.TestsRawSQL(ctx,
		`INSERT INTO public.env_build_assignments (env_id, build_id, tag, source, created_at)
		VALUES ($1, $2, 'default', 'app', NOW() - $3::interval)`,
		templateID, buildID, assignmentAge.String(),
	)
	require.NoError(t, err, "Failed to create build assignment")

	return buildID
}

// TestListTeamTemplates_AllVariantsExecute smoke-tests every sort variant with
// include_defaults enabled, so SQL errors in any of the four generated queries
// surface here instead of in production.
func TestListTeamTemplates_AllVariantsExecute(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	templateID := testutils.CreateTestTemplate(t, db, teamID)
	testutils.CreateTestTemplateAlias(t, db, templateID)
	createReadyBuildWithAssignment(t, ctx, db, templateID, 2, 2048, 0)

	farFuture := time.Now().Add(100 * 365 * 24 * time.Hour)
	d := db.SqlcClient.Dashboard

	variants := map[string]func() (int, error){
		"CreatedAtAsc": func() (int, error) {
			rows, err := d.ListTeamTemplatesByCreatedAtAsc(ctx, dashboardqueries.ListTeamTemplatesByCreatedAtAscParams{
				TeamID: teamID, IncludeDefaults: true, FilterPublic: -1, LimitPlusOne: 10,
				CursorCreatedAt: time.Time{}, CursorID: "",
			})

			return len(rows), err
		},
		"CreatedAtDesc": func() (int, error) {
			rows, err := d.ListTeamTemplatesByCreatedAtDesc(ctx, dashboardqueries.ListTeamTemplatesByCreatedAtDescParams{
				TeamID: teamID, IncludeDefaults: true, FilterPublic: -1, LimitPlusOne: 10,
				CursorCreatedAt: farFuture, CursorID: "",
			})

			return len(rows), err
		},
		"UpdatedAtAsc": func() (int, error) {
			rows, err := d.ListTeamTemplatesByUpdatedAtAsc(ctx, dashboardqueries.ListTeamTemplatesByUpdatedAtAscParams{
				TeamID: teamID, IncludeDefaults: true, FilterPublic: -1, LimitPlusOne: 10,
				CursorUpdatedAt: time.Time{}, CursorID: "",
			})

			return len(rows), err
		},
		"UpdatedAtDesc": func() (int, error) {
			rows, err := d.ListTeamTemplatesByUpdatedAtDesc(ctx, dashboardqueries.ListTeamTemplatesByUpdatedAtDescParams{
				TeamID: teamID, IncludeDefaults: true, FilterPublic: -1, LimitPlusOne: 10,
				CursorUpdatedAt: farFuture, CursorID: "",
			})

			return len(rows), err
		},
	}

	for name, run := range variants {
		count, err := run()
		require.NoError(t, err, "variant %s failed", name)
		require.Equal(t, 1, count, "variant %s returned unexpected row count", name)
	}
}

func listCreatedAtAscParams(teamID uuid.UUID, limitPlusOne int32) dashboardqueries.ListTeamTemplatesByCreatedAtAscParams {
	return dashboardqueries.ListTeamTemplatesByCreatedAtAscParams{
		TeamID:          teamID,
		IncludeDefaults: false,
		FilterPublic:    -1,
		Search:          "",
		CursorCreatedAt: time.Time{},
		CursorID:        "",
		LimitPlusOne:    limitPlusOne,
	}
}

func TestListTeamTemplatesByCreatedAt_SortsAndPaginates(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)

	templateIDs := make([]string, 3)
	for i := range templateIDs {
		templateIDs[i] = testutils.CreateTestTemplate(t, db, teamID)
		// Distinct created_at values so the sort order is deterministic.
		err := db.SqlcClient.TestsRawSQL(ctx,
			"UPDATE public.envs SET created_at = NOW() - ($2 || ' hours')::interval WHERE id = $1",
			templateIDs[i], 3-i,
		)
		require.NoError(t, err)
	}

	rows, err := db.SqlcClient.Dashboard.ListTeamTemplatesByCreatedAtAsc(ctx, listCreatedAtAscParams(teamID, 10))
	require.NoError(t, err)
	require.Len(t, rows, 3)
	require.Equal(t, []string{rows[0].TemplateID, rows[1].TemplateID, rows[2].TemplateID}, templateIDs)

	descRows, err := db.SqlcClient.Dashboard.ListTeamTemplatesByCreatedAtDesc(ctx, dashboardqueries.ListTeamTemplatesByCreatedAtDescParams{
		TeamID:          teamID,
		IncludeDefaults: false,
		FilterPublic:    -1,
		CursorCreatedAt: time.Now().Add(time.Hour),
		CursorID:        "",
		LimitPlusOne:    10,
	})
	require.NoError(t, err)
	require.Len(t, descRows, 3)
	require.Equal(t, templateIDs[2], descRows[0].TemplateID)

	// Keyset pagination: page of 2, then continue from the last row's cursor.
	firstPage, err := db.SqlcClient.Dashboard.ListTeamTemplatesByCreatedAtAsc(ctx, listCreatedAtAscParams(teamID, 2))
	require.NoError(t, err)
	require.Len(t, firstPage, 2)

	params := listCreatedAtAscParams(teamID, 10)
	params.CursorCreatedAt = firstPage[1].CreatedAt
	params.CursorID = firstPage[1].TemplateID
	secondPage, err := db.SqlcClient.Dashboard.ListTeamTemplatesByCreatedAtAsc(ctx, params)
	require.NoError(t, err)
	require.Len(t, secondPage, 1)
	require.Equal(t, templateIDs[2], secondPage[0].TemplateID)
}

func TestListTeamTemplates_BuildDisplayFields(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)

	buildlessTemplate := testutils.CreateTestTemplate(t, db, teamID)
	builtTemplate := testutils.CreateTestTemplate(t, db, teamID)
	err := db.SqlcClient.TestsRawSQL(ctx,
		"UPDATE public.envs SET created_at = NOW() - interval '1 hour' WHERE id = $1", buildlessTemplate)
	require.NoError(t, err)
	buildID := createReadyBuildWithAssignment(t, ctx, db, builtTemplate, 4, 4096, 0)

	rows, err := db.SqlcClient.Dashboard.ListTeamTemplatesByCreatedAtAsc(ctx, listCreatedAtAscParams(teamID, 10))
	require.NoError(t, err)
	require.Len(t, rows, 2)

	require.Equal(t, buildlessTemplate, rows[0].TemplateID)
	require.Equal(t, uuid.Nil, rows[0].BuildID, "Template without a build must return the zero build id")
	require.Zero(t, rows[0].CpuCount)
	require.Zero(t, rows[0].MemoryMb)

	require.Equal(t, builtTemplate, rows[1].TemplateID)
	require.Equal(t, buildID, rows[1].BuildID)
	require.Equal(t, int64(4), rows[1].CpuCount)
	require.Equal(t, int64(4096), rows[1].MemoryMb)
}
