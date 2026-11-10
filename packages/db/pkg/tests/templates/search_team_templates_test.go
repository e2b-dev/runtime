package templates

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	"github.com/e2b-dev/infra/packages/db/queries"
)

func TestSearchTeamTemplatesByCreatedAtDesc_MergesPublicDefaults(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	platformTeamID := testutils.CreateTestTeam(t, db)

	ownOld := testutils.CreateTestTemplate(t, db, teamID)
	setCreatedHoursAgo(t, db, ownOld, 4)
	ownDefault := testutils.CreateTestTemplate(t, db, teamID)
	setCreatedHoursAgo(t, db, ownDefault, 1)
	markDefault(t, db, ownDefault)

	publicDefault := testutils.CreateTestTemplate(t, db, platformTeamID)
	setCreatedHoursAgo(t, db, publicDefault, 2)
	markDefault(t, db, publicDefault)

	privateDefault := testutils.CreateTestTemplate(t, db, platformTeamID)
	setCreatedHoursAgo(t, db, privateDefault, 3)
	markDefault(t, db, privateDefault)
	require.NoError(t, db.SqlcClient.TestsRawSQL(ctx, "UPDATE public.envs SET public = false WHERE id = $1", privateDefault))

	// Another team's template that is not a default never appears.
	testutils.CreateTestTemplate(t, db, platformTeamID)

	cursorTime, cursorID := firstPageCursor()
	params := queries.SearchTeamTemplatesByCreatedAtDescParams{
		TeamID:          teamID,
		CursorTime:      cursorTime,
		CursorID:        cursorID,
		LimitPlusOne:    10,
		IncludeDefaults: true,
	}

	rows, err := db.SqlcClient.SearchTeamTemplatesByCreatedAtDesc(ctx, params)
	require.NoError(t, err)
	require.Equal(t, []string{ownDefault, publicDefault, ownOld}, rowIDs(rows), "interleaved by created_at, newest first")
	require.Equal(t, []bool{true, true, false}, []bool{rows[0].IsDefault, rows[1].IsDefault, rows[2].IsDefault})
	require.Equal(t, platformTeamID, rows[1].TeamID)

	params.IncludeDefaults = false
	rows, err = db.SqlcClient.SearchTeamTemplatesByCreatedAtDesc(ctx, params)
	require.NoError(t, err)
	require.Equal(t, []string{ownDefault, ownOld}, rowIDs(rows), "only the team's own templates without defaults")
}

func setCreatedHoursAgo(t *testing.T, db *testutils.Database, templateID string, hours int) {
	t.Helper()

	err := db.SqlcClient.TestsRawSQL(t.Context(),
		"UPDATE public.envs SET created_at = NOW() - ($2 || ' hours')::interval WHERE id = $1",
		templateID, hours,
	)
	require.NoError(t, err)
}

func markDefault(t *testing.T, db *testutils.Database, templateID string) {
	t.Helper()

	err := db.SqlcClient.TestsRawSQL(t.Context(), "INSERT INTO public.env_defaults (env_id) VALUES ($1)", templateID)
	require.NoError(t, err)
}

func rowIDs(rows []queries.SearchTeamTemplatesByCreatedAtDescRow) []string {
	ids := make([]string, 0, len(rows))
	for _, row := range rows {
		ids = append(ids, row.TemplateID)
	}

	return ids
}
