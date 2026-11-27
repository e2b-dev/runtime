package templates

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/db/pkg/testutils"
	"github.com/e2b-dev/infra/packages/api/internal/db/queries"
)

// firstPageCursor returns a cursor that selects the first page of a descending
// listing (newer than any real row).
func firstPageCursor() (time.Time, string) {
	return time.Now().Add(100 * 365 * 24 * time.Hour), ""
}

func TestGetTeamTemplatesWithCursor_OrdersDescAndPaginates(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)

	// Three templates with distinct, increasing created_at (index 2 is newest).
	templateIDs := make([]string, 3)
	for i := range templateIDs {
		templateIDs[i] = testutils.CreateTestTemplate(t, db, teamID)
		err := db.SqlcClient.TestsRawSQL(ctx,
			"UPDATE public.envs SET created_at = NOW() - ($2 || ' hours')::interval WHERE id = $1",
			templateIDs[i], 3-i,
		)
		require.NoError(t, err)
	}

	cursorTime, cursorID := firstPageCursor()
	rows, err := db.SqlcClient.GetTeamTemplatesWithCursor(ctx, queries.GetTeamTemplatesWithCursorParams{
		TeamID:          teamID,
		CursorCreatedAt: cursorTime,
		CursorID:        cursorID,
		LimitPlusOne:    10,
	})
	require.NoError(t, err)
	require.Len(t, rows, 3)
	// Newest first.
	require.Equal(t, []string{templateIDs[2], templateIDs[1], templateIDs[0]},
		[]string{rows[0].TemplateID, rows[1].TemplateID, rows[2].TemplateID})

	// Keyset pagination: page of 2 (request 3 = limit+1 to detect more), then
	// continue from the last returned row's cursor.
	firstPage, err := db.SqlcClient.GetTeamTemplatesWithCursor(ctx, queries.GetTeamTemplatesWithCursorParams{
		TeamID:          teamID,
		CursorCreatedAt: cursorTime,
		CursorID:        cursorID,
		LimitPlusOne:    3,
	})
	require.NoError(t, err)
	require.Len(t, firstPage, 3) // 2 + 1 sentinel
	last := firstPage[1]         // the 2nd item is the page boundary

	secondPage, err := db.SqlcClient.GetTeamTemplatesWithCursor(ctx, queries.GetTeamTemplatesWithCursorParams{
		TeamID:          teamID,
		CursorCreatedAt: last.CreatedAt,
		CursorID:        last.TemplateID,
		LimitPlusOne:    10,
	})
	require.NoError(t, err)
	require.Len(t, secondPage, 1)
	require.Equal(t, templateIDs[0], secondPage[0].TemplateID)
}
