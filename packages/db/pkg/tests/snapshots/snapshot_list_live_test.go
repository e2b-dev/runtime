package snapshots

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	"github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/db/queries"
)

type cursorRow interface {
	queries.GetSnapshotsWithCursorRow |
		queries.GetSnapshotsWithCursorAscRow |
		queries.GetSnapshotsByTemplateWithCursorRow |
		queries.GetSnapshotsByTemplateWithCursorAscRow
}

func sandboxIDs[Row cursorRow](rows []Row) []string {
	ids := make([]string, 0, len(rows))
	for _, row := range rows {
		ids = append(ids, queries.GetSnapshotsWithCursorRow(row).Snapshot.SandboxID)
	}

	return ids
}

// cursorQuery lists the first page of a team's paused sandboxes (of one
// template, for the template variants) through one of the four cursor queries.
type cursorQuery func(ctx context.Context, q *queries.Queries, teamID uuid.UUID, templateID string, metadata types.JSONBStringMap) ([]string, error)

var cursorQueries = map[string]cursorQuery{
	"GetSnapshotsWithCursor": func(ctx context.Context, q *queries.Queries, teamID uuid.UUID, _ string, metadata types.JSONBStringMap) ([]string, error) {
		rows, err := q.GetSnapshotsWithCursor(ctx, queries.GetSnapshotsWithCursorParams{
			Limit: 10, TeamID: teamID, Metadata: metadata,
			CursorTime: pgtype.Timestamptz{Time: time.Now().Add(time.Hour), Valid: true},
		})

		return sandboxIDs(rows), err
	},
	"GetSnapshotsWithCursorAsc": func(ctx context.Context, q *queries.Queries, teamID uuid.UUID, _ string, metadata types.JSONBStringMap) ([]string, error) {
		rows, err := q.GetSnapshotsWithCursorAsc(ctx, queries.GetSnapshotsWithCursorAscParams{
			Limit: 10, TeamID: teamID, Metadata: metadata,
			CursorTime: pgtype.Timestamptz{Time: time.Time{}, Valid: true},
		})

		return sandboxIDs(rows), err
	},
	"GetSnapshotsByTemplateWithCursor": func(ctx context.Context, q *queries.Queries, teamID uuid.UUID, templateID string, metadata types.JSONBStringMap) ([]string, error) {
		rows, err := q.GetSnapshotsByTemplateWithCursor(ctx, queries.GetSnapshotsByTemplateWithCursorParams{
			Limit: 10, TeamID: teamID, TemplateID: templateID, Metadata: metadata,
			CursorTime: pgtype.Timestamptz{Time: time.Now().Add(time.Hour), Valid: true},
		})

		return sandboxIDs(rows), err
	},
	"GetSnapshotsByTemplateWithCursorAsc": func(ctx context.Context, q *queries.Queries, teamID uuid.UUID, templateID string, metadata types.JSONBStringMap) ([]string, error) {
		rows, err := q.GetSnapshotsByTemplateWithCursorAsc(ctx, queries.GetSnapshotsByTemplateWithCursorAscParams{
			Limit: 10, TeamID: teamID, TemplateID: templateID, Metadata: metadata,
			CursorTime: pgtype.Timestamptz{Time: time.Time{}, Valid: true},
		})

		return sandboxIDs(rows), err
	},
}

// listFixture pauses three sandboxes of one team and template, all carrying
// the same metadata: one live, one whose env a soft delete stamped, and one
// whose env was deleted before stamping existed and has not been backfilled.
func listFixture(t *testing.T, db *testutils.Database) (uuid.UUID, string, string) {
	t.Helper()
	ctx := t.Context()

	teamID := testutils.CreateTestTeam(t, db)
	templateID := testutils.CreateTestTemplate(t, db, teamID)
	live, _ := pausedSandbox(t, db, teamID, templateID)
	stamped, stampedEnv := pausedSandbox(t, db, teamID, templateID)
	unstamped, unstampedEnv := pausedSandbox(t, db, teamID, templateID)

	for _, sandboxID := range []string{live, stamped, unstamped} {
		require.NoError(t, db.SqlcClient.TestsRawSQL(ctx,
			"UPDATE public.snapshots SET metadata = $2::jsonb WHERE sandbox_id = $1",
			sandboxID, types.JSONBStringMap{"owner": "alice"}))
	}

	_, err := db.SqlcClient.SoftDeleteTemplate(ctx, queries.SoftDeleteTemplateParams{TemplateID: stampedEnv, TeamID: teamID})
	require.NoError(t, err)
	testutils.SoftDeleteEnvUnstamped(t, ctx, db, unstampedEnv)
	require.Equal(t, []string{"live", "stamped", "unstamped"}, stamps(t, db, []string{live, stamped, unstamped}))

	return teamID, templateID, live
}

func TestSnapshotCursorQueries_NeverListDeletedEnvs(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	teamID, templateID, live := listFixture(t, db)

	for name, list := range cursorQueries {
		for _, metadata := range []types.JSONBStringMap{nil, {"owner": "alice"}} {
			listed, err := list(t.Context(), db.SqlcClient.Queries, teamID, templateID, metadata)
			require.NoError(t, err)
			assert.Equal(t, []string{live}, listed, "%s with metadata %v", name, metadata)
		}
	}
}
