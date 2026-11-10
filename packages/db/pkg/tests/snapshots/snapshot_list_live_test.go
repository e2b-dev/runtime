package snapshots

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
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

type planNode struct {
	IndexName string     `json:"Index Name"`
	Plans     []planNode `json:"Plans"`
}

func (n planNode) indexNames() []string {
	var names []string
	if n.IndexName != "" {
		names = append(names, n.IndexName)
	}
	for _, child := range n.Plans {
		names = append(names, child.indexNames()...)
	}

	return names
}

// planRecorder explains each query on the same connection before running it
// and keeps the names of the indexes the plan scans.
type planRecorder struct {
	queries.DBTX

	t       *testing.T
	plan    string
	indexes []string
}

func (r *planRecorder) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	r.t.Helper()
	require.NoError(r.t, r.DBTX.QueryRow(ctx, "EXPLAIN (FORMAT JSON) "+sql, args...).Scan(&r.plan))
	var plans []struct {
		Plan planNode `json:"Plan"`
	}
	require.NoError(r.t, json.Unmarshal([]byte(r.plan), &plans))
	for _, plan := range plans {
		r.indexes = append(r.indexes, plan.Plan.indexNames()...)
	}

	return r.DBTX.Query(ctx, sql, args...)
}

// Which index the planner prefers on a table this small is not stable, so each
// case drops the other indexes that could serve the query and disables the
// plan shapes that need none, leaving a scan of the live index as the only
// plan that is not disabled. A query that no longer implies the index
// predicate cannot use it at all and falls back to a disabled plan.
func TestSnapshotCursorQueries_CanWalkLiveIndexes(t *testing.T) {
	t.Parallel()

	const (
		team         = "idx_snapshots_team_time_id"
		teamLive     = "idx_snapshots_team_time_id_live"
		template     = "idx_snapshots_team_base_env_time_id"
		templateLive = "idx_snapshots_team_base_env_time_id_live"
		metadata     = "idx_snapshots_team_metadata_gin"
		metadataLive = "idx_snapshots_team_metadata_gin_live"
	)
	ordered := []string{"SET enable_seqscan = off", "SET enable_sort = off"}

	cases := []struct {
		query    string
		metadata types.JSONBStringMap
		drop     []string
		settings []string
		want     string
	}{
		{query: "GetSnapshotsWithCursor", drop: []string{team}, settings: ordered, want: teamLive},
		{query: "GetSnapshotsWithCursorAsc", drop: []string{team}, settings: ordered, want: teamLive},
		{query: "GetSnapshotsByTemplateWithCursor", drop: []string{template, team, teamLive}, settings: ordered, want: templateLive},
		{query: "GetSnapshotsByTemplateWithCursorAsc", drop: []string{template, team, teamLive}, settings: ordered, want: templateLive},
		{
			query:    "GetSnapshotsWithCursor",
			metadata: types.JSONBStringMap{"owner": "alice"},
			drop:     []string{metadata, team, teamLive, template, templateLive},
			settings: []string{"SET enable_seqscan = off"},
			want:     metadataLive,
		},
	}
	for _, tc := range cases {
		t.Run(tc.query+"_"+tc.want, func(t *testing.T) {
			t.Parallel()
			db := testutils.SetupDatabase(t)
			ctx := t.Context()
			teamID, templateID, live := listFixture(t, db)

			config, err := pgx.ParseConfig(db.ConnStr())
			require.NoError(t, err)
			config.DefaultQueryExecMode = pgx.QueryExecModeExec
			conn, err := pgx.ConnectConfig(ctx, config)
			require.NoError(t, err)
			defer func() { require.NoError(t, conn.Close(ctx)) }()

			for _, index := range tc.drop {
				_, err := conn.Exec(ctx, "DROP INDEX public."+index)
				require.NoError(t, err)
			}
			for _, setting := range tc.settings {
				_, err := conn.Exec(ctx, setting)
				require.NoError(t, err)
			}

			recorder := &planRecorder{DBTX: conn, t: t}
			listed, err := cursorQueries[tc.query](ctx, queries.New(recorder), teamID, templateID, tc.metadata)
			require.NoError(t, err)
			assert.Equal(t, []string{live}, listed)
			assert.Contains(t, recorder.indexes, tc.want, "plan: %s", recorder.plan)
		})
	}
}
