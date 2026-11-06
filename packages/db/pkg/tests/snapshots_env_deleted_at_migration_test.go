package tests

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/database"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
)

const (
	snapshotsEnvDeletedAtVersion      int64 = 20261028205101
	stampSnapshotsEnvDeletedAtVersion int64 = 20261028205102

	// stampMaxHeapBytes is the heap size from which the stamp migration
	// leaves the table to the backfill script.
	stampMaxHeapBytes = 256 << 20
)

// The column is added by one migration and the snapshots of already-deleted
// envs are stamped by the next, only while the table's heap is small; a large
// table is left to the backfill script. The planner's row estimate is unknown
// in both cases, as on a table not yet analyzed.
func TestSnapshotsEnvDeletedAtMigration(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		large     bool
		wantStamp string
	}{
		{name: "small table is stamped", wantStamp: "stamped"},
		{name: "large table is left to the backfill", large: true, wantStamp: "unstamped"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			db := testutils.SetupDatabase(t)
			ctx := t.Context()
			sqlDB, err := sql.Open("pgx", db.ConnStr())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })

			store, err := database.NewStore(goose.DialectPostgres, testutils.TrackingTable)
			require.NoError(t, err)
			migrations, err := goose.NewProvider("", sqlDB, os.DirFS(filepath.Join("..", "..", "migrations")), goose.WithStore(store))
			require.NoError(t, err)
			_, err = migrations.DownTo(ctx, snapshotsEnvDeletedAtVersion-1)
			require.NoError(t, err)

			teamID := testutils.CreateTestTeam(t, db)
			baseTemplateID := testutils.CreateTestTemplate(t, db, teamID)
			snapshotOf := func(deleted bool) string {
				t.Helper()
				envID := testutils.CreateTestTemplate(t, db, teamID)
				sandboxID := "sandbox-" + uuid.NewString()
				testutils.CreateSnapshotRecord(t, ctx, db, envID, sandboxID, teamID, baseTemplateID)
				if deleted {
					testutils.SoftDeleteEnvUnstamped(t, ctx, db, envID)
				}

				return sandboxID
			}
			deleted := snapshotOf(true)
			live := snapshotOf(false)

			if tc.large {
				// Live snapshots fill the heap to the threshold one page each, the
				// fewest rows it takes: at fillfactor 10 a 400-byte row leaves no
				// room for a second. Autovacuum is off so nothing analyzes them.
				_, err = sqlDB.ExecContext(ctx,
					"ALTER TABLE public.snapshots SET (fillfactor = 10, autovacuum_enabled = off)")
				require.NoError(t, err)
				_, err = sqlDB.ExecContext(ctx, `
					WITH padding AS (
						INSERT INTO public.envs (id, team_id, updated_at, source)
						SELECT 'padding-' || g, $1, now(), 'snapshot'
						FROM generate_series(1, $2::int) g
						RETURNING id
					)
					INSERT INTO public.snapshots (sandbox_id, env_id, team_id, base_env_id, sandbox_started_at, origin_node_id)
					SELECT id, id, $1, $3, now(), repeat('n', 400)
					FROM padding`,
					teamID, stampMaxHeapBytes/8192, baseTemplateID)
				require.NoError(t, err)
			}
			var heapBytes int64
			require.NoError(t, sqlDB.QueryRowContext(ctx,
				"SELECT pg_relation_size('public.snapshots'::regclass)").Scan(&heapBytes))
			require.Equal(t, tc.large, heapBytes >= stampMaxHeapBytes, "heap of %d bytes", heapBytes)
			_, err = sqlDB.ExecContext(ctx,
				"UPDATE pg_catalog.pg_class SET reltuples = -1 WHERE oid = 'public.snapshots'::regclass")
			require.NoError(t, err)

			results, err := migrations.UpTo(ctx, stampSnapshotsEnvDeletedAtVersion)
			require.NoError(t, err)
			require.Len(t, results, 2)

			assert.Equal(t, tc.wantStamp, testutils.SnapshotEnvStamp(t, ctx, db, deleted))
			assert.Equal(t, "live", testutils.SnapshotEnvStamp(t, ctx, db, live))
		})
	}
}
