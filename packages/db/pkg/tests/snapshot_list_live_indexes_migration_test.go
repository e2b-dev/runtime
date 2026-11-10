package tests

import (
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/database"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dbmodule "github.com/e2b-dev/infra/packages/db"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
)

const snapshotListLiveIndexesVersion int64 = 20261103090000

// Each live index keeps the keys of the full index it stands in for -- columns,
// order, direction and access method -- so the list queries keep their plan
// shapes when the full ones are dropped. A rerun after an interrupted build
// replaces the INVALID leftover rather than skipping it, and keeps an index
// that is already valid, such as one built by hand ahead of the deploy.
func TestSnapshotListLiveIndexesMigration(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	ctx := t.Context()
	sqlDB, err := sql.Open("pgx", db.ConnStr())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })

	liveOf := map[string]string{
		"idx_snapshots_team_time_id":          "idx_snapshots_team_time_id_live",
		"idx_snapshots_team_base_env_time_id": "idx_snapshots_team_base_env_time_id_live",
		"idx_snapshots_team_metadata_gin":     "idx_snapshots_team_metadata_gin_live",
	}
	definition := func(name string) (string, bool) {
		t.Helper()
		var def string
		var valid bool
		require.NoError(t, sqlDB.QueryRowContext(ctx, `
			SELECT pg_get_indexdef(i.indexrelid), i.indisvalid
			FROM pg_catalog.pg_index i
			WHERE i.indexrelid = to_regclass('public.' || $1)`, name).Scan(&def, &valid))

		return def, valid
	}
	assertMirrored := func() {
		t.Helper()
		for full, live := range liveOf {
			fullDef, _ := definition(full)
			liveDef, valid := definition(live)
			assert.Equal(t, strings.Replace(fullDef, full, live, 1)+" WHERE (env_deleted_at IS NULL)", liveDef)
			assert.True(t, valid, "%s is valid", live)
		}
	}
	assertMirrored()

	store, err := database.NewStore(goose.DialectPostgres, testutils.TrackingTable)
	require.NoError(t, err)
	migrations, err := goose.NewProvider("", sqlDB, dbmodule.Migrations(), goose.WithStore(store))
	require.NoError(t, err)
	_, err = migrations.DownTo(ctx, snapshotListLiveIndexesVersion-1)
	require.NoError(t, err)
	for _, live := range liveOf {
		var exists bool
		require.NoError(t, sqlDB.QueryRowContext(ctx, "SELECT to_regclass('public.' || $1) IS NOT NULL", live).Scan(&exists))
		assert.False(t, exists, "%s is dropped", live)
	}

	// Built by hand ahead of the deploy: kept as it is, not rebuilt.
	_, err = sqlDB.ExecContext(ctx, `CREATE INDEX idx_snapshots_team_time_id_live
		ON public.snapshots (team_id, sandbox_started_at DESC, sandbox_id)
		WHERE env_deleted_at IS NULL`)
	require.NoError(t, err)
	handBuilt := indexOID(t, sqlDB, "idx_snapshots_team_time_id_live")

	// What an interrupted concurrent build leaves: an INVALID index under the
	// name, which IF NOT EXISTS alone would keep.
	_, err = sqlDB.ExecContext(ctx, `CREATE INDEX idx_snapshots_team_metadata_gin_live ON public.snapshots (team_id)`)
	require.NoError(t, err)
	invalidate(t, sqlDB, "idx_snapshots_team_metadata_gin_live")

	// What an interrupted drop of a leftover the migration set aside leaves, next to
	// a later interrupted build under the live name: the set-aside name is taken.
	_, err = sqlDB.ExecContext(ctx, `CREATE INDEX idx_snapshots_team_base_env_time_id_live_invalid ON public.snapshots (team_id)`)
	require.NoError(t, err)
	invalidate(t, sqlDB, "idx_snapshots_team_base_env_time_id_live_invalid")
	_, err = sqlDB.ExecContext(ctx, `CREATE INDEX idx_snapshots_team_base_env_time_id_live ON public.snapshots (team_id)`)
	require.NoError(t, err)
	invalidate(t, sqlDB, "idx_snapshots_team_base_env_time_id_live")

	results, err := migrations.UpTo(ctx, snapshotListLiveIndexesVersion)
	require.NoError(t, err)
	require.Len(t, results, 1)
	assertMirrored()
	assert.Equal(t, handBuilt, indexOID(t, sqlDB, "idx_snapshots_team_time_id_live"), "the valid index is kept")

	var leftovers string
	require.NoError(t, sqlDB.QueryRowContext(ctx, `
		SELECT COALESCE(string_agg(indexrelid::regclass::text, ', '), '') FROM pg_catalog.pg_index
		WHERE indrelid = 'public.snapshots'::regclass AND indexrelid::regclass::text LIKE '%\_invalid'`).Scan(&leftovers))
	assert.Empty(t, leftovers, "set-aside leftovers are dropped")
}

// A concurrent build still running, such as one started by hand ahead of the
// deploy, is INVALID like an interrupted one. The migration stops rather than set
// it aside and drop it, and keeps it once the build has finished. With a leftover
// an earlier run set aside, it stops before dropping that too, since the drop
// would wait out the build.
func TestSnapshotListLiveIndexesMigrationStopsWhileABuildRuns(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		leftover bool
		wantErr  string
	}{
		"no leftover":   {wantErr: "public.idx_snapshots_team_base_env_time_id_live is not valid yet"},
		"with leftover": {leftover: true, wantErr: "is building an index on public.snapshots"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			db := testutils.SetupDatabase(t)
			ctx := t.Context()
			sqlDB, err := sql.Open("pgx", db.ConnStr())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })

			store, err := database.NewStore(goose.DialectPostgres, testutils.TrackingTable)
			require.NoError(t, err)
			migrations, err := goose.NewProvider("", sqlDB, dbmodule.Migrations(), goose.WithStore(store))
			require.NoError(t, err)
			_, err = migrations.DownTo(ctx, snapshotListLiveIndexesVersion-1)
			require.NoError(t, err)

			if tc.leftover {
				// What a run interrupted between setting an index aside and dropping it leaves.
				_, err = sqlDB.ExecContext(ctx, `CREATE INDEX idx_snapshots_team_time_id_live_invalid ON public.snapshots (team_id)`)
				require.NoError(t, err)
				invalidate(t, sqlDB, "idx_snapshots_team_time_id_live_invalid")
			}

			// An open writer holds the build in its first wait, after the INVALID index exists.
			writer, err := sqlDB.BeginTx(ctx, nil)
			require.NoError(t, err)
			t.Cleanup(func() { _ = writer.Rollback() })
			_, err = writer.ExecContext(ctx, "LOCK TABLE public.snapshots IN ROW EXCLUSIVE MODE")
			require.NoError(t, err)

			built := make(chan error, 1)
			go func() {
				_, err := sqlDB.ExecContext(ctx, `CREATE INDEX CONCURRENTLY idx_snapshots_team_base_env_time_id_live
					ON public.snapshots (team_id, base_env_id, sandbox_started_at DESC, sandbox_id ASC)
					WHERE env_deleted_at IS NULL`)
				built <- err
			}()
			require.Eventually(t, func() bool {
				var building bool
				err := sqlDB.QueryRowContext(ctx, `SELECT EXISTS (
					SELECT FROM pg_catalog.pg_stat_progress_create_index
					WHERE index_relid = to_regclass('public.idx_snapshots_team_base_env_time_id_live'))`).Scan(&building)

				return err == nil && building
			}, 30*time.Second, 50*time.Millisecond)
			building := indexOID(t, sqlDB, "idx_snapshots_team_base_env_time_id_live")

			stopped := make(chan error, 1)
			go func() {
				_, err := migrations.UpTo(ctx, snapshotListLiveIndexesVersion)
				stopped <- err
			}()
			select {
			case err = <-stopped:
			case <-time.After(30 * time.Second):
				require.FailNow(t, "the migration waits on the running build instead of stopping")
			}
			require.ErrorContains(t, err, tc.wantErr)
			assert.Equal(t, building, indexOID(t, sqlDB, "idx_snapshots_team_base_env_time_id_live"), "the running build keeps its index")

			require.NoError(t, writer.Rollback())
			require.NoError(t, <-built)

			results, err := migrations.UpTo(ctx, snapshotListLiveIndexesVersion)
			require.NoError(t, err)
			require.Len(t, results, 1)
			assert.Equal(t, building, indexOID(t, sqlDB, "idx_snapshots_team_base_env_time_id_live"), "the finished build is kept")
			if tc.leftover {
				var exists bool
				require.NoError(t, sqlDB.QueryRowContext(ctx, "SELECT to_regclass('public.idx_snapshots_team_time_id_live_invalid') IS NOT NULL").Scan(&exists))
				assert.False(t, exists, "the set-aside leftover is dropped")
			}
		})
	}
}

func indexOID(t *testing.T, sqlDB *sql.DB, name string) uint32 {
	t.Helper()
	var oid uint32
	require.NoError(t, sqlDB.QueryRowContext(t.Context(), "SELECT to_regclass('public.' || $1)::oid", name).Scan(&oid))

	return oid
}

func invalidate(t *testing.T, sqlDB *sql.DB, name string) {
	t.Helper()
	_, err := sqlDB.ExecContext(t.Context(), `UPDATE pg_catalog.pg_index SET indisvalid = false
		WHERE indexrelid = to_regclass('public.' || $1)`, name)
	require.NoError(t, err)
}
