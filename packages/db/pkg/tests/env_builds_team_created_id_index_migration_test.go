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

const envBuildsTeamCreatedIDIndexVersion int64 = 20261110103628

// The new index keeps the keys of the one it stands in for, without the
// INCLUDE columns, so the build list keeps its plan. A rerun after an
// interrupted build replaces the INVALID leftover rather than skipping it, and
// keeps an index that is already valid, such as one built by hand ahead of the
// deploy.
func TestEnvBuildsTeamCreatedIDIndexMigration(t *testing.T) {
	t.Parallel()

	for name, prepare := range map[string]func(t *testing.T, sqlDB *sql.DB) (kept uint32){
		"built by hand ahead of the deploy": func(t *testing.T, sqlDB *sql.DB) uint32 {
			t.Helper()
			_, err := sqlDB.ExecContext(t.Context(), `CREATE INDEX idx_env_builds_team_created_id
				ON public.env_builds (team_id, created_at DESC, id DESC)`)
			require.NoError(t, err)

			return indexOID(t, sqlDB, "idx_env_builds_team_created_id")
		},
		"interrupted build next to an interrupted drop of a set-aside leftover": func(t *testing.T, sqlDB *sql.DB) uint32 {
			t.Helper()
			for _, name := range []string{"idx_env_builds_team_created_id_invalid", "idx_env_builds_team_created_id"} {
				_, err := sqlDB.ExecContext(t.Context(), `CREATE INDEX `+name+` ON public.env_builds (team_id)`)
				require.NoError(t, err)
				invalidate(t, sqlDB, name)
			}

			return 0
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			sqlDB, migrations := envBuildsMigrations(t)
			_, err := migrations.DownTo(ctx, envBuildsTeamCreatedIDIndexVersion-1)
			require.NoError(t, err)

			full, _ := indexDefinition(t, sqlDB, "idx_env_builds_team_status_pagination")
			want := strings.Replace(
				strings.TrimSuffix(full, " INCLUDE (status, status_group)"),
				"idx_env_builds_team_status_pagination", "idx_env_builds_team_created_id", 1)

			kept := prepare(t, sqlDB)

			results, err := migrations.UpTo(ctx, envBuildsTeamCreatedIDIndexVersion)
			require.NoError(t, err)
			require.Len(t, results, 1)

			def, valid := indexDefinition(t, sqlDB, "idx_env_builds_team_created_id")
			assert.Equal(t, want, def)
			assert.True(t, valid)
			if kept != 0 {
				assert.Equal(t, kept, indexOID(t, sqlDB, "idx_env_builds_team_created_id"), "the valid index is kept")
			}

			var leftover bool
			require.NoError(t, sqlDB.QueryRowContext(ctx,
				"SELECT to_regclass('public.idx_env_builds_team_created_id_invalid') IS NOT NULL").Scan(&leftover))
			assert.False(t, leftover, "the set-aside leftover is dropped")
		})
	}
}

// A concurrent build still running, such as one started by hand ahead of the
// deploy, is INVALID like an interrupted one. The migration stops rather than
// set it aside and drop it, and keeps it once the build has finished. With a
// leftover an earlier run set aside, it stops before dropping that too, since
// the drop would wait out the build.
func TestEnvBuildsTeamCreatedIDIndexMigrationStopsWhileABuildRuns(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		leftover bool
		wantErr  string
	}{
		"no leftover":   {wantErr: "public.idx_env_builds_team_created_id is not valid yet"},
		"with leftover": {leftover: true, wantErr: "is building an index on public.env_builds"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			sqlDB, migrations := envBuildsMigrations(t)
			_, err := migrations.DownTo(ctx, envBuildsTeamCreatedIDIndexVersion-1)
			require.NoError(t, err)

			if tc.leftover {
				_, err = sqlDB.ExecContext(ctx, `CREATE INDEX idx_env_builds_team_created_id_invalid ON public.env_builds (team_id)`)
				require.NoError(t, err)
				invalidate(t, sqlDB, "idx_env_builds_team_created_id_invalid")
			}

			// An open writer holds the build in its first wait, after the INVALID index exists.
			writer, err := sqlDB.BeginTx(ctx, nil)
			require.NoError(t, err)
			t.Cleanup(func() { _ = writer.Rollback() })
			_, err = writer.ExecContext(ctx, "LOCK TABLE public.env_builds IN ROW EXCLUSIVE MODE")
			require.NoError(t, err)

			built := make(chan error, 1)
			go func() {
				_, err := sqlDB.ExecContext(ctx, `CREATE INDEX CONCURRENTLY idx_env_builds_team_created_id
					ON public.env_builds (team_id, created_at DESC, id DESC)`)
				built <- err
			}()
			require.Eventually(t, func() bool {
				var building bool
				err := sqlDB.QueryRowContext(ctx, `SELECT EXISTS (
					SELECT FROM pg_catalog.pg_stat_progress_create_index
					WHERE index_relid = to_regclass('public.idx_env_builds_team_created_id'))`).Scan(&building)

				return err == nil && building
			}, 30*time.Second, 50*time.Millisecond)
			building := indexOID(t, sqlDB, "idx_env_builds_team_created_id")

			stopped := make(chan error, 1)
			go func() {
				_, err := migrations.UpTo(ctx, envBuildsTeamCreatedIDIndexVersion)
				stopped <- err
			}()
			select {
			case err = <-stopped:
			case <-time.After(30 * time.Second):
				require.FailNow(t, "the migration waits on the running build instead of stopping")
			}
			require.ErrorContains(t, err, tc.wantErr)
			assert.Equal(t, building, indexOID(t, sqlDB, "idx_env_builds_team_created_id"), "the running build keeps its index")

			require.NoError(t, writer.Rollback())
			require.NoError(t, <-built)

			results, err := migrations.UpTo(ctx, envBuildsTeamCreatedIDIndexVersion)
			require.NoError(t, err)
			require.Len(t, results, 1)
			assert.Equal(t, building, indexOID(t, sqlDB, "idx_env_builds_team_created_id"), "the finished build is kept")
		})
	}
}

// A build started by hand that is still waiting for its table lock has no index
// yet for the checks to find, and the migration's CREATE queues behind it. If
// that build fails, the CREATE skips its INVALID leftover by name; the
// migration then fails rather than record itself as applied, and a rerun
// replaces the leftover.
func TestEnvBuildsTeamCreatedIDIndexMigrationFailsOnASkippedInvalidIndex(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	sqlDB, migrations := envBuildsMigrations(t)
	_, err := migrations.DownTo(ctx, envBuildsTeamCreatedIDIndexVersion-1)
	require.NoError(t, err)

	// Stands in for the failed hand build: its index stays invisible until the
	// commit, and the lock it holds until then makes the migration's CREATE wait.
	failed, err := sqlDB.BeginTx(ctx, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = failed.Rollback() })
	_, err = failed.ExecContext(ctx, `CREATE INDEX idx_env_builds_team_created_id ON public.env_builds (team_id)`)
	require.NoError(t, err)
	_, err = failed.ExecContext(ctx, `UPDATE pg_catalog.pg_index SET indisvalid = false
		WHERE indexrelid = 'public.idx_env_builds_team_created_id'::regclass`)
	require.NoError(t, err)

	stopped := make(chan error, 1)
	go func() {
		_, err := migrations.UpTo(ctx, envBuildsTeamCreatedIDIndexVersion)
		stopped <- err
	}()
	require.Eventually(t, func() bool {
		var waiting bool
		err := sqlDB.QueryRowContext(ctx, `SELECT EXISTS (
			SELECT FROM pg_catalog.pg_locks
			WHERE database = (SELECT oid FROM pg_catalog.pg_database WHERE datname = current_database())
				AND relation = 'public.env_builds'::regclass AND NOT granted)`).Scan(&waiting)

		return err == nil && waiting
	}, 30*time.Second, 50*time.Millisecond)
	require.NoError(t, failed.Commit())

	select {
	case err = <-stopped:
	case <-time.After(30 * time.Second):
		require.FailNow(t, "the migration did not finish")
	}
	require.ErrorContains(t, err, "public.idx_env_builds_team_created_id is not valid")

	results, err := migrations.UpTo(ctx, envBuildsTeamCreatedIDIndexVersion)
	require.NoError(t, err)
	require.Len(t, results, 1)
	_, valid := indexDefinition(t, sqlDB, "idx_env_builds_team_created_id")
	assert.True(t, valid, "the rerun replaces the leftover")
}

func envBuildsMigrations(t *testing.T) (*sql.DB, *goose.Provider) {
	t.Helper()

	db := testutils.SetupDatabase(t)
	sqlDB, err := sql.Open("pgx", db.ConnStr())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })

	store, err := database.NewStore(goose.DialectPostgres, testutils.TrackingTable)
	require.NoError(t, err)
	migrations, err := goose.NewProvider("", sqlDB, dbmodule.Migrations(), goose.WithStore(store))
	require.NoError(t, err)

	return sqlDB, migrations
}

func indexDefinition(t *testing.T, sqlDB *sql.DB, name string) (string, bool) {
	t.Helper()

	var def string
	var valid bool
	require.NoError(t, sqlDB.QueryRowContext(t.Context(), `
		SELECT pg_get_indexdef(i.indexrelid), i.indisvalid
		FROM pg_catalog.pg_index i
		WHERE i.indexrelid = to_regclass('public.' || $1)`, name).Scan(&def, &valid))

	return def, valid
}
