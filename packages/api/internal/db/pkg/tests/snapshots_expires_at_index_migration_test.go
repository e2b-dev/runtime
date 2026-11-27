package tests

import (
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const snapshotsExpiresAtIndexVersion int64 = 20261111164948

// A rerun after an interrupted build replaces the INVALID leftover rather than
// skipping it, and keeps an index that is already valid, such as one built by
// hand ahead of the deploy.
func TestSnapshotsExpiresAtIndexMigration(t *testing.T) {
	t.Parallel()

	for name, prepare := range map[string]func(t *testing.T, sqlDB *sql.DB) (kept uint32){
		"built by hand ahead of the deploy": func(t *testing.T, sqlDB *sql.DB) uint32 {
			t.Helper()
			_, err := sqlDB.ExecContext(t.Context(), `CREATE INDEX idx_snapshots_expires_at
				ON public.snapshots (expires_at)
				WHERE env_deleted_at IS NULL AND expires_at IS NOT NULL`)
			require.NoError(t, err)

			return indexOID(t, sqlDB, "idx_snapshots_expires_at")
		},
		"interrupted build next to an interrupted drop of a set-aside leftover": func(t *testing.T, sqlDB *sql.DB) uint32 {
			t.Helper()
			for _, name := range []string{"idx_snapshots_expires_at_invalid", "idx_snapshots_expires_at"} {
				_, err := sqlDB.ExecContext(t.Context(), `CREATE INDEX `+name+` ON public.snapshots (team_id)`)
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
			_, err := migrations.DownTo(ctx, snapshotsExpiresAtIndexVersion-1)
			require.NoError(t, err)

			kept := prepare(t, sqlDB)

			results, err := migrations.UpTo(ctx, snapshotsExpiresAtIndexVersion)
			require.NoError(t, err)
			require.Len(t, results, 1)

			_, valid := indexDefinition(t, sqlDB, "idx_snapshots_expires_at")
			assert.True(t, valid)
			if kept != 0 {
				assert.Equal(t, kept, indexOID(t, sqlDB, "idx_snapshots_expires_at"), "the valid index is kept")
			}
			assertSweepUsesExpiresAtIndex(t, sqlDB)

			var leftover bool
			require.NoError(t, sqlDB.QueryRowContext(ctx,
				"SELECT to_regclass('public.idx_snapshots_expires_at_invalid') IS NOT NULL").Scan(&leftover))
			assert.False(t, leftover, "the set-aside leftover is dropped")
		})
	}
}

// A build started by hand that is still waiting for its table lock has no index
// yet for the checks to find, and the migration's CREATE queues behind it. If
// that build fails, the CREATE skips its INVALID leftover by name; the
// migration then fails rather than record itself as applied, and a rerun
// replaces the leftover.
func TestSnapshotsExpiresAtIndexMigrationFailsOnASkippedInvalidIndex(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	sqlDB, migrations := envBuildsMigrations(t)
	_, err := migrations.DownTo(ctx, snapshotsExpiresAtIndexVersion-1)
	require.NoError(t, err)

	failed, err := sqlDB.BeginTx(ctx, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = failed.Rollback() })
	_, err = failed.ExecContext(ctx, `CREATE INDEX idx_snapshots_expires_at ON public.snapshots (team_id)`)
	require.NoError(t, err)
	_, err = failed.ExecContext(ctx, `UPDATE pg_catalog.pg_index SET indisvalid = false
		WHERE indexrelid = 'public.idx_snapshots_expires_at'::regclass`)
	require.NoError(t, err)

	stopped := make(chan error, 1)
	go func() {
		_, err := migrations.UpTo(ctx, snapshotsExpiresAtIndexVersion)
		stopped <- err
	}()
	require.Eventually(t, func() bool {
		var waiting bool
		err := sqlDB.QueryRowContext(ctx, `SELECT EXISTS (
			SELECT FROM pg_catalog.pg_locks
			WHERE database = (SELECT oid FROM pg_catalog.pg_database WHERE datname = current_database())
				AND relation = 'public.snapshots'::regclass AND NOT granted)`).Scan(&waiting)

		return err == nil && waiting
	}, 30*time.Second, 50*time.Millisecond)
	require.NoError(t, failed.Commit())

	select {
	case err = <-stopped:
	case <-time.After(30 * time.Second):
		require.Fail(t, "the migration did not finish")
	}
	require.ErrorContains(t, err, "public.idx_snapshots_expires_at is not valid")

	results, err := migrations.UpTo(ctx, snapshotsExpiresAtIndexVersion)
	require.NoError(t, err)
	require.Len(t, results, 1)
	_, valid := indexDefinition(t, sqlDB, "idx_snapshots_expires_at")
	assert.True(t, valid, "the rerun replaces the leftover")
}

// The sweep finds the live snapshots whose expiry has passed, oldest first.
// Seq scans are off because the test table is too small for the planner to
// prefer an index on its own.
func assertSweepUsesExpiresAtIndex(t *testing.T, sqlDB *sql.DB) {
	t.Helper()

	ctx := t.Context()
	tx, err := sqlDB.BeginTx(ctx, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	_, err = tx.ExecContext(ctx, "SET LOCAL enable_seqscan = off")
	require.NoError(t, err)

	rows, err := tx.QueryContext(ctx, `EXPLAIN (COSTS OFF)
		SELECT sandbox_id FROM public.snapshots
		WHERE env_deleted_at IS NULL AND expires_at IS NOT NULL AND expires_at <= now()
		ORDER BY expires_at
		LIMIT 100`)
	require.NoError(t, err)
	defer rows.Close()

	var plan string
	for rows.Next() {
		var line string
		require.NoError(t, rows.Scan(&line))
		plan += line + "\n"
	}
	require.NoError(t, rows.Err())

	assert.Contains(t, plan, "Index Scan using idx_snapshots_expires_at on snapshots")
}
