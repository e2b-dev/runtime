package db_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/scheduling-api/internal/db"
	"github.com/e2b-dev/infra/packages/scheduling-api/internal/db/dbtest"
	"github.com/e2b-dev/infra/packages/shared/pkg/dbmigrate"
)

var tables = []string{"scheduling_actor", "scheduling_actor_run", "scheduling_project_live_count", "scheduling_kill_claim"}

func TestMigrationsEmbedsTheDirectory(t *testing.T) {
	t.Parallel()

	paths, err := filepath.Glob(filepath.Join("migrations", "*.sql"))
	require.NoError(t, err)
	onDisk := make([]string, 0, len(paths))
	for _, path := range paths {
		onDisk = append(onDisk, filepath.Base(path))
	}

	embedded, err := fs.Glob(db.Migrations(), "*")
	require.NoError(t, err)

	require.NotEmpty(t, onDisk)
	require.ElementsMatch(t, onDisk, embedded)
}

func TestMigrateUpDownUp(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	database := openDatabase(t, dbtest.EmptyPool(t))

	require.NoError(t, db.Migrate(ctx, database))
	requireTables(t, database, true)
	require.Equal(t, latestVersion(t, database), dbVersion(t, database))

	require.ElementsMatch(t, append([]string{"scheduling_migrations"}, tables...), userTables(t, database))

	require.NoError(t, db.Migrate(ctx, database), "a second run applies nothing")
	require.Equal(t, latestVersion(t, database), dbVersion(t, database))

	provider, err := dbmigrate.NewGooseProvider(database, db.Migrations(), db.TrackingTable)
	require.NoError(t, err)
	_, err = provider.DownTo(ctx, 0)
	require.NoError(t, err)
	requireTables(t, database, false)
	require.Equal(t, []string{"scheduling_migrations"}, userTables(t, database))
	require.Zero(t, dbVersion(t, database))

	require.NoError(t, db.Migrate(ctx, database))
	requireTables(t, database, true)
	require.Equal(t, latestVersion(t, database), dbVersion(t, database))
}

func TestMigrateConcurrently(t *testing.T) {
	t.Parallel()
	databaseURL := dbtest.ConnString(t, dbtest.EmptyPool(t))

	const runs = 4
	errs := make(chan error, runs)
	var wg sync.WaitGroup
	for range runs {
		wg.Go(func() {
			database, err := db.Open(databaseURL)
			if err != nil {
				errs <- err

				return
			}
			errs <- errors.Join(db.Migrate(t.Context(), database), database.Close())
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	database, err := db.Open(databaseURL)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, database.Close()) })
	requireTables(t, database, true)
	require.Equal(t, latestVersion(t, database), dbVersion(t, database))
}

// Only these indexes keep the frequent actor_run updates HOT.
func TestActorRunIndexesAndStorage(t *testing.T) {
	t.Parallel()
	pool := dbtest.MigratedPool(t)

	rows, err := pool.Query(t.Context(),
		`SELECT indexname FROM pg_indexes WHERE schemaname = 'public' AND tablename = 'scheduling_actor_run'`)
	require.NoError(t, err)
	indexes, err := pgx.CollectRows(rows, pgx.RowTo[string])
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"scheduling_actor_run_pkey", "scheduling_actor_run_end_time_idx"}, indexes)

	for table, fillfactor := range map[string]string{"scheduling_actor_run": "fillfactor=50", "scheduling_project_live_count": "fillfactor=20"} {
		var options []string
		require.NoError(t, pool.QueryRow(t.Context(),
			`SELECT reloptions FROM pg_class WHERE oid = to_regclass($1)`, "public."+table).Scan(&options))
		assert.Contains(t, options, fillfactor, table)
	}
}

type runRow struct {
	state            string
	transitionID     string
	transitionAction string
	transitionAt     string
	goneAt           string
	counterSlot      int
}

func (r runRow) insert(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, fmt.Sprintf(`
INSERT INTO public.scheduling_actor_run (actor_id, project_id, state, end_time, reservation_id, reserved_at,
                                         transition_id, transition_action, transition_at, gone_at, counter_slot)
VALUES (gen_random_uuid(), gen_random_uuid(), $1, now(), gen_random_uuid(), now(), %s, %s, %s, %s, %d)`,
		r.transitionID, r.transitionAction, r.transitionAt, r.goneAt, r.counterSlot), r.state)

	return err
}

func idle(state string) runRow {
	return runRow{state: state, transitionID: "NULL", transitionAction: "NULL", transitionAt: "NULL", goneAt: "NULL"}
}

func inTransition(state string, action int) runRow {
	r := idle(state)
	r.transitionID, r.transitionAction, r.transitionAt = "gen_random_uuid()", fmt.Sprint(action), "now()"

	return r
}

func gone(r runRow) runRow {
	r.state, r.goneAt = "gone", "now()"

	return r
}

func TestActorRunAcceptsStoreStates(t *testing.T) {
	t.Parallel()
	pool := dbtest.MigratedPool(t)

	maxSlot := idle("running")
	maxSlot.counterSlot = 63

	for name, row := range map[string]runRow{
		"starting":                   idle("starting"),
		"running":                    idle("running"),
		"running after a transition": inTransition("running", 3),
		"pausing":                    inTransition("pausing", 1),
		"killing":                    inTransition("killing", 2),
		"snapshotting":               inTransition("snapshotting", 3),
		"gone after a failed start":  gone(idle("starting")),
		"gone after a transition":    gone(inTransition("killing", 2)),
		"highest counter slot":       maxSlot,
	} {
		require.NoError(t, row.insert(t.Context(), pool), name)
	}
}

func TestActorRunRejectsInvalidRows(t *testing.T) {
	t.Parallel()
	pool := dbtest.MigratedPool(t)

	with := func(r runRow, change func(*runRow)) runRow {
		change(&r)

		return r
	}

	for name, tc := range map[string]struct {
		row        runRow
		constraint string
	}{
		"unknown action":                  {inTransition("pausing", 4), "scheduling_actor_run_transition_action_check"},
		"negative counter slot":           {with(idle("running"), func(r *runRow) { r.counterSlot = -1 }), "scheduling_actor_run_counter_slot_check"},
		"counter slot above 63":           {with(idle("running"), func(r *runRow) { r.counterSlot = 64 }), "scheduling_actor_run_counter_slot_check"},
		"gone without gone_at":            {with(gone(idle("starting")), func(r *runRow) { r.goneAt = "NULL" }), "scheduling_actor_run_gone_at_check"},
		"running with gone_at":            {with(idle("running"), func(r *runRow) { r.goneAt = "now()" }), "scheduling_actor_run_gone_at_check"},
		"partial transition":              {with(inTransition("running", 1), func(r *runRow) { r.transitionAt = "NULL" }), "scheduling_actor_run_transition_check"},
		"pausing without transition":      {idle("pausing"), "scheduling_actor_run_in_flight_check"},
		"killing without transition":      {idle("killing"), "scheduling_actor_run_in_flight_check"},
		"snapshotting without transition": {idle("snapshotting"), "scheduling_actor_run_in_flight_check"},
	} {
		requireViolation(t, tc.row.insert(t.Context(), pool), pgerrCheckViolation, tc.constraint, name)
	}

	for _, state := range []string{"unknown", "2"} {
		requireViolation(t, idle(state).insert(t.Context(), pool), "22P02", "", state)
	}
}

func TestProjectLiveCountConstraints(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	pool := dbtest.MigratedPool(t)

	const insert = `INSERT INTO public.scheduling_project_live_count (project_id, slot, live) VALUES ($1, $2, $3)`
	const project = "6f1c2c0e-4a8e-4d4f-9a59-2f7c8c1f0b11"

	_, err := pool.Exec(ctx, insert, project, 0, 0)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, insert, project, 63, 1)
	require.NoError(t, err)

	_, err = pool.Exec(ctx, insert, project, -1, 0)
	requireViolation(t, err, pgerrCheckViolation, "scheduling_project_live_count_slot_check", "negative slot")
	_, err = pool.Exec(ctx, insert, project, 64, 0)
	requireViolation(t, err, pgerrCheckViolation, "scheduling_project_live_count_slot_check", "slot above 63")
	_, err = pool.Exec(ctx, insert, project, 1, -1)
	requireViolation(t, err, pgerrCheckViolation, "scheduling_project_live_count_live_check", "negative live")
	_, err = pool.Exec(ctx, insert, project, 0, 1)
	requireViolation(t, err, pgerrUniqueViolation, "scheduling_project_live_count_pkey", "second row for a slot")

	_, err = pool.Exec(ctx, `UPDATE public.scheduling_project_live_count SET live = live - 1 WHERE project_id = $1 AND slot = 0`, project)
	requireViolation(t, err, pgerrCheckViolation, "scheduling_project_live_count_live_check", "decrement below zero")
}

const (
	pgerrCheckViolation  = "23514"
	pgerrUniqueViolation = "23505"
)

func requireViolation(t *testing.T, err error, code, constraint, name string) {
	t.Helper()

	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr, name)
	assert.Equal(t, code, pgErr.Code, name)
	assert.Equal(t, constraint, pgErr.ConstraintName, name)
}

func requireTables(t *testing.T, database *sql.DB, exist bool) {
	t.Helper()

	for _, table := range tables {
		var found bool
		require.NoError(t, database.QueryRowContext(t.Context(),
			`SELECT to_regclass($1) IS NOT NULL`, "public."+table).Scan(&found))
		require.Equal(t, exist, found, table)
	}
}

func userTables(t *testing.T, database *sql.DB) []string {
	t.Helper()

	rows, err := database.QueryContext(t.Context(), `
SELECT CASE schemaname WHEN 'public' THEN tablename ELSE schemaname || '.' || tablename END
FROM pg_tables
WHERE schemaname NOT IN ('pg_catalog', 'information_schema')`)
	require.NoError(t, err)
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		names = append(names, name)
	}
	require.NoError(t, rows.Err())

	return names
}

func latestVersion(t *testing.T, database *sql.DB) int64 {
	t.Helper()

	provider, err := dbmigrate.NewGooseProvider(database, db.Migrations(), db.TrackingTable)
	require.NoError(t, err)
	sources := provider.ListSources()
	require.NotEmpty(t, sources)

	return sources[len(sources)-1].Version
}

func dbVersion(t *testing.T, database *sql.DB) int64 {
	t.Helper()

	provider, err := dbmigrate.NewGooseProvider(database, db.Migrations(), db.TrackingTable)
	require.NoError(t, err)
	version, err := provider.GetDBVersion(t.Context())
	require.NoError(t, err)

	return version
}

func openDatabase(t *testing.T, pool *pgxpool.Pool) *sql.DB {
	t.Helper()

	database := stdlib.OpenDBFromPool(pool)
	t.Cleanup(func() { assert.NoError(t, database.Close()) })

	return database
}
