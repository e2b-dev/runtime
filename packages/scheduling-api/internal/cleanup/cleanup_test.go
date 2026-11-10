package cleanup

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/scheduling-api/internal/db/dbtest"
	"github.com/e2b-dev/infra/packages/scheduling-api/internal/store"
)

var fast = Config{Interval: 20 * time.Millisecond, BatchSize: 1000, Yield: time.Millisecond, IdleSessionTimeout: time.Minute}

func TestWorkerExpiresAndPurges(t *testing.T) {
	t.Parallel()
	pool := dbtest.MigratedPool(t)
	st := store.New(pool, pool)
	project := uuid.New()
	starting := expiredStart(t, st, pool, project)
	gone := oldTombstone(t, st, pool, project)
	fresh := uuid.New()
	reserve(t, st, project, fresh)

	w := newWorker(t, st, pool, fast)
	stop := run(t, w)

	require.Eventually(t, func() bool {
		return state(t, pool, starting) == store.StateGone && state(t, pool, gone) == ""
	}, 5*time.Second, 10*time.Millisecond)
	require.Equal(t, store.StateStarting, state(t, pool, fresh))
	require.Equal(t, int32(1), live(t, pool, project))
	requireCountsMatch(t, pool)

	stop()
	requireLockReleased(t, pool)
	require.Eventually(t, func() bool { return cleanupBackends(t, pool) == 0 }, 5*time.Second, 10*time.Millisecond,
		"the leader connection is closed")
}

func TestWorkerOneLeader(t *testing.T) {
	t.Parallel()
	pool := dbtest.MigratedPool(t)
	st := store.New(pool, pool)
	workers := make([]*Worker, 3)
	for i := range workers {
		workers[i] = newWorker(t, store.New(pool, pool), pool, fast)
		run(t, workers[i])
	}

	var projects []uuid.UUID
	for range 20 {
		project := uuid.New()
		expiredStart(t, st, pool, project)
		projects = append(projects, project)
	}
	require.Eventually(t, func() bool {
		for _, project := range projects {
			if live(t, pool, project) != 0 {
				return false
			}
		}

		return true
	}, 5*time.Second, 10*time.Millisecond)
	requireCountsMatch(t, pool)

	require.Len(t, lockHolders(t, pool), 1)
	leaders := 0
	for _, w := range workers {
		if w.rounds.Load() > 0 {
			leaders++
		}
	}
	require.Equal(t, 1, leaders, "only the lock holder runs rounds")
}

func TestWorkerLeaderBackendTerminated(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	pool := dbtest.MigratedPool(t)
	st := store.New(pool, pool)
	for range 2 {
		run(t, newWorker(t, store.New(pool, pool), pool, fast))
	}
	require.Eventually(t, func() bool { return len(lockHolders(t, pool)) == 1 }, 5*time.Second, 10*time.Millisecond)
	first := lockHolders(t, pool)[0]

	var terminated bool
	require.NoError(t, pool.QueryRow(ctx, `SELECT pg_terminate_backend($1)`, first).Scan(&terminated))
	require.True(t, terminated)
	require.Eventually(t, func() bool {
		holders := lockHolders(t, pool)

		return len(holders) == 1 && holders[0] != first
	}, 5*time.Second, 10*time.Millisecond)

	project := uuid.New()
	actor := expiredStart(t, st, pool, project)
	require.Eventually(t, func() bool { return state(t, pool, actor) == store.StateGone }, 5*time.Second, 10*time.Millisecond)
	requireCountsMatch(t, pool)
}

func TestWorkerStopReleasesLeadership(t *testing.T) {
	t.Parallel()
	pool := dbtest.MigratedPool(t)
	leader := newWorker(t, store.New(pool, pool), pool, fast)
	stopLeader := run(t, leader)
	require.Eventually(t, func() bool { return leader.rounds.Load() > 0 }, 5*time.Second, 10*time.Millisecond)
	follower := newWorker(t, store.New(pool, pool), pool, fast)
	stopFollower := run(t, follower)

	stopLeader()
	require.Eventually(t, func() bool { return follower.rounds.Load() > 0 }, 5*time.Second, 10*time.Millisecond)
	require.Len(t, lockHolders(t, pool), 1)

	stopFollower()
	requireLockReleased(t, pool)
	require.Eventually(t, func() bool { return cleanupBackends(t, pool) == 0 }, 5*time.Second, 10*time.Millisecond)
}

// A start whose counter row is locked past the lock timeout stays for a
// later round. The other starts end in the same round.
func TestRoundSkipsLockedCounter(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	pool := dbtest.MigratedPool(t)
	st := store.New(pool, pool)
	hot, other := uuid.New(), uuid.New()
	blocked := expiredStart(t, st, pool, hot)
	free := expiredStart(t, st, pool, other)
	w := newWorker(t, st, pool, DefaultConfig)
	t.Cleanup(func() { w.resign(t.Context()) })

	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	_, err = tx.Exec(ctx, `SELECT FROM public.scheduling_project_live_count WHERE project_id = $1 FOR UPDATE`, hot)
	require.NoError(t, err)

	stats := w.round(ctx)
	require.Equal(t, 1, stats.expired)
	require.Equal(t, 1, stats.failed, "a lock timeout is a failure")
	require.Len(t, stats.errs, 1)
	require.ErrorContains(t, stats.errs[0], "lock timeout")
	require.Equal(t, store.StateStarting, state(t, pool, blocked))
	require.Equal(t, store.StateGone, state(t, pool, free))
	require.Equal(t, int32(0), live(t, pool, other))
	require.NoError(t, tx.Commit(ctx))
	require.Equal(t, int32(1), live(t, pool, hot))

	stats = w.round(ctx)
	require.Equal(t, roundStats{expired: 1}, stats)
	require.Equal(t, store.StateGone, state(t, pool, blocked))
	require.Equal(t, int32(0), live(t, pool, hot))
	requireCountsMatch(t, pool)
}

// The expiry step runs out of its budget while a start waits for its row. The
// purge still runs, the waiting start is not a failure, and the next round
// continues with it.
func TestRoundBudgets(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	pool := dbtest.MigratedPool(t)
	st := store.New(pool, pool)
	project := uuid.New()
	first, waiting := actorWithPrefix(1), actorWithPrefix(2)
	for _, actor := range []uuid.UUID{first, waiting} {
		reserve(t, st, project, actor)
		_, err := pool.Exec(ctx, `UPDATE public.scheduling_actor_run SET reserved_at = now() - interval '1 hour' WHERE actor_id = $1`, actor)
		require.NoError(t, err)
	}
	gone := oldTombstone(t, st, pool, project)
	config := DefaultConfig
	config.Interval = 400 * time.Millisecond
	w := newWorker(t, st, pool, config)
	t.Cleanup(func() { w.resign(t.Context()) })

	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	_, err = tx.Exec(ctx, `SELECT FROM public.scheduling_actor_run WHERE actor_id = $1 FOR UPDATE`, waiting)
	require.NoError(t, err)

	stats := w.round(ctx)
	require.Equal(t, roundStats{expired: 1, purged: 1}, stats)
	require.Equal(t, first, w.after, "the cursor stays before the waiting start")
	require.Equal(t, store.StateStarting, state(t, pool, waiting))
	require.Equal(t, store.State(""), state(t, pool, gone))
	require.NoError(t, tx.Rollback(ctx))

	stats = w.round(ctx)
	require.Equal(t, roundStats{expired: 1}, stats)
	require.Equal(t, uuid.Nil, w.after)
	require.Equal(t, int32(0), live(t, pool, project))
	requireCountsMatch(t, pool)
}

// PostgreSQL ends the lock session of a leader that stops using it, as after
// a host failure, and another worker takes the lock.
func TestIdleLeaderLosesLock(t *testing.T) {
	t.Parallel()
	pool := dbtest.MigratedPool(t)
	config := fast
	config.Interval, config.IdleSessionTimeout = 100*time.Millisecond, 300*time.Millisecond
	stuck := newWorker(t, store.New(pool, pool), pool, config)
	require.True(t, stuck.lead(t.Context()))
	t.Cleanup(func() { stuck.resign(t.Context()) })
	require.Len(t, lockHolders(t, pool), 1)

	requireLockReleased(t, pool)
	follower := newWorker(t, store.New(pool, pool), pool, fast)
	run(t, follower)
	require.Eventually(t, func() bool { return follower.rounds.Load() > 0 }, 5*time.Second, 10*time.Millisecond)
	require.False(t, stuck.holdsLock(t.Context(), t.Context()), "the stuck worker finds out when it runs again")
}

func TestNewSetsSessionParameters(t *testing.T) {
	t.Parallel()
	connConfig, err := pgx.ParseConfig("postgres://localhost/scheduling?application_name=other&idle_session_timeout=0")
	require.NoError(t, err)

	w, err := New(nil, connConfig, DefaultConfig)
	require.NoError(t, err)
	require.Equal(t, "scheduling-api-cleanup", w.connConfig.RuntimeParams["application_name"])
	require.Equal(t, "60000ms", w.connConfig.RuntimeParams["idle_session_timeout"])
	require.Equal(t, "other", connConfig.RuntimeParams["application_name"], "the caller's config is not changed")
}

func TestWorkerDatabaseUnavailable(t *testing.T) {
	t.Parallel()

	t.Run("no connection", func(t *testing.T) {
		t.Parallel()
		pool := dbtest.MigratedPool(t)
		connConfig := pool.Config().ConnConfig.Copy()
		connConfig.Host, connConfig.Port = "127.0.0.1", freePort(t)
		connConfig.ConnectTimeout = time.Second
		w, err := New(store.New(pool, pool), connConfig, fast)
		require.NoError(t, err)

		stop := run(t, w)
		time.Sleep(10 * fast.Interval)
		stop()
		require.Zero(t, w.rounds.Load())
	})
	t.Run("store fails", func(t *testing.T) {
		t.Parallel()
		pool := dbtest.MigratedPool(t)
		closed, err := pgxpool.NewWithConfig(t.Context(), pool.Config())
		require.NoError(t, err)
		closed.Close()
		w := newWorker(t, store.New(closed, closed), pool, fast)

		stop := run(t, w)
		require.Eventually(t, func() bool { return w.rounds.Load() > 3 }, 5*time.Second, 10*time.Millisecond)
		stop()
		requireLockReleased(t, pool)
	})
}

func TestNewRejectsInvalidConfig(t *testing.T) {
	t.Parallel()
	for _, config := range []Config{
		{Interval: 0, BatchSize: 1, IdleSessionTimeout: time.Minute},
		{Interval: time.Second, BatchSize: 0, IdleSessionTimeout: time.Minute},
		{Interval: time.Second, BatchSize: 1, Yield: -1, IdleSessionTimeout: time.Minute},
		{Interval: time.Second, BatchSize: 1, IdleSessionTimeout: 2 * time.Second},
		{Interval: time.Second, BatchSize: 1, IdleSessionTimeout: time.Minute + time.Microsecond},
	} {
		_, err := New(nil, &pgx.ConnConfig{}, config)
		require.Error(t, err, "%+v", config)
	}
}

func newWorker(t *testing.T, st *store.Store, pool *pgxpool.Pool, config Config) *Worker {
	t.Helper()

	w, err := New(st, pool.Config().ConnConfig, config)
	require.NoError(t, err)

	return w
}

// run starts w and returns a function that stops it and waits for Run to
// return. The test stops it at the end in any case.
func run(t *testing.T, w *Worker) func() {
	t.Helper()

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.Run(ctx)
	}()
	stop := func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * releaseTimeout):
			t.Error("Run did not return after cancel")
		}
	}
	t.Cleanup(stop)

	return stop
}

func reserve(t *testing.T, st *store.Store, project, actor uuid.UUID) uuid.UUID {
	t.Helper()

	rid := uuid.New()
	result, err := st.Reserve(t.Context(), store.Reservation{
		ProjectID: project, ActorID: actor, ReservationID: rid, Limit: 1000, EndTime: time.Now().Add(time.Hour),
	})
	require.NoError(t, err)
	require.Equal(t, store.ReserveReserved, result.Outcome)

	return rid
}

func expiredStart(t *testing.T, st *store.Store, pool *pgxpool.Pool, project uuid.UUID) uuid.UUID {
	t.Helper()

	actor := uuid.New()
	reserve(t, st, project, actor)
	_, err := pool.Exec(t.Context(),
		`UPDATE public.scheduling_actor_run SET reserved_at = now() - interval '1 hour' WHERE actor_id = $1`, actor)
	require.NoError(t, err)

	return actor
}

func oldTombstone(t *testing.T, st *store.Store, pool *pgxpool.Pool, project uuid.UUID) uuid.UUID {
	t.Helper()

	actor := uuid.New()
	rid := reserve(t, st, project, actor)
	outcome, err := st.FinishStart(t.Context(), project, actor, rid, store.Failure{Message: "node failed"})
	require.NoError(t, err)
	require.Equal(t, store.FinishStartFinished, outcome)
	_, err = pool.Exec(t.Context(),
		`UPDATE public.scheduling_actor_run SET gone_at = now() - interval '1 hour' WHERE actor_id = $1`, actor)
	require.NoError(t, err)

	return actor
}

func state(t *testing.T, pool *pgxpool.Pool, actor uuid.UUID) store.State {
	t.Helper()

	var s store.State
	err := pool.QueryRow(t.Context(), `SELECT state FROM public.scheduling_actor_run WHERE actor_id = $1`, actor).Scan(&s)
	if err == pgx.ErrNoRows { //nolint:errorlint,forbidigo // QueryRow returns the sentinel itself
		return ""
	}
	require.NoError(t, err)

	return s
}

func live(t *testing.T, pool *pgxpool.Pool, project uuid.UUID) int32 {
	t.Helper()

	var n int32
	require.NoError(t, pool.QueryRow(t.Context(),
		`SELECT coalesce(sum(live), 0)::integer FROM public.scheduling_project_live_count WHERE project_id = $1`, project).Scan(&n))

	return n
}

// requireCountsMatch checks that every slot counts exactly the actors that
// are not gone and name it.
func requireCountsMatch(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()

	var mismatches int
	require.NoError(t, pool.QueryRow(t.Context(), `
SELECT count(*)
  FROM (SELECT project_id, counter_slot AS slot, count(*)::int AS live
          FROM public.scheduling_actor_run WHERE state <> 'gone'
         GROUP BY project_id, counter_slot) r
  FULL JOIN (SELECT project_id, slot, live FROM public.scheduling_project_live_count WHERE live > 0) c
 USING (project_id, slot)
 WHERE r.live IS DISTINCT FROM c.live`).Scan(&mismatches))
	require.Zero(t, mismatches)
}

// lockHolders returns the backends that hold an advisory lock in the test
// database. Advisory locks are per database, so other tests do not appear.
func lockHolders(t *testing.T, pool *pgxpool.Pool) []int32 {
	t.Helper()

	rows, err := pool.Query(t.Context(), `
SELECT pid FROM pg_locks
 WHERE locktype = 'advisory' AND granted
   AND database = (SELECT oid FROM pg_database WHERE datname = current_database())`)
	require.NoError(t, err)
	pids, err := pgx.CollectRows(rows, pgx.RowTo[int32])
	require.NoError(t, err)

	return pids
}

// requireLockReleased waits for the lock to be free. A connection that a
// cancelled query closed releases it only when its backend exits.
func requireLockReleased(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()

	require.Eventually(t, func() bool { return len(lockHolders(t, pool)) == 0 }, 5*time.Second, 10*time.Millisecond)
}

func cleanupBackends(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()

	var n int
	require.NoError(t, pool.QueryRow(t.Context(), `
SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND application_name = $1`,
		applicationName).Scan(&n))

	return n
}

func actorWithPrefix(b byte) uuid.UUID {
	actor := uuid.New()
	actor[0] = b

	return actor
}

func freePort(t *testing.T) uint16 {
	t.Helper()

	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := listener.Addr().(*net.TCPAddr).Port
	require.NoError(t, listener.Close())

	return uint16(port)
}
