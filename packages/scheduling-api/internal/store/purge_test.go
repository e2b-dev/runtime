package store

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	queries "github.com/e2b-dev/infra/packages/scheduling-api/internal/db/queries/gen"
)

func TestPurgeExpiredSnapshotClaims(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	st, pool := newStore(t)
	project, actor, active := uuid.New(), uuid.New(), uuid.New()
	for range 3 {
		claimed, err := st.ClaimSnapshotDeletion(ctx, project, actor, uuid.New())
		require.NoError(t, err)
		require.True(t, claimed)
	}
	_, err := pool.Exec(ctx, `UPDATE scheduling_kill_claim SET until = now() - interval '1 second'`)
	require.NoError(t, err)
	claimed, err := st.ClaimSnapshotDeletion(ctx, project, actor, active)
	require.NoError(t, err)
	require.True(t, claimed)
	for _, remaining := range []int{2, 1} {
		_, err = st.PurgeTombstones(ctx, 2)
		require.NoError(t, err)
		var count int
		require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM scheduling_kill_claim`).Scan(&count))
		require.Equal(t, remaining, count)
	}
	_, err = st.Reserve(ctx, Reservation{ProjectID: project, ActorID: actor, ReservationID: uuid.New(), Limit: 10, EndTime: endTime})
	require.ErrorIs(t, err, ErrKillClaim)
	require.NoError(t, st.ReleaseSnapshotDeletionClaim(ctx, project, actor, active))
	reserve(t, st, project, actor, uuid.New(), strict, ReserveReserved)
}

func TestPurgeTombstonesSelectsOnlyOldTombstones(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	st, pool := newStore(t)
	project := uuid.New()

	old := goneStart(t, st, project)
	setGoneAgo(t, pool, old, time.Hour)
	boundary := goneStart(t, st, project)
	setGoneAgo(t, pool, boundary, tombstoneRetention)
	recent := goneStart(t, st, project)
	setGoneAgo(t, pool, recent, tombstoneRetention-10*time.Second)

	starting := uuid.New()
	reserve(t, st, project, starting, uuid.New(), strict, ReserveReserved)
	running, runningRID := uuid.New(), uuid.New()
	reserve(t, st, project, running, runningRID, strict, ReserveReserved)
	put(t, st, runningRID, newActor(project, running), PutStored)
	killing, killingRID := uuid.New(), uuid.New()
	reserve(t, st, project, killing, killingRID, strict, ReserveReserved)
	put(t, st, killingRID, newActor(project, killing), PutStored)
	begun, err := st.BeginTransition(ctx, TransitionRequest{ProjectID: project, ActorID: killing, TransitionID: uuid.New(), Action: ActionKill})
	require.NoError(t, err)
	require.Equal(t, BeginStarted, begun.Outcome)
	_, err = pool.Exec(ctx, `
UPDATE public.scheduling_actor_run
   SET reserved_at = now() - interval '1 day', end_time = now() - interval '1 day',
       transition_at = CASE WHEN transition_at IS NULL THEN NULL ELSE now() - interval '1 day' END
 WHERE state <> 'gone'`)
	require.NoError(t, err)
	counts := liveCounts(t, pool, project)

	deleted, err := st.PurgeTombstones(ctx, 100)
	require.NoError(t, err)
	require.EqualValues(t, 2, deleted)

	require.False(t, runExists(t, pool, old))
	require.False(t, runExists(t, pool, boundary))
	require.Equal(t, StateGone, runOf(t, pool, recent).state)
	require.Equal(t, StateStarting, runOf(t, pool, starting).state)
	require.Equal(t, StateRunning, runOf(t, pool, running).state)
	require.Equal(t, StateKilling, runOf(t, pool, killing).state)
	require.Equal(t, counts, liveCounts(t, pool, project), "the counters do not change")
	requireCountsMatch(t, pool)
}

func TestPurgeTombstonesLimit(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	st, pool := newStore(t)

	for range 3 {
		setGoneAgo(t, pool, goneStart(t, st, uuid.New()), time.Hour)
	}
	for _, want := range []int64{2, 1, 0} {
		deleted, err := st.PurgeTombstones(ctx, 2)
		require.NoError(t, err)
		require.Equal(t, want, deleted)
	}
	for _, maxRows := range []int32{0, -1} {
		_, err := st.PurgeTombstones(ctx, maxRows)
		require.ErrorContains(t, err, "must be positive")
	}
}

// After the purge, the service keeps nothing of the ended start or
// transition: repeated calls see no actor, and the old reservation ID starts
// the sandbox again.
func TestAfterPurge(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	st, pool := newStore(t)
	project, other := uuid.New(), uuid.New()

	failed, failedRID := uuid.New(), uuid.New()
	reserve(t, st, project, failed, failedRID, strict, ReserveReserved)
	finishStart(t, st, project, failed, failedRID, Failure{Message: "node failed"}, FinishStartFinished)

	killed, killedRID, tid := uuid.New(), uuid.New(), uuid.New()
	reserve(t, st, project, killed, killedRID, strict, ReserveReserved)
	put(t, st, killedRID, newActor(project, killed), PutStored)
	begun, err := st.BeginTransition(ctx, TransitionRequest{ProjectID: project, ActorID: killed, TransitionID: tid, Action: ActionKill})
	require.NoError(t, err)
	require.Equal(t, BeginStarted, begun.Outcome)
	finished, err := st.FinishTransition(ctx, project, killed, tid, nil)
	require.NoError(t, err)
	require.Equal(t, FinishTransitionFinished, finished)

	again, err := st.BeginTransition(ctx, TransitionRequest{ProjectID: project, ActorID: killed, TransitionID: tid, Action: ActionKill})
	require.NoError(t, err)
	require.Equal(t, BeginAlreadyFinished, again.Outcome, "before the purge")

	setGoneAgo(t, pool, failed, time.Hour)
	setGoneAgo(t, pool, killed, time.Hour)
	deleted, err := st.PurgeTombstones(ctx, 10)
	require.NoError(t, err)
	require.EqualValues(t, 2, deleted)

	finishStart(t, st, project, failed, failedRID, Failure{Message: "node failed"}, FinishStartNotFound)
	finished, err = st.FinishTransition(ctx, project, killed, tid, nil)
	require.NoError(t, err)
	require.Equal(t, FinishTransitionNotFound, finished)
	waited, err := st.WaitForTransition(ctx, project, killed, tid, time.Second)
	require.NoError(t, err)
	require.Equal(t, WaitNotFound, waited.Outcome)
	again, err = st.BeginTransition(ctx, TransitionRequest{ProjectID: project, ActorID: killed, TransitionID: tid, Action: ActionKill})
	require.NoError(t, err)
	require.Equal(t, BeginNotFound, again.Outcome)

	reserve(t, st, project, failed, failedRID, strict, ReserveReserved)
	reserve(t, st, other, killed, uuid.New(), strict, ReserveReserved)
	require.Equal(t, map[int16]int32{0: 1}, liveCounts(t, pool, project))
	require.Equal(t, map[int16]int32{0: 1}, liveCounts(t, pool, other))
	requireCountsMatch(t, pool)
}

func TestPurgeTombstonesRace(t *testing.T) {
	t.Parallel()

	t.Run("purge, then reserve", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		st, pool := newStore(t)
		project := uuid.New()
		actor := goneStart(t, st, project)
		setGoneAgo(t, pool, actor, time.Hour)

		tx, err := pool.Begin(ctx)
		require.NoError(t, err)
		defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
		deleted, err := st.queries.WithTx(tx).PurgeTombstones(ctx, queries.PurgeTombstonesParams{Retention: interval(tombstoneRetention), MaxRows: 10})
		require.NoError(t, err)
		require.EqualValues(t, 1, deleted)

		reserved := reserveInBackground(st, project, actor)
		requireLockWaiters(t, pool, 1)
		require.NoError(t, tx.Commit(ctx))
		require.NoError(t, <-reserved)

		require.Equal(t, StateStarting, runOf(t, pool, actor).state)
		require.Equal(t, map[int16]int32{0: 1}, liveCounts(t, pool, project))
		requireCountsMatch(t, pool)
	})
	t.Run("reserve, then purge", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		st, pool := newStore(t)
		project := uuid.New()
		actor := goneStart(t, st, project)
		setGoneAgo(t, pool, actor, time.Hour)

		tx, err := pool.Begin(ctx)
		require.NoError(t, err)
		defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
		_, err = tx.Exec(ctx, `SELECT FROM public.scheduling_actor_run WHERE actor_id = $1 FOR UPDATE`, actor)
		require.NoError(t, err)
		reserved := reserveInBackground(st, project, actor)
		requireLockWaiters(t, pool, 1)

		deleted, err := st.PurgeTombstones(ctx, 10)
		require.NoError(t, err)
		require.Zero(t, deleted, "the purge skips the row that Reserve waits for")
		require.NoError(t, tx.Commit(ctx))
		require.NoError(t, <-reserved)

		require.Equal(t, StateStarting, runOf(t, pool, actor).state)
		require.Equal(t, map[int16]int32{0: 1}, liveCounts(t, pool, project))
		requireCountsMatch(t, pool)
	})
	t.Run("purge while reserve waits for the counter", func(t *testing.T) {
		t.Parallel()
		st, pool := newStore(t)
		project := uuid.New()
		actor := goneStart(t, st, project)
		setGoneAgo(t, pool, actor, time.Hour)

		outcomes, errs := blockedOnCounter(t, pool, project, 0, 1, func() (ReserveResult, error) {
			return st.Reserve(t.Context(), Reservation{
				ProjectID: project, ActorID: actor, ReservationID: uuid.New(), Limit: strict, EndTime: endTime,
			})
		}, func() {
			deleted, err := st.PurgeTombstones(t.Context(), 10)
			require.NoError(t, err)
			require.EqualValues(t, 1, deleted)
		})
		require.Empty(t, errs)
		require.Equal(t, map[ReserveOutcome]int{ReserveReserved: 1}, outcomes)
		require.Equal(t, StateStarting, runOf(t, pool, actor).state)
		require.Equal(t, map[int16]int32{0: 1}, liveCounts(t, pool, project))
		requireCountsMatch(t, pool)
	})
}

func TestPurgeTombstonesConcurrent(t *testing.T) {
	t.Parallel()
	st, pool := newStore(t)
	project := uuid.New()

	const tombstones, purgers = 20, 4
	for range tombstones {
		setGoneAgo(t, pool, goneStart(t, st, project), time.Hour)
	}
	live := uuid.New()
	reserve(t, st, project, live, uuid.New(), strict, ReserveReserved)

	var total atomic.Int64
	var wg sync.WaitGroup
	errs := make(chan error, purgers)
	for range purgers {
		wg.Go(func() {
			for {
				deleted, err := st.PurgeTombstones(t.Context(), 3)
				if err != nil {
					errs <- err

					return
				}
				if deleted == 0 {
					return
				}
				total.Add(deleted)
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	require.EqualValues(t, tombstones, total.Load())
	require.Equal(t, StateStarting, runOf(t, pool, live).state)
	var left int
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT count(*) FROM public.scheduling_actor_run`).Scan(&left))
	require.Equal(t, 1, left)
	requireCountsMatch(t, pool)
}

func TestPurgeTombstonesRollsBack(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	st, pool := newStore(t)

	actors := make([]uuid.UUID, 3)
	for i := range actors {
		actors[i] = goneStart(t, st, uuid.New())
		setGoneAgo(t, pool, actors[i], time.Hour)
	}
	_, err := pool.Exec(ctx, `
CREATE FUNCTION refuse_delete() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'refuse_delete'; END $$;
CREATE TRIGGER refuse_delete BEFORE DELETE ON public.scheduling_actor_run
   FOR EACH ROW WHEN (OLD.actor_id = '`+actors[1].String()+`') EXECUTE FUNCTION refuse_delete()`)
	require.NoError(t, err)

	_, err = st.PurgeTombstones(ctx, 10)
	require.ErrorContains(t, err, "refuse_delete")
	for _, actor := range actors {
		require.True(t, runExists(t, pool, actor))
	}
}

func reserveInBackground(st *Store, project, actor uuid.UUID) <-chan error {
	reserved := make(chan error, 1)
	go func() {
		result, err := st.Reserve(context.Background(), Reservation{
			ProjectID: project, ActorID: actor, ReservationID: uuid.New(), Limit: strict, EndTime: endTime,
		})
		if err == nil && result.Outcome != ReserveReserved {
			err = fmt.Errorf("reserve: outcome %d", result.Outcome)
		}
		reserved <- err
	}()

	return reserved
}

func goneStart(t *testing.T, st *Store, project uuid.UUID) uuid.UUID {
	t.Helper()

	actor, rid := uuid.New(), uuid.New()
	reserve(t, st, project, actor, rid, strict, ReserveReserved)
	finishStart(t, st, project, actor, rid, Failure{Message: "node failed"}, FinishStartFinished)

	return actor
}

func setGoneAgo(t *testing.T, pool *pgxpool.Pool, actor uuid.UUID, ago time.Duration) {
	t.Helper()

	tag, err := pool.Exec(t.Context(),
		`UPDATE public.scheduling_actor_run SET gone_at = now() - $2::interval WHERE actor_id = $1 AND state = 'gone'`, actor, interval(ago))
	require.NoError(t, err)
	require.EqualValues(t, 1, tag.RowsAffected())
}

func runExists(t *testing.T, pool *pgxpool.Pool, actor uuid.UUID) bool {
	t.Helper()

	var exists bool
	require.NoError(t, pool.QueryRow(t.Context(),
		`SELECT EXISTS (SELECT 1 FROM public.scheduling_actor_run WHERE actor_id = $1)`, actor).Scan(&exists))

	return exists
}
