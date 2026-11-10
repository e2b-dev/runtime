package store

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	queries "github.com/e2b-dev/infra/packages/scheduling-api/internal/db/queries/gen"
)

func TestReserveStartDeadline(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	st, pool := newStore(t)
	project, actor, rid := uuid.New(), uuid.New(), uuid.New()

	reserved := reserve(t, st, project, actor, rid, strict, ReserveReserved)
	require.True(t, reservedAt(t, pool, actor).Add(90*time.Second).Equal(reserved.StartDeadline))

	_, err := pool.Exec(ctx, `UPDATE public.scheduling_actor_run SET reserved_at = reserved_at - interval '10 seconds' WHERE actor_id = $1`, actor)
	require.NoError(t, err)
	want := reserved.StartDeadline.Add(-10 * time.Second)

	repeated := reserve(t, st, project, actor, rid, strict, ReserveReserved)
	require.True(t, want.Equal(repeated.StartDeadline), "a repeated Reserve keeps the deadline of the first")
	pending := reserve(t, st, project, actor, uuid.New(), strict, ReserveAlreadyPending)
	require.True(t, want.Equal(pending.StartDeadline), "the deadline of the pending start")

	put(t, st, rid, newActor(project, actor), PutStored)
	running := reserve(t, st, project, actor, uuid.New(), strict, ReserveAlreadyRunning)
	require.True(t, running.StartDeadline.IsZero())
	limited := reserve(t, st, project, uuid.New(), uuid.New(), 1, ReserveLimitExceeded)
	require.True(t, limited.StartDeadline.IsZero())
}

func TestExpireStartsSelectsOnlyExpiredStarts(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	st, pool := newStore(t)
	project := uuid.New()

	fresh, freshRID := uuid.New(), uuid.New()
	_, err := st.Reserve(ctx, Reservation{
		ProjectID: project, ActorID: fresh, ReservationID: freshRID, Limit: strict, EndTime: time.Now().Add(-time.Hour),
	})
	require.NoError(t, err)

	young, youngRID := uuid.New(), uuid.New()
	reserve(t, st, project, young, youngRID, strict, ReserveReserved)
	setReservedAgo(t, pool, young, 80*time.Second)

	boundary, boundaryRID := uuid.New(), uuid.New()
	reserve(t, st, project, boundary, boundaryRID, strict, ReserveReserved)
	setReservedAgo(t, pool, boundary, startTimeout)

	running, runningRID := uuid.New(), uuid.New()
	reserve(t, st, project, running, runningRID, strict, ReserveReserved)
	put(t, st, runningRID, newActor(project, running), PutStored)
	setReservedAgo(t, pool, running, time.Hour)

	pausing, pausingRID := uuid.New(), uuid.New()
	reserve(t, st, project, pausing, pausingRID, strict, ReserveReserved)
	put(t, st, pausingRID, newActor(project, pausing), PutStored)
	begun, err := st.BeginTransition(ctx, TransitionRequest{ProjectID: project, ActorID: pausing, TransitionID: uuid.New(), Action: ActionPause})
	require.NoError(t, err)
	require.Equal(t, BeginStarted, begun.Outcome)
	setReservedAgo(t, pool, pausing, time.Hour)

	failed, failedRID := uuid.New(), uuid.New()
	reserve(t, st, project, failed, failedRID, strict, ReserveReserved)
	finishStart(t, st, project, failed, failedRID, Failure{Message: "node failed"}, FinishStartFinished)
	setReservedAgo(t, pool, failed, time.Hour)

	result, err := st.ExpireStarts(ctx, uuid.Nil, 100)
	require.NoError(t, err)
	require.Equal(t, ExpireResult{Ended: 1}, result)

	require.Equal(t, run{state: StateGone, gone: true, failure: startTimeoutFailure}, runOf(t, pool, boundary))
	require.Equal(t, StateStarting, runOf(t, pool, fresh).state, "end_time does not end a start")
	require.Equal(t, StateStarting, runOf(t, pool, young).state)
	require.Equal(t, StateRunning, runOf(t, pool, running).state)
	require.Equal(t, StatePausing, runOf(t, pool, pausing).state)
	require.Equal(t, run{state: StateGone, gone: true, failure: Failure{Message: "node failed"}}, runOf(t, pool, failed))
	require.Equal(t, map[int16]int32{0: 4}, liveCounts(t, pool, project))
	requireCountsMatch(t, pool)
}

func TestExpireStart(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	st, pool := newStore(t)
	project, actor, rid := uuid.New(), uuid.New(), onSlot(0x37)

	reserve(t, st, project, actor, rid, medium, ReserveReserved)
	setReservedAgo(t, pool, actor, startTimeout+time.Second)
	require.Equal(t, map[int16]int32{7: 1}, liveCounts(t, pool, project))

	require.True(t, expireStart(t, st, project, actor, rid))
	require.Equal(t, map[int16]int32{7: 0}, liveCounts(t, pool, project))
	require.False(t, expireStart(t, st, project, actor, rid))
	require.Equal(t, map[int16]int32{7: 0}, liveCounts(t, pool, project), "decremented once")
	require.Equal(t, run{state: StateGone, gone: true, failure: startTimeoutFailure}, runOf(t, pool, actor))

	finishStart(t, st, project, actor, rid, Failure{Message: "node failed"}, FinishStartFinished)
	require.Equal(t, startTimeoutFailure, runOf(t, pool, actor).failure, "the first ending keeps its failure")
	result := reserve(t, st, project, actor, rid, medium, ReserveFinished)
	require.Equal(t, &startTimeoutFailure, result.Failure)
	put(t, st, rid, newActor(project, actor), PutNotFound)
	got, err := st.Get(ctx, project, actor)
	require.NoError(t, err)
	require.Nil(t, got)
	require.False(t, actorExists(t, pool, actor))
	require.Equal(t, map[int16]int32{7: 0}, liveCounts(t, pool, project))

	next := onSlot(0x31)
	reserve(t, st, project, actor, next, medium, ReserveReserved)
	put(t, st, next, newActor(project, actor), PutStored)
	require.Equal(t, map[int16]int32{7: 0, 1: 1}, liveCounts(t, pool, project))
	requireCountsMatch(t, pool)
}

func TestExpireStartsPages(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	st, pool := newStore(t)

	projects := []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}
	actors := []uuid.UUID{actorWithPrefix(1), actorWithPrefix(2), actorWithPrefix(3)}
	for i, project := range projects {
		reserve(t, st, project, actors[i], uuid.New(), strict, ReserveReserved)
		setReservedAgo(t, pool, actors[i], time.Hour)
	}

	result, err := st.ExpireStarts(ctx, uuid.Nil, 2)
	require.NoError(t, err)
	require.Equal(t, ExpireResult{Ended: 2, Next: actors[1]}, result)
	result, err = st.ExpireStarts(ctx, result.Next, 2)
	require.NoError(t, err)
	require.Equal(t, ExpireResult{Ended: 1}, result)
	result, err = st.ExpireStarts(ctx, uuid.Nil, 2)
	require.NoError(t, err)
	require.Equal(t, ExpireResult{}, result)
	for _, project := range projects {
		require.Equal(t, map[int16]int32{0: 0}, liveCounts(t, pool, project))
	}
	requireCountsMatch(t, pool)

	for _, maxStarts := range []int32{0, -1} {
		_, err := st.ExpireStarts(ctx, uuid.Nil, maxStarts)
		require.ErrorContains(t, err, "must be positive")
	}
}

// Starts that fail to end on every attempt take a place in each page, so a
// caller that follows Next still reaches the starts after them.
func TestExpireStartsPagesPastFailures(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	st, pool := newStore(t)

	broken := uuid.New()
	brokenActors := []uuid.UUID{actorWithPrefix(1), actorWithPrefix(2), actorWithPrefix(3)}
	for _, actor := range brokenActors {
		reserve(t, st, broken, actor, uuid.New(), strict, ReserveReserved)
		setReservedAgo(t, pool, actor, time.Hour)
	}
	_, err := pool.Exec(ctx, `DELETE FROM public.scheduling_project_live_count WHERE project_id = $1`, broken)
	require.NoError(t, err)
	project, last := uuid.New(), actorWithPrefix(0xff)
	reserve(t, st, project, last, uuid.New(), strict, ReserveReserved)
	setReservedAgo(t, pool, last, time.Hour)

	result, err := st.ExpireStarts(ctx, uuid.Nil, 2)
	require.ErrorContains(t, err, "2 of 2 failed")
	require.Equal(t, ExpireResult{Failed: 2, Next: brokenActors[1]}, result)
	result, err = st.ExpireStarts(ctx, result.Next, 2)
	require.ErrorContains(t, err, "1 of 2 failed")
	require.Equal(t, ExpireResult{Ended: 1, Failed: 1, Next: last}, result)
	require.Equal(t, StateGone, runOf(t, pool, last).state)
	require.Equal(t, map[int16]int32{0: 0}, liveCounts(t, pool, project))
}

// When ctx ends while a start is ending, Next stays before that start and the
// start is not a failure, so the next call tries it again.
func TestExpireStartsStopsWhenCtxEnds(t *testing.T) {
	t.Parallel()
	st, pool := newStore(t)
	actors := []uuid.UUID{actorWithPrefix(1), actorWithPrefix(2), actorWithPrefix(3)}
	for _, actor := range actors {
		reserve(t, st, uuid.New(), actor, uuid.New(), strict, ReserveReserved)
		setReservedAgo(t, pool, actor, time.Hour)
	}

	tx, err := pool.Begin(t.Context())
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(context.WithoutCancel(t.Context())) }()
	_, err = tx.Exec(t.Context(), `SELECT FROM public.scheduling_actor_run WHERE actor_id = $1 FOR UPDATE`, actors[1])
	require.NoError(t, err)

	type expired struct {
		result ExpireResult
		err    error
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan expired, 1)
	go func() {
		result, err := st.ExpireStarts(ctx, uuid.Nil, 10)
		done <- expired{result, err}
	}()
	requireLockWaiters(t, pool, 1)
	cancel()
	got := <-done
	require.ErrorIs(t, got.err, context.Canceled)
	require.Equal(t, ExpireResult{Ended: 1, Next: actors[0]}, got.result)
	require.NoError(t, tx.Rollback(t.Context()))

	result, err := st.ExpireStarts(t.Context(), got.result.Next, 10)
	require.NoError(t, err)
	require.Equal(t, ExpireResult{Ended: 2}, result)
	requireCountsMatch(t, pool)
}

// expireStart gets the project, actor and reservation from a list that can be
// stale by the time it runs.
func TestExpireStartStale(t *testing.T) {
	t.Parallel()

	t.Run("new reservation", func(t *testing.T) {
		t.Parallel()
		st, pool := newStore(t)
		project, actor, rid := uuid.New(), uuid.New(), uuid.New()
		reserve(t, st, project, actor, rid, strict, ReserveReserved)
		setReservedAgo(t, pool, actor, time.Hour)
		listed := listExpiredStarts(t, st)
		require.Equal(t, []uuid.UUID{rid}, listed)

		finishStart(t, st, project, actor, rid, Failure{Message: "node failed"}, FinishStartFinished)
		next := uuid.New()
		reserve(t, st, project, actor, next, strict, ReserveReserved)
		setReservedAgo(t, pool, actor, time.Hour)

		require.False(t, expireStart(t, st, project, actor, rid))
		require.Equal(t, StateStarting, runOf(t, pool, actor).state)
		require.Equal(t, map[int16]int32{0: 1}, liveCounts(t, pool, project))
		requireCountsMatch(t, pool)
	})
	t.Run("put", func(t *testing.T) {
		t.Parallel()
		st, pool := newStore(t)
		project, actor, rid := uuid.New(), uuid.New(), uuid.New()
		reserve(t, st, project, actor, rid, strict, ReserveReserved)
		setReservedAgo(t, pool, actor, time.Hour)
		require.Equal(t, []uuid.UUID{rid}, listExpiredStarts(t, st))

		record := newActor(project, actor)
		put(t, st, rid, record, PutStored)

		require.False(t, expireStart(t, st, project, actor, rid))
		requireLive(t, st, record)
		require.Equal(t, map[int16]int32{0: 1}, liveCounts(t, pool, project))
		requireCountsMatch(t, pool)
	})
	t.Run("not expired", func(t *testing.T) {
		t.Parallel()
		st, pool := newStore(t)
		project, actor, rid := uuid.New(), uuid.New(), uuid.New()
		reserve(t, st, project, actor, rid, strict, ReserveReserved)
		setReservedAgo(t, pool, actor, 80*time.Second)

		require.False(t, expireStart(t, st, project, actor, rid))
		require.Equal(t, StateStarting, runOf(t, pool, actor).state)
		require.Equal(t, map[int16]int32{0: 1}, liveCounts(t, pool, project))
	})
	t.Run("other project", func(t *testing.T) {
		t.Parallel()
		st, pool := newStore(t)
		owner, intruder, actor, rid := uuid.New(), uuid.New(), uuid.New(), uuid.New()
		reserve(t, st, owner, actor, rid, strict, ReserveReserved)
		reserve(t, st, intruder, uuid.New(), uuid.New(), strict, ReserveReserved)
		setReservedAgo(t, pool, actor, time.Hour)

		require.False(t, expireStart(t, st, intruder, actor, rid))
		require.Equal(t, StateStarting, runOf(t, pool, actor).state)
		require.Equal(t, map[int16]int32{0: 1}, liveCounts(t, pool, owner))
		require.Equal(t, map[int16]int32{0: 1}, liveCounts(t, pool, intruder))
		requireCountsMatch(t, pool)
	})
}

// Each case holds the row lock of the run, queues the first call and then
// the second behind it, and releases the lock. PostgreSQL grants a row lock
// to its waiters in queue order, so the first call commits before the second
// reads the row again.
func TestExpireStartRace(t *testing.T) {
	t.Parallel()

	nodeFailure := Failure{Message: "node failed", Code: 13}

	t.Run("expire, then put", func(t *testing.T) {
		t.Parallel()
		r := newRace(t)
		record := newActor(r.project, r.actor)

		inOrder(t, r.pool, r.actor, r.expire, r.put(record))

		require.True(t, r.expired)
		require.Equal(t, PutNotFound, r.putOutcome)
		require.Equal(t, run{state: StateGone, gone: true, failure: startTimeoutFailure}, runOf(t, r.pool, r.actor))
		require.False(t, actorExists(t, r.pool, r.actor))
		require.Equal(t, map[int16]int32{0: 0}, liveCounts(t, r.pool, r.project))
		requireCountsMatch(t, r.pool)
	})
	t.Run("put, then expire", func(t *testing.T) {
		t.Parallel()
		r := newRace(t)
		record := newActor(r.project, r.actor)

		inOrder(t, r.pool, r.actor, r.put(record), r.expire)

		require.Equal(t, PutStored, r.putOutcome)
		require.False(t, r.expired)
		requireLive(t, r.st, record)
		require.Equal(t, map[int16]int32{0: 1}, liveCounts(t, r.pool, r.project))
		requireCountsMatch(t, r.pool)
	})
	t.Run("expire, then finish start", func(t *testing.T) {
		t.Parallel()
		r := newRace(t)

		inOrder(t, r.pool, r.actor, r.expire, r.finishStart(nodeFailure))

		require.True(t, r.expired)
		require.Equal(t, FinishStartFinished, r.finishOutcome)
		require.Equal(t, startTimeoutFailure, runOf(t, r.pool, r.actor).failure)
		require.Equal(t, map[int16]int32{0: 0}, liveCounts(t, r.pool, r.project))
		requireCountsMatch(t, r.pool)
	})
	t.Run("finish start, then expire", func(t *testing.T) {
		t.Parallel()
		r := newRace(t)

		inOrder(t, r.pool, r.actor, r.finishStart(nodeFailure), r.expire)

		require.Equal(t, FinishStartFinished, r.finishOutcome)
		require.False(t, r.expired)
		require.Equal(t, nodeFailure, runOf(t, r.pool, r.actor).failure)
		require.Equal(t, map[int16]int32{0: 0}, liveCounts(t, r.pool, r.project))
		requireCountsMatch(t, r.pool)
	})
}

type race struct {
	st                  *Store
	pool                *pgxpool.Pool
	project, actor, rid uuid.UUID

	expired       bool
	putOutcome    PutOutcome
	finishOutcome FinishStartOutcome
}

func newRace(t *testing.T) *race {
	t.Helper()

	r := &race{}
	r.st, r.pool, r.project, r.actor, r.rid = expiredStart(t)

	return r
}

func (r *race) expire(ctx context.Context) error {
	var err error
	r.expired, err = r.st.expireStart(ctx, r.project, r.actor, r.rid)

	return err
}

func (r *race) put(record Actor) func(context.Context) error {
	return func(ctx context.Context) error {
		var err error
		r.putOutcome, err = r.st.Put(ctx, r.rid, record)

		return err
	}
}

func (r *race) finishStart(failure Failure) func(context.Context) error {
	return func(ctx context.Context) error {
		var err error
		r.finishOutcome, err = r.st.FinishStart(ctx, r.project, r.actor, r.rid, failure)

		return err
	}
}

func TestExpireStartRollsBack(t *testing.T) {
	t.Parallel()

	t.Run("counter row missing", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		st, pool, project, actor, _ := expiredStart(t)
		other, otherActor := uuid.New(), uuid.New()
		reserve(t, st, other, otherActor, uuid.New(), strict, ReserveReserved)
		setReservedAgo(t, pool, otherActor, time.Hour)
		_, err := pool.Exec(ctx, `DELETE FROM public.scheduling_project_live_count WHERE project_id = $1`, project)
		require.NoError(t, err)

		result, err := st.ExpireStarts(ctx, uuid.Nil, 10)
		require.ErrorContains(t, err, "the slot has no row")
		require.Equal(t, 1, result.Ended, "the other start still ends")
		require.Equal(t, 1, result.Failed)
		require.Equal(t, run{state: StateStarting}, runOf(t, pool, actor))
		require.Equal(t, StateGone, runOf(t, pool, otherActor).state)
		require.Equal(t, map[int16]int32{0: 0}, liveCounts(t, pool, other))
	})
	t.Run("counter at zero", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		st, pool, project, actor, rid := expiredStart(t)
		_, err := pool.Exec(ctx, `UPDATE public.scheduling_project_live_count SET live = 0 WHERE project_id = $1`, project)
		require.NoError(t, err)

		_, err = st.expireStart(ctx, project, actor, rid)
		require.ErrorContains(t, err, "scheduling_project_live_count_live_check")
		require.Equal(t, run{state: StateStarting}, runOf(t, pool, actor))
		require.Equal(t, map[int16]int32{0: 0}, liveCounts(t, pool, project))
	})
}

func actorWithPrefix(b byte) uuid.UUID {
	actor := uuid.New()
	actor[0] = b

	return actor
}

func expiredStart(t *testing.T) (*Store, *pgxpool.Pool, uuid.UUID, uuid.UUID, uuid.UUID) {
	t.Helper()

	st, pool := newStore(t)
	project, actor, rid := uuid.New(), uuid.New(), uuid.New()
	reserve(t, st, project, actor, rid, strict, ReserveReserved)
	setReservedAgo(t, pool, actor, time.Hour)

	return st, pool, project, actor, rid
}

// inOrder runs first and then second while it holds the row lock of the run
// of actor, and returns when both have returned. second starts only when
// first waits for the lock.
func inOrder(t *testing.T, pool *pgxpool.Pool, actor uuid.UUID, first, second func(context.Context) error) {
	t.Helper()
	ctx := t.Context()

	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	tag, err := tx.Exec(ctx, `SELECT FROM public.scheduling_actor_run WHERE actor_id = $1 FOR UPDATE`, actor)
	require.NoError(t, err)
	require.EqualValues(t, 1, tag.RowsAffected())

	errs := make(chan error, 2)
	for i, call := range []func(context.Context) error{first, second} {
		go func() { errs <- call(ctx) }()
		requireLockWaiters(t, pool, i+1)
	}
	require.NoError(t, tx.Commit(ctx))
	for range 2 {
		require.NoError(t, <-errs)
	}
}

func requireLockWaiters(t *testing.T, pool *pgxpool.Pool, n int) {
	t.Helper()

	require.Eventually(t, func() bool {
		var waiting int
		err := pool.QueryRow(t.Context(), `
SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock'`).Scan(&waiting)

		return err == nil && waiting == n
	}, 10*time.Second, 5*time.Millisecond)
}

func expireStart(t *testing.T, st *Store, project, actor, rid uuid.UUID) bool {
	t.Helper()

	expired, err := st.expireStart(t.Context(), project, actor, rid)
	require.NoError(t, err)

	return expired
}

func listExpiredStarts(t *testing.T, st *Store) []uuid.UUID {
	t.Helper()

	rows, err := st.reads.ListExpiredStarts(t.Context(), queries.ListExpiredStartsParams{StartTimeout: interval(startTimeout), MaxStarts: 10})
	require.NoError(t, err)
	var rids []uuid.UUID
	for _, row := range rows {
		rids = append(rids, row.ReservationID)
	}

	return rids
}

func finishStart(t *testing.T, st *Store, project, actor, rid uuid.UUID, failure Failure, want FinishStartOutcome) {
	t.Helper()

	outcome, err := st.FinishStart(t.Context(), project, actor, rid, failure)
	require.NoError(t, err)
	require.Equal(t, want, outcome)
}

func setReservedAgo(t *testing.T, pool *pgxpool.Pool, actor uuid.UUID, ago time.Duration) {
	t.Helper()

	tag, err := pool.Exec(t.Context(),
		`UPDATE public.scheduling_actor_run SET reserved_at = now() - $2::interval WHERE actor_id = $1`, actor, interval(ago))
	require.NoError(t, err)
	require.EqualValues(t, 1, tag.RowsAffected())
}

func reservedAt(t *testing.T, pool *pgxpool.Pool, actor uuid.UUID) time.Time {
	t.Helper()

	var at time.Time
	require.NoError(t, pool.QueryRow(t.Context(),
		`SELECT reserved_at FROM public.scheduling_actor_run WHERE actor_id = $1`, actor).Scan(&at))

	return at
}
