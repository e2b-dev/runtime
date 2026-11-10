package store

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/scheduling-api/internal/db/dbtest"
	queries "github.com/e2b-dev/infra/packages/scheduling-api/internal/db/queries/gen"
)

const (
	strict = 1_000
	medium = 50_000
	large  = 200_000
)

var endTime = time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)

func TestCounterSlot(t *testing.T) {
	t.Parallel()

	reservationID := onSlot(0x37)
	for limit, want := range map[int64]int16{
		1:               0,
		strictLimit:     0,
		strictLimit + 1: 0x37 % mediumLimitSlots,
		mediumLimit:     0x37 % mediumLimitSlots,
		mediumLimit + 1: 0x37,
	} {
		assert.Equal(t, want, counterSlot(limit, reservationID), limit)
	}
	for b := range 256 {
		assert.Less(t, counterSlot(large, onSlot(byte(b))), int16(largeLimitSlots))
	}
}

func TestStartLifecycle(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	st, pool := newStore(t)
	project, actor, rid := uuid.New(), uuid.New(), uuid.New()

	reserve(t, st, project, actor, rid, strict, ReserveReserved)
	got, err := st.Get(ctx, project, actor)
	require.NoError(t, err)
	require.Nil(t, got, "a starting actor is not visible")
	require.Equal(t, map[int16]int32{0: 1}, liveCounts(t, pool, project))

	record := newActor(project, actor)
	put(t, st, rid, record, PutStored)
	requireLive(t, st, record)

	retry := record
	retry.NodeID = "another-node"
	retry.Record = []byte(`{"retry":true}`)
	put(t, st, rid, retry, PutStored)
	requireLive(t, st, record)

	result := reserve(t, st, project, actor, uuid.New(), strict, ReserveAlreadyRunning)
	require.NotNil(t, result.Actor)
	require.Equal(t, record.ExecutionID, result.Actor.ExecutionID)

	require.Equal(t, map[int16]int32{0: 1}, liveCounts(t, pool, project))
	requireCountsMatch(t, pool)
}

func TestPutClampsDeadline(t *testing.T) {
	t.Parallel()
	for _, supplied := range []bool{false, true} {
		t.Run(map[bool]string{false: "reserved", true: "supplied"}[supplied], func(t *testing.T) {
			t.Parallel()
			st, pool := newStore(t)
			project, actor, rid := uuid.New(), uuid.New(), uuid.New()
			reserve(t, st, project, actor, rid, strict, ReserveReserved)
			record := newActor(project, actor)
			record.MaxInstanceLength = time.Hour
			if supplied {
				deadline := record.StartTime.Add(3 * time.Hour)
				record.EndTime = &deadline
			}
			put(t, st, rid, record, PutStored)
			got, err := st.Get(t.Context(), project, actor)
			require.NoError(t, err)
			require.True(t, record.StartTime.Add(time.Hour).Equal(got.EndTime))
			deadline := record.StartTime.Add(30 * time.Minute)
			_, err = st.UpdateDeadline(t.Context(), project, actor, record.ExecutionID, deadline, got.EndTime)
			require.NoError(t, err)
			put(t, st, rid, record, PutStored)
			got, err = st.Get(t.Context(), project, actor)
			require.NoError(t, err)
			require.True(t, deadline.Equal(got.EndTime))
			require.Equal(t, map[int16]int32{0: 1}, liveCounts(t, pool, project))
		})
	}
}

func TestReserveRepeat(t *testing.T) {
	t.Parallel()
	st, pool := newStore(t)
	project, actor, rid := uuid.New(), uuid.New(), uuid.New()

	reserve(t, st, project, actor, rid, strict, ReserveReserved)
	reserve(t, st, project, actor, rid, strict, ReserveReserved)
	reserve(t, st, project, actor, uuid.New(), strict, ReserveAlreadyPending)

	require.Equal(t, map[int16]int32{0: 1}, liveCounts(t, pool, project))
	requireCountsMatch(t, pool)
}

func TestReserveConcurrentRetryAtLimit(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	st, pool := newStore(t)
	project, actor, rid := uuid.New(), uuid.New(), uuid.New()

	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	first, err := queries.New(tx).Reserve(ctx, queries.ReserveParams{
		ActorID: actor, ProjectID: project, ReservationID: rid,
		CounterSlot: 0, LiveLimit: 1, EndTime: endTime,
	})
	require.NoError(t, err)
	require.True(t, first.Inserted)
	require.True(t, first.Bumped)

	var retry ReserveResult
	var retryErr error
	done := make(chan struct{})
	go func() {
		defer close(done)
		retry, retryErr = st.Reserve(ctx, Reservation{
			ActorID: actor, ProjectID: project, ReservationID: rid, Limit: 1, EndTime: endTime,
		})
	}()
	require.Eventually(t, func() bool {
		var waiting bool
		err := pool.QueryRow(ctx, `
SELECT EXISTS (
    SELECT 1 FROM pg_stat_activity
    WHERE datname = current_database() AND wait_event_type = 'Lock'
      AND $1 = ANY(pg_blocking_pids(pid))
)`, tx.Conn().PgConn().PID()).Scan(&waiting)

		return err == nil && waiting
	}, 10*time.Second, 5*time.Millisecond)
	require.NoError(t, tx.Commit(ctx))
	<-done
	require.NoError(t, retryErr)
	require.Equal(t, ReserveReserved, retry.Outcome)
	require.Equal(t, map[int16]int32{0: 1}, liveCounts(t, pool, project))
	requireCountsMatch(t, pool)
}

func TestReserveLimit(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	st, pool := newStore(t)
	project, other := uuid.New(), uuid.New()
	first, firstRID := uuid.New(), uuid.New()

	reserve(t, st, project, first, firstRID, 2, ReserveReserved)
	reserve(t, st, project, uuid.New(), uuid.New(), 2, ReserveReserved)
	third, thirdRID := uuid.New(), uuid.New()
	reserve(t, st, project, third, thirdRID, 2, ReserveLimitExceeded)
	reserve(t, st, other, uuid.New(), uuid.New(), 2, ReserveReserved)

	outcome, err := st.FinishStart(ctx, project, first, firstRID, Failure{Message: "node failed"})
	require.NoError(t, err)
	require.Equal(t, FinishStartFinished, outcome)
	reserve(t, st, project, third, thirdRID, 2, ReserveReserved)
	reserve(t, st, project, first, firstRID, 2, ReserveFinished)

	require.Equal(t, map[int16]int32{0: 2}, liveCounts(t, pool, project))
	require.Equal(t, map[int16]int32{0: 1}, liveCounts(t, pool, other))
	requireCountsMatch(t, pool)
}

func TestReserveConcurrentStrictLimit(t *testing.T) {
	t.Parallel()
	st, pool := newStore(t)
	project := uuid.New()

	const limit, callers = 5, 40
	outcomes := concurrently(t, callers, func(int) (ReserveResult, error) {
		return st.Reserve(t.Context(), Reservation{
			ProjectID: project, ActorID: uuid.New(), ReservationID: uuid.New(), Limit: limit, EndTime: endTime,
		})
	})

	require.Equal(t, map[ReserveOutcome]int{ReserveReserved: limit, ReserveLimitExceeded: callers - limit}, outcomes)
	require.Equal(t, map[int16]int32{0: limit}, liveCounts(t, pool, project))
	requireCountsMatch(t, pool)
}

// The callers below all take their snapshot while the counter row is locked,
// so each sees the actor as free and the slot below the limit, and then
// queues on the lock.
func TestReserveBlockedOnCounter(t *testing.T) {
	t.Parallel()

	t.Run("limit", func(t *testing.T) {
		t.Parallel()
		st, pool := newStore(t)
		project := uuid.New()
		reserve(t, st, project, uuid.New(), uuid.New(), 4, ReserveReserved)

		outcomes, errs := blockedOnCounter(t, pool, project, 0, blockedCallers, func() (ReserveResult, error) {
			return st.Reserve(t.Context(), Reservation{
				ProjectID: project, ActorID: uuid.New(), ReservationID: uuid.New(), Limit: 4, EndTime: endTime,
			})
		}, nil)
		require.Empty(t, errs)
		require.Equal(t, map[ReserveOutcome]int{ReserveReserved: 3, ReserveLimitExceeded: blockedCallers - 3}, outcomes)
		require.Equal(t, map[int16]int32{0: 4}, liveCounts(t, pool, project))
		requireCountsMatch(t, pool)
	})
	t.Run("same actor", func(t *testing.T) {
		t.Parallel()
		st, pool := newStore(t)
		project, actor := uuid.New(), uuid.New()
		reserve(t, st, project, uuid.New(), uuid.New(), strict, ReserveReserved)

		outcomes, errs := blockedOnCounter(t, pool, project, 0, blockedCallers, func() (ReserveResult, error) {
			return st.Reserve(t.Context(), Reservation{
				ProjectID: project, ActorID: actor, ReservationID: uuid.New(), Limit: strict, EndTime: endTime,
			})
		}, nil)
		require.Empty(t, errs)
		require.Equal(t, map[ReserveOutcome]int{ReserveReserved: 1, ReserveAlreadyPending: blockedCallers - 1}, outcomes)
		require.Equal(t, map[int16]int32{0: 2}, liveCounts(t, pool, project))
		requireCountsMatch(t, pool)
	})
	t.Run("same reservation", func(t *testing.T) {
		t.Parallel()
		st, pool := newStore(t)
		project, actor, rid := uuid.New(), uuid.New(), onSlot(9)
		reserve(t, st, project, uuid.New(), onSlot(9), large, ReserveReserved)

		outcomes, errs := blockedOnCounter(t, pool, project, 9, blockedCallers, func() (ReserveResult, error) {
			return st.Reserve(t.Context(), Reservation{
				ProjectID: project, ActorID: actor, ReservationID: rid, Limit: large, EndTime: endTime,
			})
		}, nil)
		require.Empty(t, errs)
		require.Equal(t, map[ReserveOutcome]int{ReserveReserved: blockedCallers}, outcomes)
		require.Equal(t, map[int16]int32{9: 2}, liveCounts(t, pool, project))
		requireCountsMatch(t, pool)
	})
	t.Run("start of the same reservation ends", func(t *testing.T) {
		t.Parallel()
		st, pool := newStore(t)
		project, actor, rid := uuid.New(), uuid.New(), uuid.New()
		reserve(t, st, project, uuid.New(), uuid.New(), strict, ReserveReserved)

		outcomes, errs := blockedOnCounter(t, pool, project, 0, 1, func() (ReserveResult, error) {
			return st.Reserve(t.Context(), Reservation{
				ProjectID: project, ActorID: actor, ReservationID: rid, Limit: strict, EndTime: endTime,
			})
		}, func() {
			_, err := pool.Exec(t.Context(), `
INSERT INTO public.scheduling_actor_run (actor_id, project_id, state, end_time, reservation_id, reserved_at,
                                         failure_message, failure_code, gone_at, counter_slot)
VALUES ($1, $2, 'gone', now(), $3, now(), 'node failed', 13, now(), 0)`, actor, project, rid)
			require.NoError(t, err)
		})
		require.Empty(t, errs)
		require.Equal(t, map[ReserveOutcome]int{ReserveFinished: 1}, outcomes)
		require.Equal(t, StateGone, runOf(t, pool, actor).state)
		require.Equal(t, map[int16]int32{0: 1}, liveCounts(t, pool, project))
		requireCountsMatch(t, pool)
	})
	t.Run("tombstone of another project", func(t *testing.T) {
		t.Parallel()
		st, pool := newStore(t)
		owner, intruder, actor := uuid.New(), uuid.New(), uuid.New()
		reserve(t, st, intruder, uuid.New(), uuid.New(), strict, ReserveReserved)

		outcomes, errs := blockedOnCounter(t, pool, intruder, 0, 1, func() (ReserveResult, error) {
			return st.Reserve(t.Context(), Reservation{
				ProjectID: intruder, ActorID: actor, ReservationID: uuid.New(), Limit: strict, EndTime: endTime,
			})
		}, func() {
			rid := uuid.New()
			_, err := pool.Exec(t.Context(), `INSERT INTO public.scheduling_actor_run
			(actor_id,project_id,state,end_time,reservation_id,reserved_at,failure_message,failure_code,gone_at,counter_slot)
			VALUES ($1,$2,'gone',now(),$3,now(),'node failed',13,now(),0)`, actor, owner, rid)
			require.NoError(t, err)
		})
		require.Empty(t, outcomes)
		require.Len(t, errs, 1)
		require.ErrorIs(t, errs[0], ErrOtherProject)
		require.Equal(t, map[int16]int32{0: 1}, liveCounts(t, pool, intruder))
		requireCountsMatch(t, pool)
	})
}

func TestProjectIsolation(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	st, pool := newStore(t)
	owner, intruder := uuid.New(), uuid.New()
	actor, rid := uuid.New(), uuid.New()

	reserve(t, st, owner, actor, rid, strict, ReserveReserved)

	_, err := st.Reserve(ctx, Reservation{ProjectID: intruder, ActorID: actor, ReservationID: rid, Limit: strict, EndTime: endTime})
	require.ErrorIs(t, err, ErrOtherProject)
	put(t, st, rid, newActor(intruder, actor), PutNotFound)
	outcome, err := st.FinishStart(ctx, intruder, actor, rid, Failure{Message: "intruder"})
	require.NoError(t, err)
	require.Equal(t, FinishStartNotFound, outcome)
	require.Equal(t, StateStarting, runOf(t, pool, actor).state)

	record := newActor(owner, actor)
	put(t, st, rid, record, PutStored)
	got, err := st.Get(ctx, intruder, actor)
	require.NoError(t, err)
	require.Nil(t, got)
	_, err = st.Reserve(ctx, Reservation{ProjectID: intruder, ActorID: actor, ReservationID: uuid.New(), Limit: strict, EndTime: endTime})
	require.ErrorIs(t, err, ErrOtherProject)
	requireLive(t, st, record)

	failed, failedRID := uuid.New(), uuid.New()
	reserve(t, st, owner, failed, failedRID, strict, ReserveReserved)
	outcome, err = st.FinishStart(ctx, owner, failed, failedRID, Failure{Message: "node failed"})
	require.NoError(t, err)
	require.Equal(t, FinishStartFinished, outcome)
	_, err = st.Reserve(ctx, Reservation{ProjectID: intruder, ActorID: failed, ReservationID: uuid.New(), Limit: strict, EndTime: endTime})
	require.ErrorIs(t, err, ErrOtherProject, "a tombstone stays with its project")
	reserve(t, st, intruder, uuid.New(), uuid.New(), 1, ReserveReserved)
	_, err = st.Reserve(ctx, Reservation{ProjectID: intruder, ActorID: failed, ReservationID: uuid.New(), Limit: 1, EndTime: endTime})
	require.ErrorIs(t, err, ErrOtherProject, "at its limit too")

	require.Equal(t, map[int16]int32{0: 1}, liveCounts(t, pool, owner))
	require.Equal(t, map[int16]int32{0: 1}, liveCounts(t, pool, intruder))
	requireCountsMatch(t, pool)
}

func TestPutWrongReservation(t *testing.T) {
	t.Parallel()
	st, pool := newStore(t)
	project, actor, rid := uuid.New(), uuid.New(), uuid.New()

	put(t, st, rid, newActor(project, actor), PutNotFound)

	reserve(t, st, project, actor, rid, strict, ReserveReserved)
	put(t, st, uuid.New(), newActor(project, actor), PutNotFound)
	require.Equal(t, StateStarting, runOf(t, pool, actor).state)
	require.False(t, actorExists(t, pool, actor))
	requireCountsMatch(t, pool)
}

func TestFinishStart(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	st, pool := newStore(t)
	project, actor := uuid.New(), uuid.New()
	rid := onSlot(0x37)

	reserve(t, st, project, actor, rid, medium, ReserveReserved)
	require.Equal(t, map[int16]int32{7: 1}, liveCounts(t, pool, project))

	failure := Failure{Message: "node failed", Code: 13}
	for range 2 {
		outcome, err := st.FinishStart(ctx, project, actor, rid, failure)
		require.NoError(t, err)
		require.Equal(t, FinishStartFinished, outcome)
		require.Equal(t, map[int16]int32{7: 0}, liveCounts(t, pool, project), "decremented once")
	}
	run := runOf(t, pool, actor)
	require.Equal(t, StateGone, run.state)
	require.True(t, run.gone)
	require.Equal(t, failure, run.failure)

	put(t, st, rid, newActor(project, actor), PutNotFound)
	got, err := st.Get(ctx, project, actor)
	require.NoError(t, err)
	require.Nil(t, got)

	retryRID := onSlot(0x3f)
	reserve(t, st, project, actor, retryRID, large, ReserveReserved)
	require.Equal(t, map[int16]int32{7: 0, 0x3f: 1}, liveCounts(t, pool, project))
	run = runOf(t, pool, actor)
	require.Equal(t, StateStarting, run.state)
	require.False(t, run.gone)
	require.Equal(t, Failure{}, run.failure, "a new reservation clears the old failure")

	outcome, err := st.FinishStart(ctx, project, actor, rid, failure)
	require.NoError(t, err)
	require.Equal(t, FinishStartNotFound, outcome, "a late call for the old reservation")
	require.Equal(t, StateStarting, runOf(t, pool, actor).state)
	requireCountsMatch(t, pool)
}

func TestReserveAfterFinishedStart(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	st, pool := newStore(t)
	project, actor, rid := uuid.New(), uuid.New(), onSlot(0x37)
	failure := Failure{Message: "node failed", Code: 13}

	reserve(t, st, project, actor, rid, medium, ReserveReserved)
	outcome, err := st.FinishStart(ctx, project, actor, rid, failure)
	require.NoError(t, err)
	require.Equal(t, FinishStartFinished, outcome)

	for range 2 {
		result := reserve(t, st, project, actor, rid, medium, ReserveFinished)
		require.Equal(t, &failure, result.Failure)
	}
	put(t, st, rid, newActor(project, actor), PutNotFound)
	run := runOf(t, pool, actor)
	require.Equal(t, StateGone, run.state)
	require.Equal(t, failure, run.failure)
	require.Equal(t, map[int16]int32{7: 0}, liveCounts(t, pool, project))

	outcomes := concurrently(t, 8, func(int) (ReserveResult, error) {
		return st.Reserve(t.Context(), Reservation{ProjectID: project, ActorID: actor, ReservationID: rid, Limit: medium, EndTime: endTime})
	})
	require.Equal(t, map[ReserveOutcome]int{ReserveFinished: 8}, outcomes)
	require.Equal(t, map[int16]int32{7: 0}, liveCounts(t, pool, project))

	next := onSlot(0x31)
	reserve(t, st, project, actor, next, medium, ReserveReserved)
	reserve(t, st, project, actor, rid, medium, ReserveAlreadyPending)
	put(t, st, rid, newActor(project, actor), PutNotFound)
	outcome, err = st.FinishStart(ctx, project, actor, rid, failure)
	require.NoError(t, err)
	require.Equal(t, FinishStartNotFound, outcome)
	require.Equal(t, map[int16]int32{7: 0, 1: 1}, liveCounts(t, pool, project))

	record := newActor(project, actor)
	put(t, st, next, record, PutStored)
	requireLive(t, st, record)
	requireCountsMatch(t, pool)
}

func TestFinishStartAfterPut(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	st, pool := newStore(t)
	project, actor, rid := uuid.New(), uuid.New(), onSlot(0x12)

	reserve(t, st, project, actor, rid, large, ReserveReserved)
	record := newActor(project, actor)
	put(t, st, rid, record, PutStored)

	outcome, err := st.FinishStart(ctx, project, actor, rid, Failure{Message: "late failure"})
	require.NoError(t, err)
	require.Equal(t, FinishStartAlreadyStored, outcome)
	requireLive(t, st, record)
	require.Equal(t, map[int16]int32{0x12: 1}, liveCounts(t, pool, project))
	requireCountsMatch(t, pool)
}

func TestSlotsPairAcrossProjectsAndSlots(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	st, pool := newStore(t)
	project := uuid.New()

	type start struct{ actor, rid uuid.UUID }
	var starts []start
	for _, slot := range []byte{3, 63, 3, 0} {
		s := start{actor: uuid.New(), rid: onSlot(slot)}
		reserve(t, st, project, s.actor, s.rid, large, ReserveReserved)
		starts = append(starts, s)
	}
	require.Equal(t, map[int16]int32{0: 1, 3: 2, 63: 1}, liveCounts(t, pool, project))

	put(t, st, starts[0].rid, newActor(project, starts[0].actor), PutStored)
	for _, s := range starts[1:3] {
		outcome, err := st.FinishStart(ctx, project, s.actor, s.rid, Failure{Message: "node failed"})
		require.NoError(t, err)
		require.Equal(t, FinishStartFinished, outcome)
	}
	require.Equal(t, map[int16]int32{0: 1, 3: 1, 63: 0}, liveCounts(t, pool, project))
	requireCountsMatch(t, pool)
}

func TestFailuresRollBack(t *testing.T) {
	t.Parallel()

	t.Run("put", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		st, pool := newStore(t)
		project, actor, rid := uuid.New(), uuid.New(), uuid.New()
		reserve(t, st, project, actor, rid, strict, ReserveReserved)
		_, err := pool.Exec(ctx, `ALTER TABLE public.scheduling_actor ADD CONSTRAINT refuse_all CHECK (false) NOT VALID`)
		require.NoError(t, err)

		_, err = st.Put(ctx, rid, newActor(project, actor))
		require.ErrorContains(t, err, "refuse_all")
		require.Equal(t, StateStarting, runOf(t, pool, actor).state)
		require.False(t, actorExists(t, pool, actor))
	})
	t.Run("finish start", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		st, pool := newStore(t)
		project, actor, rid := uuid.New(), uuid.New(), uuid.New()
		reserve(t, st, project, actor, rid, strict, ReserveReserved)
		_, err := pool.Exec(ctx, `UPDATE public.scheduling_project_live_count SET live = 0 WHERE project_id = $1`, project)
		require.NoError(t, err)

		_, err = st.FinishStart(ctx, project, actor, rid, Failure{Message: "node failed"})
		require.ErrorContains(t, err, "scheduling_project_live_count_live_check")
		run := runOf(t, pool, actor)
		require.Equal(t, StateStarting, run.state)
		require.Equal(t, Failure{}, run.failure)
	})
	t.Run("closed pool", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		st, pool := newStore(t)
		pool.Close()

		_, err := st.Reserve(ctx, Reservation{ProjectID: uuid.New(), ActorID: uuid.New(), ReservationID: uuid.New(), Limit: strict, EndTime: endTime})
		require.Error(t, err)
		_, err = st.Get(ctx, uuid.New(), uuid.New())
		require.Error(t, err)
	})
}

func TestGetTransitionStates(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	st, pool := newStore(t)
	project, actor, rid := uuid.New(), uuid.New(), uuid.New()
	reserve(t, st, project, actor, rid, strict, ReserveReserved)
	put(t, st, rid, newActor(project, actor), PutStored)

	for state, action := range map[State]int{StatePausing: 1, StateKilling: 2, StateSnapshotting: 3} {
		_, err := pool.Exec(ctx, `
UPDATE public.scheduling_actor_run
   SET state = $2, transition_id = gen_random_uuid(), transition_action = $3, transition_at = now()
 WHERE actor_id = $1`, actor, state, action)
		require.NoError(t, err)
		got, err := st.Get(ctx, project, actor)
		require.NoError(t, err)
		require.NotNil(t, got)
		require.Equal(t, state, got.State)
	}
}

func TestEndedAfterPut(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	st, pool := newStore(t)
	project, actor, rid := uuid.New(), uuid.New(), uuid.New()
	reserve(t, st, project, actor, rid, strict, ReserveReserved)
	put(t, st, rid, newActor(project, actor), PutStored)
	_, err := pool.Exec(ctx, `
UPDATE public.scheduling_actor_run
   SET state = 'gone', gone_at = now(), transition_id = gen_random_uuid(), transition_action = 2, transition_at = now()
 WHERE actor_id = $1`, actor)
	require.NoError(t, err)

	got, err := st.Get(ctx, project, actor)
	require.NoError(t, err)
	require.Nil(t, got)
	outcome, err := st.FinishStart(ctx, project, actor, rid, Failure{Message: "late failure"})
	require.NoError(t, err)
	require.Equal(t, FinishStartAlreadyStored, outcome)
	put(t, st, rid, newActor(project, actor), PutNotFound)
	result := reserve(t, st, project, actor, rid, strict, ReserveFinished)
	require.Nil(t, result.Failure, "the start did not fail")
	require.Equal(t, StateGone, runOf(t, pool, actor).state)
	require.Equal(t, map[int16]int32{0: 1}, liveCounts(t, pool, project), "Reserve counted nothing")
}

func TestPutWithoutMetadata(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	st, pool := newStore(t)
	project, actor, rid := uuid.New(), uuid.New(), uuid.New()

	reserve(t, st, project, actor, rid, strict, ReserveReserved)
	record := newActor(project, actor)
	record.Metadata = nil
	put(t, st, rid, record, PutStored)

	var empty bool
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT metadata = '{}'::jsonb FROM public.scheduling_actor WHERE actor_id = $1`, actor).Scan(&empty))
	require.True(t, empty, "nil metadata is stored as an empty object")

	got, err := st.Get(ctx, project, actor)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, map[string]string{}, got.Metadata)
}

func newStore(t *testing.T) (*Store, *pgxpool.Pool) {
	t.Helper()
	pool := dbtest.MigratedPool(t)

	return New(pool, pool), pool
}

func onSlot(b byte) uuid.UUID {
	rid := uuid.New()
	rid[15] = b

	return rid
}

func newActor(project, actor uuid.UUID) Actor {
	return Actor{
		ProjectID:               project,
		ActorID:                 actor,
		ExecutionID:             uuid.NewString(),
		StartTime:               time.Date(2030, 1, 1, 0, 0, 0, 123000, time.UTC),
		MaxInstanceLength:       48*time.Hour + time.Microsecond,
		NodeID:                  "node-1",
		ClusterID:               uuid.New(),
		BaseTemplateID:          "base",
		TemplateID:              "template",
		Metadata:                map[string]string{"owner": "tests"},
		AutoPause:               true,
		AutoPauseFilesystemOnly: false,
		Record:                  []byte(`{"vcpu": 2}`),
	}
}

func reserve(t *testing.T, st *Store, project, actor, rid uuid.UUID, limit int64, want ReserveOutcome) ReserveResult {
	t.Helper()

	result, err := st.Reserve(t.Context(), Reservation{ProjectID: project, ActorID: actor, ReservationID: rid, Limit: limit, EndTime: endTime})
	require.NoError(t, err)
	require.Equal(t, want, result.Outcome)

	return result
}

func put(t *testing.T, st *Store, rid uuid.UUID, a Actor, want PutOutcome) {
	t.Helper()

	outcome, err := st.Put(t.Context(), rid, a)
	require.NoError(t, err)
	require.Equal(t, want, outcome)
}

func requireLive(t *testing.T, st *Store, want Actor) {
	t.Helper()

	got, err := st.Get(t.Context(), want.ProjectID, want.ActorID)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, StateRunning, got.State)
	assert.True(t, endTime.Equal(got.EndTime), "Put keeps the reserved end time")
	assert.True(t, want.StartTime.Equal(got.StartTime))
	got.StartTime = want.StartTime
	assert.JSONEq(t, string(want.Record), string(got.Record))
	got.Record = want.Record
	assert.Equal(t, want, got.Actor)
}

func concurrently(t *testing.T, n int, call func(int) (ReserveResult, error)) map[ReserveOutcome]int {
	t.Helper()

	start := make(chan struct{})
	results := make([]ReserveResult, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			<-start
			results[i], errs[i] = call(i)
		})
	}
	close(start)
	wg.Wait()

	outcomes := map[ReserveOutcome]int{}
	for i := range n {
		require.NoError(t, errs[i])
		outcomes[results[i].Outcome]++
	}

	return outcomes
}

// blockedCallers plus the lock holder and the wait check fit the test pool.
const blockedCallers = 6

// blockedOnCounter runs call callers times while it holds the lock on the
// counter row of project and slot. Once every call waits for the lock, it runs
// whileBlocked and releases the lock.
func blockedOnCounter(
	t *testing.T, pool *pgxpool.Pool, project uuid.UUID, slot int16, callers int,
	call func() (ReserveResult, error), whileBlocked func(),
) (map[ReserveOutcome]int, []error) {
	t.Helper()

	tx, err := pool.Begin(t.Context())
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(context.WithoutCancel(t.Context())) }()
	tag, err := tx.Exec(t.Context(),
		`SELECT FROM public.scheduling_project_live_count WHERE project_id = $1 AND slot = $2 FOR UPDATE`, project, slot)
	require.NoError(t, err)
	require.EqualValues(t, 1, tag.RowsAffected())

	results := make(chan ReserveResult, callers)
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	for range callers {
		wg.Go(func() {
			result, err := call()
			results <- result
			errs <- err
		})
	}
	require.Eventually(t, func() bool {
		var waiting int
		err := pool.QueryRow(t.Context(), `
SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock'`).Scan(&waiting)

		return err == nil && waiting == callers
	}, 10*time.Second, 5*time.Millisecond)
	if whileBlocked != nil {
		whileBlocked()
	}
	require.NoError(t, tx.Commit(t.Context()))
	wg.Wait()
	close(results)
	close(errs)

	var failed []error
	for err := range errs {
		if err != nil {
			failed = append(failed, err)
		}
	}
	outcomes := map[ReserveOutcome]int{}
	for result := range results {
		if result.Outcome != 0 {
			outcomes[result.Outcome]++
		}
	}

	return outcomes, failed
}

func liveCounts(t *testing.T, pool *pgxpool.Pool, project uuid.UUID) map[int16]int32 {
	t.Helper()

	rows, err := pool.Query(t.Context(), `SELECT slot, live FROM public.scheduling_project_live_count WHERE project_id = $1`, project)
	require.NoError(t, err)
	counts := map[int16]int32{}
	var (
		slot int16
		live int32
	)
	_, err = pgx.ForEachRow(rows, []any{&slot, &live}, func() error {
		counts[slot] = live

		return nil
	})
	require.NoError(t, err)

	return counts
}

// requireCountsMatch checks that every slot counts exactly the actors that
// are not gone and name it.
func requireCountsMatch(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()

	var mismatches int
	require.NoError(t, pool.QueryRow(context.WithoutCancel(t.Context()), `
SELECT count(*)
  FROM (SELECT project_id, counter_slot AS slot, count(*)::int AS live
          FROM public.scheduling_actor_run WHERE state <> 'gone'
         GROUP BY project_id, counter_slot) r
  FULL JOIN (SELECT project_id, slot, live FROM public.scheduling_project_live_count WHERE live > 0) c
 USING (project_id, slot)
 WHERE r.live IS DISTINCT FROM c.live`).Scan(&mismatches))
	require.Zero(t, mismatches)
}

type run struct {
	state   State
	gone    bool
	failure Failure
}

func runOf(t *testing.T, pool *pgxpool.Pool, actor uuid.UUID) run {
	t.Helper()

	var (
		r       run
		message *string
		code    *int32
	)
	require.NoError(t, pool.QueryRow(t.Context(), `
SELECT state, gone_at IS NOT NULL, failure_message, failure_code FROM public.scheduling_actor_run WHERE actor_id = $1`,
		actor).Scan(&r.state, &r.gone, &message, &code))
	if message != nil {
		r.failure.Message = *message
	}
	if code != nil {
		r.failure.Code = *code
	}

	return r
}

func actorExists(t *testing.T, pool *pgxpool.Pool, actor uuid.UUID) bool {
	t.Helper()

	var exists bool
	require.NoError(t, pool.QueryRow(t.Context(),
		`SELECT EXISTS (SELECT 1 FROM public.scheduling_actor WHERE actor_id = $1)`, actor).Scan(&exists))

	return exists
}
