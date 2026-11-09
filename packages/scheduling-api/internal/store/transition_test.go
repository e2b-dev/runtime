package store

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/scheduling-api/internal/db/dbtest"
)

func TestTransitionLifecycle(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		action  TransitionAction
		state   State
		failure *Failure
	}{
		"pause":         {ActionPause, StatePausing, nil},
		"kill":          {ActionKill, StateKilling, nil},
		"failed pause":  {ActionPause, StatePausing, &Failure{Message: "snapshot upload failed", Code: 7}},
		"kill, failure": {ActionKill, StateKilling, &Failure{Message: "node unreachable, VM removed"}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			st, pool := newStore(t)
			project, record, rid := runningActor(t, st, onSlot(0x37), medium)
			actor := record.ActorID
			tid := uuid.New()

			result := begin(t, st, TransitionRequest{ProjectID: project, ActorID: actor, TransitionID: tid, Action: tc.action}, BeginStarted)
			assert.Equal(t, tid, result.TransitionID)
			assert.Equal(t, tc.action, result.Action)
			require.NotNil(t, result.Actor)
			assert.Equal(t, tc.state, result.Actor.State)
			assert.Equal(t, record.ExecutionID, result.Actor.ExecutionID)
			assert.Equal(t, record.NodeID, result.Actor.NodeID)
			got, err := st.Get(ctx, project, actor)
			require.NoError(t, err)
			require.Equal(t, tc.state, got.State)

			for range 2 {
				finishTransition(t, st, project, actor, tid, tc.failure, FinishTransitionFinished)
				require.Equal(t, map[int16]int32{7: 0}, liveCounts(t, pool, project), "decremented once")
			}
			got, err = st.Get(ctx, project, actor)
			require.NoError(t, err)
			require.Nil(t, got)
			require.False(t, actorExists(t, pool, actor))
			run := runOf(t, pool, actor)
			require.Equal(t, StateGone, run.state)
			require.True(t, run.gone)
			if tc.failure != nil {
				require.Equal(t, *tc.failure, run.failure)
			}

			wait := waitFor(t, st, project, actor, tid, time.Second)
			require.Equal(t, WaitResult{Outcome: WaitFinished, Failure: tc.failure}, wait)
			begin(t, st, TransitionRequest{ProjectID: project, ActorID: actor, TransitionID: tid, Action: tc.action}, BeginAlreadyFinished)
			begin(t, st, TransitionRequest{ProjectID: project, ActorID: actor, TransitionID: uuid.New(), Action: tc.action}, BeginNotFound)
			finishTransition(t, st, project, actor, tid, &Failure{Message: "late"}, FinishTransitionFinished)
			if tc.failure != nil {
				require.Equal(t, *tc.failure, runOf(t, pool, actor).failure, "the first failure stays")
			} else {
				require.Equal(t, Failure{}, runOf(t, pool, actor).failure)
			}

			reserved := reserve(t, st, project, actor, rid, medium, ReserveFinished)
			require.Nil(t, reserved.Failure, "the start did not fail")
			outcome, err := st.FinishStart(ctx, project, actor, rid, Failure{Message: "late failure"})
			require.NoError(t, err)
			require.Equal(t, FinishStartAlreadyStored, outcome)
			require.Equal(t, map[int16]int32{7: 0}, liveCounts(t, pool, project))
			requireCountsMatch(t, pool)
		})
	}
}

func TestBeginCompeting(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	st, pool := newStore(t)
	project, record, _ := runningActor(t, st, uuid.New(), strict)
	actor := record.ActorID
	tid := uuid.New()
	pause := TransitionRequest{ProjectID: project, ActorID: actor, TransitionID: tid, Action: ActionPause}

	begin(t, st, pause, BeginStarted)
	retry := begin(t, st, pause, BeginStarted)
	require.Equal(t, tid, retry.TransitionID)
	require.NotNil(t, retry.Actor)

	other := pause
	other.TransitionID = uuid.New()
	joined := begin(t, st, other, BeginJoined)
	require.Equal(t, tid, joined.TransitionID)
	require.Equal(t, ActionPause, joined.Action)
	require.Equal(t, StatePausing, joined.Actor.State)

	kill := other
	kill.Action = ActionKill
	conflict := begin(t, st, kill, BeginConflict)
	require.Equal(t, tid, conflict.TransitionID)
	require.Equal(t, ActionPause, conflict.Action)

	mismatch := pause
	mismatch.Action = ActionKill
	begin(t, st, mismatch, BeginActionMismatch)

	var current uuid.UUID
	require.NoError(t, pool.QueryRow(ctx, `SELECT transition_id FROM public.scheduling_actor_run WHERE actor_id = $1`, actor).Scan(&current))
	require.Equal(t, tid, current)
	require.Equal(t, StatePausing, runOf(t, pool, actor).state)

	finishTransition(t, st, project, actor, tid, nil, FinishTransitionFinished)
	begin(t, st, mismatch, BeginActionMismatch)
	begin(t, st, kill, BeginNotFound, "the conflicting transition ended the sandbox")
	require.Equal(t, map[int16]int32{0: 0}, liveCounts(t, pool, project))
}

func TestBeginOutcomes(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	st, pool := newStore(t)
	project, record, _ := runningActor(t, st, uuid.New(), strict)
	actor := record.ActorID

	begin(t, st, TransitionRequest{ProjectID: project, ActorID: uuid.New(), TransitionID: uuid.New(), Action: ActionKill}, BeginNotFound)

	starting := uuid.New()
	startingRID := uuid.New()
	reserve(t, st, project, starting, startingRID, strict, ReserveReserved)
	begin(t, st, TransitionRequest{ProjectID: project, ActorID: starting, TransitionID: uuid.New(), Action: ActionKill}, BeginNotRunning)
	outcome, err := st.FinishStart(ctx, project, starting, startingRID, Failure{Message: "node failed"})
	require.NoError(t, err)
	require.Equal(t, FinishStartFinished, outcome)
	begin(t, st, TransitionRequest{ProjectID: project, ActorID: starting, TransitionID: uuid.New(), Action: ActionKill}, BeginNotFound)

	fenced := begin(t, st, TransitionRequest{
		ProjectID: project, ActorID: actor, TransitionID: uuid.New(), Action: ActionKill, ExpectedExecutionID: "another-execution",
	}, BeginExecutionMismatch)
	require.Equal(t, record.ExecutionID, fenced.Actor.ExecutionID)
	require.Equal(t, StateRunning, fenced.Actor.State)

	notExpired := begin(t, st, TransitionRequest{
		ProjectID: project, ActorID: actor, TransitionID: uuid.New(), Action: ActionPause, RequireExpired: true,
	}, BeginNotExpired)
	require.Equal(t, StateRunning, notExpired.Actor.State)

	expire(t, pool, actor)
	begin(t, st, TransitionRequest{
		ProjectID: project, ActorID: actor, TransitionID: uuid.New(), Action: ActionPause,
		ExpectedExecutionID: record.ExecutionID, RequireExpired: true,
	}, BeginStarted)
	require.Equal(t, map[int16]int32{0: 1}, liveCounts(t, pool, project))
	requireCountsMatch(t, pool)
}

func TestTransitionProjectIsolation(t *testing.T) {
	t.Parallel()
	st, pool := newStore(t)
	owner, record, _ := runningActor(t, st, uuid.New(), strict)
	actor := record.ActorID
	intruder := uuid.New()

	begin(t, st, TransitionRequest{ProjectID: intruder, ActorID: actor, TransitionID: uuid.New(), Action: ActionKill}, BeginNotFound)
	require.Equal(t, StateRunning, runOf(t, pool, actor).state)

	tid := uuid.New()
	begin(t, st, TransitionRequest{ProjectID: owner, ActorID: actor, TransitionID: tid, Action: ActionPause}, BeginStarted)
	begin(t, st, TransitionRequest{ProjectID: intruder, ActorID: actor, TransitionID: tid, Action: ActionPause}, BeginNotFound)
	finishTransition(t, st, intruder, actor, tid, nil, FinishTransitionNotFound)
	restore(t, st, intruder, actor, tid, time.Second, RestoreNotFound)
	require.Equal(t, WaitResult{Outcome: WaitNotFound}, waitFor(t, st, intruder, actor, tid, time.Second))
	require.Equal(t, StatePausing, runOf(t, pool, actor).state)
	require.True(t, actorExists(t, pool, actor))
	require.Equal(t, map[int16]int32{0: 1}, liveCounts(t, pool, owner))

	finishTransition(t, st, owner, actor, tid, nil, FinishTransitionFinished)
	begin(t, st, TransitionRequest{ProjectID: intruder, ActorID: actor, TransitionID: tid, Action: ActionPause}, BeginNotFound)
	require.Equal(t, WaitResult{Outcome: WaitNotFound}, waitFor(t, st, intruder, actor, tid, time.Second))
	requireCountsMatch(t, pool)
}

func TestRestoreRunning(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	st, pool := newStore(t)
	project, record, _ := runningActor(t, st, onSlot(0x12), large)
	actor := record.ActorID
	expire(t, pool, actor)

	first := uuid.New()
	pause := TransitionRequest{ProjectID: project, ActorID: actor, TransitionID: first, Action: ActionPause, RequireExpired: true}
	begin(t, st, pause, BeginStarted)
	before := dbNow(t, pool)
	restored := restore(t, st, project, actor, first, 10*time.Second, RestoreRestored)
	assert.WithinDuration(t, before, restored.RefusedSince, time.Second)
	assert.Equal(t, 10*time.Second, restored.RefusedUntil.Sub(restored.RefusedSince), "a new series starts at the refusal")
	again := restore(t, st, project, actor, first, time.Hour, RestoreRestored)
	require.Equal(t, restored, again, "a repeat changes nothing")

	requireRunning(t, st, record)
	require.Equal(t, map[int16]int32{0x12: 1}, liveCounts(t, pool, project))
	require.Equal(t, WaitResult{Outcome: WaitRestored}, waitFor(t, st, project, actor, first, time.Second))
	finishTransition(t, st, project, actor, first, nil, FinishTransitionRestored)
	requireRunning(t, st, record)
	begin(t, st, pause, BeginAlreadyRestored)
	pause.RequireExpired = false
	begin(t, st, pause, BeginAlreadyRestored)
	kill := pause
	kill.Action = ActionKill
	begin(t, st, kill, BeginActionMismatch)

	second := uuid.New()
	pause.TransitionID = second
	pause.RequireExpired = true
	begin(t, st, pause, BeginNotExpired, "the retry window is open")
	pause.RequireExpired = false
	begin(t, st, pause, BeginStarted)
	restore(t, st, project, actor, first, time.Second, RestoreNotFound)
	finishTransition(t, st, project, actor, first, nil, FinishTransitionNotFound)
	require.Equal(t, WaitResult{Outcome: WaitUnknown}, waitFor(t, st, project, actor, first, time.Second))
	continued := restore(t, st, project, actor, second, 0, RestoreRestored)
	require.Equal(t, restored.RefusedSince, continued.RefusedSince, "a refusal inside the window continues the series")

	third := uuid.New()
	begin(t, st, TransitionRequest{ProjectID: project, ActorID: actor, TransitionID: third, Action: ActionPause, RequireExpired: true}, BeginStarted)
	_, err := pool.Exec(ctx, `
UPDATE public.scheduling_actor_run SET refused_until = now() - interval '31 seconds' WHERE actor_id = $1`, actor)
	require.NoError(t, err)
	later := restore(t, st, project, actor, third, time.Second, RestoreRestored)
	require.True(t, later.RefusedSince.After(continued.RefusedSince), "a refusal 31 s after the window starts a new series")

	require.Equal(t, map[int16]int32{0x12: 1}, liveCounts(t, pool, project))
	requireCountsMatch(t, pool)
}

func TestRestoreKillOrFinished(t *testing.T) {
	t.Parallel()
	st, pool := newStore(t)
	project, record, _ := runningActor(t, st, uuid.New(), strict)
	actor := record.ActorID

	kill := uuid.New()
	begin(t, st, TransitionRequest{ProjectID: project, ActorID: actor, TransitionID: kill, Action: ActionKill}, BeginStarted)
	restore(t, st, project, actor, kill, time.Second, RestoreNotRestorable)
	require.Equal(t, StateKilling, runOf(t, pool, actor).state)
	finishTransition(t, st, project, actor, kill, nil, FinishTransitionFinished)
	restore(t, st, project, actor, kill, time.Second, RestoreNotRestorable)
	require.Equal(t, StateGone, runOf(t, pool, actor).state)

	project, record, _ = runningActor(t, st, uuid.New(), strict)
	actor = record.ActorID
	pause := uuid.New()
	begin(t, st, TransitionRequest{ProjectID: project, ActorID: actor, TransitionID: pause, Action: ActionPause}, BeginStarted)
	finishTransition(t, st, project, actor, pause, nil, FinishTransitionFinished)
	restore(t, st, project, actor, pause, time.Second, RestoreAlreadyFinished)
	require.Equal(t, StateGone, runOf(t, pool, actor).state)
	require.False(t, actorExists(t, pool, actor))
	requireCountsMatch(t, pool)
}

// A transition ID or an execution ID of an earlier execution never acts on
// the execution that a new start stored under the same actor ID.
func TestTransitionOfEarlierExecution(t *testing.T) {
	t.Parallel()
	st, pool := newStore(t)
	project, record, _ := runningActor(t, st, uuid.New(), strict)
	actor := record.ActorID

	old := uuid.New()
	begin(t, st, TransitionRequest{ProjectID: project, ActorID: actor, TransitionID: old, Action: ActionPause}, BeginStarted)
	finishTransition(t, st, project, actor, old, nil, FinishTransitionFinished)

	rid := uuid.New()
	reserve(t, st, project, actor, rid, strict, ReserveReserved)
	require.Equal(t, WaitResult{Outcome: WaitUnknown}, waitFor(t, st, project, actor, old, time.Second), "a new start replaced the transition")
	next := newActor(project, actor)
	put(t, st, rid, next, PutStored)

	finishTransition(t, st, project, actor, old, nil, FinishTransitionNotFound)
	restore(t, st, project, actor, old, time.Second, RestoreNotFound)
	require.Equal(t, WaitResult{Outcome: WaitUnknown}, waitFor(t, st, project, actor, old, time.Second))
	begin(t, st, TransitionRequest{
		ProjectID: project, ActorID: actor, TransitionID: uuid.New(), Action: ActionKill, ExpectedExecutionID: record.ExecutionID,
	}, BeginExecutionMismatch)
	requireLive(t, st, next)
	require.Equal(t, map[int16]int32{0: 1}, liveCounts(t, pool, project))
	requireCountsMatch(t, pool)
}

func TestConcurrentTransitions(t *testing.T) {
	t.Parallel()

	t.Run("begin", func(t *testing.T) {
		t.Parallel()
		st, pool := newStore(t)
		project, record, _ := runningActor(t, st, uuid.New(), strict)

		const callers = 6
		results := make([]BeginResult, callers)
		parallel(t, callers, func(i int) error {
			action := ActionPause
			if i%2 == 1 {
				action = ActionKill
			}
			var err error
			results[i], err = st.BeginTransition(t.Context(), TransitionRequest{
				ProjectID: project, ActorID: record.ActorID, TransitionID: uuid.New(), Action: action,
			})

			return err
		})

		var started []BeginResult
		for _, result := range results {
			if result.Outcome == BeginStarted {
				started = append(started, result)
			}
		}
		require.Len(t, started, 1)
		for _, result := range results {
			require.Equal(t, started[0].TransitionID, result.TransitionID, "every caller learns the one active transition")
			require.Contains(t, []BeginOutcome{BeginStarted, BeginJoined, BeginConflict}, result.Outcome)
		}
		require.Equal(t, map[int16]int32{0: 1}, liveCounts(t, pool, project))
	})
	t.Run("finish and restore", func(t *testing.T) {
		t.Parallel()
		st, pool := newStore(t)
		project, record, _ := runningActor(t, st, onSlot(0x21), large)
		actor := record.ActorID
		tid := uuid.New()
		begin(t, st, TransitionRequest{ProjectID: project, ActorID: actor, TransitionID: tid, Action: ActionPause}, BeginStarted)

		const callers = 6
		finished := make([]FinishTransitionOutcome, callers)
		restored := make([]RestoreOutcome, callers)
		parallel(t, callers, func(i int) error {
			if i%2 == 0 {
				var err error
				finished[i], err = st.FinishTransition(t.Context(), project, actor, tid, nil)

				return err
			}
			result, err := st.RestoreRunning(t.Context(), project, actor, tid, time.Second)
			restored[i] = result.Outcome

			return err
		})

		if runOf(t, pool, actor).state == StateGone {
			for i := range callers {
				if i%2 == 0 {
					require.Equal(t, FinishTransitionFinished, finished[i])
				} else {
					require.Equal(t, RestoreAlreadyFinished, restored[i])
				}
			}
			require.False(t, actorExists(t, pool, actor))
			require.Equal(t, map[int16]int32{0x21: 0}, liveCounts(t, pool, project))
		} else {
			for i := range callers {
				if i%2 == 0 {
					require.Equal(t, FinishTransitionRestored, finished[i])
				} else {
					require.Equal(t, RestoreRestored, restored[i])
				}
			}
			requireLive(t, st, record)
			require.Equal(t, map[int16]int32{0x21: 1}, liveCounts(t, pool, project))
		}
		requireCountsMatch(t, pool)
	})
	t.Run("late finish and a new transition", func(t *testing.T) {
		t.Parallel()
		st, pool := newStore(t)
		project, record, _ := runningActor(t, st, uuid.New(), strict)
		actor := record.ActorID
		old := uuid.New()
		begin(t, st, TransitionRequest{ProjectID: project, ActorID: actor, TransitionID: old, Action: ActionPause}, BeginStarted)
		restore(t, st, project, actor, old, 0, RestoreRestored)

		next := uuid.New()
		var (
			finished FinishTransitionOutcome
			began    BeginResult
		)
		parallel(t, 2, func(i int) error {
			var err error
			if i == 0 {
				finished, err = st.FinishTransition(t.Context(), project, actor, old, nil)
			} else {
				began, err = st.BeginTransition(t.Context(), TransitionRequest{
					ProjectID: project, ActorID: actor, TransitionID: next, Action: ActionKill,
				})
			}

			return err
		})

		require.Contains(t, []FinishTransitionOutcome{FinishTransitionRestored, FinishTransitionNotFound}, finished)
		require.Equal(t, BeginStarted, began.Outcome)
		require.Equal(t, StateKilling, runOf(t, pool, actor).state)
		require.True(t, actorExists(t, pool, actor))
		require.Equal(t, map[int16]int32{0: 1}, liveCounts(t, pool, project))
	})
}

func TestFinishTransitionRollsBack(t *testing.T) {
	t.Parallel()

	for name, corrupt := range map[string]string{
		"counter at zero": `UPDATE public.scheduling_project_live_count SET live = 0 WHERE project_id = $1`,
		"no counter row":  `DELETE FROM public.scheduling_project_live_count WHERE project_id = $1`,
		"no actor record": `DELETE FROM public.scheduling_actor WHERE project_id = $1`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			st, pool := newStore(t)
			project, record, _ := runningActor(t, st, uuid.New(), strict)
			actor := record.ActorID
			tid := uuid.New()
			begin(t, st, TransitionRequest{ProjectID: project, ActorID: actor, TransitionID: tid, Action: ActionKill}, BeginStarted)
			_, err := pool.Exec(ctx, corrupt, project)
			require.NoError(t, err)
			hasActor := actorExists(t, pool, actor)

			_, err = st.FinishTransition(ctx, project, actor, tid, nil)
			require.Error(t, err)
			run := runOf(t, pool, actor)
			require.Equal(t, StateKilling, run.state)
			require.False(t, run.gone)
			require.Equal(t, hasActor, actorExists(t, pool, actor), "the actor record is not deleted")
			require.Equal(t, WaitResult{Outcome: WaitWaiting}, waitFor(t, st, project, actor, tid, time.Microsecond))
		})
	}
}

func TestWaitForTransition(t *testing.T) {
	t.Parallel()

	t.Run("budget", func(t *testing.T) {
		t.Parallel()
		st, _ := newStore(t)
		project, record, _ := runningActor(t, st, uuid.New(), strict)
		tid := uuid.New()
		begin(t, st, TransitionRequest{ProjectID: project, ActorID: record.ActorID, TransitionID: tid, Action: ActionPause}, BeginStarted)

		started := time.Now()
		require.Equal(t, WaitResult{Outcome: WaitWaiting}, waitFor(t, st, project, record.ActorID, tid, 300*time.Millisecond))
		require.GreaterOrEqual(t, time.Since(started), 300*time.Millisecond)
	})
	t.Run("unknown transition", func(t *testing.T) {
		t.Parallel()
		st, _ := newStore(t)
		project, record, _ := runningActor(t, st, uuid.New(), strict)

		require.Equal(t, WaitResult{Outcome: WaitUnknown}, waitFor(t, st, project, record.ActorID, uuid.New(), time.Second))
		require.Equal(t, WaitResult{Outcome: WaitNotFound}, waitFor(t, st, project, uuid.New(), uuid.New(), time.Second))
	})
	for name, end := range map[string]func(t *testing.T, st *Store, project, actor, tid uuid.UUID) WaitResult{
		"finish": func(t *testing.T, st *Store, project, actor, tid uuid.UUID) WaitResult {
			t.Helper()
			finishTransition(t, st, project, actor, tid, &Failure{Message: "snapshot failed", Code: 3}, FinishTransitionFinished)

			return WaitResult{Outcome: WaitFinished, Failure: &Failure{Message: "snapshot failed", Code: 3}}
		},
		"restore": func(t *testing.T, st *Store, project, actor, tid uuid.UUID) WaitResult {
			t.Helper()
			restore(t, st, project, actor, tid, time.Second, RestoreRestored)

			return WaitResult{Outcome: WaitRestored}
		},
	} {
		t.Run("another store sees "+name, func(t *testing.T) {
			t.Parallel()
			st, pool := newStore(t)
			project, record, _ := runningActor(t, st, uuid.New(), strict)
			tid := uuid.New()
			begin(t, st, TransitionRequest{ProjectID: project, ActorID: record.ActorID, TransitionID: tid, Action: ActionPause}, BeginStarted)

			replicaPool := secondPool(t, pool)
			replica := New(pool, replicaPool)
			replica.pollDelay = fixedDelay(10 * time.Millisecond)
			done := make(chan WaitResult, 1)
			go func() {
				result, err := replica.WaitForTransition(t.Context(), project, record.ActorID, tid, 5*time.Second)
				assert.NoError(t, err)
				done <- result
			}()
			require.Eventually(t, func() bool { return replicaPool.Stat().AcquireCount() >= 2 }, 5*time.Second, time.Millisecond,
				"the waiter is polling")
			require.Empty(t, done)

			want := end(t, st, project, record.ActorID, tid)
			select {
			case got := <-done:
				require.Equal(t, want, got)
			case <-time.After(5 * time.Second):
				t.Fatal("the waiter did not see the end of the transition")
			}
		})
	}
	t.Run("cancelled", func(t *testing.T) {
		t.Parallel()
		st, pool := newStore(t)
		project, record, _ := runningActor(t, st, uuid.New(), strict)
		tid := uuid.New()
		begin(t, st, TransitionRequest{ProjectID: project, ActorID: record.ActorID, TransitionID: tid, Action: ActionPause}, BeginStarted)

		replicaPool := secondPool(t, pool)
		replica := New(pool, replicaPool)
		replica.pollDelay = fixedDelay(time.Hour)
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() {
			_, err := replica.WaitForTransition(ctx, project, record.ActorID, tid, 5*time.Second)
			done <- err
		}()
		require.Eventually(t, func() bool {
			stat := replicaPool.Stat()

			return stat.AcquireCount() == 1 && stat.AcquiredConns() == 0
		}, 5*time.Second, time.Millisecond, "the waiter holds no connection between polls")
		require.Empty(t, done)

		cancel()
		select {
		case err := <-done:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(5 * time.Second):
			t.Fatal("the waiter did not stop on cancellation")
		}
		require.Equal(t, int64(1), replicaPool.Stat().AcquireCount(), "no poll after cancellation")
		require.Empty(t, watchSubscribers(replica), "the last caller stopped the watch")

		cancelled, cancelNow := context.WithCancel(t.Context())
		cancelNow()
		_, err := replica.WaitForTransition(cancelled, project, record.ActorID, tid, 5*time.Second)
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, int64(1), replicaPool.Stat().AcquireCount(), "a cancelled call does not poll")
	})
	t.Run("deadline", func(t *testing.T) {
		t.Parallel()
		st, _ := newStore(t)
		project, record, _ := runningActor(t, st, uuid.New(), strict)
		tid := uuid.New()
		begin(t, st, TransitionRequest{ProjectID: project, ActorID: record.ActorID, TransitionID: tid, Action: ActionPause}, BeginStarted)

		ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer cancel()
		_, err := st.WaitForTransition(ctx, project, record.ActorID, tid, 5*time.Second)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})
}

func runningActor(t *testing.T, st *Store, rid uuid.UUID, limit int64) (uuid.UUID, Actor, uuid.UUID) {
	t.Helper()

	project, actor := uuid.New(), uuid.New()
	reserve(t, st, project, actor, rid, limit, ReserveReserved)
	record := newActor(project, actor)
	put(t, st, rid, record, PutStored)

	return project, record, rid
}

func begin(t *testing.T, st *Store, r TransitionRequest, want BeginOutcome, msgAndArgs ...any) BeginResult {
	t.Helper()

	result, err := st.BeginTransition(t.Context(), r)
	require.NoError(t, err)
	require.Equal(t, want, result.Outcome, msgAndArgs...)

	return result
}

func finishTransition(t *testing.T, st *Store, project, actor, tid uuid.UUID, failure *Failure, want FinishTransitionOutcome) {
	t.Helper()

	outcome, err := st.FinishTransition(t.Context(), project, actor, tid, failure)
	require.NoError(t, err)
	require.Equal(t, want, outcome)
}

func restore(t *testing.T, st *Store, project, actor, tid uuid.UUID, retryAfter time.Duration, want RestoreOutcome) RestoreResult {
	t.Helper()

	result, err := st.RestoreRunning(t.Context(), project, actor, tid, retryAfter)
	require.NoError(t, err)
	require.Equal(t, want, result.Outcome)

	return result
}

func waitFor(t *testing.T, st *Store, project, actor, tid uuid.UUID, maxWait time.Duration) WaitResult {
	t.Helper()

	result, err := st.WaitForTransition(t.Context(), project, actor, tid, maxWait)
	require.NoError(t, err)

	return result
}

func requireRunning(t *testing.T, st *Store, want Actor) {
	t.Helper()

	got, err := st.Get(t.Context(), want.ProjectID, want.ActorID)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, StateRunning, got.State)
	require.Equal(t, want.ExecutionID, got.ExecutionID)
}

func expire(t *testing.T, pool *pgxpool.Pool, actor uuid.UUID) {
	t.Helper()

	_, err := pool.Exec(t.Context(), `UPDATE public.scheduling_actor_run SET end_time = now() - interval '1 second' WHERE actor_id = $1`, actor)
	require.NoError(t, err)
}

func dbNow(t *testing.T, pool *pgxpool.Pool) time.Time {
	t.Helper()

	var now time.Time
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT now()`).Scan(&now))

	return now
}

func secondPool(t *testing.T, pool *pgxpool.Pool) *pgxpool.Pool {
	t.Helper()

	config, err := pgxpool.ParseConfig(dbtest.ConnString(t, pool))
	require.NoError(t, err)
	config.MaxConns = 2
	replica, err := pgxpool.NewWithConfig(t.Context(), config)
	require.NoError(t, err)
	t.Cleanup(replica.Close)

	return replica
}

func parallel(t *testing.T, n int, call func(int) error) {
	t.Helper()

	start := make(chan struct{})
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			<-start
			errs[i] = call(i)
		})
	}
	close(start)
	wg.Wait()
	for _, err := range errs {
		require.NoError(t, err)
	}
}
