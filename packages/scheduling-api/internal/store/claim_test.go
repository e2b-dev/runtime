package store

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestClaimTransition(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	st, pool := newStore(t)
	tr := inTransition(t, st, pool, ActionPause, time.Hour)
	_, err := pool.Exec(ctx, `UPDATE public.scheduling_actor_run SET refused_until = now(), refused_since = now() WHERE actor_id = $1`, tr.actor.ActorID)
	require.NoError(t, err)
	before := runRow(t, pool, tr.actor.ActorID)
	counts := liveCounts(t, pool, tr.project)
	oldWaiter := waitInBackground(t, st, tr.project, tr.actor.ActorID, tr.tid)

	claimed := claim(t, st, tr.claim(uuid.New()), ClaimClaimed)
	require.Equal(t, WaitUnknown, (<-oldWaiter).Outcome, "a waiter for the previous owner learns that it was replaced")
	newID := claimed.TransitionID
	require.Equal(t, ActionPause, claimed.Action)
	require.NotNil(t, claimed.Actor)
	require.Equal(t, tr.actor.ExecutionID, claimed.Actor.ExecutionID)
	require.Equal(t, StatePausing, claimed.Actor.State)
	after := runRow(t, pool, tr.actor.ActorID)
	require.Equal(t, newID, after.transitionID)
	require.True(t, claimed.TransitionAt.Equal(after.transitionAt))
	require.WithinDuration(t, dbNow(t, pool), after.transitionAt, 5*time.Second)
	after.transitionID, after.transitionAt = before.transitionID, before.transitionAt
	require.Equal(t, before, after, "only the transition ID and time change")
	require.Equal(t, counts, liveCounts(t, pool, tr.project))

	repeated := claim(t, st, ClaimRequest{
		ProjectID: tr.project, ActorID: tr.actor.ActorID, OldTransitionID: tr.tid, NewTransitionID: newID,
		ExpectedExecutionID: tr.actor.ExecutionID,
	}, ClaimClaimed)
	require.True(t, claimed.TransitionAt.Equal(repeated.TransitionAt), "a repeat does not move transition_at")
	require.True(t, claimed.TransitionAt.Equal(transitionAt(t, pool, tr.actor.ActorID)))

	finishTransition(t, st, tr.project, tr.actor.ActorID, tr.tid, nil, FinishTransitionNotFound)
	restore(t, st, tr.project, tr.actor.ActorID, tr.tid, time.Second, RestoreNotFound)
	require.Equal(t, newID, runRow(t, pool, tr.actor.ActorID).transitionID, "the previous owner cannot end it")
	require.Equal(t, WaitUnknown, waitFor(t, st, tr.project, tr.actor.ActorID, tr.tid, time.Second).Outcome)
	claim(t, st, ClaimRequest{
		ProjectID: tr.project, ActorID: tr.actor.ActorID, OldTransitionID: newID, NewTransitionID: uuid.New(),
		ExpectedExecutionID: tr.actor.ExecutionID,
	}, ClaimNotStale)

	waited := waitInBackground(t, st, tr.project, tr.actor.ActorID, newID)
	finishTransition(t, st, tr.project, tr.actor.ActorID, newID, nil, FinishTransitionFinished)
	require.Equal(t, WaitFinished, (<-waited).Outcome)
	require.Equal(t, map[int16]int32{0: 0}, liveCounts(t, pool, tr.project))
	claim(t, st, tr.claim(newID), ClaimAlreadyFinished)
	requireCountsMatch(t, pool)
}

func TestClaimTransitionOutcomes(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	st, pool := newStore(t)

	fresh := inTransition(t, st, pool, ActionKill, 80*time.Second)
	claim(t, st, fresh.claim(uuid.New()), ClaimNotStale)
	boundary := inTransition(t, st, pool, ActionKill, staleTransitionAfter)
	claim(t, st, boundary.claim(uuid.New()), ClaimClaimed)

	tr := inTransition(t, st, pool, ActionKill, time.Hour)
	mismatch := tr.claim(uuid.New())
	mismatch.ExpectedExecutionID = "another-execution"
	claim(t, st, mismatch, ClaimExecutionMismatch)
	other := tr.claim(uuid.New())
	other.ProjectID = uuid.New()
	claim(t, st, other, ClaimNotFound)
	missing := tr.claim(uuid.New())
	missing.ActorID = uuid.New()
	claim(t, st, missing, ClaimNotFound)
	wrong := tr.claim(uuid.New())
	wrong.OldTransitionID = uuid.New()
	conflict := claim(t, st, wrong, ClaimConflict)
	require.Equal(t, tr.tid, conflict.TransitionID)
	require.Equal(t, ActionKill, conflict.Action)
	require.Equal(t, tr.tid, runRow(t, pool, tr.actor.ActorID).transitionID, "nothing changed")

	restored := inTransition(t, st, pool, ActionPause, 0)
	restore(t, st, restored.project, restored.actor.ActorID, restored.tid, 0, RestoreRestored)
	setTransitionAgo(t, pool, restored.actor.ActorID, time.Hour)
	claim(t, st, restored.claim(uuid.New()), ClaimAlreadyRestored)

	snapshot := inTransition(t, st, pool, ActionPause, time.Hour)
	_, err := pool.Exec(ctx, `UPDATE public.scheduling_actor_run SET state = 'snapshotting', transition_action = 3 WHERE actor_id = $1`, snapshot.actor.ActorID)
	require.NoError(t, err)
	claim(t, st, snapshot.claim(uuid.New()), ClaimNotFound)

	starting := uuid.New()
	reserve(t, st, tr.project, starting, uuid.New(), strict, ReserveReserved)
	claim(t, st, ClaimRequest{
		ProjectID: tr.project, ActorID: starting, OldTransitionID: uuid.New(), NewTransitionID: uuid.New(), ExpectedExecutionID: "e",
	}, ClaimNotFound)

	same := tr.claim(tr.tid)
	_, err = st.ClaimTransition(ctx, same)
	require.ErrorContains(t, err, "equal")
	requireCountsMatch(t, pool)
}

// A claim for an earlier execution does not take over a transition of a
// newer execution with the same sandbox ID.
func TestClaimTransitionOfEarlierExecution(t *testing.T) {
	t.Parallel()
	st, pool := newStore(t)
	first := inTransition(t, st, pool, ActionKill, 0)
	finishTransition(t, st, first.project, first.actor.ActorID, first.tid, nil, FinishTransitionFinished)

	rid := uuid.New()
	reserve(t, st, first.project, first.actor.ActorID, rid, strict, ReserveReserved)
	second := newActor(first.project, first.actor.ActorID)
	put(t, st, rid, second, PutStored)
	tid := uuid.New()
	begin(t, st, TransitionRequest{ProjectID: first.project, ActorID: second.ActorID, TransitionID: tid, Action: ActionKill}, BeginStarted)
	setTransitionAgo(t, pool, second.ActorID, time.Hour)

	claim(t, st, first.claim(uuid.New()), ClaimExecutionMismatch)
	stale := first.claim(uuid.New())
	stale.OldTransitionID = tid
	claim(t, st, stale, ClaimExecutionMismatch)
	require.Equal(t, tid, runRow(t, pool, second.ActorID).transitionID)
}

// The service keeps only the latest transition ID. After a claim, the old ID
// cannot end the claimed attempt, but once the new owner restores the
// sandbox, a BeginTransition with the old ID starts a new pause. The
// contract requires owners to stop retrying BeginTransition before the claim
// can happen.
func TestClaimKeepsOnlyLatestTransition(t *testing.T) {
	t.Parallel()
	st, pool := newStore(t)
	tr := inTransition(t, st, pool, ActionPause, time.Hour)
	newID := claim(t, st, tr.claim(uuid.New()), ClaimClaimed).TransitionID

	finishTransition(t, st, tr.project, tr.actor.ActorID, tr.tid, nil, FinishTransitionNotFound)
	restore(t, st, tr.project, tr.actor.ActorID, tr.tid, time.Second, RestoreNotFound)
	oldPause := TransitionRequest{ProjectID: tr.project, ActorID: tr.actor.ActorID, TransitionID: tr.tid, Action: ActionPause}
	joined := begin(t, st, oldPause, BeginJoined)
	require.Equal(t, newID, joined.TransitionID)
	oldKill := oldPause
	oldKill.Action = ActionKill
	require.Equal(t, newID, begin(t, st, oldKill, BeginConflict).TransitionID)
	require.Equal(t, newID, runRow(t, pool, tr.actor.ActorID).transitionID)

	restore(t, st, tr.project, tr.actor.ActorID, newID, 0, RestoreRestored)
	begin(t, st, oldPause, BeginStarted, "the old ID is no longer recorded")
	require.Equal(t, tr.tid, runRow(t, pool, tr.actor.ActorID).transitionID)
	require.Equal(t, map[int16]int32{0: 1}, liveCounts(t, pool, tr.project))
}

func TestClaimTransitionCompeting(t *testing.T) {
	t.Parallel()
	st, pool := newStore(t)
	replica := New(secondPool(t, pool), secondPool(t, pool))
	tr := inTransition(t, st, pool, ActionPause, time.Hour)

	const callers = 6
	results := make([]ClaimResult, callers)
	newIDs := make([]uuid.UUID, callers)
	parallel(t, callers, func(i int) error {
		stores := []*Store{st, replica}
		newIDs[i] = uuid.New()
		var err error
		results[i], err = stores[i%2].ClaimTransition(t.Context(), tr.claim(newIDs[i]))

		return err
	})

	winner := -1
	for i, result := range results {
		if result.Outcome == ClaimClaimed {
			require.Equal(t, -1, winner, "one claim wins")
			winner = i
		}
	}
	require.NotEqual(t, -1, winner)
	for i, result := range results {
		if i != winner {
			require.Equal(t, ClaimConflict, result.Outcome)
			require.Equal(t, newIDs[winner], result.TransitionID)
		}
	}
	require.Equal(t, newIDs[winner], runRow(t, pool, tr.actor.ActorID).transitionID)
	requireCountsMatch(t, pool)
}

func TestClaimTransitionRace(t *testing.T) {
	t.Parallel()

	t.Run("finish, then claim", func(t *testing.T) {
		t.Parallel()
		st, pool := newStore(t)
		tr := inTransition(t, st, pool, ActionKill, time.Hour)
		var finished FinishTransitionOutcome
		var claimed ClaimResult

		inOrder(t, pool, tr.actor.ActorID,
			func(ctx context.Context) error {
				var err error
				finished, err = st.FinishTransition(ctx, tr.project, tr.actor.ActorID, tr.tid, nil)

				return err
			},
			tr.claimCall(st, &claimed))

		require.Equal(t, FinishTransitionFinished, finished)
		require.Equal(t, ClaimAlreadyFinished, claimed.Outcome)
		require.Equal(t, map[int16]int32{0: 0}, liveCounts(t, pool, tr.project))
		requireCountsMatch(t, pool)
	})
	t.Run("claim, then finish", func(t *testing.T) {
		t.Parallel()
		st, pool := newStore(t)
		tr := inTransition(t, st, pool, ActionKill, time.Hour)
		var finished FinishTransitionOutcome
		var claimed ClaimResult

		inOrder(t, pool, tr.actor.ActorID,
			tr.claimCall(st, &claimed),
			func(ctx context.Context) error {
				var err error
				finished, err = st.FinishTransition(ctx, tr.project, tr.actor.ActorID, tr.tid, nil)

				return err
			})

		require.Equal(t, ClaimClaimed, claimed.Outcome)
		require.Equal(t, FinishTransitionNotFound, finished)
		require.Equal(t, StateKilling, runOf(t, pool, tr.actor.ActorID).state)
		require.True(t, actorExists(t, pool, tr.actor.ActorID))
		require.Equal(t, map[int16]int32{0: 1}, liveCounts(t, pool, tr.project))
	})
	t.Run("restore, then claim", func(t *testing.T) {
		t.Parallel()
		st, pool := newStore(t)
		tr := inTransition(t, st, pool, ActionPause, time.Hour)
		var restored RestoreResult
		var claimed ClaimResult

		inOrder(t, pool, tr.actor.ActorID,
			func(ctx context.Context) error {
				var err error
				restored, err = st.RestoreRunning(ctx, tr.project, tr.actor.ActorID, tr.tid, time.Second)

				return err
			},
			tr.claimCall(st, &claimed))

		require.Equal(t, RestoreRestored, restored.Outcome)
		require.Equal(t, ClaimAlreadyRestored, claimed.Outcome)
		require.Equal(t, StateRunning, runOf(t, pool, tr.actor.ActorID).state)
	})
	t.Run("claim, then restore", func(t *testing.T) {
		t.Parallel()
		st, pool := newStore(t)
		tr := inTransition(t, st, pool, ActionPause, time.Hour)
		var restored RestoreResult
		var claimed ClaimResult

		inOrder(t, pool, tr.actor.ActorID,
			tr.claimCall(st, &claimed),
			func(ctx context.Context) error {
				var err error
				restored, err = st.RestoreRunning(ctx, tr.project, tr.actor.ActorID, tr.tid, time.Second)

				return err
			})

		require.Equal(t, ClaimClaimed, claimed.Outcome)
		require.Equal(t, RestoreNotFound, restored.Outcome)
		require.Equal(t, StatePausing, runOf(t, pool, tr.actor.ActorID).state)
		restore(t, st, tr.project, tr.actor.ActorID, claimed.TransitionID, time.Second, RestoreRestored)
	})
}

func TestClaimTransitionClosedPool(t *testing.T) {
	t.Parallel()
	st, pool := newStore(t)
	pool.Close()

	_, err := st.ClaimTransition(t.Context(), ClaimRequest{
		ProjectID: uuid.New(), ActorID: uuid.New(), OldTransitionID: uuid.New(), NewTransitionID: uuid.New(), ExpectedExecutionID: "e",
	})
	require.Error(t, err)
}

func TestRecoveryReadsWithUnavailableWritePool(t *testing.T) {
	t.Parallel()
	st, pool := newStore(t)
	tr := inTransition(t, st, pool, ActionPause, time.Hour)
	writes := secondPool(t, pool)
	reads := secondPool(t, pool)
	st = New(writes, reads)
	writes.Close()

	expired, err := st.ExpireStarts(t.Context(), uuid.Nil, 10)
	require.NoError(t, err)
	require.Zero(t, expired.Ended)
	stale, err := st.ListStaleTransitions(t.Context(), StaleCursor{}, 10)
	require.NoError(t, err)
	require.Len(t, stale, 1)
	require.Equal(t, tr.tid, stale[0].TransitionID)
	run, actor, found, err := st.inspectClaim(t.Context(), tr.project, tr.actor.ActorID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, tr.tid, *run.TransitionID)
	require.Equal(t, tr.actor.ExecutionID, actor.ExecutionID)
}

func (tr transition) claim(newID uuid.UUID) ClaimRequest {
	return ClaimRequest{
		ProjectID:           tr.project,
		ActorID:             tr.actor.ActorID,
		OldTransitionID:     tr.tid,
		NewTransitionID:     newID,
		ExpectedExecutionID: tr.actor.ExecutionID,
	}
}

func (tr transition) claimCall(st *Store, result *ClaimResult) func(context.Context) error {
	return func(ctx context.Context) error {
		var err error
		*result, err = st.ClaimTransition(ctx, tr.claim(uuid.New()))

		return err
	}
}

// waitInBackground waits for the transition on st and returns once the watch
// has read the transition as active.
func waitInBackground(t *testing.T, st *Store, project, actor, tid uuid.UUID) <-chan WaitResult {
	t.Helper()

	waited := make(chan WaitResult, 1)
	go func() {
		result, err := st.WaitForTransition(t.Context(), project, actor, tid, 5*time.Second)
		if err != nil {
			result = WaitResult{}
		}
		waited <- result
	}()
	requireObserved(t, st, watchKey{projectID: project, actorID: actor, transitionID: tid})

	return waited
}

func claim(t *testing.T, st *Store, r ClaimRequest, want ClaimOutcome) ClaimResult {
	t.Helper()

	result, err := st.ClaimTransition(t.Context(), r)
	require.NoError(t, err)
	require.Equal(t, want, result.Outcome)

	return result
}
