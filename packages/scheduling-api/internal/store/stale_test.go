package store

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func TestListStaleTransitions(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	st, pool := newStore(t)

	pause := inTransition(t, st, pool, ActionPause, time.Hour)
	kill := inTransition(t, st, pool, ActionKill, 2*time.Hour)
	boundary := inTransition(t, st, pool, ActionPause, staleTransitionAfter)
	inTransition(t, st, pool, ActionPause, 80*time.Second)

	restored := inTransition(t, st, pool, ActionPause, 0)
	restore(t, st, restored.project, restored.actor.ActorID, restored.tid, 0, RestoreRestored)
	finished := inTransition(t, st, pool, ActionKill, 0)
	finishTransition(t, st, finished.project, finished.actor.ActorID, finished.tid, nil, FinishTransitionFinished)
	snapshotting, _, _ := runningActor(t, st, uuid.New(), strict)
	_, err := pool.Exec(ctx, `
UPDATE public.scheduling_actor_run
   SET state = 'snapshotting', transition_id = gen_random_uuid(), transition_action = 3, transition_at = now()
 WHERE project_id = $1`, snapshotting)
	require.NoError(t, err)
	reserve(t, st, uuid.New(), uuid.New(), uuid.New(), strict, ReserveReserved)
	_, err = pool.Exec(ctx, `UPDATE public.scheduling_actor_run SET transition_at = now() - interval '1 day' WHERE transition_at IS NOT NULL AND state NOT IN ('pausing', 'killing')`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `UPDATE public.scheduling_actor_run SET reserved_at = now() - interval '1 day', end_time = now() - interval '1 day'`)
	require.NoError(t, err)

	got, err := st.ListStaleTransitions(ctx, StaleCursor{}, 100)
	require.NoError(t, err)
	require.Len(t, got, 3)
	for i, want := range []transition{kill, pause, boundary} {
		require.Equal(t, want.project, got[i].ProjectID)
		require.Equal(t, want.actor.ActorID, got[i].ActorID)
		require.Equal(t, want.tid, got[i].TransitionID)
		require.Equal(t, want.action, got[i].Action)
		require.Equal(t, want.actor.ExecutionID, got[i].ExecutionID)
		require.Equal(t, want.actor.NodeID, got[i].NodeID)
		require.Equal(t, want.actor.ClusterID, got[i].ClusterID)
		require.True(t, transitionAt(t, pool, want.actor.ActorID).Equal(got[i].TransitionAt))
	}

	_, err = st.ListStaleTransitions(ctx, StaleCursor{}, 0)
	require.ErrorContains(t, err, "must be positive")
}

// Transitions with the same transition_at are ordered by actor ID, so a page
// boundary between them neither repeats nor skips one.
func TestListStaleTransitionsPages(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	st, pool := newStore(t)

	empty, err := st.ListStaleTransitions(ctx, StaleCursor{}, 2)
	require.NoError(t, err)
	require.Empty(t, empty)

	want := map[uuid.UUID]bool{}
	for i := range 5 {
		tr := inTransition(t, st, pool, ActionKill, time.Hour)
		want[tr.actor.ActorID] = true
		if i < 3 {
			_, err := pool.Exec(ctx, `UPDATE public.scheduling_actor_run SET transition_at = '2020-01-01T00:00:00.000001Z' WHERE actor_id = $1`, tr.actor.ActorID)
			require.NoError(t, err)
		}
	}

	seen := map[uuid.UUID]bool{}
	var after StaleCursor
	pages := 0
	for {
		page, err := st.ListStaleTransitions(ctx, after, 2)
		require.NoError(t, err)
		for _, tr := range page {
			require.False(t, seen[tr.ActorID], "listed twice")
			seen[tr.ActorID] = true
		}
		pages++
		if len(page) < 2 {
			break
		}
		last := page[len(page)-1]
		after = StaleCursor{TransitionAt: last.TransitionAt, ActorID: last.ActorID}
	}
	require.Equal(t, want, seen)
	require.Equal(t, 3, pages)
}

func TestListStaleTransitionsClosedPool(t *testing.T) {
	t.Parallel()
	st, pool := newStore(t)
	pool.Close()

	_, err := st.ListStaleTransitions(t.Context(), StaleCursor{}, 10)
	require.Error(t, err)
}

type transition struct {
	project uuid.UUID
	actor   Actor
	tid     uuid.UUID
	action  TransitionAction
}

func inTransition(t *testing.T, st *Store, pool *pgxpool.Pool, action TransitionAction, ago time.Duration) transition {
	t.Helper()

	project, record, _ := runningActor(t, st, uuid.New(), strict)
	tid := uuid.New()
	begin(t, st, TransitionRequest{ProjectID: project, ActorID: record.ActorID, TransitionID: tid, Action: action}, BeginStarted)
	setTransitionAgo(t, pool, record.ActorID, ago)

	return transition{project: project, actor: record, tid: tid, action: action}
}

func setTransitionAgo(t *testing.T, pool *pgxpool.Pool, actor uuid.UUID, ago time.Duration) {
	t.Helper()

	tag, err := pool.Exec(t.Context(),
		`UPDATE public.scheduling_actor_run SET transition_at = now() - $2::interval WHERE actor_id = $1`, actor, interval(ago))
	require.NoError(t, err)
	require.EqualValues(t, 1, tag.RowsAffected())
}

func transitionAt(t *testing.T, pool *pgxpool.Pool, actor uuid.UUID) time.Time {
	t.Helper()

	return runRow(t, pool, actor).transitionAt
}

type fullRun struct {
	state                      State
	transitionID               uuid.UUID
	transitionAction           int16
	transitionAt               time.Time
	endTime, reservedAt        time.Time
	refusedUntil, refusedSince *time.Time
	revision                   int64
	counterSlot                int16
	reservationID, projectID   uuid.UUID
	failureMessage             *string
}

func runRow(t *testing.T, pool *pgxpool.Pool, actor uuid.UUID) fullRun {
	t.Helper()

	var r fullRun
	require.NoError(t, pool.QueryRow(t.Context(), `
SELECT state, transition_id, transition_action, transition_at, end_time, reserved_at, refused_until, refused_since,
       revision, counter_slot, reservation_id, project_id, failure_message
  FROM public.scheduling_actor_run WHERE actor_id = $1`, actor).Scan(
		&r.state, &r.transitionID, &r.transitionAction, &r.transitionAt, &r.endTime, &r.reservedAt,
		&r.refusedUntil, &r.refusedSince, &r.revision, &r.counterSlot, &r.reservationID, &r.projectID, &r.failureMessage))

	return r
}
