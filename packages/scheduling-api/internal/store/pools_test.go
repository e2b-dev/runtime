package store

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func TestReadsWithWritePoolFull(t *testing.T) {
	t.Parallel()
	_, pool := newStore(t)
	st := New(pool, secondPool(t, pool))
	project, actor, tid := activeTransition(t, st)
	release := holdPool(t, pool)
	defer release()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	got, err := st.Get(ctx, project, actor)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, StatePausing, got.State)
	waited, err := st.WaitForTransition(ctx, project, actor, tid, time.Microsecond)
	require.NoError(t, err)
	require.Equal(t, WaitWaiting, waited.Outcome)
	run, record, found, err := st.inspectWithActor(ctx, project, actor)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, &tid, run.TransitionID)
	require.Equal(t, got, record)
}

func TestWritesWithReadPoolFull(t *testing.T) {
	t.Parallel()
	_, pool := newStore(t)
	reads := secondPool(t, pool)
	st := New(pool, reads)
	release := holdPool(t, reads)
	defer release()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	project, actor, rid := uuid.New(), uuid.New(), uuid.New()
	reservation := Reservation{ProjectID: project, ActorID: actor, ReservationID: rid, Limit: 10, EndTime: endTime}
	reserved, err := st.Reserve(ctx, reservation)
	require.NoError(t, err)
	require.Equal(t, ReserveReserved, reserved.Outcome)
	putResult, err := st.Put(ctx, rid, newActor(project, actor))
	require.NoError(t, err)
	require.Equal(t, PutStored, putResult)

	tid := uuid.New()
	transition := TransitionRequest{ProjectID: project, ActorID: actor, TransitionID: tid, Action: ActionPause}
	began, err := st.BeginTransition(ctx, transition)
	require.NoError(t, err)
	require.Equal(t, BeginStarted, began.Outcome)
	require.NotNil(t, began.Actor, "the transaction reads its own state change")
	require.Equal(t, StatePausing, began.Actor.State)
	restored, err := st.RestoreRunning(ctx, project, actor, tid, time.Second)
	require.NoError(t, err)
	require.Equal(t, RestoreRestored, restored.Outcome)

	transition.TransitionID = uuid.New()
	transition.Action = ActionKill
	began, err = st.BeginTransition(ctx, transition)
	require.NoError(t, err)
	require.Equal(t, BeginStarted, began.Outcome)
	finished, err := st.FinishTransition(ctx, project, actor, transition.TransitionID, nil)
	require.NoError(t, err)
	require.Equal(t, FinishTransitionFinished, finished)

	reservation.ReservationID = uuid.New()
	reserved, err = st.Reserve(ctx, reservation)
	require.NoError(t, err)
	require.Equal(t, ReserveReserved, reserved.Outcome)
	failed, err := st.FinishStart(ctx, project, actor, reservation.ReservationID, Failure{Message: "start failed"})
	require.NoError(t, err)
	require.Equal(t, FinishStartFinished, failed)
	require.Equal(t, map[int16]int32{0: 0}, liveCounts(t, pool, project))
}

func holdPool(t *testing.T, pool *pgxpool.Pool) func() {
	t.Helper()
	connections := make([]*pgxpool.Conn, 0, pool.Config().MaxConns)
	for range pool.Config().MaxConns {
		conn, err := pool.Acquire(t.Context())
		require.NoError(t, err)
		connections = append(connections, conn)
		t.Cleanup(conn.Release)
	}

	return func() {
		for _, conn := range connections {
			conn.Release()
		}
	}
}
