package server_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/e2b-dev/infra/packages/scheduling-api/internal/db/dbtest"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/scheduling"
)

func TestPauseAndKill(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		action scheduling.TransitionAction
		state  scheduling.SandboxState
	}{
		"pause": {scheduling.TransitionAction_TRANSITION_ACTION_PAUSE, scheduling.SandboxState_SANDBOX_STATE_PAUSING},
		"kill":  {scheduling.TransitionAction_TRANSITION_ACTION_KILL, scheduling.SandboxState_SANDBOX_STATE_KILLING},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			client, pool := newClient(t)
			project := uuid.NewString()
			rid := uuid.New()
			rid[15] = 0x37
			start(t, client, project, rid.String(), 50_000)
			require.Equal(t, int32(1), slotCount(t, pool, project, 7))

			tid := uuid.NewString()
			begun, err := client.BeginTransition(ctx, beginRequest(project, tid, tc.action))
			require.NoError(t, err)
			require.Equal(t, scheduling.BeginTransitionOutcome_BEGIN_TRANSITION_OUTCOME_STARTED, begun.GetOutcome())
			require.Equal(t, tid, begun.GetTransitionId())
			require.Equal(t, tc.action, begun.GetAction())
			require.Equal(t, tc.state, begun.GetSandbox().GetState())
			require.Equal(t, "execution-1", begun.GetSandbox().GetExecutionId())
			require.Equal(t, "node-1", begun.GetSandbox().GetNodeId())
			got, err := client.Get(ctx, scheduling.GetRequest_builder{ProjectId: project, SandboxId: sandboxID}.Build())
			require.NoError(t, err)
			require.Equal(t, tc.state, got.GetSandbox().GetState())

			joined, err := client.BeginTransition(ctx, beginRequest(project, uuid.NewString(), tc.action))
			require.NoError(t, err)
			require.Equal(t, scheduling.BeginTransitionOutcome_BEGIN_TRANSITION_OUTCOME_JOINED, joined.GetOutcome())
			require.Equal(t, tid, joined.GetTransitionId())

			for range 2 {
				finished, err := client.FinishTransition(ctx, finishTransitionRequest(project, tid))
				require.NoError(t, err)
				require.Equal(t, scheduling.FinishTransitionOutcome_FINISH_TRANSITION_OUTCOME_FINISHED, finished.GetOutcome())
				require.Equal(t, int32(0), slotCount(t, pool, project, 7))
			}
			got, err = client.Get(ctx, scheduling.GetRequest_builder{ProjectId: project, SandboxId: sandboxID}.Build())
			require.NoError(t, err)
			require.False(t, got.HasSandbox())

			waited, err := client.WaitForTransition(ctx, waitRequest(project, tid, time.Second))
			require.NoError(t, err)
			require.Equal(t, scheduling.WaitForTransitionOutcome_WAIT_FOR_TRANSITION_OUTCOME_FINISHED, waited.GetOutcome())
			require.False(t, waited.HasFailure())
			repeat, err := client.BeginTransition(ctx, beginRequest(project, tid, tc.action))
			require.NoError(t, err)
			require.Equal(t, scheduling.BeginTransitionOutcome_BEGIN_TRANSITION_OUTCOME_ALREADY_FINISHED, repeat.GetOutcome())
			require.False(t, repeat.HasSandbox())
			require.Empty(t, repeat.GetTransitionId())
		})
	}
}

func TestBeginTransitionOutcomes(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	client, _ := newClient(t)
	project, other := uuid.NewString(), uuid.NewString()

	notFound, err := client.BeginTransition(ctx, beginRequest(project, uuid.NewString(), scheduling.TransitionAction_TRANSITION_ACTION_KILL))
	require.NoError(t, err)
	require.Equal(t, scheduling.BeginTransitionOutcome_BEGIN_TRANSITION_OUTCOME_NOT_FOUND, notFound.GetOutcome())

	rid := uuid.NewString()
	reserved, err := client.Reserve(ctx, reserveRequest(project, sandboxID, rid, 10))
	require.NoError(t, err)
	require.Equal(t, scheduling.ReserveOutcome_RESERVE_OUTCOME_RESERVED, reserved.GetOutcome())
	starting, err := client.BeginTransition(ctx, beginRequest(project, uuid.NewString(), scheduling.TransitionAction_TRANSITION_ACTION_KILL))
	require.NoError(t, err)
	require.Equal(t, scheduling.BeginTransitionOutcome_BEGIN_TRANSITION_OUTCOME_NOT_RUNNING, starting.GetOutcome())
	stored, err := client.Put(ctx, putRequest(project, rid))
	require.NoError(t, err)
	require.Equal(t, scheduling.PutOutcome_PUT_OUTCOME_STORED, stored.GetOutcome())

	intruder, err := client.BeginTransition(ctx, beginRequest(other, uuid.NewString(), scheduling.TransitionAction_TRANSITION_ACTION_KILL))
	require.NoError(t, err)
	require.Equal(t, scheduling.BeginTransitionOutcome_BEGIN_TRANSITION_OUTCOME_NOT_FOUND, intruder.GetOutcome())
	require.False(t, intruder.HasSandbox())

	fenced := beginRequest(project, uuid.NewString(), scheduling.TransitionAction_TRANSITION_ACTION_KILL)
	fenced.SetExpectedExecutionId("execution-0")
	mismatch, err := client.BeginTransition(ctx, fenced)
	require.NoError(t, err)
	require.Equal(t, scheduling.BeginTransitionOutcome_BEGIN_TRANSITION_OUTCOME_EXECUTION_MISMATCH, mismatch.GetOutcome())
	require.Equal(t, "execution-1", mismatch.GetSandbox().GetExecutionId())

	expired := beginRequest(project, uuid.NewString(), scheduling.TransitionAction_TRANSITION_ACTION_PAUSE)
	expired.SetRequireExpired(true)
	notExpired, err := client.BeginTransition(ctx, expired)
	require.NoError(t, err)
	require.Equal(t, scheduling.BeginTransitionOutcome_BEGIN_TRANSITION_OUTCOME_NOT_EXPIRED, notExpired.GetOutcome())

	tid := uuid.NewString()
	pause := beginRequest(project, tid, scheduling.TransitionAction_TRANSITION_ACTION_PAUSE)
	pause.SetExpectedExecutionId("execution-1")
	began, err := client.BeginTransition(ctx, pause)
	require.NoError(t, err)
	require.Equal(t, scheduling.BeginTransitionOutcome_BEGIN_TRANSITION_OUTCOME_STARTED, began.GetOutcome())

	conflict, err := client.BeginTransition(ctx, beginRequest(project, uuid.NewString(), scheduling.TransitionAction_TRANSITION_ACTION_KILL))
	require.NoError(t, err)
	require.Equal(t, scheduling.BeginTransitionOutcome_BEGIN_TRANSITION_OUTCOME_CONFLICT, conflict.GetOutcome())
	require.Equal(t, tid, conflict.GetTransitionId())
	require.Equal(t, scheduling.TransitionAction_TRANSITION_ACTION_PAUSE, conflict.GetAction())

	reused, err := client.BeginTransition(ctx, beginRequest(project, tid, scheduling.TransitionAction_TRANSITION_ACTION_KILL))
	require.NoError(t, err)
	require.Equal(t, scheduling.BeginTransitionOutcome_BEGIN_TRANSITION_OUTCOME_ACTION_MISMATCH, reused.GetOutcome())

	finished, err := client.FinishTransition(ctx, finishTransitionRequest(other, tid))
	require.NoError(t, err)
	require.Equal(t, scheduling.FinishTransitionOutcome_FINISH_TRANSITION_OUTCOME_NOT_FOUND, finished.GetOutcome())
	restored, err := client.RestoreRunning(ctx, restoreRequest(other, tid, time.Second))
	require.NoError(t, err)
	require.Equal(t, scheduling.RestoreRunningOutcome_RESTORE_RUNNING_OUTCOME_NOT_FOUND, restored.GetOutcome())
	waited, err := client.WaitForTransition(ctx, waitRequest(other, tid, time.Second))
	require.NoError(t, err)
	require.Equal(t, scheduling.WaitForTransitionOutcome_WAIT_FOR_TRANSITION_OUTCOME_NOT_FOUND, waited.GetOutcome())
	got, err := client.Get(ctx, scheduling.GetRequest_builder{ProjectId: project, SandboxId: sandboxID}.Build())
	require.NoError(t, err)
	require.Equal(t, scheduling.SandboxState_SANDBOX_STATE_PAUSING, got.GetSandbox().GetState(), "another project changed nothing")
}

// Two replicas share one database. A caller on one replica waits for the
// transition that a caller on the other replica runs.
func TestWaitAcrossReplicas(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	pool := dbtest.MigratedPool(t)
	replicaPool := secondPool(t, pool)
	owner, other := serve(t, pool), serve(t, replicaPool)
	project := uuid.NewString()
	start(t, owner, project, uuid.NewString(), 10)

	pause := uuid.NewString()
	began, err := owner.BeginTransition(ctx, beginRequest(project, pause, scheduling.TransitionAction_TRANSITION_ACTION_PAUSE))
	require.NoError(t, err)
	require.Equal(t, scheduling.BeginTransitionOutcome_BEGIN_TRANSITION_OUTCOME_STARTED, began.GetOutcome())
	joined, err := other.BeginTransition(ctx, beginRequest(project, uuid.NewString(), scheduling.TransitionAction_TRANSITION_ACTION_PAUSE))
	require.NoError(t, err)
	require.Equal(t, scheduling.BeginTransitionOutcome_BEGIN_TRANSITION_OUTCOME_JOINED, joined.GetOutcome())
	require.Equal(t, pause, joined.GetTransitionId())

	waiter := waitAsync(t, other, replicaPool, waitRequest(project, joined.GetTransitionId(), 5*time.Second))
	restored, err := owner.RestoreRunning(ctx, restoreRequest(project, pause, 10*time.Second))
	require.NoError(t, err)
	require.Equal(t, scheduling.RestoreRunningOutcome_RESTORE_RUNNING_OUTCOME_RESTORED, restored.GetOutcome())
	require.Equal(t, 10*time.Second, restored.GetRefusedUntil().AsTime().Sub(restored.GetRefusedSince().AsTime()))
	require.Equal(t, scheduling.WaitForTransitionOutcome_WAIT_FOR_TRANSITION_OUTCOME_RESTORED, (<-waiter).GetOutcome())
	got, err := other.Get(ctx, scheduling.GetRequest_builder{ProjectId: project, SandboxId: sandboxID}.Build())
	require.NoError(t, err)
	require.Equal(t, scheduling.SandboxState_SANDBOX_STATE_RUNNING, got.GetSandbox().GetState())
	require.Equal(t, int32(1), slotCount(t, pool, project, 0))
	repeat, err := owner.BeginTransition(ctx, beginRequest(project, pause, scheduling.TransitionAction_TRANSITION_ACTION_PAUSE))
	require.NoError(t, err)
	require.Equal(t, scheduling.BeginTransitionOutcome_BEGIN_TRANSITION_OUTCOME_ALREADY_RESTORED, repeat.GetOutcome())
	require.Equal(t, scheduling.SandboxState_SANDBOX_STATE_RUNNING, repeat.GetSandbox().GetState())

	kill := uuid.NewString()
	began, err = other.BeginTransition(ctx, beginRequest(project, kill, scheduling.TransitionAction_TRANSITION_ACTION_KILL))
	require.NoError(t, err)
	require.Equal(t, scheduling.BeginTransitionOutcome_BEGIN_TRANSITION_OUTCOME_STARTED, began.GetOutcome())
	waiter = waitAsync(t, owner, pool, waitRequest(project, kill, 5*time.Second))
	finish := finishTransitionRequest(project, kill)
	finish.SetFailure(scheduling.Failure_builder{Message: "node unreachable, VM removed", Code: 14}.Build())
	finished, err := other.FinishTransition(ctx, finish)
	require.NoError(t, err)
	require.Equal(t, scheduling.FinishTransitionOutcome_FINISH_TRANSITION_OUTCOME_FINISHED, finished.GetOutcome())
	waited := <-waiter
	require.Equal(t, scheduling.WaitForTransitionOutcome_WAIT_FOR_TRANSITION_OUTCOME_FINISHED, waited.GetOutcome())
	require.Equal(t, "node unreachable, VM removed", waited.GetFailure().GetMessage())
	require.Equal(t, int32(14), waited.GetFailure().GetCode())
	require.Equal(t, int32(0), slotCount(t, pool, project, 0))

	stale, err := owner.WaitForTransition(ctx, waitRequest(project, pause, time.Second))
	require.NoError(t, err)
	require.Equal(t, scheduling.WaitForTransitionOutcome_WAIT_FOR_TRANSITION_OUTCOME_UNKNOWN, stale.GetOutcome())
	late, err := owner.FinishTransition(ctx, finishTransitionRequest(project, pause))
	require.NoError(t, err)
	require.Equal(t, scheduling.FinishTransitionOutcome_FINISH_TRANSITION_OUTCOME_NOT_FOUND, late.GetOutcome())
}

// 100 callers on three replicas wait for one transition. Each replica reads
// the transition for all of its callers.
func TestWaitSharedAcrossReplicas(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	pool := dbtest.MigratedPool(t)
	owner := serve(t, pool)
	project := uuid.NewString()
	start(t, owner, project, uuid.NewString(), 10)
	tid := uuid.NewString()
	began, err := owner.BeginTransition(ctx, beginRequest(project, tid, scheduling.TransitionAction_TRANSITION_ACTION_KILL))
	require.NoError(t, err)
	require.Equal(t, scheduling.BeginTransitionOutcome_BEGIN_TRANSITION_OUTCOME_STARTED, began.GetOutcome())

	pools := []*pgxpool.Pool{secondPool(t, pool), secondPool(t, pool), secondPool(t, pool)}
	clients := make([]scheduling.SchedulingServiceClient, len(pools))
	for i, replicaPool := range pools {
		clients[i] = serve(t, replicaPool)
	}

	const callers = 100
	const budget = time.Second
	responses := make(chan *scheduling.WaitForTransitionResponse, callers)
	var wg sync.WaitGroup
	for i := range callers {
		wg.Go(func() {
			resp, err := clients[i%len(clients)].WaitForTransition(ctx, waitRequest(project, tid, budget))
			assert.NoError(t, err)
			responses <- resp
		})
	}
	wg.Wait()
	close(responses)
	for resp := range responses {
		require.Equal(t, scheduling.WaitForTransitionOutcome_WAIT_FOR_TRANSITION_OUTCOME_WAITING, resp.GetOutcome())
	}

	var reads int64
	for _, replicaPool := range pools {
		replicaReads := replicaPool.Stat().AcquireCount()
		require.Positive(t, replicaReads)
		reads += replicaReads
	}
	t.Logf("%d callers on %d replicas, %d reads", callers, len(pools), reads)
	require.Less(t, reads, int64(callers), "callers share the reads of their replica")

	waiters := make([]<-chan *scheduling.WaitForTransitionResponse, len(clients))
	for i, client := range clients {
		waiters[i] = waitAsync(t, client, pools[i], waitRequest(project, tid, 5*time.Second))
	}
	finished, err := owner.FinishTransition(ctx, finishTransitionRequest(project, tid))
	require.NoError(t, err)
	require.Equal(t, scheduling.FinishTransitionOutcome_FINISH_TRANSITION_OUTCOME_FINISHED, finished.GetOutcome())
	for _, waiter := range waiters {
		require.Equal(t, scheduling.WaitForTransitionOutcome_WAIT_FOR_TRANSITION_OUTCOME_FINISHED, (<-waiter).GetOutcome())
	}
}

func TestWaitForTransitionEnds(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	client, _ := newClient(t)
	project := uuid.NewString()
	start(t, client, project, uuid.NewString(), 10)
	tid := uuid.NewString()
	began, err := client.BeginTransition(ctx, beginRequest(project, tid, scheduling.TransitionAction_TRANSITION_ACTION_PAUSE))
	require.NoError(t, err)
	require.Equal(t, scheduling.BeginTransitionOutcome_BEGIN_TRANSITION_OUTCOME_STARTED, began.GetOutcome())

	started := time.Now()
	waited, err := client.WaitForTransition(ctx, waitRequest(project, tid, 300*time.Millisecond))
	require.NoError(t, err)
	require.Equal(t, scheduling.WaitForTransitionOutcome_WAIT_FOR_TRANSITION_OUTCOME_WAITING, waited.GetOutcome())
	require.GreaterOrEqual(t, time.Since(started), 300*time.Millisecond)

	short, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	_, err = client.WaitForTransition(short, waitRequest(project, tid, 5*time.Second))
	require.Equal(t, codes.DeadlineExceeded, status.Code(err))

	cancelled, cancelNow := context.WithCancel(ctx)
	cancelNow()
	_, err = client.WaitForTransition(cancelled, waitRequest(project, tid, 5*time.Second))
	require.Equal(t, codes.Canceled, status.Code(err))

	missing, err := client.WaitForTransition(ctx, waitRequest(uuid.NewString(), tid, time.Second))
	require.NoError(t, err)
	require.Equal(t, scheduling.WaitForTransitionOutcome_WAIT_FOR_TRANSITION_OUTCOME_NOT_FOUND, missing.GetOutcome())
	unknown, err := client.WaitForTransition(ctx, waitRequest(project, uuid.NewString(), time.Second))
	require.NoError(t, err)
	require.Equal(t, scheduling.WaitForTransitionOutcome_WAIT_FOR_TRANSITION_OUTCOME_UNKNOWN, unknown.GetOutcome())
}

func TestRestoreRunningOutcomes(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	client, pool := newClient(t)
	project := uuid.NewString()
	start(t, client, project, uuid.NewString(), 10)

	kill := uuid.NewString()
	began, err := client.BeginTransition(ctx, beginRequest(project, kill, scheduling.TransitionAction_TRANSITION_ACTION_KILL))
	require.NoError(t, err)
	require.Equal(t, scheduling.BeginTransitionOutcome_BEGIN_TRANSITION_OUTCOME_STARTED, began.GetOutcome())
	restored, err := client.RestoreRunning(ctx, restoreRequest(project, kill, time.Second))
	require.NoError(t, err)
	require.Equal(t, scheduling.RestoreRunningOutcome_RESTORE_RUNNING_OUTCOME_RESTORED, restored.GetOutcome())
	require.True(t, restored.HasRefusedUntil())
	finished, err := client.FinishTransition(ctx, finishTransitionRequest(project, kill))
	require.NoError(t, err)
	require.Equal(t, scheduling.FinishTransitionOutcome_FINISH_TRANSITION_OUTCOME_RESTORED, finished.GetOutcome())
	require.Equal(t, int32(1), slotCount(t, pool, project, 0))
	waited, err := client.WaitForTransition(ctx, scheduling.WaitForTransitionRequest_builder{ProjectId: project, SandboxId: sandboxID, TransitionId: kill, MaxWait: durationpb.New(time.Second)}.Build())
	require.NoError(t, err)
	require.Equal(t, scheduling.WaitForTransitionOutcome_WAIT_FOR_TRANSITION_OUTCOME_RESTORED, waited.GetOutcome())

	missing, err := client.RestoreRunning(ctx, restoreRequest(project, uuid.NewString(), time.Second))
	require.NoError(t, err)
	require.Equal(t, scheduling.RestoreRunningOutcome_RESTORE_RUNNING_OUTCOME_NOT_FOUND, missing.GetOutcome())
}

func start(t *testing.T, client scheduling.SchedulingServiceClient, project, rid string, limit int64) {
	t.Helper()

	reserved, err := client.Reserve(t.Context(), reserveRequest(project, sandboxID, rid, limit))
	require.NoError(t, err)
	require.Equal(t, scheduling.ReserveOutcome_RESERVE_OUTCOME_RESERVED, reserved.GetOutcome())
	stored, err := client.Put(t.Context(), putRequest(project, rid))
	require.NoError(t, err)
	require.Equal(t, scheduling.PutOutcome_PUT_OUTCOME_STORED, stored.GetOutcome())
}

// waitAsync calls WaitForTransition and returns once the call has read the
// database twice through pool, the pool of the replica that serves it.
func waitAsync(
	t *testing.T, client scheduling.SchedulingServiceClient, pool *pgxpool.Pool, req *scheduling.WaitForTransitionRequest,
) <-chan *scheduling.WaitForTransitionResponse {
	t.Helper()

	before := pool.Stat().AcquireCount()
	done := make(chan *scheduling.WaitForTransitionResponse, 1)
	go func() {
		resp, err := client.WaitForTransition(t.Context(), req)
		assert.NoError(t, err)
		done <- resp
	}()
	require.Eventually(t, func() bool { return pool.Stat().AcquireCount() >= before+2 }, 5*time.Second, time.Millisecond)
	require.Empty(t, done, "the transition is still active")

	return done
}

func secondPool(t *testing.T, pool *pgxpool.Pool) *pgxpool.Pool {
	t.Helper()

	config, err := pgxpool.ParseConfig(dbtest.ConnString(t, pool))
	require.NoError(t, err)
	config.MaxConns = 4
	replica, err := pgxpool.NewWithConfig(t.Context(), config)
	require.NoError(t, err)
	t.Cleanup(replica.Close)

	return replica
}

func beginRequest(project, tid string, action scheduling.TransitionAction) *scheduling.BeginTransitionRequest {
	return scheduling.BeginTransitionRequest_builder{
		ProjectId:    project,
		SandboxId:    sandboxID,
		TransitionId: tid,
		Action:       action,
	}.Build()
}

func finishTransitionRequest(project, tid string) *scheduling.FinishTransitionRequest {
	return scheduling.FinishTransitionRequest_builder{
		ProjectId:    project,
		SandboxId:    sandboxID,
		TransitionId: tid,
	}.Build()
}

func restoreRequest(project, tid string, retryAfter time.Duration) *scheduling.RestoreRunningRequest {
	return scheduling.RestoreRunningRequest_builder{
		ProjectId:    project,
		SandboxId:    sandboxID,
		TransitionId: tid,
		RetryAfter:   durationpb.New(retryAfter),
	}.Build()
}

func waitRequest(project, tid string, maxWait time.Duration) *scheduling.WaitForTransitionRequest {
	return scheduling.WaitForTransitionRequest_builder{
		ProjectId:    project,
		SandboxId:    sandboxID,
		TransitionId: tid,
		MaxWait:      durationpb.New(maxWait),
	}.Build()
}
