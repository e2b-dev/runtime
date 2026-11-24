package state_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	statepb "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/scheduling/state"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxstore/sandboxtypes"
)

const shortWait = 50 * time.Millisecond

func nanosecondRecord() sandboxtypes.Sandbox {
	sbx := minimalRecord(uuid.New(), newSandboxID())
	sbx.EndTime = sbx.EndTime.Add(123456789 * time.Nanosecond)

	return sbx
}

func (s redisSetup) beginRemoving(ctx context.Context, t *testing.T, sbx sandboxtypes.Sandbox, options *statepb.RemoveOptions) *statepb.BeginRemovingResponse {
	t.Helper()

	resp, err := s.client.BeginRemoving(ctx, statepb.BeginRemovingRequest_builder{
		TeamId: sbx.TeamID.String(), SandboxId: sbx.SandboxID, Options: options,
	}.Build())
	require.NoError(t, err)

	return resp
}

func (s redisSetup) startTransition(ctx context.Context, t *testing.T, sbx sandboxtypes.Sandbox, action statepb.RemoveAction) *statepb.StateTransition {
	t.Helper()

	resp := s.beginRemoving(ctx, t, sbx, removeOptions(action))
	require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, resp.GetOutcome())
	require.NotEmpty(t, resp.GetTransition().GetId())

	return resp.GetTransition()
}

func (s redisSetup) finishTransition(ctx context.Context, t *testing.T, sbx sandboxtypes.Sandbox, transitionID string, action statepb.RemoveAction, outcome sandboxtypes.TransitionOutcome) {
	t.Helper()

	resp, err := s.client.FinishTransition(ctx, statepb.FinishTransitionRequest_builder{
		TeamId: sbx.TeamID.String(), SandboxId: sbx.SandboxID, TransitionId: transitionID, Action: action,
		Result: statepb.TransitionResult_builder{
			Failed: outcome.Failed, Error: outcome.Error, SkipRestore: outcome.SkipRestore, Restored: outcome.Restored, RefusalClass: outcome.RefusalClass,
		}.Build(),
	}.Build())
	require.NoError(t, err)
	require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, resp.GetOutcome())
}

func waitTransition(ctx context.Context, t *testing.T, client statepb.StateServiceClient, sbx sandboxtypes.Sandbox, transitionID string) *statepb.WaitForTransitionResponse {
	t.Helper()

	resp, err := client.WaitForTransition(ctx, statepb.WaitForTransitionRequest_builder{
		TeamId: sbx.TeamID.String(), SandboxId: sbx.SandboxID, TransitionId: transitionID,
	}.Build())
	require.NoError(t, err)

	return resp
}

func TestRedisBeginRemovingActions(t *testing.T) {
	t.Parallel()

	s := setupRedis(t)
	tests := map[statepb.RemoveAction]sandboxtypes.StateAction{
		statepb.RemoveAction_REMOVE_ACTION_PAUSE:    sandboxtypes.StateActionPause,
		statepb.RemoveAction_REMOVE_ACTION_KILL:     sandboxtypes.StateActionKill,
		statepb.RemoveAction_REMOVE_ACTION_SNAPSHOT: sandboxtypes.StateActionSnapshot,
	}
	for action, want := range tests {
		t.Run(action.String(), func(t *testing.T) {
			t.Parallel()

			ctx := callContext(t)
			sbx := nanosecondRecord()
			require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.add(ctx, t, sbx))

			resp := s.beginRemoving(ctx, t, sbx, removeOptions(action))
			require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, resp.GetOutcome())
			assert.False(t, resp.GetAlreadyDone())
			transition := resp.GetTransition()
			require.NotEmpty(t, transition.GetId(), "this call started the transition")
			assert.Empty(t, transition.GetInFlightId())
			require.True(t, transition.HasOriginalEndTime())
			assert.Equal(t, sbx.EndTime, transition.GetOriginalEndTime().AsTime(), "the original end time to the nanosecond")

			stored := s.stored(ctx, t, sbx)
			assert.Equal(t, want.TargetState, stored.State)
			assert.Equal(t, stored, decode(t, transition.GetRecord()), "the transition carries the record as written")

			s.finishTransition(ctx, t, sbx, transition.GetId(), action, sandboxtypes.TransitionOutcome{})
			if want.Effect == sandboxtypes.TransitionTransient {
				assert.Equal(t, sandboxtypes.StateRunning, s.stored(ctx, t, sbx).State, "a finished transient action restores running")
			} else {
				assert.Equal(t, want.TargetState, s.stored(ctx, t, sbx).State)
			}

			waited := waitTransition(ctx, t, s.client, sbx, transition.GetId())
			assert.Equal(t, statepb.WaitForTransitionOutcome_WAIT_FOR_TRANSITION_OUTCOME_FINISHED, waited.GetOutcome())
			assert.Equal(t, transition.GetId(), waited.GetTransitionId())
		})
	}
}

func TestRedisBeginRemovingRefusals(t *testing.T) {
	t.Parallel()

	s := setupRedis(t)
	kill := statepb.RemoveAction_REMOVE_ACTION_KILL
	pause := statepb.RemoveAction_REMOVE_ACTION_PAUSE

	requireRead := func(t *testing.T, resp *statepb.BeginRemovingResponse, stored sandboxtypes.Sandbox) {
		t.Helper()

		require.NotNil(t, resp.GetTransition(), "the refusal carries the transition")
		assert.Equal(t, stored, decode(t, resp.GetTransition().GetRecord()), "with the record as read")
		assert.Empty(t, resp.GetTransition().GetId())
		assert.False(t, resp.GetTransition().HasOriginalEndTime())
	}

	t.Run("already in the target state", func(t *testing.T) {
		t.Parallel()

		ctx := callContext(t)
		sbx := minimalRecord(uuid.New(), newSandboxID())
		require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.add(ctx, t, sbx))
		s.finishTransition(ctx, t, sbx, s.startTransition(ctx, t, sbx, kill).GetId(), kill, sandboxtypes.TransitionOutcome{})

		resp := s.beginRemoving(ctx, t, sbx, removeOptions(kill))
		assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, resp.GetOutcome())
		assert.True(t, resp.GetAlreadyDone())
		requireRead(t, resp, s.stored(ctx, t, sbx))
	})

	t.Run("absent", func(t *testing.T) {
		t.Parallel()

		resp := s.beginRemoving(callContext(t), t, minimalRecord(uuid.New(), newSandboxID()), removeOptions(kill))
		assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_NOT_FOUND, resp.GetOutcome())
		assert.Nil(t, resp.GetTransition())
	})

	t.Run("another execution expected", func(t *testing.T) {
		t.Parallel()

		ctx := callContext(t)
		sbx := minimalRecord(uuid.New(), newSandboxID())
		require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.add(ctx, t, sbx))
		resp := s.beginRemoving(ctx, t, sbx, statepb.RemoveOptions_builder{Action: kill, ExpectExecutionId: uuid.NewString()}.Build())
		assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_EXECUTION_MISMATCH, resp.GetOutcome())
		requireRead(t, resp, sbx)
		assert.Equal(t, sbx, s.stored(ctx, t, sbx), "nothing written")
	})

	t.Run("held", func(t *testing.T) {
		t.Parallel()

		ctx := callContext(t)
		sbx := s.addHeldRecord(ctx, t)
		require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.holdRPC(ctx, t, holdOf(sbx, "hold"), time.Now().Add(time.Hour)))
		resp := s.beginRemoving(ctx, t, sbx, removeOptions(kill))
		assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_LIFECYCLE_HELD, resp.GetOutcome())
		requireRead(t, resp, s.stored(ctx, t, sbx))
	})

	t.Run("eviction of a live record", func(t *testing.T) {
		t.Parallel()

		ctx := callContext(t)
		sbx := minimalRecord(uuid.New(), newSandboxID())
		require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.add(ctx, t, sbx))
		resp := s.beginRemoving(ctx, t, sbx, statepb.RemoveOptions_builder{Action: pause, Eviction: true}.Build())
		assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_EVICTION_NOT_NEEDED, resp.GetOutcome())
		requireRead(t, resp, sbx)
	})

	t.Run("eviction with a transition in flight", func(t *testing.T) {
		t.Parallel()

		ctx := callContext(t)
		sbx := minimalRecord(uuid.New(), newSandboxID())
		require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.add(ctx, t, sbx))
		open := s.startTransition(ctx, t, sbx, pause)
		resp := s.beginRemoving(ctx, t, sbx, statepb.RemoveOptions_builder{Action: pause, Eviction: true}.Build())
		assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_EVICTION_IN_PROGRESS, resp.GetOutcome())
		requireRead(t, resp, s.stored(ctx, t, sbx))
		s.finishTransition(ctx, t, sbx, open.GetId(), pause, sandboxtypes.TransitionOutcome{})
	})

	t.Run("disallowed target", func(t *testing.T) {
		t.Parallel()

		ctx := callContext(t)
		sbx := minimalRecord(uuid.New(), newSandboxID())
		require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.add(ctx, t, sbx))
		s.finishTransition(ctx, t, sbx, s.startTransition(ctx, t, sbx, kill).GetId(), kill, sandboxtypes.TransitionOutcome{})

		resp := s.beginRemoving(ctx, t, sbx, removeOptions(pause))
		assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_INVALID_TRANSITION, resp.GetOutcome())
		assert.Equal(t, statepb.SandboxState_SANDBOX_STATE_KILLING, resp.GetCurrentState())
		requireRead(t, resp, s.stored(ctx, t, sbx))
	})

	t.Run("transition in flight", func(t *testing.T) {
		t.Parallel()

		ctx := callContext(t)
		sbx := minimalRecord(uuid.New(), newSandboxID())
		require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.add(ctx, t, sbx))
		open := s.startTransition(ctx, t, sbx, pause)

		for _, action := range []statepb.RemoveAction{pause, kill} {
			resp := s.beginRemoving(ctx, t, sbx, removeOptions(action))
			assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, resp.GetOutcome(), action.String())
			assert.False(t, resp.GetAlreadyDone())
			assert.Equal(t, open.GetId(), resp.GetTransition().GetInFlightId(), "%v returns the open transition without waiting for it", action)
			assert.Empty(t, resp.GetTransition().GetId())
		}
		resp := s.beginRemoving(ctx, t, sbx, removeOptions(statepb.RemoveAction_REMOVE_ACTION_SNAPSHOT))
		assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_INVALID_TRANSITION, resp.GetOutcome())
		assert.Equal(t, statepb.SandboxState_SANDBOX_STATE_PAUSING, resp.GetCurrentState())

		assert.Equal(t, sandboxtypes.StatePausing, s.stored(ctx, t, sbx).State)
		s.finishTransition(ctx, t, sbx, open.GetId(), pause, sandboxtypes.TransitionOutcome{})
	})
}

func TestRedisRestoreRunning(t *testing.T) {
	t.Parallel()

	s := setupRedis(t)
	pause := statepb.RemoveAction_REMOVE_ACTION_PAUSE

	t.Run("refused pause", func(t *testing.T) {
		t.Parallel()

		ctx := callContext(t)
		sbx := nanosecondRecord()
		require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.add(ctx, t, sbx))
		transition := s.startTransition(ctx, t, sbx, pause)

		before := time.Now()
		resp, err := s.client.RestoreRunning(ctx, statepb.RestoreRunningRequest_builder{
			Transition: transition,
			Options:    statepb.RestoreOptions_builder{RetryAfter: durationpb.New(time.Minute)}.Build(),
		}.Build())
		after := time.Now()
		require.NoError(t, err)
		require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, resp.GetOutcome())
		restored := decode(t, resp.GetRecord())
		assert.Equal(t, sandboxtypes.StateRunning, restored.State)
		assert.Equal(t, sbx.EndTime, restored.EndTime, "the original end time")
		assert.False(t, restored.RefusedUntil.Before(before.Add(time.Minute)), "retry window from retry_after")
		assert.False(t, restored.RefusedUntil.After(after.Add(time.Minute)), "retry window from retry_after")
		assert.Equal(t, restored, s.stored(ctx, t, sbx))

		s.finishTransition(ctx, t, sbx, transition.GetId(), pause, sandboxtypes.TransitionOutcomeOf(&sandboxtypes.RestoredTransitionError{Refusal: sandboxtypes.PauseQueueExhaustedError{}}))
		waited := waitTransition(ctx, t, s.client, sbx, transition.GetId())
		assert.Equal(t, statepb.WaitForTransitionOutcome_WAIT_FOR_TRANSITION_OUTCOME_RESTORED, waited.GetOutcome())
		assert.Empty(t, waited.GetRefusalClass())
	})

	t.Run("stale transition", func(t *testing.T) {
		t.Parallel()

		ctx := callContext(t)
		sbx := minimalRecord(uuid.New(), newSandboxID())
		require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.add(ctx, t, sbx))
		transition := s.startTransition(ctx, t, sbx, pause)
		stale := proto.CloneOf(transition)
		record := decode(t, stale.GetRecord())
		record.ExecutionID = uuid.NewString()
		stale.SetRecord(encode(t, record))

		resp, err := s.client.RestoreRunning(ctx, statepb.RestoreRunningRequest_builder{Transition: stale}.Build())
		require.NoError(t, err)
		assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_EXECUTION_MISMATCH, resp.GetOutcome())
		assert.Empty(t, resp.GetRecord())
		assert.Equal(t, sandboxtypes.StatePausing, s.stored(ctx, t, sbx).State, "nothing written")
		s.finishTransition(ctx, t, sbx, transition.GetId(), pause, sandboxtypes.TransitionOutcome{})
	})

	t.Run("record left the transition's state", func(t *testing.T) {
		t.Parallel()

		ctx := callContext(t)
		sbx := minimalRecord(uuid.New(), newSandboxID())
		require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.add(ctx, t, sbx))
		snapshot := statepb.RemoveAction_REMOVE_ACTION_SNAPSHOT
		transition := s.startTransition(ctx, t, sbx, snapshot)
		s.finishTransition(ctx, t, sbx, transition.GetId(), snapshot, sandboxtypes.TransitionOutcome{})

		resp, err := s.client.RestoreCheckpointed(ctx, statepb.RestoreCheckpointedRequest_builder{Transition: transition}.Build())
		require.NoError(t, err)
		assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_INVALID_TRANSITION, resp.GetOutcome())
		assert.Equal(t, statepb.SandboxState_SANDBOX_STATE_RUNNING, resp.GetCurrentState())
	})

	for name, placement := range map[string]*statepb.NetworkPlacement{
		"checkpointed with a placement": statepb.NetworkPlacement_builder{
			ExecutionId: uuid.NewString(), NetworkMode: "full-router", RouterId: "router-b", IngressAddress: "10.8.0.2", IngressPort: 5010,
		}.Build(),
		"checkpointed on the worker's own network": nil,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ctx := callContext(t)
			sbx := minimalRecord(uuid.New(), newSandboxID())
			sbx.NetworkPlacement = &orchestrator.SandboxNetworkPlacement{ExecutionId: sbx.ExecutionID, NetworkMode: "egress-router", RouterId: "router-a"}
			require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.add(ctx, t, sbx))
			snapshot := statepb.RemoveAction_REMOVE_ACTION_SNAPSHOT
			transition := s.startTransition(ctx, t, sbx, snapshot)

			resp, err := s.client.RestoreCheckpointed(ctx, statepb.RestoreCheckpointedRequest_builder{Transition: transition, Placement: placement}.Build())
			require.NoError(t, err)
			require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, resp.GetOutcome())
			restored := decode(t, resp.GetRecord())
			assert.Equal(t, sandboxtypes.StateRunning, restored.State)
			if placement == nil {
				assert.Nil(t, restored.NetworkPlacement)
			} else {
				want := &orchestrator.SandboxNetworkPlacement{}
				copyByName(placement.ProtoReflect(), want.ProtoReflect())
				assert.True(t, proto.Equal(want, restored.NetworkPlacement), "got %v", restored.NetworkPlacement)
			}
			assert.JSONEq(t, string(encode(t, restored)), string(encode(t, s.stored(ctx, t, sbx))))
			s.finishTransition(ctx, t, sbx, transition.GetId(), snapshot, sandboxtypes.TransitionOutcome{SkipRestore: true})
		})
	}
}

func TestRedisWaitForTransition(t *testing.T) {
	t.Parallel()

	s := setupRedis(t)
	short := newClientWithWait(t, s.records, shortWait)
	pause := statepb.RemoveAction_REMOVE_ACTION_PAUSE

	t.Run("nothing open", func(t *testing.T) {
		t.Parallel()

		ctx := callContext(t)
		sbx := minimalRecord(uuid.New(), newSandboxID())
		require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.add(ctx, t, sbx))
		resp := waitTransition(ctx, t, s.client, sbx, "")
		assert.Equal(t, statepb.WaitForTransitionOutcome_WAIT_FOR_TRANSITION_OUTCOME_FINISHED, resp.GetOutcome())
		assert.Empty(t, resp.GetTransitionId())
	})

	restored := func(refusal error) sandboxtypes.TransitionOutcome {
		return sandboxtypes.TransitionOutcomeOf(&sandboxtypes.RestoredTransitionError{Refusal: refusal})
	}
	tests := []struct {
		name    string
		outcome sandboxtypes.TransitionOutcome
		want    statepb.WaitForTransitionOutcome
		class   string
		failure string
	}{
		{"success", sandboxtypes.TransitionOutcome{}, statepb.WaitForTransitionOutcome_WAIT_FOR_TRANSITION_OUTCOME_FINISHED, "", ""},
		{"failure", sandboxtypes.TransitionOutcomeOf(errors.New("boom")), statepb.WaitForTransitionOutcome_WAIT_FOR_TRANSITION_OUTCOME_FAILED, "", "boom"},
		{"restored busy node", restored(sandboxtypes.PauseQueueExhaustedError{}), statepb.WaitForTransitionOutcome_WAIT_FOR_TRANSITION_OUTCOME_RESTORED, "", ""},
		{"restored checkpoint", restored(sandboxtypes.PauseRefusedCheckpointInProgressError{}), statepb.WaitForTransitionOutcome_WAIT_FOR_TRANSITION_OUTCOME_RESTORED, "checkpoint_in_progress", ""},
		{"restored envd", restored(sandboxtypes.PauseRefusedEnvdUnresponsiveError{}), statepb.WaitForTransitionOutcome_WAIT_FOR_TRANSITION_OUTCOME_RESTORED, "envd_unresponsive", ""},
		{"restored unsupported", restored(sandboxtypes.PauseUnsupportedError{}), statepb.WaitForTransitionOutcome_WAIT_FOR_TRANSITION_OUTCOME_RESTORED, "unsupported", ""},
	}
	for _, tt := range tests {
		t.Run("ends between two polls "+tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := callContext(t)
			sbx := minimalRecord(uuid.New(), newSandboxID())
			require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.add(ctx, t, sbx))
			id := s.startTransition(ctx, t, sbx, pause).GetId()

			resp := waitTransition(ctx, t, short, sbx, id)
			assert.Equal(t, statepb.WaitForTransitionOutcome_WAIT_FOR_TRANSITION_OUTCOME_WAITING, resp.GetOutcome())
			assert.Equal(t, id, resp.GetTransitionId())

			for {
				resp = waitTransition(ctx, t, short, sbx, "")
				require.Equal(t, statepb.WaitForTransitionOutcome_WAIT_FOR_TRANSITION_OUTCOME_WAITING, resp.GetOutcome())
				require.Contains(t, []string{"", id}, resp.GetTransitionId(), "an empty ID resolves to the open transition, or stays empty when the wait ended before the marker was read")
				if resp.GetTransitionId() == id {
					break
				}
			}

			s.finishTransition(ctx, t, sbx, id, pause, tt.outcome)

			resp = waitTransition(ctx, t, s.client, sbx, id)
			assert.Equal(t, tt.want, resp.GetOutcome(), "the outcome survives the gap between polls")
			assert.Equal(t, id, resp.GetTransitionId())
			assert.Equal(t, tt.class, resp.GetRefusalClass())
			assert.Equal(t, tt.failure, resp.GetFailure())
		})
	}

	t.Run("another team", func(t *testing.T) {
		t.Parallel()

		ctx := callContext(t)
		sbx := minimalRecord(uuid.New(), newSandboxID())
		require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.add(ctx, t, sbx))
		id := s.startTransition(ctx, t, sbx, pause).GetId()
		other := sbx
		other.TeamID = uuid.New()
		for _, requested := range []string{id, ""} {
			resp := waitTransition(ctx, t, s.client, other, requested)
			assert.Equal(t, statepb.WaitForTransitionOutcome_WAIT_FOR_TRANSITION_OUTCOME_NOT_FOUND, resp.GetOutcome())
			assert.Equal(t, requested, resp.GetTransitionId())
		}
		s.finishTransition(ctx, t, sbx, id, pause, sandboxtypes.TransitionOutcome{})
	})

	t.Run("caller deadline", func(t *testing.T) {
		t.Parallel()

		ctx := callContext(t)
		sbx := minimalRecord(uuid.New(), newSandboxID())
		require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, s.add(ctx, t, sbx))
		id := s.startTransition(ctx, t, sbx, pause).GetId()

		waitCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		defer cancel()
		_, err := s.client.WaitForTransition(waitCtx, statepb.WaitForTransitionRequest_builder{
			TeamId: sbx.TeamID.String(), SandboxId: sbx.SandboxID, TransitionId: id,
		}.Build())
		requireCode(t, err, codes.DeadlineExceeded)
		s.finishTransition(ctx, t, sbx, id, pause, sandboxtypes.TransitionOutcome{})
	})
}

func TestRedisWaitForTransitionAcrossServers(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	waiting, finishing := setupRedisOn(t, client), setupRedisOn(t, client)
	observed := newObservedStorage(waiting.records)
	waiter := newClient(t, observed)
	ctx := callContext(t)
	sbx := minimalRecord(uuid.New(), newSandboxID())
	require.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, finishing.add(ctx, t, sbx))
	id := finishing.startTransition(ctx, t, sbx, statepb.RemoveAction_REMOVE_ACTION_PAUSE).GetId()

	done := make(chan *statepb.WaitForTransitionResponse, 1)
	go func() {
		resp, err := waiter.WaitForTransition(ctx, statepb.WaitForTransitionRequest_builder{
			TeamId: sbx.TeamID.String(), SandboxId: sbx.SandboxID, TransitionId: id,
		}.Build())
		assert.NoError(t, err)
		done <- resp
	}()
	observed.awaitWaits(t, 1)
	finishing.finishTransition(ctx, t, sbx, id, statepb.RemoveAction_REMOVE_ACTION_PAUSE, sandboxtypes.TransitionOutcomeOf(errors.New("boom")))

	select {
	case resp := <-done:
		assert.Equal(t, statepb.WaitForTransitionOutcome_WAIT_FOR_TRANSITION_OUTCOME_FAILED, resp.GetOutcome())
		assert.Equal(t, "boom", resp.GetFailure())
		assert.Equal(t, id, resp.GetTransitionId())
	case <-time.After(hangTimeout):
		require.Fail(t, "the wait never ended")
	}
}
