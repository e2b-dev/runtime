package remote_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	reservationpb "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/scheduling/reservation"
	statepb "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/scheduling/state"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxstore/remote"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxstore/sandboxtypes"
)

type stateCall struct {
	respond func(record []byte, outcome statepb.StateOutcome) proto.Message
	call    func(ctx context.Context, s *remote.Storage) (any, error)
	ok      any
	refused any
	target  sandboxtypes.State
}

var outcomeSentinels = map[statepb.StateOutcome]error{
	statepb.StateOutcome_STATE_OUTCOME_NOT_FOUND:            sandboxtypes.ErrNotFound,
	statepb.StateOutcome_STATE_OUTCOME_LIFECYCLE_HELD:       sandboxtypes.ErrLifecycleHeld,
	statepb.StateOutcome_STATE_OUTCOME_EXECUTION_MISMATCH:   sandboxtypes.ErrExecutionMismatch,
	statepb.StateOutcome_STATE_OUTCOME_LOCK_LOST:            sandboxtypes.ErrLockLost,
	statepb.StateOutcome_STATE_OUTCOME_HOLD_CONFLICT:        sandboxtypes.ErrHoldConflict,
	statepb.StateOutcome_STATE_OUTCOME_NOT_HELD:             sandboxtypes.ErrNotHeld,
	statepb.StateOutcome_STATE_OUTCOME_EVICTION_IN_PROGRESS: sandboxtypes.ErrEvictionInProgress,
	statepb.StateOutcome_STATE_OUTCOME_EVICTION_NOT_NEEDED:  sandboxtypes.ErrEvictionNotNeeded,
	statepb.StateOutcome_STATE_OUTCOME_RESTORE_CONFLICT:     sandboxtypes.ErrRestoreConflict,
}

func stateCalls(t *testing.T) map[string]stateCall {
	t.Helper()

	sbx := testRecord()
	end := sbx.EndTime.Add(-time.Minute)
	transition := sandboxtypes.StateTransition{Sandbox: sbx, OriginalEndTime: &end, ID: "transition-1"}
	hold := sandboxtypes.Hold{TeamID: teamID, SandboxID: sandboxID, ExecutionID: sbx.ExecutionID, SourceNodeID: "node", ID: "hold"}
	token := sandboxtypes.UpdateToken(`{"token":"secret"}`)

	return map[string]stateCall{
		"Add": {
			respond: func(_ []byte, outcome statepb.StateOutcome) proto.Message {
				return statepb.AddResponse_builder{Outcome: outcome}.Build()
			},
			call: func(ctx context.Context, s *remote.Storage) (any, error) { return nil, s.Add(ctx, sbx) },
		},
		"Get": {
			respond: func(record []byte, outcome statepb.StateOutcome) proto.Message {
				return statepb.GetResponse_builder{Outcome: outcome, Record: record}.Build()
			},
			call: func(ctx context.Context, s *remote.Storage) (any, error) { return s.Get(ctx, teamID, sandboxID) },
			ok:   sbx, refused: sandboxtypes.Sandbox{},
		},
		"Remove": {
			respond: func(_ []byte, outcome statepb.StateOutcome) proto.Message {
				return statepb.RemoveResponse_builder{Outcome: outcome}.Build()
			},
			call: func(ctx context.Context, s *remote.Storage) (any, error) {
				return nil, s.Remove(ctx, teamID, sandboxID, sbx.ExecutionID)
			},
		},
		"BeginUpdate": {
			respond: func(record []byte, outcome statepb.StateOutcome) proto.Message {
				return statepb.BeginUpdateResponse_builder{Outcome: outcome, Record: record, Token: []byte(token)}.Build()
			},
			call: func(ctx context.Context, s *remote.Storage) (any, error) {
				got, gotToken, err := s.BeginUpdate(ctx, teamID, sandboxID)

				return []any{got, gotToken}, err
			},
			ok: []any{sbx, token}, refused: []any{sandboxtypes.Sandbox{}, sandboxtypes.UpdateToken("")},
		},
		"CommitUpdate": {
			respond: func(record []byte, outcome statepb.StateOutcome) proto.Message {
				return statepb.CommitUpdateResponse_builder{Outcome: outcome, Record: record}.Build()
			},
			call: func(ctx context.Context, s *remote.Storage) (any, error) {
				return s.CommitUpdate(ctx, teamID, sandboxID, token, sbx)
			},
			ok: sbx, refused: sandboxtypes.Sandbox{},
		},
		"AbortUpdate": {
			respond: func(_ []byte, outcome statepb.StateOutcome) proto.Message {
				return statepb.AbortUpdateResponse_builder{Outcome: outcome}.Build()
			},
			call: func(ctx context.Context, s *remote.Storage) (any, error) {
				return nil, s.AbortUpdate(ctx, teamID, sandboxID, token)
			},
		},
		"HoldExecution": {
			respond: func(_ []byte, outcome statepb.StateOutcome) proto.Message {
				return statepb.HoldExecutionResponse_builder{Outcome: outcome}.Build()
			},
			call: func(ctx context.Context, s *remote.Storage) (any, error) {
				return nil, s.HoldExecution(ctx, hold, end)
			},
		},
		"MoveHeldExecution": {
			respond: func(_ []byte, outcome statepb.StateOutcome) proto.Message {
				return statepb.MoveHeldExecutionResponse_builder{Outcome: outcome, Moved: outcome == statepb.StateOutcome_STATE_OUTCOME_OK}.Build()
			},
			call: func(ctx context.Context, s *remote.Storage) (any, error) {
				return s.MoveHeldExecution(ctx, hold, "node-b")
			},
			ok: true, refused: false,
		},
		"ReleaseHeldExecution": {
			respond: func(_ []byte, outcome statepb.StateOutcome) proto.Message {
				return statepb.ReleaseHeldExecutionResponse_builder{Outcome: outcome}.Build()
			},
			call: func(ctx context.Context, s *remote.Storage) (any, error) {
				return nil, s.ReleaseHeldExecution(ctx, hold)
			},
		},
		"RemoveHeldExecution": {
			respond: func(record []byte, outcome statepb.StateOutcome) proto.Message {
				return statepb.RemoveHeldExecutionResponse_builder{Outcome: outcome, Record: record}.Build()
			},
			call: func(ctx context.Context, s *remote.Storage) (any, error) { return s.RemoveHeldExecution(ctx, hold) },
			ok:   sbx, refused: sandboxtypes.Sandbox{},
		},
		"BeginRemoving": {
			respond: func(record []byte, outcome statepb.StateOutcome) proto.Message {
				current := statepb.SandboxState_SANDBOX_STATE_UNSPECIFIED
				if outcome == statepb.StateOutcome_STATE_OUTCOME_INVALID_TRANSITION {
					current = statepb.SandboxState_SANDBOX_STATE_SNAPSHOTTING
				}

				return statepb.BeginRemovingResponse_builder{
					Outcome:      outcome,
					Transition:   statepb.StateTransition_builder{Record: record, OriginalEndTime: timestamppb.New(end), Id: "transition-1"}.Build(),
					CurrentState: current,
				}.Build()
			},
			call: func(ctx context.Context, s *remote.Storage) (any, error) {
				got, alreadyDone, err := s.BeginRemoving(ctx, teamID, sandboxID, sandboxtypes.RemoveOpts{Action: sandboxtypes.StateActionPause})

				return []any{got, alreadyDone}, err
			},
			ok:      []any{transition, false},
			refused: []any{transition, false},
			target:  sandboxtypes.StatePausing,
		},
		"FinishTransition": {
			respond: func(_ []byte, outcome statepb.StateOutcome) proto.Message {
				return statepb.FinishTransitionResponse_builder{Outcome: outcome}.Build()
			},
			call: func(ctx context.Context, s *remote.Storage) (any, error) {
				return nil, s.FinishTransition(ctx, teamID, sandboxID, "transition-1", sandboxtypes.StateActionPause, sandboxtypes.TransitionOutcome{})
			},
		},
		"RestoreRunning": {
			respond: func(record []byte, outcome statepb.StateOutcome) proto.Message {
				return statepb.RestoreRunningResponse_builder{Outcome: outcome, Record: record, CurrentState: statepb.SandboxState_SANDBOX_STATE_KILLING}.Build()
			},
			call: func(ctx context.Context, s *remote.Storage) (any, error) {
				return s.RestoreRunning(ctx, transition, sandboxtypes.RestoreOpts{})
			},
			ok: sbx, refused: sandboxtypes.Sandbox{}, target: sandboxtypes.StateRunning,
		},
		"RestoreCheckpointed": {
			respond: func(record []byte, outcome statepb.StateOutcome) proto.Message {
				return statepb.RestoreCheckpointedResponse_builder{Outcome: outcome, Record: record, CurrentState: statepb.SandboxState_SANDBOX_STATE_PAUSING}.Build()
			},
			call: func(ctx context.Context, s *remote.Storage) (any, error) {
				return s.RestoreCheckpointed(ctx, transition, &orchestrator.SandboxNetworkPlacement{RouterId: "router"})
			},
			ok: sbx, refused: sandboxtypes.Sandbox{}, target: sandboxtypes.StateRunning,
		},
	}
}

var wireToState = map[statepb.SandboxState]sandboxtypes.State{
	statepb.SandboxState_SANDBOX_STATE_RUNNING:      sandboxtypes.StateRunning,
	statepb.SandboxState_SANDBOX_STATE_PAUSING:      sandboxtypes.StatePausing,
	statepb.SandboxState_SANDBOX_STATE_KILLING:      sandboxtypes.StateKilling,
	statepb.SandboxState_SANDBOX_STATE_SNAPSHOTTING: sandboxtypes.StateSnapshotting,
}

func TestStateOutcomes(t *testing.T) {
	t.Parallel()

	record := encode(t, testRecord())
	values := statepb.StateOutcome(0).Descriptor().Values()
	outcomes := []statepb.StateOutcome{99}
	for i := range values.Len() {
		outcomes = append(outcomes, statepb.StateOutcome(values.Get(i).Number()))
	}
	require.Len(t, outcomes, 13)

	for name, tc := range stateCalls(t) {
		for _, outcome := range outcomes {
			t.Run(name+" "+outcome.String(), func(t *testing.T) {
				t.Parallel()

				resp := tc.respond(record, outcome)
				s, _ := newStores(dial(t, respond(resp, nil)))
				got, err := tc.call(callContext(t), s)

				sentinel, isSentinel := outcomeSentinels[outcome]
				switch {
				case outcome == statepb.StateOutcome_STATE_OUTCOME_OK:
					require.NoError(t, err)
					assert.Equal(t, tc.ok, got, "the result of OK")
				case isSentinel:
					require.ErrorIs(t, err, sentinel)
					assert.Equal(t, sentinel, err, "the store's sentinel itself")
					assert.Equal(t, tc.refused, got, "the result beside the refusal")
				case outcome == statepb.StateOutcome_STATE_OUTCOME_INVALID_TRANSITION && tc.target != "":
					invalid, ok := errors.AsType[*sandboxtypes.InvalidStateTransitionError](err)
					require.True(t, ok, "an invalid transition error: %v", err)
					assert.Equal(t, &sandboxtypes.InvalidStateTransitionError{
						CurrentState: wireToState[statepb.SandboxState(resp.ProtoReflect().Get(resp.ProtoReflect().Descriptor().Fields().ByName("current_state")).Enum())],
						TargetState:  tc.target,
					}, invalid)
					assert.NotEmpty(t, invalid.CurrentState, "the current state comes from the response")
					assert.Equal(t, tc.refused, got, "the result beside the refusal")
				default:
					requireNoStoreError(t, err)
				}
			})
		}
	}
}

func TestBeginRemovingKeepsTheTransitionOnARefusal(t *testing.T) {
	t.Parallel()

	sbx := testRecord()
	sbx.State = sandboxtypes.StatePausing
	resp := statepb.BeginRemovingResponse_builder{
		Outcome:      statepb.StateOutcome_STATE_OUTCOME_LIFECYCLE_HELD,
		Transition:   statepb.StateTransition_builder{Record: encode(t, sbx), InFlightId: "transition-0"}.Build(),
		CurrentState: statepb.SandboxState_SANDBOX_STATE_UNSPECIFIED,
	}.Build()
	s, _ := newStores(dial(t, respond(resp, nil)))

	transition, alreadyDone, err := s.BeginRemoving(callContext(t), teamID, sandboxID, sandboxtypes.RemoveOpts{Action: sandboxtypes.StateActionKill})
	require.ErrorIs(t, err, sandboxtypes.ErrLifecycleHeld)
	assert.False(t, alreadyDone)
	assert.Equal(t, sandboxtypes.StateTransition{Sandbox: sbx, InFlightID: "transition-0"}, transition, "the record as read comes with the refusal")

	none := statepb.BeginRemovingResponse_builder{Outcome: statepb.StateOutcome_STATE_OUTCOME_NOT_FOUND}.Build()
	s, _ = newStores(dial(t, respond(none, nil)))
	transition, _, err = s.BeginRemoving(callContext(t), teamID, sandboxID, sandboxtypes.RemoveOpts{Action: sandboxtypes.StateActionKill})
	require.ErrorIs(t, err, sandboxtypes.ErrNotFound)
	assert.Zero(t, transition, "an absent transition stays zero")

	done := statepb.BeginRemovingResponse_builder{
		Outcome:     statepb.StateOutcome_STATE_OUTCOME_OK,
		Transition:  statepb.StateTransition_builder{Record: encode(t, sbx)}.Build(),
		AlreadyDone: true,
	}.Build()
	s, _ = newStores(dial(t, respond(done, nil)))
	transition, alreadyDone, err = s.BeginRemoving(callContext(t), teamID, sandboxID, sandboxtypes.RemoveOpts{Action: sandboxtypes.StateActionPause})
	require.NoError(t, err)
	assert.True(t, alreadyDone)
	assert.Equal(t, sandboxtypes.StateTransition{Sandbox: sbx}, transition)
}

func TestWaitForTransitionOutcomes(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		resp  *statepb.WaitForTransitionResponse
		check func(t *testing.T, err error)
	}{
		"finished": {
			resp: statepb.WaitForTransitionResponse_builder{Outcome: statepb.WaitForTransitionOutcome_WAIT_FOR_TRANSITION_OUTCOME_FINISHED, TransitionId: "transition-1"}.Build(),
			check: func(t *testing.T, err error) {
				t.Helper()
				require.NoError(t, err)
			},
		},
		"restored busy node": {
			resp: statepb.WaitForTransitionResponse_builder{Outcome: statepb.WaitForTransitionOutcome_WAIT_FOR_TRANSITION_OUTCOME_RESTORED, TransitionId: "transition-1"}.Build(),
			check: func(t *testing.T, err error) {
				t.Helper()
				assert.Equal(t, &sandboxtypes.RestoredTransitionError{Refusal: sandboxtypes.PauseQueueExhaustedError{}}, err)
				require.ErrorIs(t, err, sandboxtypes.ErrTransitionRestored)
			},
		},
		"restored unsupported": {
			resp: statepb.WaitForTransitionResponse_builder{Outcome: statepb.WaitForTransitionOutcome_WAIT_FOR_TRANSITION_OUTCOME_RESTORED, TransitionId: "transition-1", RefusalClass: "unsupported"}.Build(),
			check: func(t *testing.T, err error) {
				t.Helper()
				assert.Equal(t, &sandboxtypes.RestoredTransitionError{Refusal: sandboxtypes.PauseUnsupportedError{}}, err)
			},
		},
		"failed": {
			resp: statepb.WaitForTransitionResponse_builder{Outcome: statepb.WaitForTransitionOutcome_WAIT_FOR_TRANSITION_OUTCOME_FAILED, TransitionId: "transition-1", Failure: "boom"}.Build(),
			check: func(t *testing.T, err error) {
				t.Helper()
				assert.Equal(t, &sandboxtypes.TransitionFailedError{Message: "boom"}, err)
			},
		},
		"not found": {
			resp: statepb.WaitForTransitionResponse_builder{Outcome: statepb.WaitForTransitionOutcome_WAIT_FOR_TRANSITION_OUTCOME_NOT_FOUND, TransitionId: "transition-1"}.Build(),
			check: func(t *testing.T, err error) {
				t.Helper()
				assert.Equal(t, sandboxtypes.ErrNotFound, err)
			},
		},
		"unspecified": {
			resp: statepb.WaitForTransitionResponse_builder{TransitionId: "transition-1"}.Build(),
			check: func(t *testing.T, err error) {
				t.Helper()
				requireNoStoreError(t, err)
			},
		},
		"unknown": {
			resp: statepb.WaitForTransitionResponse_builder{Outcome: 99, TransitionId: "transition-1"}.Build(),
			check: func(t *testing.T, err error) {
				t.Helper()
				requireNoStoreError(t, err)
			},
		},
	}
	require.Len(t, tests, statepb.WaitForTransitionOutcome(0).Descriptor().Values().Len()+1, "every outcome but WAITING, unspecified and an unknown one")
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			s, _ := newStores(dial(t, respond(tt.resp, nil)))
			id, err := s.WaitForTransition(callContext(t), teamID, sandboxID, "")
			assert.Equal(t, "transition-1", id, "the ID the server returned")
			tt.check(t, err)
		})
	}
}

func TestReserveOutcomes(t *testing.T) {
	t.Parallel()

	tests := map[reservationpb.ReserveOutcome]struct {
		reserved bool
		check    func(t *testing.T, err error)
	}{
		reservationpb.ReserveOutcome_RESERVE_OUTCOME_RESERVED: {reserved: true, check: func(t *testing.T, err error) {
			t.Helper()
			require.NoError(t, err)
		}},
		reservationpb.ReserveOutcome_RESERVE_OUTCOME_ALREADY_PENDING: {check: func(t *testing.T, err error) {
			t.Helper()
			require.NoError(t, err)
		}},
		reservationpb.ReserveOutcome_RESERVE_OUTCOME_ALREADY_EXISTS: {check: func(t *testing.T, err error) {
			t.Helper()
			assert.Equal(t, sandboxtypes.ErrAlreadyExists, err)
		}},
		reservationpb.ReserveOutcome_RESERVE_OUTCOME_LIMIT_EXCEEDED: {check: func(t *testing.T, err error) {
			t.Helper()
			limit, ok := errors.AsType[*sandboxtypes.LimitExceededError](err)
			require.True(t, ok, "a limit error: %v", err)
			assert.Equal(t, &sandboxtypes.LimitExceededError{TeamID: teamID}, limit)
		}},
		reservationpb.ReserveOutcome_RESERVE_OUTCOME_KILLED: {check: func(t *testing.T, err error) {
			t.Helper()
			assert.Equal(t, sandboxtypes.ErrSandboxKilled, err)
		}},
		reservationpb.ReserveOutcome_RESERVE_OUTCOME_UNSPECIFIED: {check: func(t *testing.T, err error) {
			t.Helper()
			requireNoStoreError(t, err)
		}},
		99: {check: func(t *testing.T, err error) {
			t.Helper()
			requireNoStoreError(t, err)
		}},
	}
	require.Len(t, tests, reservationpb.ReserveOutcome(0).Descriptor().Values().Len()+1)
	for outcome, tt := range tests {
		t.Run(outcome.String(), func(t *testing.T) {
			t.Parallel()

			_, r := newStores(dial(t, respond(reservationpb.ReserveResponse_builder{Outcome: outcome}.Build(), nil)))
			reserved, err := r.Reserve(callContext(t), teamID, sandboxID, 3)
			assert.Equal(t, tt.reserved, reserved)
			tt.check(t, err)
		})
	}
}

func TestWaitForStartOutcomes(t *testing.T) {
	t.Parallel()

	tests := map[reservationpb.WaitForStartOutcome]func(t *testing.T, result []byte, err error){
		reservationpb.WaitForStartOutcome_WAIT_FOR_START_OUTCOME_FINISHED: func(t *testing.T, result []byte, err error) {
			t.Helper()
			require.NoError(t, err)
			assert.Equal(t, []byte("result"), result)
		},
		reservationpb.WaitForStartOutcome_WAIT_FOR_START_OUTCOME_NO_RESULT: func(t *testing.T, result []byte, err error) {
			t.Helper()
			missing, ok := errors.AsType[*sandboxtypes.StartResultMissingError](err)
			require.True(t, ok, "a missing result error: %v", err)
			assert.Equal(t, &sandboxtypes.StartResultMissingError{SandboxID: sandboxID}, missing)
			assert.Nil(t, result)
		},
		reservationpb.WaitForStartOutcome_WAIT_FOR_START_OUTCOME_UNSPECIFIED: func(t *testing.T, result []byte, err error) {
			t.Helper()
			requireNoStoreError(t, err)
			assert.Nil(t, result)
		},
		99: func(t *testing.T, result []byte, err error) {
			t.Helper()
			requireNoStoreError(t, err)
			assert.Nil(t, result)
		},
	}
	require.Len(t, tests, reservationpb.WaitForStartOutcome(0).Descriptor().Values().Len(), "every outcome but WAITING, and an unknown one")
	for outcome, check := range tests {
		t.Run(outcome.String(), func(t *testing.T) {
			t.Parallel()

			resp := reservationpb.WaitForStartResponse_builder{Outcome: outcome, Result: []byte("result")}.Build()
			_, r := newStores(dial(t, respond(resp, nil)))
			result, err := r.WaitForStart(callContext(t), teamID, sandboxID)
			check(t, result, err)
		})
	}
}

func TestResultsWithoutOutcome(t *testing.T) {
	t.Parallel()

	t.Run("ClaimKill", func(t *testing.T) {
		t.Parallel()

		_, r := newStores(dial(t, respond(reservationpb.ClaimKillResponse_builder{Claimed: true, ClaimId: "claim"}.Build(), nil)))
		claimID, claimed, err := r.ClaimKill(callContext(t), teamID, sandboxID)
		require.NoError(t, err)
		assert.True(t, claimed)
		assert.Equal(t, "claim", claimID)

		_, r = newStores(dial(t, respond(reservationpb.ClaimKillResponse_builder{}.Build(), nil)))
		claimID, claimed, err = r.ClaimKill(callContext(t), teamID, sandboxID)
		require.NoError(t, err)
		assert.False(t, claimed)
		assert.Empty(t, claimID)
	})

	t.Run("TeamsWithSandboxCount", func(t *testing.T) {
		t.Parallel()

		other := teamID
		other[0]++
		s, _ := newStores(dial(t, respond(statepb.TeamsWithSandboxCountResponse_builder{Counts: map[string]int64{teamID.String(): 3, other.String(): 1}}.Build(), nil)))
		counts, err := s.TeamsWithSandboxCount(callContext(t))
		require.NoError(t, err)
		assert.Equal(t, map[uuid.UUID]int64{teamID: 3, other: 1}, counts)

		s, _ = newStores(dial(t, respond(statepb.TeamsWithSandboxCountResponse_builder{}.Build(), nil)))
		counts, err = s.TeamsWithSandboxCount(callContext(t))
		require.NoError(t, err)
		assert.Equal(t, map[uuid.UUID]int64{}, counts, "no teams is an empty map, as the store answers")

		s, _ = newStores(dial(t, respond(statepb.TeamsWithSandboxCountResponse_builder{Counts: map[string]int64{"team": 1}}.Build(), nil)))
		_, err = s.TeamsWithSandboxCount(callContext(t))
		requireNoStoreError(t, err)
	})

	t.Run("WaitForStart with an empty result", func(t *testing.T) {
		t.Parallel()

		_, r := newStores(dial(t, respond(reservationpb.WaitForStartResponse_builder{Outcome: reservationpb.WaitForStartOutcome_WAIT_FOR_START_OUTCOME_FINISHED}.Build(), nil)))
		result, err := r.WaitForStart(callContext(t), teamID, sandboxID)
		require.NoError(t, err)
		assert.Equal(t, []byte{}, result, "an empty result is not nil, as the store answers it")
	})

	t.Run("an undecodable record", func(t *testing.T) {
		t.Parallel()

		s, _ := newStores(dial(t, respond(statepb.GetResponse_builder{Outcome: statepb.StateOutcome_STATE_OUTCOME_OK, Record: []byte(`{"envdAccessToken":`)}.Build(), nil)))
		_, err := s.Get(callContext(t), teamID, sandboxID)
		requireNoStoreError(t, err)
	})
}
