package state_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/e2b-dev/infra/packages/scheduling-api/internal/state"
	statepb "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/scheduling/state"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxstore/sandboxtypes"
)

var (
	transitionTeam       = uuid.New()
	transitionSandbox    = "sbx-transition"
	originalEndTime      = time.Date(2026, 11, 13, 10, 11, 12, 123456789, time.UTC)
	errInvalidTransition = &sandboxtypes.InvalidStateTransitionError{CurrentState: sandboxtypes.StateKilling, TargetState: sandboxtypes.StatePausing}
)

var transitionOutcomes = map[error]statepb.StateOutcome{
	sandboxtypes.ErrNotFound:           statepb.StateOutcome_STATE_OUTCOME_NOT_FOUND,
	sandboxtypes.ErrLifecycleHeld:      statepb.StateOutcome_STATE_OUTCOME_LIFECYCLE_HELD,
	sandboxtypes.ErrExecutionMismatch:  statepb.StateOutcome_STATE_OUTCOME_EXECUTION_MISMATCH,
	sandboxtypes.ErrLockLost:           statepb.StateOutcome_STATE_OUTCOME_LOCK_LOST,
	sandboxtypes.ErrEvictionInProgress: statepb.StateOutcome_STATE_OUTCOME_EVICTION_IN_PROGRESS,
	sandboxtypes.ErrEvictionNotNeeded:  statepb.StateOutcome_STATE_OUTCOME_EVICTION_NOT_NEEDED,
	sandboxtypes.ErrRestoreConflict:    statepb.StateOutcome_STATE_OUTCOME_RESTORE_CONFLICT,
	errInvalidTransition:               statepb.StateOutcome_STATE_OUTCOME_INVALID_TRANSITION,
}

func transitionRecord() sandboxtypes.Sandbox {
	return minimalRecord(transitionTeam, transitionSandbox)
}

func wireTransition(t *testing.T, sbx sandboxtypes.Sandbox) *statepb.StateTransition {
	t.Helper()

	return statepb.StateTransition_builder{
		Record:          encode(t, sbx),
		OriginalEndTime: timestamppb.New(originalEndTime),
		Id:              "transition-1",
		InFlightId:      "transition-0",
	}.Build()
}

func removeOptions(action statepb.RemoveAction) *statepb.RemoveOptions {
	return statepb.RemoveOptions_builder{Action: action}.Build()
}

type transitionResponse struct {
	outcome    statepb.StateOutcome
	current    statepb.SandboxState
	transition *statepb.StateTransition
	record     []byte
}

type transitionCall struct {
	call      func(ctx context.Context, client statepb.StateServiceClient) (transitionResponse, error)
	sentinels []error
}

func transitionCalls(t *testing.T) map[string]transitionCall {
	t.Helper()

	restoreSentinels := []error{sandboxtypes.ErrNotFound, sandboxtypes.ErrExecutionMismatch, sandboxtypes.ErrRestoreConflict, errInvalidTransition}

	return map[string]transitionCall{
		"BeginRemoving": {
			call: func(ctx context.Context, c statepb.StateServiceClient) (transitionResponse, error) {
				resp, err := c.BeginRemoving(ctx, statepb.BeginRemovingRequest_builder{
					TeamId: transitionTeam.String(), SandboxId: transitionSandbox, Options: removeOptions(statepb.RemoveAction_REMOVE_ACTION_PAUSE),
				}.Build())

				return transitionResponse{resp.GetOutcome(), resp.GetCurrentState(), resp.GetTransition(), nil}, err
			},
			sentinels: []error{
				sandboxtypes.ErrNotFound, sandboxtypes.ErrExecutionMismatch, sandboxtypes.ErrLifecycleHeld, sandboxtypes.ErrLockLost,
				sandboxtypes.ErrEvictionInProgress, sandboxtypes.ErrEvictionNotNeeded, errInvalidTransition,
			},
		},
		"RestoreRunning": {
			call: func(ctx context.Context, c statepb.StateServiceClient) (transitionResponse, error) {
				resp, err := c.RestoreRunning(ctx, statepb.RestoreRunningRequest_builder{Transition: wireTransition(t, transitionRecord())}.Build())

				return transitionResponse{resp.GetOutcome(), resp.GetCurrentState(), nil, resp.GetRecord()}, err
			},
			sentinels: restoreSentinels,
		},
		"RestoreCheckpointed": {
			call: func(ctx context.Context, c statepb.StateServiceClient) (transitionResponse, error) {
				resp, err := c.RestoreCheckpointed(ctx, statepb.RestoreCheckpointedRequest_builder{Transition: wireTransition(t, transitionRecord())}.Build())

				return transitionResponse{resp.GetOutcome(), resp.GetCurrentState(), nil, resp.GetRecord()}, err
			},
			sentinels: restoreSentinels,
		},
	}
}

func TestTransitionOutcomeMapping(t *testing.T) {
	t.Parallel()

	read := sandboxtypes.StateTransition{Sandbox: transitionRecord()}
	for name, rpc := range transitionCalls(t) {
		for _, sentinel := range rpc.sentinels {
			for _, wrapped := range []bool{false, true} {
				err := sentinel
				if wrapped {
					err = fmt.Errorf("sandbox %q: %w", transitionSandbox, sentinel)
				}
				t.Run(fmt.Sprintf("%s %v wrapped=%v", name, sentinel, wrapped), func(t *testing.T) {
					t.Parallel()

					fake := newFake()
					fake.err = err
					fake.transition = read
					resp, callErr := rpc.call(callContext(t), newClient(t, fake))
					require.NoError(t, callErr)
					assert.Equal(t, transitionOutcomes[sentinel], resp.outcome)
					assert.Empty(t, resp.record, "a refusal carries no restored record")
					if errors.Is(sentinel, errInvalidTransition) {
						assert.Equal(t, statepb.SandboxState_SANDBOX_STATE_KILLING, resp.current)
					} else {
						assert.Equal(t, statepb.SandboxState_SANDBOX_STATE_UNSPECIFIED, resp.current)
					}
					if name == "BeginRemoving" {
						require.NotNil(t, resp.transition, "a refusal carries the transition the store returned")
						assert.Equal(t, read.Sandbox, decode(t, resp.transition.GetRecord()))
						assert.False(t, resp.transition.HasOriginalEndTime())
					}
				})
			}
		}

		t.Run(name+" nil is OK", func(t *testing.T) {
			t.Parallel()

			fake := newFake()
			fake.record = transitionRecord()
			resp, err := rpc.call(callContext(t), newClient(t, fake))
			require.NoError(t, err)
			assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, resp.outcome)
			assert.Equal(t, statepb.SandboxState_SANDBOX_STATE_UNSPECIFIED, resp.current)
			if name == "BeginRemoving" {
				assert.Nil(t, resp.transition, "a zero transition is not sent")
			} else {
				assert.Equal(t, fake.record, decode(t, resp.record))
			}
		})
	}
}

func TestBeginRemovingCarriesTheTransition(t *testing.T) {
	t.Parallel()

	sbx := fullRecord(transitionTeam, transitionSandbox)
	end := originalEndTime
	tests := map[string]struct {
		transition  sandboxtypes.StateTransition
		alreadyDone bool
	}{
		"started":      {transition: sandboxtypes.StateTransition{Sandbox: sbx, OriginalEndTime: &end, ID: "started"}},
		"in flight":    {transition: sandboxtypes.StateTransition{Sandbox: sbx, InFlightID: "in-flight"}},
		"already done": {transition: sandboxtypes.StateTransition{Sandbox: sbx}, alreadyDone: true},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			fake := newFake()
			fake.transition = tt.transition
			fake.alreadyDone = tt.alreadyDone
			resp, err := newClient(t, fake).BeginRemoving(callContext(t), statepb.BeginRemovingRequest_builder{
				TeamId: transitionTeam.String(), SandboxId: transitionSandbox, Options: removeOptions(statepb.RemoveAction_REMOVE_ACTION_PAUSE),
			}.Build())
			require.NoError(t, err)
			assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, resp.GetOutcome())
			assert.Equal(t, tt.alreadyDone, resp.GetAlreadyDone())
			wire := resp.GetTransition()
			assert.Equal(t, sbx, decode(t, wire.GetRecord()))
			assert.Equal(t, tt.transition.ID, wire.GetId())
			assert.Equal(t, tt.transition.InFlightID, wire.GetInFlightId())
			assert.Equal(t, tt.transition.OriginalEndTime != nil, wire.HasOriginalEndTime())
			if tt.transition.OriginalEndTime != nil {
				assert.Equal(t, originalEndTime, wire.GetOriginalEndTime().AsTime(), "the original end time keeps its nanoseconds")
			}
		})
	}
}

func TestEveryActionIsMapped(t *testing.T) {
	t.Parallel()

	predefined := map[statepb.RemoveAction]sandboxtypes.StateAction{
		statepb.RemoveAction_REMOVE_ACTION_PAUSE:    sandboxtypes.StateActionPause,
		statepb.RemoveAction_REMOVE_ACTION_KILL:     sandboxtypes.StateActionKill,
		statepb.RemoveAction_REMOVE_ACTION_SNAPSHOT: sandboxtypes.StateActionSnapshot,
	}
	require.Len(t, statepb.RemoveAction_name, len(predefined)+1, "every predefined StateAction has one enum value besides UNSPECIFIED")

	for value, want := range predefined {
		fake := newFake()
		client := newClient(t, fake)
		_, err := client.BeginRemoving(callContext(t), statepb.BeginRemovingRequest_builder{
			TeamId: transitionTeam.String(), SandboxId: transitionSandbox, Options: removeOptions(value),
		}.Build())
		require.NoError(t, err)
		assert.Equal(t, want, fake.snapshot().removeOpts.Action, "%v reaches the store as the predefined action", value)

		_, err = client.FinishTransition(callContext(t), statepb.FinishTransitionRequest_builder{
			TeamId: transitionTeam.String(), SandboxId: transitionSandbox, TransitionId: "transition-1", Action: value,
		}.Build())
		require.NoError(t, err)
		assert.Equal(t, want, fake.snapshot().action, "%v reaches FinishTransition as the predefined action", value)
	}
}

func TestFinishTransitionFailuresAreInternal(t *testing.T) {
	t.Parallel()

	restoreFailed := func(err error) error {
		return fmt.Errorf("failed to restore sandbox to running: %w", err)
	}
	scriptFailed := errors.New("failed to run finish-transition script: connection refused")
	tests := map[string]error{
		"backend failure":            scriptFailed,
		"restore found no record":    errors.Join(restoreFailed(sandboxtypes.ErrNotFound), nil),
		"restore refused as held":    errors.Join(restoreFailed(sandboxtypes.ErrLifecycleHeld), nil),
		"restore and script failed":  errors.Join(restoreFailed(sandboxtypes.ErrNotFound), scriptFailed),
		"restore lost the lock":      errors.Join(restoreFailed(sandboxtypes.ErrLockLost), nil),
		"a bare sentinel is not one": sandboxtypes.ErrNotFound,
	}
	for name, storeErr := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			fake := newFake()
			fake.err = storeErr
			_, err := newClient(t, fake).FinishTransition(callContext(t), statepb.FinishTransitionRequest_builder{
				TeamId: transitionTeam.String(), SandboxId: transitionSandbox, TransitionId: "transition-1", Action: statepb.RemoveAction_REMOVE_ACTION_SNAPSHOT,
			}.Build())
			st := requireCode(t, err, codes.Internal)
			assert.Equal(t, "internal error", st.Message())
		})
	}

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		resp, err := newClient(t, newFake()).FinishTransition(callContext(t), statepb.FinishTransitionRequest_builder{
			TeamId: transitionTeam.String(), SandboxId: transitionSandbox, TransitionId: "transition-1", Action: statepb.RemoveAction_REMOVE_ACTION_PAUSE,
		}.Build())
		require.NoError(t, err)
		assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, resp.GetOutcome())
	})
}

func waitFor(ctx context.Context, client statepb.StateServiceClient, transitionID string) (*statepb.WaitForTransitionResponse, error) {
	return client.WaitForTransition(ctx, statepb.WaitForTransitionRequest_builder{
		TeamId: transitionTeam.String(), SandboxId: transitionSandbox, TransitionId: transitionID,
	}.Build())
}

func TestWaitForTransitionOutcomes(t *testing.T) {
	t.Parallel()

	restored := func(refusal error) error {
		return &sandboxtypes.RestoredTransitionError{Refusal: refusal}
	}
	tests := []struct {
		name    string
		err     error
		outcome statepb.WaitForTransitionOutcome
		class   string
		failure string
	}{
		{"finished", nil, statepb.WaitForTransitionOutcome_WAIT_FOR_TRANSITION_OUTCOME_FINISHED, "", ""},
		{"restored busy node", restored(sandboxtypes.PauseQueueExhaustedError{}), statepb.WaitForTransitionOutcome_WAIT_FOR_TRANSITION_OUTCOME_RESTORED, "", ""},
		{"restored without a refusal", restored(nil), statepb.WaitForTransitionOutcome_WAIT_FOR_TRANSITION_OUTCOME_RESTORED, "", ""},
		{"restored checkpoint", restored(sandboxtypes.PauseRefusedCheckpointInProgressError{}), statepb.WaitForTransitionOutcome_WAIT_FOR_TRANSITION_OUTCOME_RESTORED, "checkpoint_in_progress", ""},
		{"restored envd", restored(sandboxtypes.PauseRefusedEnvdUnresponsiveError{}), statepb.WaitForTransitionOutcome_WAIT_FOR_TRANSITION_OUTCOME_RESTORED, "envd_unresponsive", ""},
		{"restored unsupported", restored(sandboxtypes.PauseUnsupportedError{}), statepb.WaitForTransitionOutcome_WAIT_FOR_TRANSITION_OUTCOME_RESTORED, "unsupported", ""},
		{"failed", &sandboxtypes.TransitionFailedError{Message: "boom"}, statepb.WaitForTransitionOutcome_WAIT_FOR_TRANSITION_OUTCOME_FAILED, "", "boom"},
		{"another team", sandboxtypes.ErrNotFound, statepb.WaitForTransitionOutcome_WAIT_FOR_TRANSITION_OUTCOME_NOT_FOUND, "", ""},
	}
	for _, tt := range tests {
		for _, wrapped := range []bool{false, true} {
			err := tt.err
			if wrapped && err != nil {
				err = fmt.Errorf("x: %w", err)
			}
			t.Run(fmt.Sprintf("%s wrapped=%v", tt.name, wrapped), func(t *testing.T) {
				t.Parallel()

				fake := newFake()
				fake.waitID = "transition-1"
				fake.err = err
				resp, callErr := waitFor(callContext(t), newClient(t, fake), "")
				require.NoError(t, callErr)
				assert.Equal(t, tt.outcome, resp.GetOutcome())
				assert.Equal(t, "transition-1", resp.GetTransitionId(), "the ID the store returned")
				assert.Equal(t, tt.class, resp.GetRefusalClass())
				assert.Equal(t, tt.failure, resp.GetFailure())
			})
		}
	}
}

func TestWaitForTransitionMaxWaitIsWaiting(t *testing.T) {
	t.Parallel()

	tests := map[string]func(ctx context.Context) (string, error){
		"deadline error": func(ctx context.Context) (string, error) {
			<-ctx.Done()

			return "transition-1", fmt.Errorf("x: %w", ctx.Err())
		},
		"other error after the deadline": func(ctx context.Context) (string, error) {
			<-ctx.Done()

			return "transition-1", errors.New("i/o timeout")
		},
	}
	for name, wait := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			fake := newFake()
			fake.wait = wait
			resp, err := waitFor(t.Context(), newClientWithWait(t, fake, 10*time.Millisecond), "")
			require.NoError(t, err)
			assert.Equal(t, statepb.WaitForTransitionOutcome_WAIT_FOR_TRANSITION_OUTCOME_WAITING, resp.GetOutcome())
			assert.Equal(t, "transition-1", resp.GetTransitionId())
			assert.True(t, fake.snapshot().waitDeadline, "the store's wait is bounded")
		})
	}

	t.Run("caller ended during the wait", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithTimeout(t.Context(), time.Hour)
		defer cancel()
		fake := newFake()
		fake.wait = func(context.Context) (string, error) {
			cancel()

			return "transition-1", context.DeadlineExceeded
		}
		_, err := state.NewServer(fake, time.Hour).WaitForTransition(ctx, statepb.WaitForTransitionRequest_builder{
			TeamId: transitionTeam.String(), SandboxId: transitionSandbox,
		}.Build())
		requireCode(t, err, codes.Canceled)
	})
}

func transitionRPCs(t *testing.T) map[string]rpcCall {
	t.Helper()

	return map[string]rpcCall{
		"BeginRemoving": func(ctx context.Context, c statepb.StateServiceClient) error {
			_, err := c.BeginRemoving(ctx, statepb.BeginRemovingRequest_builder{
				TeamId: transitionTeam.String(), SandboxId: transitionSandbox, Options: removeOptions(statepb.RemoveAction_REMOVE_ACTION_KILL),
			}.Build())

			return err
		},
		"FinishTransition": func(ctx context.Context, c statepb.StateServiceClient) error {
			_, err := c.FinishTransition(ctx, statepb.FinishTransitionRequest_builder{
				TeamId: transitionTeam.String(), SandboxId: transitionSandbox, TransitionId: "transition-1", Action: statepb.RemoveAction_REMOVE_ACTION_KILL,
			}.Build())

			return err
		},
		"RestoreRunning": func(ctx context.Context, c statepb.StateServiceClient) error {
			_, err := c.RestoreRunning(ctx, statepb.RestoreRunningRequest_builder{Transition: wireTransition(t, transitionRecord())}.Build())

			return err
		},
		"RestoreCheckpointed": func(ctx context.Context, c statepb.StateServiceClient) error {
			_, err := c.RestoreCheckpointed(ctx, statepb.RestoreCheckpointedRequest_builder{Transition: wireTransition(t, transitionRecord())}.Build())

			return err
		},
		"WaitForTransition": func(ctx context.Context, c statepb.StateServiceClient) error {
			_, err := waitFor(ctx, c, "transition-1")

			return err
		},
	}
}

func TestTransitionBackendFailureIsInternal(t *testing.T) {
	t.Parallel()

	const cause = "backend exploded with secret detail"
	for name, call := range transitionRPCs(t) {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			fake := newFake()
			fake.err = errors.New(cause)
			st := requireCode(t, call(callContext(t), newClient(t, fake)), codes.Internal)
			assert.Equal(t, "internal error", st.Message())
			assert.NotContains(t, st.Message(), cause)
			assert.Equal(t, 1, fake.callCount())
		})
	}

	unknown := &sandboxtypes.InvalidStateTransitionError{CurrentState: "frozen", TargetState: sandboxtypes.StateRunning}
	for _, name := range []string{"BeginRemoving", "RestoreRunning", "RestoreCheckpointed"} {
		t.Run(name+" unknown state", func(t *testing.T) {
			t.Parallel()

			fake := newFake()
			fake.err = unknown
			fake.transition = sandboxtypes.StateTransition{Sandbox: transitionRecord()}
			st := requireCode(t, transitionRPCs(t)[name](callContext(t), newClient(t, fake)), codes.Internal)
			assert.Equal(t, "internal error", st.Message())
		})
	}
}

func TestTransitionCancelledCaller(t *testing.T) {
	t.Parallel()

	handlerCalls := map[string]func(ctx context.Context, s *state.Server) error{
		"BeginRemoving": func(ctx context.Context, s *state.Server) error {
			_, err := s.BeginRemoving(ctx, statepb.BeginRemovingRequest_builder{
				TeamId: transitionTeam.String(), SandboxId: transitionSandbox, Options: removeOptions(statepb.RemoveAction_REMOVE_ACTION_PAUSE),
			}.Build())

			return err
		},
		"FinishTransition": func(ctx context.Context, s *state.Server) error {
			_, err := s.FinishTransition(ctx, statepb.FinishTransitionRequest_builder{
				TeamId: transitionTeam.String(), SandboxId: transitionSandbox, TransitionId: "transition-1", Action: statepb.RemoveAction_REMOVE_ACTION_PAUSE,
			}.Build())

			return err
		},
		"RestoreRunning": func(ctx context.Context, s *state.Server) error {
			_, err := s.RestoreRunning(ctx, statepb.RestoreRunningRequest_builder{Transition: wireTransition(t, transitionRecord())}.Build())

			return err
		},
		"RestoreCheckpointed": func(ctx context.Context, s *state.Server) error {
			_, err := s.RestoreCheckpointed(ctx, statepb.RestoreCheckpointedRequest_builder{Transition: wireTransition(t, transitionRecord())}.Build())

			return err
		},
		"WaitForTransition": func(ctx context.Context, s *state.Server) error {
			_, err := s.WaitForTransition(ctx, statepb.WaitForTransitionRequest_builder{
				TeamId: transitionTeam.String(), SandboxId: transitionSandbox, TransitionId: "transition-1",
			}.Build())

			return err
		},
	}
	for name, call := range handlerCalls {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			fake := newFake()
			fake.err = errors.New("backend gave up")
			st := requireCode(t, call(ctx, state.NewServer(fake, hangTimeout)), codes.Canceled)
			assert.NotContains(t, st.Message(), "backend gave up")
		})
	}
}

func TestTransitionInvalidRequests(t *testing.T) {
	t.Parallel()

	team := transitionTeam.String()
	begin := func(team, sandbox string, options *statepb.RemoveOptions) rpcCall {
		return func(ctx context.Context, c statepb.StateServiceClient) error {
			_, err := c.BeginRemoving(ctx, statepb.BeginRemovingRequest_builder{TeamId: team, SandboxId: sandbox, Options: options}.Build())

			return err
		}
	}
	finish := func(team, sandbox, transitionID string, action statepb.RemoveAction) rpcCall {
		return func(ctx context.Context, c statepb.StateServiceClient) error {
			_, err := c.FinishTransition(ctx, statepb.FinishTransitionRequest_builder{TeamId: team, SandboxId: sandbox, TransitionId: transitionID, Action: action}.Build())

			return err
		}
	}
	restore := func(transition *statepb.StateTransition, options *statepb.RestoreOptions) rpcCall {
		return func(ctx context.Context, c statepb.StateServiceClient) error {
			_, err := c.RestoreRunning(ctx, statepb.RestoreRunningRequest_builder{Transition: transition, Options: options}.Build())

			return err
		}
	}
	checkpointed := func(transition *statepb.StateTransition) rpcCall {
		return func(ctx context.Context, c statepb.StateServiceClient) error {
			_, err := c.RestoreCheckpointed(ctx, statepb.RestoreCheckpointedRequest_builder{Transition: transition}.Build())

			return err
		}
	}
	wait := func(team, sandbox string) rpcCall {
		return func(ctx context.Context, c statepb.StateServiceClient) error {
			_, err := c.WaitForTransition(ctx, statepb.WaitForTransitionRequest_builder{TeamId: team, SandboxId: sandbox}.Build())

			return err
		}
	}
	withRecord := func(mutate func(*sandboxtypes.Sandbox)) *statepb.StateTransition {
		sbx := transitionRecord()
		mutate(&sbx)

		return wireTransition(t, sbx)
	}
	withTransition := func(mutate func(*statepb.StateTransition)) *statepb.StateTransition {
		transition := wireTransition(t, transitionRecord())
		mutate(transition)

		return transition
	}
	pause := removeOptions(statepb.RemoveAction_REMOVE_ACTION_PAUSE)

	tests := map[string]rpcCall{
		"BeginRemoving team not a UUID":       begin("team-1", transitionSandbox, pause),
		"BeginRemoving braced sandbox":        begin(team, "a{b", pause),
		"BeginRemoving no options":            begin(team, transitionSandbox, nil),
		"BeginRemoving unspecified action":    begin(team, transitionSandbox, removeOptions(statepb.RemoveAction_REMOVE_ACTION_UNSPECIFIED)),
		"BeginRemoving unknown action":        begin(team, transitionSandbox, removeOptions(statepb.RemoveAction(99))),
		"FinishTransition team not a UUID":    finish("team-1", transitionSandbox, "transition-1", statepb.RemoveAction_REMOVE_ACTION_KILL),
		"FinishTransition empty sandbox":      finish(team, "", "transition-1", statepb.RemoveAction_REMOVE_ACTION_KILL),
		"FinishTransition empty transition":   finish(team, transitionSandbox, "", statepb.RemoveAction_REMOVE_ACTION_KILL),
		"FinishTransition unspecified action": finish(team, transitionSandbox, "transition-1", statepb.RemoveAction_REMOVE_ACTION_UNSPECIFIED),
		"FinishTransition unknown action":     finish(team, transitionSandbox, "transition-1", statepb.RemoveAction(99)),
		"RestoreRunning no transition":        restore(nil, nil),
		"RestoreRunning record not JSON":      restore(withTransition(func(tr *statepb.StateTransition) { tr.SetRecord([]byte("not json")) }), nil),
		"RestoreRunning colon in sandbox ID":  restore(withRecord(func(s *sandboxtypes.Sandbox) { s.SandboxID = "a:b" }), nil),
		"RestoreRunning braced sandbox ID":    restore(withRecord(func(s *sandboxtypes.Sandbox) { s.SandboxID = "a{b" }), nil),
		"RestoreRunning nil team":             restore(withRecord(func(s *sandboxtypes.Sandbox) { s.TeamID = uuid.Nil }), nil),
		"RestoreRunning non-canonical execution": restore(withRecord(func(s *sandboxtypes.Sandbox) {
			s.ExecutionID = "execution-1"
		}), nil),
		"RestoreRunning no original end time": restore(withTransition(func(tr *statepb.StateTransition) { tr.ClearOriginalEndTime() }), nil),
		"RestoreRunning invalid original end time": restore(withTransition(func(tr *statepb.StateTransition) {
			tr.SetOriginalEndTime(&timestamppb.Timestamp{Seconds: 1, Nanos: -1})
		}), nil),
		"RestoreRunning invalid retry_after": restore(wireTransition(t, transitionRecord()), statepb.RestoreOptions_builder{
			RetryAfter: &durationpb.Duration{Seconds: 1, Nanos: -1},
		}.Build()),
		"RestoreCheckpointed no transition":        checkpointed(nil),
		"RestoreCheckpointed no original end time": checkpointed(withTransition(func(tr *statepb.StateTransition) { tr.ClearOriginalEndTime() })),
		"RestoreCheckpointed colon in sandbox ID":  checkpointed(withRecord(func(s *sandboxtypes.Sandbox) { s.SandboxID = "a:b" })),
		"RestoreCheckpointed empty execution":      checkpointed(withRecord(func(s *sandboxtypes.Sandbox) { s.ExecutionID = "" })),
		"WaitForTransition team not a UUID":        wait("team-1", transitionSandbox),
		"WaitForTransition empty sandbox":          wait(team, ""),
		"WaitForTransition braced sandbox":         wait(team, "a}b"),
	}
	for name, call := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			fake := newFake()
			requireCode(t, call(callContext(t), newClient(t, fake)), codes.InvalidArgument)
			assert.Zero(t, fake.callCount(), "the store is not called")
		})
	}
}

func copyByName(from, to protoreflect.Message) {
	fields := to.Descriptor().Fields()
	from.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		to.Set(fields.ByName(field.Name()), value)

		return true
	})
}

func requireEveryFieldSet(t *testing.T, message protoreflect.Message) {
	t.Helper()

	fields := message.Descriptor().Fields()
	for i := range fields.Len() {
		require.True(t, message.Has(fields.Get(i)), "field %s is set", fields.Get(i).Name())
	}
}

func TestTransitionStoreReceivesTheCallersValues(t *testing.T) {
	t.Parallel()

	t.Run("BeginRemoving", func(t *testing.T) {
		t.Parallel()

		want := sandboxtypes.RemoveOpts{
			Action:               sandboxtypes.StateActionSnapshot,
			Eviction:             true,
			Reason:               sandboxtypes.KillReasonTimeout,
			FilesystemOnly:       true,
			RequireEnvdHealthy:   true,
			ExpectExecutionID:    uuid.NewString(),
			BeforeEndTime:        true,
			WaitForLifecycleHold: true,
		}
		requireNoZeroField(t, want)
		// The API's store reads it; the request does not carry it.
		want.WaitForLifecycleHold = false

		fake := newFake()
		_, err := newClient(t, fake).BeginRemoving(callContext(t), statepb.BeginRemovingRequest_builder{
			TeamId: transitionTeam.String(), SandboxId: transitionSandbox,
			Options: statepb.RemoveOptions_builder{
				Action:             statepb.RemoveAction_REMOVE_ACTION_SNAPSHOT,
				Eviction:           want.Eviction,
				Reason:             string(want.Reason),
				FilesystemOnly:     want.FilesystemOnly,
				RequireEnvdHealthy: want.RequireEnvdHealthy,
				ExpectExecutionId:  want.ExpectExecutionID,
				BeforeEndTime:      want.BeforeEndTime,
			}.Build(),
		}.Build())
		require.NoError(t, err)
		got := fake.snapshot().removeOpts
		assert.Equal(t, want, got)
		assert.Equal(t, sandboxtypes.StateActionSnapshot, got.Action, "the predefined action value")
	})

	t.Run("FinishTransition", func(t *testing.T) {
		t.Parallel()

		want := sandboxtypes.TransitionOutcome{Failed: true, Error: "boom", SkipRestore: true, Restored: true, RefusalClass: "unsupported"}
		requireNoZeroField(t, want)

		fake := newFake()
		_, err := newClient(t, fake).FinishTransition(callContext(t), statepb.FinishTransitionRequest_builder{
			TeamId: transitionTeam.String(), SandboxId: transitionSandbox, TransitionId: "transition-1", Action: statepb.RemoveAction_REMOVE_ACTION_SNAPSHOT,
			Result: statepb.TransitionResult_builder{
				Failed: want.Failed, Error: want.Error, SkipRestore: want.SkipRestore, Restored: want.Restored, RefusalClass: want.RefusalClass,
			}.Build(),
		}.Build())
		require.NoError(t, err)
		seen := fake.snapshot()
		assert.Equal(t, want, seen.result)
		assert.Equal(t, "transition-1", seen.transitionID)
		assert.Equal(t, sandboxtypes.StateActionSnapshot, seen.action)
	})

	end := originalEndTime
	full := fullRecord(transitionTeam, transitionSandbox)
	wantTransition := sandboxtypes.StateTransition{Sandbox: full, OriginalEndTime: &end, ID: "transition-1", InFlightID: "transition-0"}

	t.Run("RestoreRunning", func(t *testing.T) {
		t.Parallel()

		requireNoZeroField(t, wantTransition)
		wantOpts := sandboxtypes.RestoreOpts{RetryAfter: 90*time.Second + 123*time.Nanosecond, HoldOnly: true, EnvdRefusal: true}
		requireNoZeroField(t, wantOpts)

		fake := newFake()
		fake.record = full
		resp, err := newClient(t, fake).RestoreRunning(callContext(t), statepb.RestoreRunningRequest_builder{
			Transition: wireTransition(t, full),
			Options: statepb.RestoreOptions_builder{
				RetryAfter: durationpb.New(wantOpts.RetryAfter), HoldOnly: wantOpts.HoldOnly, EnvdRefusal: wantOpts.EnvdRefusal,
			}.Build(),
		}.Build())
		require.NoError(t, err)
		assert.Equal(t, statepb.StateOutcome_STATE_OUTCOME_OK, resp.GetOutcome())
		assert.Equal(t, full, decode(t, resp.GetRecord()))
		seen := fake.snapshot()
		assert.Equal(t, wantTransition, seen.restoring)
		assert.Equal(t, wantOpts, seen.restoreOpts)
	})

	t.Run("RestoreCheckpointed", func(t *testing.T) {
		t.Parallel()

		mirror := &statepb.NetworkPlacement{}
		original := &orchestrator.SandboxNetworkPlacement{
			ExecutionId: uuid.NewString(), NetworkMode: "full-router", RouterId: "router", IngressAddress: "10.0.0.2", IngressPort: 5010,
		}
		require.Equal(t, original.ProtoReflect().Descriptor().Fields().Len(), mirror.ProtoReflect().Descriptor().Fields().Len(), "the mirror has every field of the orchestrator's placement")
		requireEveryFieldSet(t, original.ProtoReflect())
		copyByName(original.ProtoReflect(), mirror.ProtoReflect())
		requireEveryFieldSet(t, mirror.ProtoReflect())

		fake := newFake()
		fake.record = full
		client := newClient(t, fake)
		_, err := client.RestoreCheckpointed(callContext(t), statepb.RestoreCheckpointedRequest_builder{Transition: wireTransition(t, full), Placement: mirror}.Build())
		require.NoError(t, err)
		seen := fake.snapshot()
		assert.Equal(t, wantTransition, seen.restoring)
		assert.True(t, proto.Equal(original, seen.placement), "orchestrator to mirror to orchestrator: got %v", seen.placement)

		back := &statepb.NetworkPlacement{}
		copyByName(seen.placement.ProtoReflect(), back.ProtoReflect())
		assert.True(t, proto.Equal(mirror, back), "mirror to orchestrator to mirror: got %v", back)

		_, err = client.RestoreCheckpointed(callContext(t), statepb.RestoreCheckpointedRequest_builder{Transition: wireTransition(t, full)}.Build())
		require.NoError(t, err)
		assert.Nil(t, fake.snapshot().placement, "an absent placement is nil")
	})

	t.Run("WaitForTransition", func(t *testing.T) {
		t.Parallel()

		fake := newFake()
		fake.waitID = "transition-1"
		_, err := waitFor(callContext(t), newClient(t, fake), "transition-1")
		require.NoError(t, err)
		seen := fake.snapshot()
		assert.Equal(t, "transition-1", seen.transitionID)
		assert.True(t, seen.waitDeadline)
	})
}
