package state_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/scheduling"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxstore/sandboxtypes"
)

type observedStorage struct {
	sandboxtypes.Storage

	waits chan struct{}

	mu    sync.Mutex
	opts  []sandboxtypes.RemoveOpts
	begun []sandboxtypes.StateTransition
}

func newObservedStorage(storage sandboxtypes.Storage) *observedStorage {
	return &observedStorage{Storage: storage, waits: make(chan struct{}, 64)}
}

func (o *observedStorage) BeginRemoving(ctx context.Context, teamID uuid.UUID, sandboxID string, opts sandboxtypes.RemoveOpts) (sandboxtypes.StateTransition, bool, error) {
	transition, alreadyDone, err := o.Storage.BeginRemoving(ctx, teamID, sandboxID, opts)
	o.mu.Lock()
	defer o.mu.Unlock()
	o.opts = append(o.opts, opts)
	o.begun = append(o.begun, transition)

	return transition, alreadyDone, err
}

func (o *observedStorage) WaitForTransition(ctx context.Context, teamID uuid.UUID, sandboxID string, transitionID string) (string, error) {
	select {
	case o.waits <- struct{}{}:
	default:
	}

	return o.Storage.WaitForTransition(ctx, teamID, sandboxID, transitionID)
}

func (o *observedStorage) awaitWaits(t *testing.T, n int) {
	t.Helper()

	for range n {
		select {
		case <-o.waits:
		case <-time.After(hangTimeout):
			require.Fail(t, "the wait never reached the store")
		}
	}
}

func (o *observedStorage) removeOpts() []sandboxtypes.RemoveOpts {
	o.mu.Lock()
	defer o.mu.Unlock()

	return append([]sandboxtypes.RemoveOpts(nil), o.opts...)
}

type rpcTransitions struct {
	client  scheduling.StateServiceClient
	waiting chan struct{}
}

var _ sandboxtypes.StateTransitions = rpcTransitions{}

var wireActions = map[sandboxtypes.StateAction]scheduling.RemoveAction{
	sandboxtypes.StateActionPause:    scheduling.RemoveAction_REMOVE_ACTION_PAUSE,
	sandboxtypes.StateActionKill:     scheduling.RemoveAction_REMOVE_ACTION_KILL,
	sandboxtypes.StateActionSnapshot: scheduling.RemoveAction_REMOVE_ACTION_SNAPSHOT,
}

var goStates = map[scheduling.SandboxState]sandboxtypes.State{
	scheduling.SandboxState_SANDBOX_STATE_RUNNING:      sandboxtypes.StateRunning,
	scheduling.SandboxState_SANDBOX_STATE_PAUSING:      sandboxtypes.StatePausing,
	scheduling.SandboxState_SANDBOX_STATE_KILLING:      sandboxtypes.StateKilling,
	scheduling.SandboxState_SANDBOX_STATE_SNAPSHOTTING: sandboxtypes.StateSnapshotting,
}

func transitionErrorOf(outcome scheduling.StateOutcome, current scheduling.SandboxState, target sandboxtypes.State) error {
	if outcome == scheduling.StateOutcome_STATE_OUTCOME_INVALID_TRANSITION {
		return &sandboxtypes.InvalidStateTransitionError{CurrentState: goStates[current], TargetState: target}
	}

	return errorOf(outcome)
}

func decodeTransition(wire *scheduling.StateTransition) (sandboxtypes.StateTransition, error) {
	if wire == nil {
		return sandboxtypes.StateTransition{}, nil
	}
	var sbx sandboxtypes.Sandbox
	if err := json.Unmarshal(wire.GetRecord(), &sbx); err != nil {
		return sandboxtypes.StateTransition{}, err
	}
	transition := sandboxtypes.StateTransition{Sandbox: sbx, ID: wire.GetId(), InFlightID: wire.GetInFlightId()}
	if wire.HasOriginalEndTime() {
		end := wire.GetOriginalEndTime().AsTime()
		transition.OriginalEndTime = &end
	}

	return transition, nil
}

func encodeWireTransition(transition sandboxtypes.StateTransition) (*scheduling.StateTransition, error) {
	record, err := json.Marshal(transition.Sandbox)
	if err != nil {
		return nil, err
	}
	wire := scheduling.StateTransition_builder{Record: record, Id: transition.ID, InFlightId: transition.InFlightID}.Build()
	if transition.OriginalEndTime != nil {
		wire.SetOriginalEndTime(timestamppb.New(*transition.OriginalEndTime))
	}

	return wire, nil
}

func (r rpcTransitions) BeginRemoving(ctx context.Context, teamID uuid.UUID, sandboxID string, opts sandboxtypes.RemoveOpts) (sandboxtypes.StateTransition, bool, error) {
	resp, err := r.client.BeginRemoving(ctx, scheduling.BeginRemovingRequest_builder{
		TeamId: teamID.String(), SandboxId: sandboxID,
		Options: scheduling.RemoveOptions_builder{
			Action:             wireActions[opts.Action],
			Eviction:           opts.Eviction,
			Reason:             string(opts.Reason),
			FilesystemOnly:     opts.FilesystemOnly,
			RequireEnvdHealthy: opts.RequireEnvdHealthy,
			ExpectExecutionId:  opts.ExpectExecutionID,
			BeforeEndTime:      opts.BeforeEndTime,
		}.Build(),
	}.Build())
	if err != nil {
		return sandboxtypes.StateTransition{}, false, err
	}
	transition, err := decodeTransition(resp.GetTransition())
	if err != nil {
		return sandboxtypes.StateTransition{}, false, err
	}

	return transition, resp.GetAlreadyDone(), transitionErrorOf(resp.GetOutcome(), resp.GetCurrentState(), opts.Action.TargetState)
}

func (r rpcTransitions) FinishTransition(ctx context.Context, teamID uuid.UUID, sandboxID string, transitionID string, action sandboxtypes.StateAction, outcome sandboxtypes.TransitionOutcome) error {
	resp, err := r.client.FinishTransition(ctx, scheduling.FinishTransitionRequest_builder{
		TeamId: teamID.String(), SandboxId: sandboxID, TransitionId: transitionID, Action: wireActions[action],
		Result: scheduling.TransitionResult_builder{
			Failed: outcome.Failed, Error: outcome.Error, SkipRestore: outcome.SkipRestore, Restored: outcome.Restored, RefusalClass: outcome.RefusalClass,
		}.Build(),
	}.Build())
	if err != nil {
		return err
	}

	return errorOf(resp.GetOutcome())
}

func (r rpcTransitions) restored(record []byte, outcome scheduling.StateOutcome, current scheduling.SandboxState, err error) (sandboxtypes.Sandbox, error) {
	if err != nil {
		return sandboxtypes.Sandbox{}, err
	}
	if err := transitionErrorOf(outcome, current, sandboxtypes.StateRunning); err != nil {
		return sandboxtypes.Sandbox{}, err
	}
	var sbx sandboxtypes.Sandbox
	if err := json.Unmarshal(record, &sbx); err != nil {
		return sandboxtypes.Sandbox{}, err
	}

	return sbx, nil
}

func (r rpcTransitions) RestoreRunning(ctx context.Context, transition sandboxtypes.StateTransition, opts sandboxtypes.RestoreOpts) (sandboxtypes.Sandbox, error) {
	wire, err := encodeWireTransition(transition)
	if err != nil {
		return sandboxtypes.Sandbox{}, err
	}
	resp, err := r.client.RestoreRunning(ctx, scheduling.RestoreRunningRequest_builder{
		Transition: wire,
		Options:    scheduling.RestoreOptions_builder{RetryAfter: durationpb.New(opts.RetryAfter), HoldOnly: opts.HoldOnly, EnvdRefusal: opts.EnvdRefusal}.Build(),
	}.Build())

	return r.restored(resp.GetRecord(), resp.GetOutcome(), resp.GetCurrentState(), err)
}

func (r rpcTransitions) RestoreCheckpointed(ctx context.Context, transition sandboxtypes.StateTransition, placement *orchestrator.SandboxNetworkPlacement) (sandboxtypes.Sandbox, error) {
	wire, err := encodeWireTransition(transition)
	if err != nil {
		return sandboxtypes.Sandbox{}, err
	}
	req := scheduling.RestoreCheckpointedRequest_builder{Transition: wire}.Build()
	if placement != nil {
		mirror := &scheduling.NetworkPlacement{}
		copyByName(placement.ProtoReflect(), mirror.ProtoReflect())
		req.SetPlacement(mirror)
	}
	resp, err := r.client.RestoreCheckpointed(ctx, req)

	return r.restored(resp.GetRecord(), resp.GetOutcome(), resp.GetCurrentState(), err)
}

func (r rpcTransitions) WaitForTransition(ctx context.Context, teamID uuid.UUID, sandboxID string, transitionID string) (string, error) {
	for {
		resp, err := r.client.WaitForTransition(ctx, scheduling.WaitForTransitionRequest_builder{
			TeamId: teamID.String(), SandboxId: sandboxID, TransitionId: transitionID,
		}.Build())
		if err != nil {
			if ctx.Err() != nil {
				return transitionID, ctx.Err()
			}

			return transitionID, err
		}
		transitionID = resp.GetTransitionId()
		switch resp.GetOutcome() {
		case scheduling.WaitForTransitionOutcome_WAIT_FOR_TRANSITION_OUTCOME_WAITING:
			select {
			case r.waiting <- struct{}{}:
			default:
			}

			continue
		case scheduling.WaitForTransitionOutcome_WAIT_FOR_TRANSITION_OUTCOME_FINISHED:
			return transitionID, nil
		case scheduling.WaitForTransitionOutcome_WAIT_FOR_TRANSITION_OUTCOME_RESTORED:
			return transitionID, &sandboxtypes.RestoredTransitionError{Refusal: sandboxtypes.RefusalFromClass(resp.GetRefusalClass())}
		case scheduling.WaitForTransitionOutcome_WAIT_FOR_TRANSITION_OUTCOME_FAILED:
			return transitionID, &sandboxtypes.TransitionFailedError{Message: resp.GetFailure()}
		case scheduling.WaitForTransitionOutcome_WAIT_FOR_TRANSITION_OUTCOME_NOT_FOUND:
			return transitionID, sandboxtypes.ErrNotFound
		default:
			return transitionID, fmt.Errorf("unknown wait outcome %v", resp.GetOutcome())
		}
	}
}

type removeResult struct {
	alreadyDone bool
	errType     string
	errText     string
	state       sandboxtypes.State
	started     bool
	inFlightID  string
	hasOrigEnd  bool
	opts        []sandboxtypes.RemoveOpts
}

type joinScenario struct {
	starter     sandboxtypes.StateAction
	joiner      func(sbx sandboxtypes.Sandbox) sandboxtypes.RemoveOpts
	finish      sandboxtypes.TransitionOutcome
	cancel      bool
	check       func(t *testing.T, err error)
	alreadyDone bool
	started     bool
}

func runJoin(t *testing.T, s redisSetup, path string, tt joinScenario) removeResult {
	t.Helper()

	ctx := callContext(t)
	sbx := minimalRecord(uuid.New(), newSandboxID())
	require.Equal(t, scheduling.StateOutcome_STATE_OUTCOME_OK, s.add(ctx, t, sbx))
	starter, _, err := s.records.BeginRemoving(ctx, sbx.TeamID, sbx.SandboxID, sandboxtypes.RemoveOpts{Action: tt.starter})
	require.NoError(t, err)
	require.NotEmpty(t, starter.ID)

	observed := newObservedStorage(s.records)
	var transitions sandboxtypes.StateTransitions = observed
	awaitWait := func() { observed.awaitWaits(t, 1) }
	if path == "rpc" {
		waiting := make(chan struct{}, 64)
		transitions = rpcTransitions{client: newClientWithWait(t, observed, shortWait), waiting: waiting}
		awaitWait = func() {
			for range 2 {
				select {
				case <-waiting:
				case <-time.After(hangTimeout):
					require.Fail(t, "the join never crossed two polls")
				}
			}
		}
	}

	joinCtx, cancelJoin := context.WithCancel(ctx)
	defer cancelJoin()
	opts := tt.joiner(sbx)
	type joined struct {
		transition  sandboxtypes.StateTransition
		alreadyDone bool
		err         error
	}
	done := make(chan joined, 1)
	go func() {
		transition, alreadyDone, err := sandboxtypes.StartRemoving(joinCtx, transitions, sbx.TeamID, sbx.SandboxID, opts)
		done <- joined{transition, alreadyDone, err}
	}()
	awaitWait()

	if tt.cancel {
		cancelJoin()
	} else {
		require.NoError(t, s.records.FinishTransition(ctx, sbx.TeamID, sbx.SandboxID, starter.ID, tt.starter, tt.finish))
	}

	var got joined
	select {
	case got = <-done:
	case <-time.After(hangTimeout):
		require.Fail(t, "the join never returned")
	}
	if tt.cancel {
		require.NoError(t, s.records.FinishTransition(ctx, sbx.TeamID, sbx.SandboxID, starter.ID, tt.starter, sandboxtypes.TransitionOutcome{}))
	}
	if got.transition.ID != "" {
		require.NoError(t, s.records.FinishTransition(ctx, sbx.TeamID, sbx.SandboxID, got.transition.ID, opts.Action, sandboxtypes.TransitionOutcome{}))
	}

	assert.Equal(t, tt.alreadyDone, got.alreadyDone, path)
	assert.Equal(t, tt.started, got.transition.ID != "", path)
	tt.check(t, got.err)
	for _, seen := range observed.removeOpts() {
		assert.Equal(t, opts, seen, "%s: every begin gets the caller's options", path)
	}

	result := removeResult{
		alreadyDone: got.alreadyDone,
		state:       got.transition.Sandbox.State,
		started:     got.transition.ID != "",
		inFlightID:  got.transition.InFlightID,
		hasOrigEnd:  got.transition.OriginalEndTime != nil,
		opts:        observed.removeOpts(),
	}
	for i := range result.opts {
		result.opts[i].ExpectExecutionID = ""
	}
	if got.err != nil {
		result.errType = fmt.Sprintf("%T", got.err)
		result.errText = got.err.Error()
	}
	if tt.cancel {
		result.errText = ""
	}

	return result
}

func TestStartRemovingThroughRPC(t *testing.T) {
	t.Parallel()

	s := setupRedis(t)
	pause := func(sandboxtypes.Sandbox) sandboxtypes.RemoveOpts {
		return sandboxtypes.RemoveOpts{Action: sandboxtypes.StateActionPause}
	}
	fsPause := func(sandboxtypes.Sandbox) sandboxtypes.RemoveOpts {
		return sandboxtypes.RemoveOpts{Action: sandboxtypes.StateActionPause, FilesystemOnly: true}
	}
	restoredAs := func(refusal error) sandboxtypes.TransitionOutcome {
		return sandboxtypes.TransitionOutcomeOf(&sandboxtypes.RestoredTransitionError{Refusal: refusal})
	}
	refused := func(want error) func(t *testing.T, err error) {
		return func(t *testing.T, err error) {
			t.Helper()
			require.Error(t, err)
			assert.Equal(t, want, err, "the joiner gets the refusal the starter got")
		}
	}

	tests := map[string]joinScenario{
		"same target joins": {
			starter: sandboxtypes.StateActionPause, joiner: pause, alreadyDone: true,
			check: func(t *testing.T, err error) {
				t.Helper()
				require.NoError(t, err)
			},
		},
		"restored busy node": {
			starter: sandboxtypes.StateActionPause, joiner: pause, finish: restoredAs(sandboxtypes.PauseQueueExhaustedError{}),
			check: refused(sandboxtypes.PauseQueueExhaustedError{}),
		},
		"restored envd unresponsive": {
			starter: sandboxtypes.StateActionPause, joiner: pause, finish: restoredAs(sandboxtypes.PauseRefusedEnvdUnresponsiveError{}),
			check: refused(sandboxtypes.PauseRefusedEnvdUnresponsiveError{}),
		},
		"restored unsupported, filesystem joiner": {
			starter: sandboxtypes.StateActionPause, joiner: fsPause, finish: restoredAs(sandboxtypes.PauseUnsupportedError{}),
			check: refused(sandboxtypes.PauseUnsupportedError{}),
		},
		"restored unsupported, memory joiner": {
			starter: sandboxtypes.StateActionPause, joiner: pause, finish: restoredAs(sandboxtypes.PauseUnsupportedError{}),
			check: refused(sandboxtypes.PauseQueueExhaustedError{}),
		},
		"starter failed": {
			starter: sandboxtypes.StateActionPause, joiner: pause, finish: sandboxtypes.TransitionOutcomeOf(errors.New("boom")),
			check: func(t *testing.T, err error) {
				t.Helper()
				require.EqualError(t, err, "failed waiting for transition: transition failed: boom")
				failed, ok := errors.AsType[*sandboxtypes.TransitionFailedError](err)
				require.True(t, ok, "the failure is typed")
				assert.Equal(t, "boom", failed.Message)
			},
		},
		"another target begins again": {
			starter: sandboxtypes.StateActionPause,
			joiner: func(sbx sandboxtypes.Sandbox) sandboxtypes.RemoveOpts {
				return sandboxtypes.RemoveOpts{Action: sandboxtypes.StateActionKill, Reason: sandboxtypes.KillReasonRequest, ExpectExecutionID: sbx.ExecutionID}
			},
			started: true,
			check: func(t *testing.T, err error) {
				t.Helper()
				require.NoError(t, err)
			},
		},
		"joiner's context ends": {
			starter: sandboxtypes.StateActionPause, joiner: pause, cancel: true,
			check: func(t *testing.T, err error) {
				t.Helper()
				require.ErrorIs(t, err, context.Canceled)
			},
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			direct := runJoin(t, s, "direct", tt)
			rpc := runJoin(t, s, "rpc", tt)
			assert.Equal(t, direct, rpc, "the RPC path returns what the in-process path returns")
		})
	}
}
