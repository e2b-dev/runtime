package state

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	statepb "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/scheduling/state"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxstore/sandboxtypes"
)

// The store compares actions by value, so only these exact values reach it.
var actions = map[statepb.RemoveAction]sandboxtypes.StateAction{
	statepb.RemoveAction_REMOVE_ACTION_PAUSE:    sandboxtypes.StateActionPause,
	statepb.RemoveAction_REMOVE_ACTION_KILL:     sandboxtypes.StateActionKill,
	statepb.RemoveAction_REMOVE_ACTION_SNAPSHOT: sandboxtypes.StateActionSnapshot,
}

var wireStates = func() map[sandboxtypes.State]statepb.SandboxState {
	wire := make(map[sandboxtypes.State]statepb.SandboxState, len(states))
	for value, state := range states {
		wire[state] = value
	}

	return wire
}()

func (s *Server) BeginRemoving(ctx context.Context, req *statepb.BeginRemovingRequest) (*statepb.BeginRemovingResponse, error) {
	teamID, err := parseIDs(req.GetTeamId(), req.GetSandboxId())
	if err != nil {
		return nil, err
	}
	opts, err := removeOpts(req.GetOptions())
	if err != nil {
		return nil, err
	}

	transition, alreadyDone, err := s.storage.BeginRemoving(ctx, teamID, req.GetSandboxId(), opts)
	outcome, current, err := stateOutcome(err)
	if err != nil {
		return nil, storeError(ctx, err, logger.WithTeamID(req.GetTeamId()), logger.WithSandboxID(req.GetSandboxId()))
	}

	resp := statepb.BeginRemovingResponse_builder{Outcome: outcome, AlreadyDone: alreadyDone, CurrentState: current}.Build()
	if !reflect.ValueOf(transition).IsZero() {
		wire, err := encodeTransition(transition)
		if err != nil {
			return nil, storeError(ctx, err, logger.WithTeamID(req.GetTeamId()), logger.WithSandboxID(req.GetSandboxId()))
		}
		resp.SetTransition(wire)
	}

	return resp, nil
}

func (s *Server) FinishTransition(ctx context.Context, req *statepb.FinishTransitionRequest) (*statepb.FinishTransitionResponse, error) {
	teamID, err := parseIDs(req.GetTeamId(), req.GetSandboxId())
	if err != nil {
		return nil, err
	}
	if req.GetTransitionId() == "" {
		return nil, status.Error(codes.InvalidArgument, "transition_id: must not be empty")
	}
	action, ok := actions[req.GetAction()]
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "action: must be defined and not unspecified")
	}
	result := req.GetResult()
	outcome := sandboxtypes.TransitionOutcome{
		Failed:       result.GetFailed(),
		Error:        result.GetError(),
		SkipRestore:  result.GetSkipRestore(),
		Restored:     result.GetRestored(),
		RefusalClass: result.GetRefusalClass(),
	}

	if err := s.storage.FinishTransition(ctx, teamID, req.GetSandboxId(), req.GetTransitionId(), action, outcome); err != nil {
		return nil, storeError(ctx, err, logger.WithTeamID(req.GetTeamId()), logger.WithSandboxID(req.GetSandboxId()))
	}

	return statepb.FinishTransitionResponse_builder{Outcome: statepb.StateOutcome_STATE_OUTCOME_OK}.Build(), nil
}

func (s *Server) RestoreRunning(ctx context.Context, req *statepb.RestoreRunningRequest) (*statepb.RestoreRunningResponse, error) {
	transition, err := parseTransition(req.GetTransition())
	if err != nil {
		return nil, err
	}
	options := req.GetOptions()
	if options.HasRetryAfter() && !options.GetRetryAfter().IsValid() {
		return nil, status.Error(codes.InvalidArgument, "options.retry_after: must be a valid duration")
	}
	opts := sandboxtypes.RestoreOpts{
		RetryAfter:  options.GetRetryAfter().AsDuration(),
		HoldOnly:    options.GetHoldOnly(),
		EnvdRefusal: options.GetEnvdRefusal(),
	}

	restored, err := s.storage.RestoreRunning(ctx, transition, opts)
	outcome, current, record, err := restoreResult(restored, err)
	if err != nil {
		return nil, storeError(ctx, err, logger.WithTeamID(transition.Sandbox.TeamID.String()), logger.WithSandboxID(transition.Sandbox.SandboxID))
	}

	return statepb.RestoreRunningResponse_builder{Outcome: outcome, Record: record, CurrentState: current}.Build(), nil
}

func (s *Server) RestoreCheckpointed(ctx context.Context, req *statepb.RestoreCheckpointedRequest) (*statepb.RestoreCheckpointedResponse, error) {
	transition, err := parseTransition(req.GetTransition())
	if err != nil {
		return nil, err
	}
	var placement *orchestrator.SandboxNetworkPlacement
	if p := req.GetPlacement(); p != nil {
		placement = &orchestrator.SandboxNetworkPlacement{
			ExecutionId:    p.GetExecutionId(),
			NetworkMode:    p.GetNetworkMode(),
			RouterId:       p.GetRouterId(),
			IngressAddress: p.GetIngressAddress(),
			IngressPort:    p.GetIngressPort(),
		}
	}

	restored, err := s.storage.RestoreCheckpointed(ctx, transition, placement)
	outcome, current, record, err := restoreResult(restored, err)
	if err != nil {
		return nil, storeError(ctx, err, logger.WithTeamID(transition.Sandbox.TeamID.String()), logger.WithSandboxID(transition.Sandbox.SandboxID))
	}

	return statepb.RestoreCheckpointedResponse_builder{Outcome: outcome, Record: record, CurrentState: current}.Build(), nil
}

func (s *Server) WaitForTransition(ctx context.Context, req *statepb.WaitForTransitionRequest) (*statepb.WaitForTransitionResponse, error) {
	teamID, err := parseIDs(req.GetTeamId(), req.GetSandboxId())
	if err != nil {
		return nil, err
	}

	waitCtx, cancel := context.WithTimeout(ctx, s.maxWait)
	defer cancel()

	transitionID, err := s.storage.WaitForTransition(waitCtx, teamID, req.GetSandboxId(), req.GetTransitionId())
	resp := statepb.WaitForTransitionResponse_builder{TransitionId: transitionID}.Build()
	if err == nil {
		resp.SetOutcome(statepb.WaitForTransitionOutcome_WAIT_FOR_TRANSITION_OUTCOME_FINISHED)

		return resp, nil
	}
	if restored, ok := errors.AsType[*sandboxtypes.RestoredTransitionError](err); ok {
		resp.SetOutcome(statepb.WaitForTransitionOutcome_WAIT_FOR_TRANSITION_OUTCOME_RESTORED)
		resp.SetRefusalClass(sandboxtypes.RefusalClass(restored.Refusal))

		return resp, nil
	}
	if failed, ok := errors.AsType[*sandboxtypes.TransitionFailedError](err); ok {
		resp.SetOutcome(statepb.WaitForTransitionOutcome_WAIT_FOR_TRANSITION_OUTCOME_FAILED)
		resp.SetFailure(failed.Message)

		return resp, nil
	}
	if errors.Is(err, sandboxtypes.ErrNotFound) {
		resp.SetOutcome(statepb.WaitForTransitionOutcome_WAIT_FOR_TRANSITION_OUTCOME_NOT_FOUND)

		return resp, nil
	}
	// A Redis call cut by the wait's deadline can fail with a socket timeout
	// instead of context.DeadlineExceeded, so the expired wait context counts too.
	waitEnded := errors.Is(err, context.DeadlineExceeded) || errors.Is(waitCtx.Err(), context.DeadlineExceeded)
	if waitEnded && ctx.Err() == nil {
		resp.SetOutcome(statepb.WaitForTransitionOutcome_WAIT_FOR_TRANSITION_OUTCOME_WAITING)

		return resp, nil
	}

	return nil, storeError(ctx, err, logger.WithTeamID(req.GetTeamId()), logger.WithSandboxID(req.GetSandboxId()))
}

func removeOpts(options *statepb.RemoveOptions) (sandboxtypes.RemoveOpts, error) {
	if options == nil {
		return sandboxtypes.RemoveOpts{}, status.Error(codes.InvalidArgument, "options: must be set")
	}
	action, ok := actions[options.GetAction()]
	if !ok {
		return sandboxtypes.RemoveOpts{}, status.Error(codes.InvalidArgument, "options.action: must be defined and not unspecified")
	}

	return sandboxtypes.RemoveOpts{
		Action:             action,
		Eviction:           options.GetEviction(),
		Reason:             sandboxtypes.KillReason(options.GetReason()),
		FilesystemOnly:     options.GetFilesystemOnly(),
		RequireEnvdHealthy: options.GetRequireEnvdHealthy(),
		ExpectExecutionID:  options.GetExpectExecutionId(),
		BeforeEndTime:      options.GetBeforeEndTime(),
	}, nil
}

func parseTransition(transition *statepb.StateTransition) (sandboxtypes.StateTransition, error) {
	if transition == nil {
		return sandboxtypes.StateTransition{}, status.Error(codes.InvalidArgument, "transition: must be set")
	}
	var sbx sandboxtypes.Sandbox
	if err := json.Unmarshal(transition.GetRecord(), &sbx); err != nil {
		return sandboxtypes.StateTransition{}, status.Error(codes.InvalidArgument, "transition.record: must be the JSON of a sandbox record")
	}
	if err := checkRecord(sbx); err != nil {
		return sandboxtypes.StateTransition{}, err
	}
	if !transition.HasOriginalEndTime() || !transition.GetOriginalEndTime().IsValid() {
		return sandboxtypes.StateTransition{}, status.Error(codes.InvalidArgument, "transition.original_end_time: must be a valid timestamp")
	}
	originalEndTime := transition.GetOriginalEndTime().AsTime()

	return sandboxtypes.StateTransition{
		Sandbox:         sbx,
		OriginalEndTime: &originalEndTime,
		ID:              transition.GetId(),
		InFlightID:      transition.GetInFlightId(),
	}, nil
}

func encodeTransition(transition sandboxtypes.StateTransition) (*statepb.StateTransition, error) {
	record, err := json.Marshal(transition.Sandbox)
	if err != nil {
		return nil, err
	}
	wire := statepb.StateTransition_builder{
		Record:     record,
		Id:         transition.ID,
		InFlightId: transition.InFlightID,
	}.Build()
	if transition.OriginalEndTime != nil {
		wire.SetOriginalEndTime(timestamppb.New(*transition.OriginalEndTime))
	}

	return wire, nil
}

func restoreResult(restored sandboxtypes.Sandbox, err error) (statepb.StateOutcome, statepb.SandboxState, []byte, error) {
	outcome, current, err := stateOutcome(err)
	if err != nil || outcome != statepb.StateOutcome_STATE_OUTCOME_OK {
		return outcome, current, nil, err
	}
	record, err := json.Marshal(restored)
	if err != nil {
		return outcome, current, nil, err
	}

	return outcome, current, record, nil
}

func stateOutcome(err error) (statepb.StateOutcome, statepb.SandboxState, error) {
	outcome, ok := outcomeOf(err)
	if !ok {
		return outcome, statepb.SandboxState_SANDBOX_STATE_UNSPECIFIED, err
	}
	invalid, ok := errors.AsType[*sandboxtypes.InvalidStateTransitionError](err)
	if !ok || outcome != statepb.StateOutcome_STATE_OUTCOME_INVALID_TRANSITION {
		return outcome, statepb.SandboxState_SANDBOX_STATE_UNSPECIFIED, nil
	}
	current, ok := wireStates[invalid.CurrentState]
	if !ok {
		return outcome, statepb.SandboxState_SANDBOX_STATE_UNSPECIFIED, fmt.Errorf("unknown sandbox state %q: %w", invalid.CurrentState, err)
	}

	return outcome, current, nil
}
