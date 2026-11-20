package state

import (
	"context"
	"encoding/json"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/scheduling"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxstore/sandboxtypes"
)

func (s *Server) HoldExecution(ctx context.Context, req *scheduling.HoldExecutionRequest) (*scheduling.HoldExecutionResponse, error) {
	hold, err := parseHold(req.GetHold())
	if err != nil {
		return nil, err
	}
	if hold.SourceNodeID == "" {
		return nil, status.Error(codes.InvalidArgument, "hold.source_node_id: must not be empty")
	}
	if !req.HasUntil() || !req.GetUntil().IsValid() {
		return nil, status.Error(codes.InvalidArgument, "until: must be a valid timestamp")
	}

	err = s.storage.HoldExecution(ctx, hold, req.GetUntil().AsTime())
	outcome, ok := outcomeOf(err)
	if !ok {
		return nil, storeError(ctx, err, logger.WithTeamID(hold.TeamID.String()), logger.WithSandboxID(hold.SandboxID))
	}

	return scheduling.HoldExecutionResponse_builder{Outcome: outcome}.Build(), nil
}

func (s *Server) MoveHeldExecution(ctx context.Context, req *scheduling.MoveHeldExecutionRequest) (*scheduling.MoveHeldExecutionResponse, error) {
	hold, err := parseHold(req.GetHold())
	if err != nil {
		return nil, err
	}
	if req.GetNodeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id: must not be empty")
	}

	moved, err := s.storage.MoveHeldExecution(ctx, hold, req.GetNodeId())
	outcome, ok := outcomeOf(err)
	if !ok {
		return nil, storeError(ctx, err, logger.WithTeamID(hold.TeamID.String()), logger.WithSandboxID(hold.SandboxID))
	}

	return scheduling.MoveHeldExecutionResponse_builder{Outcome: outcome, Moved: moved}.Build(), nil
}

func (s *Server) ReleaseHeldExecution(ctx context.Context, req *scheduling.ReleaseHeldExecutionRequest) (*scheduling.ReleaseHeldExecutionResponse, error) {
	hold, err := parseHold(req.GetHold())
	if err != nil {
		return nil, err
	}

	err = s.storage.ReleaseHeldExecution(ctx, hold)
	outcome, ok := outcomeOf(err)
	if !ok {
		return nil, storeError(ctx, err, logger.WithTeamID(hold.TeamID.String()), logger.WithSandboxID(hold.SandboxID))
	}

	return scheduling.ReleaseHeldExecutionResponse_builder{Outcome: outcome}.Build(), nil
}

func (s *Server) RemoveHeldExecution(ctx context.Context, req *scheduling.RemoveHeldExecutionRequest) (*scheduling.RemoveHeldExecutionResponse, error) {
	hold, err := parseHold(req.GetHold())
	if err != nil {
		return nil, err
	}

	removed, err := s.storage.RemoveHeldExecution(ctx, hold)
	outcome, ok := outcomeOf(err)
	if !ok {
		return nil, storeError(ctx, err, logger.WithTeamID(hold.TeamID.String()), logger.WithSandboxID(hold.SandboxID))
	}
	if outcome != scheduling.StateOutcome_STATE_OUTCOME_OK {
		return scheduling.RemoveHeldExecutionResponse_builder{Outcome: outcome}.Build(), nil
	}

	record, err := json.Marshal(removed)
	if err != nil {
		return nil, storeError(ctx, err, logger.WithTeamID(hold.TeamID.String()), logger.WithSandboxID(hold.SandboxID))
	}

	return scheduling.RemoveHeldExecutionResponse_builder{Outcome: outcome, Record: record}.Build(), nil
}

func parseHold(hold *scheduling.Hold) (sandboxtypes.Hold, error) {
	if hold == nil {
		return sandboxtypes.Hold{}, status.Error(codes.InvalidArgument, "hold: must be set")
	}
	teamID, err := parseIDs(hold.GetTeamId(), hold.GetSandboxId())
	if err != nil {
		return sandboxtypes.Hold{}, err
	}
	if hold.GetId() == "" {
		return sandboxtypes.Hold{}, status.Error(codes.InvalidArgument, "hold.id: must not be empty")
	}

	return sandboxtypes.Hold{
		TeamID:       teamID,
		SandboxID:    hold.GetSandboxId(),
		ExecutionID:  hold.GetExecutionId(),
		SourceNodeID: hold.GetSourceNodeId(),
		ID:           hold.GetId(),
	}, nil
}
