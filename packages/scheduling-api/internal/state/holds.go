package state

import (
	"context"
	"encoding/json"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	statepb "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/scheduling/state"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxstore/sandboxtypes"
)

func (s *Server) HoldExecution(ctx context.Context, req *statepb.HoldExecutionRequest) (*statepb.HoldExecutionResponse, error) {
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

	return statepb.HoldExecutionResponse_builder{Outcome: outcome}.Build(), nil
}

func (s *Server) MoveHeldExecution(ctx context.Context, req *statepb.MoveHeldExecutionRequest) (*statepb.MoveHeldExecutionResponse, error) {
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

	return statepb.MoveHeldExecutionResponse_builder{Outcome: outcome, Moved: moved}.Build(), nil
}

func (s *Server) ReleaseHeldExecution(ctx context.Context, req *statepb.ReleaseHeldExecutionRequest) (*statepb.ReleaseHeldExecutionResponse, error) {
	hold, err := parseHold(req.GetHold())
	if err != nil {
		return nil, err
	}

	err = s.storage.ReleaseHeldExecution(ctx, hold)
	outcome, ok := outcomeOf(err)
	if !ok {
		return nil, storeError(ctx, err, logger.WithTeamID(hold.TeamID.String()), logger.WithSandboxID(hold.SandboxID))
	}

	return statepb.ReleaseHeldExecutionResponse_builder{Outcome: outcome}.Build(), nil
}

func (s *Server) RemoveHeldExecution(ctx context.Context, req *statepb.RemoveHeldExecutionRequest) (*statepb.RemoveHeldExecutionResponse, error) {
	hold, err := parseHold(req.GetHold())
	if err != nil {
		return nil, err
	}

	removed, err := s.storage.RemoveHeldExecution(ctx, hold)
	outcome, ok := outcomeOf(err)
	if !ok {
		return nil, storeError(ctx, err, logger.WithTeamID(hold.TeamID.String()), logger.WithSandboxID(hold.SandboxID))
	}
	if outcome != statepb.StateOutcome_STATE_OUTCOME_OK {
		return statepb.RemoveHeldExecutionResponse_builder{Outcome: outcome}.Build(), nil
	}

	record, err := json.Marshal(removed)
	if err != nil {
		return nil, storeError(ctx, err, logger.WithTeamID(hold.TeamID.String()), logger.WithSandboxID(hold.SandboxID))
	}

	return statepb.RemoveHeldExecutionResponse_builder{Outcome: outcome, Record: record}.Build(), nil
}

func parseHold(hold *statepb.Hold) (sandboxtypes.Hold, error) {
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
