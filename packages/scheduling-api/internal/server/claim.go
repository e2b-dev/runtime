package server

import (
	"context"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/e2b-dev/infra/packages/scheduling-api/internal/store"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/scheduling"
)

func (s *service) ClaimTransition(ctx context.Context, req *scheduling.ClaimTransitionRequest) (*scheduling.ClaimTransitionResponse, error) {
	projectID, actorID, err := parseIDs(req.GetProjectId(), req.GetSandboxId())
	if err != nil {
		return nil, err
	}
	oldID, err := parseUUID("old_transition_id", req.GetOldTransitionId())
	if err != nil {
		return nil, err
	}
	newID, err := parseUUID("new_transition_id", req.GetNewTransitionId())
	if err != nil {
		return nil, err
	}
	if oldID == newID {
		return nil, status.Error(codes.InvalidArgument, "new_transition_id: must differ from old_transition_id")
	}

	result, err := s.store.ClaimTransition(ctx, store.ClaimRequest{
		ProjectID:           projectID,
		ActorID:             actorID,
		OldTransitionID:     oldID,
		NewTransitionID:     newID,
		ExpectedExecutionID: req.GetExpectedExecutionId(),
	})
	if err != nil {
		return nil, internalError(ctx, err)
	}

	switch result.Outcome {
	case store.ClaimClaimed:
		a := result.Actor
		wire, err := wireActor(a)
		if err != nil {
			return nil, internalError(ctx, err)
		}
		action, err := transitionAction(result.Action)
		if err != nil {
			return nil, internalError(ctx, err)
		}

		return scheduling.ClaimTransitionResponse_builder{
			Outcome:      scheduling.ClaimTransitionOutcome_CLAIM_TRANSITION_OUTCOME_CLAIMED,
			TransitionId: result.TransitionID.String(),
			Action:       action,
			TransitionAt: timestamppb.New(result.TransitionAt),
			Sandbox:      wire,
		}.Build(), nil
	case store.ClaimConflict:
		action, err := transitionAction(result.Action)
		if err != nil {
			return nil, internalError(ctx, err)
		}

		return scheduling.ClaimTransitionResponse_builder{
			Outcome:      scheduling.ClaimTransitionOutcome_CLAIM_TRANSITION_OUTCOME_CONFLICT,
			TransitionId: result.TransitionID.String(),
			Action:       action,
		}.Build(), nil
	case store.ClaimNotFound:
		return scheduling.ClaimTransitionResponse_builder{Outcome: scheduling.ClaimTransitionOutcome_CLAIM_TRANSITION_OUTCOME_NOT_FOUND}.Build(), nil
	case store.ClaimNotStale:
		return scheduling.ClaimTransitionResponse_builder{Outcome: scheduling.ClaimTransitionOutcome_CLAIM_TRANSITION_OUTCOME_NOT_STALE}.Build(), nil
	case store.ClaimExecutionMismatch:
		return scheduling.ClaimTransitionResponse_builder{Outcome: scheduling.ClaimTransitionOutcome_CLAIM_TRANSITION_OUTCOME_EXECUTION_MISMATCH}.Build(), nil
	case store.ClaimAlreadyFinished:
		return scheduling.ClaimTransitionResponse_builder{Outcome: scheduling.ClaimTransitionOutcome_CLAIM_TRANSITION_OUTCOME_ALREADY_FINISHED}.Build(), nil
	case store.ClaimAlreadyRestored:
		return scheduling.ClaimTransitionResponse_builder{Outcome: scheduling.ClaimTransitionOutcome_CLAIM_TRANSITION_OUTCOME_ALREADY_RESTORED}.Build(), nil
	default:
		return nil, internalError(ctx, fmt.Errorf("claim transition: unknown outcome %d", result.Outcome))
	}
}
