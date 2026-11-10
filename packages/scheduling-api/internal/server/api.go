package server

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/e2b-dev/infra/packages/scheduling-api/internal/store"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/scheduling"
)

func wireActor(a *store.LiveActor) (*scheduling.Sandbox, error) {
	if a == nil {
		return nil, nil
	}
	id, state, err := sandboxIDAndState(a)
	if err != nil {
		return nil, err
	}
	b := scheduling.Sandbox_builder{SandboxId: id, ProjectId: a.ProjectID.String(), State: state, EndTime: timestamppb.New(a.EndTime), ExecutionId: a.ExecutionID, StartTime: timestamppb.New(a.StartTime), MaxInstanceLength: durationpb.New(a.MaxInstanceLength), NodeId: a.NodeID, ClusterId: a.ClusterID.String(), BaseTemplateId: a.BaseTemplateID, TemplateId: a.TemplateID, Metadata: a.Metadata, AutoPause: a.AutoPause, AutoPauseFilesystemOnly: a.AutoPauseFilesystemOnly, Record: a.Record}
	if a.RefusedUntil != nil {
		b.RefusedUntil = timestamppb.New(*a.RefusedUntil)
	}
	if a.RefusedSince != nil {
		b.RefusedSince = timestamppb.New(*a.RefusedSince)
	}
	if a.EnvdRefusedUntil != nil {
		b.EnvdRefusedUntil = timestamppb.New(*a.EnvdRefusedUntil)
	}
	if a.EnvdRefusedSince != nil {
		b.EnvdRefusedSince = timestamppb.New(*a.EnvdRefusedSince)
	}

	return b.Build(), nil
}

func (s *service) ListLive(ctx context.Context, req *scheduling.ListLiveRequest) (*scheduling.ListLiveResponse, error) {
	var project *uuid.UUID
	if req.GetProjectId() != "" {
		p, err := parseUUID("project_id", req.GetProjectId())
		if err != nil {
			return nil, err
		}
		project = &p
	}
	after := uuid.Nil
	if req.GetPageToken() != "" {
		var err error
		after, err = parseUUID("page_token", req.GetPageToken())
		if err != nil {
			return nil, err
		}
	}
	states := make([]string, 0, len(req.GetStates()))
	for _, state := range req.GetStates() {
		switch state {
		case scheduling.SandboxState_SANDBOX_STATE_RUNNING:
			states = append(states, "running")
		case scheduling.SandboxState_SANDBOX_STATE_PAUSING:
			states = append(states, "pausing")
		case scheduling.SandboxState_SANDBOX_STATE_KILLING:
			states = append(states, "killing")
		case scheduling.SandboxState_SANDBOX_STATE_SNAPSHOTTING:
			states = append(states, "snapshotting")
		default:
			return nil, status.Error(codes.InvalidArgument, "invalid state")
		}
	}
	size := req.GetPageSize()
	if size == 0 {
		size = 100
	}
	actors, err := s.store.ListLive(ctx, project, states, req.GetExpiredOnly(), after, size)
	if err != nil {
		return nil, internalError(ctx, err)
	}
	response := scheduling.ListLiveResponse_builder{}
	for _, a := range actors {
		wire, err := wireActor(a)
		if err != nil {
			return nil, internalError(ctx, err)
		}
		response.Sandboxes = append(response.Sandboxes, wire)
	}
	if len(actors) == int(size) {
		response.NextPageToken = actors[len(actors)-1].ActorID.String()
	}

	return response.Build(), nil
}

func (s *service) UpdateDeadline(ctx context.Context, req *scheduling.UpdateDeadlineRequest) (*scheduling.UpdateDeadlineResponse, error) {
	project, actor, err := parseIDs(req.GetProjectId(), req.GetSandboxId())
	if err != nil {
		return nil, err
	}
	if err := req.GetEndTime().CheckValid(); err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid end_time")
	}
	if err := req.GetExpectedEndTime().CheckValid(); err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid expected_end_time")
	}
	a, err := s.store.UpdateDeadline(ctx, project, actor, req.GetExpectedExecutionId(), req.GetEndTime().AsTime(), req.GetExpectedEndTime().AsTime())
	if errors.Is(err, store.ErrConflict) {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	if err != nil {
		return nil, internalError(ctx, err)
	}
	wire, err := wireActor(a)
	if err != nil {
		return nil, internalError(ctx, err)
	}

	return scheduling.UpdateDeadlineResponse_builder{Sandbox: wire}.Build(), nil
}

func (s *service) InspectReservation(ctx context.Context, req *scheduling.InspectReservationRequest) (*scheduling.InspectReservationResponse, error) {
	project, actor, err := parseIDs(req.GetProjectId(), req.GetSandboxId())
	if err != nil {
		return nil, err
	}
	result, err := s.store.InspectReservation(ctx, project, actor)
	if err != nil {
		return nil, internalError(ctx, err)
	}
	wire, err := wireActor(result.Actor)
	if err != nil {
		return nil, internalError(ctx, err)
	}
	b := scheduling.InspectReservationResponse_builder{Pending: result.Pending, Sandbox: wire}
	if result.ReservationID != uuid.Nil {
		b.ReservationId = result.ReservationID.String()
	}
	if result.TransitionID != nil {
		b.TransitionId = result.TransitionID.String()
	}
	if result.Failure != nil {
		b.Failure = scheduling.Failure_builder{Message: result.Failure.Message, Code: result.Failure.Code}.Build()
	}

	return b.Build(), nil
}

func (s *service) ClaimSnapshotDeletion(ctx context.Context, req *scheduling.ClaimSnapshotDeletionRequest) (*scheduling.ClaimSnapshotDeletionResponse, error) {
	project, actor, err := parseIDs(req.GetProjectId(), req.GetSandboxId())
	if err != nil {
		return nil, err
	}
	claim, err := parseUUID("claim_id", req.GetClaimId())
	if err != nil {
		return nil, err
	}
	claimed, err := s.store.ClaimSnapshotDeletion(ctx, project, actor, claim)
	if errors.Is(err, store.ErrOtherProject) {
		return nil, status.Error(codes.NotFound, "sandbox not found")
	}
	if err != nil {
		return nil, internalError(ctx, err)
	}

	return scheduling.ClaimSnapshotDeletionResponse_builder{Claimed: claimed}.Build(), nil
}

func (s *service) ReleaseSnapshotDeletionClaim(ctx context.Context, req *scheduling.ReleaseSnapshotDeletionClaimRequest) (*scheduling.ReleaseSnapshotDeletionClaimResponse, error) {
	project, actor, err := parseIDs(req.GetProjectId(), req.GetSandboxId())
	if err != nil {
		return nil, err
	}
	claim, err := parseUUID("claim_id", req.GetClaimId())
	if err != nil {
		return nil, err
	}
	if err := s.store.ReleaseSnapshotDeletionClaim(ctx, project, actor, claim); err != nil {
		return nil, internalError(ctx, err)
	}

	return scheduling.ReleaseSnapshotDeletionClaimResponse_builder{}.Build(), nil
}
