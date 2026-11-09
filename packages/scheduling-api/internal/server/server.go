package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"

	"buf.build/go/protovalidate"
	"github.com/google/uuid"
	protovalidatemw "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/protovalidate"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/e2b-dev/infra/packages/scheduling-api/internal/id"
	"github.com/e2b-dev/infra/packages/scheduling-api/internal/store"
	e2bgrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/scheduling"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

const (
	unaryDeadline        = 10 * time.Second
	maxWait              = 5 * time.Second
	internalMessage      = "internal error"
	invalidRecordMessage = "record: must be a JSON object that PostgreSQL jsonb can store"
)

func New(tel *telemetry.Client, st *store.Store) (*grpc.Server, error) {
	validator, err := protovalidate.New()
	if err != nil {
		return nil, fmt.Errorf("create the request validator: %w", err)
	}

	server := e2bgrpc.NewGRPCServer(
		tel,
		e2bgrpc.WithRecoveryHandler(func(any) error { return status.Error(codes.Internal, internalMessage) }),
		e2bgrpc.WithUnaryDeadline(unaryDeadline),
		e2bgrpc.WithUnaryInterceptors(protovalidatemw.UnaryServerInterceptor(validator)),
		e2bgrpc.WithWaitForHandlers(),
	)
	scheduling.RegisterSchedulingServiceServer(server, &service{store: st})

	return server, nil
}

type service struct {
	scheduling.UnimplementedSchedulingServiceServer

	store *store.Store
}

func (s *service) Reserve(ctx context.Context, req *scheduling.ReserveRequest) (*scheduling.ReserveResponse, error) {
	projectID, actorID, err := parseIDs(req.GetProjectId(), req.GetSandboxId())
	if err != nil {
		return nil, err
	}
	reservationID, err := parseUUID("reservation_id", req.GetReservationId())
	if err != nil {
		return nil, err
	}

	result, err := s.store.Reserve(ctx, store.Reservation{
		ProjectID:     projectID,
		ActorID:       actorID,
		ReservationID: reservationID,
		Limit:         req.GetLimit(),
		EndTime:       req.GetEndTime().AsTime(),
	})
	if errors.Is(err, store.ErrOtherProject) {
		return nil, status.Error(codes.AlreadyExists, "sandbox_id is held by another project")
	}
	if err != nil {
		return nil, internalError(ctx, err)
	}

	switch result.Outcome {
	case store.ReserveReserved:
		return scheduling.ReserveResponse_builder{Outcome: scheduling.ReserveOutcome_RESERVE_OUTCOME_RESERVED}.Build(), nil
	case store.ReserveAlreadyPending:
		return scheduling.ReserveResponse_builder{Outcome: scheduling.ReserveOutcome_RESERVE_OUTCOME_ALREADY_PENDING}.Build(), nil
	case store.ReserveLimitExceeded:
		return scheduling.ReserveResponse_builder{Outcome: scheduling.ReserveOutcome_RESERVE_OUTCOME_LIMIT_EXCEEDED}.Build(), nil
	case store.ReserveFinished:
		var failure *scheduling.Failure
		if result.Failure != nil {
			failure = scheduling.Failure_builder{
				Message: result.Failure.Message,
				Code:    result.Failure.Code,
			}.Build()
		}

		return scheduling.ReserveResponse_builder{
			Outcome: scheduling.ReserveOutcome_RESERVE_OUTCOME_FINISHED,
			Failure: failure,
		}.Build(), nil
	case store.ReserveAlreadyRunning:
		a := result.Actor
		sandboxID, state, err := sandboxIDAndState(a)
		if err != nil {
			return nil, internalError(ctx, err)
		}

		return scheduling.ReserveResponse_builder{
			Outcome: scheduling.ReserveOutcome_RESERVE_OUTCOME_ALREADY_RUNNING,
			Sandbox: scheduling.Sandbox_builder{
				SandboxId:               sandboxID,
				ProjectId:               a.ProjectID.String(),
				State:                   state,
				EndTime:                 timestamppb.New(a.EndTime),
				ExecutionId:             a.ExecutionID,
				StartTime:               timestamppb.New(a.StartTime),
				MaxInstanceLength:       durationpb.New(a.MaxInstanceLength),
				NodeId:                  a.NodeID,
				ClusterId:               a.ClusterID.String(),
				BaseTemplateId:          a.BaseTemplateID,
				TemplateId:              a.TemplateID,
				Metadata:                a.Metadata,
				AutoPause:               a.AutoPause,
				AutoPauseFilesystemOnly: a.AutoPauseFilesystemOnly,
				Record:                  a.Record,
			}.Build(),
		}.Build(), nil
	default:
		return nil, internalError(ctx, fmt.Errorf("reserve: unknown outcome %d", result.Outcome))
	}
}

func (s *service) Put(ctx context.Context, req *scheduling.PutRequest) (*scheduling.PutResponse, error) {
	projectID, actorID, err := parseIDs(req.GetProjectId(), req.GetSandboxId())
	if err != nil {
		return nil, err
	}
	reservationID, err := parseUUID("reservation_id", req.GetReservationId())
	if err != nil {
		return nil, err
	}
	clusterID, err := parseUUID("cluster_id", req.GetClusterId())
	if err != nil {
		return nil, err
	}
	if req.GetMaxInstanceLength().GetNanos()%1000 != 0 {
		return nil, status.Error(codes.InvalidArgument, "max_instance_length: must be a whole number of microseconds")
	}
	var record map[string]json.RawMessage
	if !utf8.Valid(req.GetRecord()) || json.Unmarshal(req.GetRecord(), &record) != nil || record == nil {
		return nil, status.Error(codes.InvalidArgument, invalidRecordMessage)
	}

	outcome, err := s.store.Put(ctx, reservationID, store.Actor{
		ProjectID:               projectID,
		ActorID:                 actorID,
		ExecutionID:             req.GetExecutionId(),
		StartTime:               req.GetStartTime().AsTime(),
		MaxInstanceLength:       req.GetMaxInstanceLength().AsDuration(),
		NodeID:                  req.GetNodeId(),
		ClusterID:               clusterID,
		BaseTemplateID:          req.GetBaseTemplateId(),
		TemplateID:              req.GetTemplateId(),
		Metadata:                req.GetMetadata(),
		AutoPause:               req.GetAutoPause(),
		AutoPauseFilesystemOnly: req.GetAutoPauseFilesystemOnly(),
		Record:                  req.GetRecord(),
	})
	if errors.Is(err, store.ErrInvalidRecord) {
		return nil, status.Error(codes.InvalidArgument, invalidRecordMessage)
	}
	if err != nil {
		return nil, internalError(ctx, err)
	}

	switch outcome {
	case store.PutStored:
		return scheduling.PutResponse_builder{Outcome: scheduling.PutOutcome_PUT_OUTCOME_STORED}.Build(), nil
	case store.PutNotFound:
		return scheduling.PutResponse_builder{Outcome: scheduling.PutOutcome_PUT_OUTCOME_NOT_FOUND}.Build(), nil
	default:
		return nil, internalError(ctx, fmt.Errorf("put: unknown outcome %d", outcome))
	}
}

func (s *service) FinishStart(ctx context.Context, req *scheduling.FinishStartRequest) (*scheduling.FinishStartResponse, error) {
	projectID, actorID, err := parseIDs(req.GetProjectId(), req.GetSandboxId())
	if err != nil {
		return nil, err
	}
	reservationID, err := parseUUID("reservation_id", req.GetReservationId())
	if err != nil {
		return nil, err
	}

	outcome, err := s.store.FinishStart(ctx, projectID, actorID, reservationID, store.Failure{
		Message: req.GetFailure().GetMessage(),
		Code:    req.GetFailure().GetCode(),
	})
	if err != nil {
		return nil, internalError(ctx, err)
	}

	switch outcome {
	case store.FinishStartFinished:
		return scheduling.FinishStartResponse_builder{Outcome: scheduling.FinishStartOutcome_FINISH_START_OUTCOME_FINISHED}.Build(), nil
	case store.FinishStartNotFound:
		return scheduling.FinishStartResponse_builder{Outcome: scheduling.FinishStartOutcome_FINISH_START_OUTCOME_NOT_FOUND}.Build(), nil
	case store.FinishStartAlreadyStored:
		return scheduling.FinishStartResponse_builder{Outcome: scheduling.FinishStartOutcome_FINISH_START_OUTCOME_ALREADY_STORED}.Build(), nil
	default:
		return nil, internalError(ctx, fmt.Errorf("finish start: unknown outcome %d", outcome))
	}
}

func (s *service) Get(ctx context.Context, req *scheduling.GetRequest) (*scheduling.GetResponse, error) {
	projectID, actorID, err := parseIDs(req.GetProjectId(), req.GetSandboxId())
	if err != nil {
		return nil, err
	}

	a, err := s.store.Get(ctx, projectID, actorID)
	if err != nil {
		return nil, internalError(ctx, err)
	}
	if a == nil {
		return scheduling.GetResponse_builder{}.Build(), nil
	}
	sandboxID, state, err := sandboxIDAndState(a)
	if err != nil {
		return nil, internalError(ctx, err)
	}

	return scheduling.GetResponse_builder{
		Sandbox: scheduling.Sandbox_builder{
			SandboxId:               sandboxID,
			ProjectId:               a.ProjectID.String(),
			State:                   state,
			EndTime:                 timestamppb.New(a.EndTime),
			ExecutionId:             a.ExecutionID,
			StartTime:               timestamppb.New(a.StartTime),
			MaxInstanceLength:       durationpb.New(a.MaxInstanceLength),
			NodeId:                  a.NodeID,
			ClusterId:               a.ClusterID.String(),
			BaseTemplateId:          a.BaseTemplateID,
			TemplateId:              a.TemplateID,
			Metadata:                a.Metadata,
			AutoPause:               a.AutoPause,
			AutoPauseFilesystemOnly: a.AutoPauseFilesystemOnly,
			Record:                  a.Record,
		}.Build(),
	}.Build(), nil
}

func (s *service) BeginTransition(ctx context.Context, req *scheduling.BeginTransitionRequest) (*scheduling.BeginTransitionResponse, error) {
	projectID, actorID, err := parseIDs(req.GetProjectId(), req.GetSandboxId())
	if err != nil {
		return nil, err
	}
	transitionID, err := parseUUID("transition_id", req.GetTransitionId())
	if err != nil {
		return nil, err
	}
	var action store.TransitionAction
	switch req.GetAction() {
	case scheduling.TransitionAction_TRANSITION_ACTION_PAUSE:
		action = store.ActionPause
	case scheduling.TransitionAction_TRANSITION_ACTION_KILL:
		action = store.ActionKill
	default:
		return nil, status.Error(codes.InvalidArgument, "action: must be pause or kill")
	}

	result, err := s.store.BeginTransition(ctx, store.TransitionRequest{
		ProjectID:           projectID,
		ActorID:             actorID,
		TransitionID:        transitionID,
		Action:              action,
		ExpectedExecutionID: req.GetExpectedExecutionId(),
		RequireExpired:      req.GetRequireExpired(),
	})
	if err != nil {
		return nil, internalError(ctx, err)
	}

	var outcome scheduling.BeginTransitionOutcome
	switch result.Outcome {
	case store.BeginStarted:
		outcome = scheduling.BeginTransitionOutcome_BEGIN_TRANSITION_OUTCOME_STARTED
	case store.BeginJoined:
		outcome = scheduling.BeginTransitionOutcome_BEGIN_TRANSITION_OUTCOME_JOINED
	case store.BeginConflict:
		outcome = scheduling.BeginTransitionOutcome_BEGIN_TRANSITION_OUTCOME_CONFLICT
	case store.BeginNotFound:
		outcome = scheduling.BeginTransitionOutcome_BEGIN_TRANSITION_OUTCOME_NOT_FOUND
	case store.BeginNotRunning:
		outcome = scheduling.BeginTransitionOutcome_BEGIN_TRANSITION_OUTCOME_NOT_RUNNING
	case store.BeginExecutionMismatch:
		outcome = scheduling.BeginTransitionOutcome_BEGIN_TRANSITION_OUTCOME_EXECUTION_MISMATCH
	case store.BeginNotExpired:
		outcome = scheduling.BeginTransitionOutcome_BEGIN_TRANSITION_OUTCOME_NOT_EXPIRED
	case store.BeginAlreadyFinished:
		outcome = scheduling.BeginTransitionOutcome_BEGIN_TRANSITION_OUTCOME_ALREADY_FINISHED
	case store.BeginAlreadyRestored:
		outcome = scheduling.BeginTransitionOutcome_BEGIN_TRANSITION_OUTCOME_ALREADY_RESTORED
	case store.BeginActionMismatch:
		outcome = scheduling.BeginTransitionOutcome_BEGIN_TRANSITION_OUTCOME_ACTION_MISMATCH
	default:
		return nil, internalError(ctx, fmt.Errorf("begin transition: unknown outcome %d", result.Outcome))
	}

	var activeID string
	var activeAction scheduling.TransitionAction
	if result.TransitionID != uuid.Nil {
		activeID = result.TransitionID.String()
		switch result.Action {
		case store.ActionPause:
			activeAction = scheduling.TransitionAction_TRANSITION_ACTION_PAUSE
		case store.ActionKill:
			activeAction = scheduling.TransitionAction_TRANSITION_ACTION_KILL
		}
	}
	var sandbox *scheduling.Sandbox
	if a := result.Actor; a != nil {
		sandboxID, state, err := sandboxIDAndState(a)
		if err != nil {
			return nil, internalError(ctx, err)
		}
		sandbox = scheduling.Sandbox_builder{
			SandboxId:               sandboxID,
			ProjectId:               a.ProjectID.String(),
			State:                   state,
			EndTime:                 timestamppb.New(a.EndTime),
			ExecutionId:             a.ExecutionID,
			StartTime:               timestamppb.New(a.StartTime),
			MaxInstanceLength:       durationpb.New(a.MaxInstanceLength),
			NodeId:                  a.NodeID,
			ClusterId:               a.ClusterID.String(),
			BaseTemplateId:          a.BaseTemplateID,
			TemplateId:              a.TemplateID,
			Metadata:                a.Metadata,
			AutoPause:               a.AutoPause,
			AutoPauseFilesystemOnly: a.AutoPauseFilesystemOnly,
			Record:                  a.Record,
		}.Build()
	}

	return scheduling.BeginTransitionResponse_builder{
		Outcome:      outcome,
		TransitionId: activeID,
		Action:       activeAction,
		Sandbox:      sandbox,
	}.Build(), nil
}

func (s *service) FinishTransition(ctx context.Context, req *scheduling.FinishTransitionRequest) (*scheduling.FinishTransitionResponse, error) {
	projectID, actorID, err := parseIDs(req.GetProjectId(), req.GetSandboxId())
	if err != nil {
		return nil, err
	}
	transitionID, err := parseUUID("transition_id", req.GetTransitionId())
	if err != nil {
		return nil, err
	}
	var failure *store.Failure
	if req.HasFailure() {
		failure = &store.Failure{
			Message: req.GetFailure().GetMessage(),
			Code:    req.GetFailure().GetCode(),
		}
	}

	outcome, err := s.store.FinishTransition(ctx, projectID, actorID, transitionID, failure)
	if err != nil {
		return nil, internalError(ctx, err)
	}

	switch outcome {
	case store.FinishTransitionFinished:
		return scheduling.FinishTransitionResponse_builder{Outcome: scheduling.FinishTransitionOutcome_FINISH_TRANSITION_OUTCOME_FINISHED}.Build(), nil
	case store.FinishTransitionNotFound:
		return scheduling.FinishTransitionResponse_builder{Outcome: scheduling.FinishTransitionOutcome_FINISH_TRANSITION_OUTCOME_NOT_FOUND}.Build(), nil
	case store.FinishTransitionRestored:
		return scheduling.FinishTransitionResponse_builder{Outcome: scheduling.FinishTransitionOutcome_FINISH_TRANSITION_OUTCOME_RESTORED}.Build(), nil
	default:
		return nil, internalError(ctx, fmt.Errorf("finish transition: unknown outcome %d", outcome))
	}
}

func (s *service) RestoreRunning(ctx context.Context, req *scheduling.RestoreRunningRequest) (*scheduling.RestoreRunningResponse, error) {
	projectID, actorID, err := parseIDs(req.GetProjectId(), req.GetSandboxId())
	if err != nil {
		return nil, err
	}
	transitionID, err := parseUUID("transition_id", req.GetTransitionId())
	if err != nil {
		return nil, err
	}
	if req.GetRetryAfter().GetNanos()%1000 != 0 {
		return nil, status.Error(codes.InvalidArgument, "retry_after: must be a whole number of microseconds")
	}

	result, err := s.store.RestoreRunning(ctx, projectID, actorID, transitionID, req.GetRetryAfter().AsDuration())
	if err != nil {
		return nil, internalError(ctx, err)
	}

	switch result.Outcome {
	case store.RestoreRestored:
		return scheduling.RestoreRunningResponse_builder{
			Outcome:      scheduling.RestoreRunningOutcome_RESTORE_RUNNING_OUTCOME_RESTORED,
			RefusedUntil: timestamppb.New(result.RefusedUntil),
			RefusedSince: timestamppb.New(result.RefusedSince),
		}.Build(), nil
	case store.RestoreNotFound:
		return scheduling.RestoreRunningResponse_builder{Outcome: scheduling.RestoreRunningOutcome_RESTORE_RUNNING_OUTCOME_NOT_FOUND}.Build(), nil
	case store.RestoreNotRestorable:
		return scheduling.RestoreRunningResponse_builder{Outcome: scheduling.RestoreRunningOutcome_RESTORE_RUNNING_OUTCOME_NOT_RESTORABLE}.Build(), nil
	case store.RestoreAlreadyFinished:
		return scheduling.RestoreRunningResponse_builder{Outcome: scheduling.RestoreRunningOutcome_RESTORE_RUNNING_OUTCOME_ALREADY_FINISHED}.Build(), nil
	default:
		return nil, internalError(ctx, fmt.Errorf("restore running: unknown outcome %d", result.Outcome))
	}
}

func (s *service) WaitForTransition(ctx context.Context, req *scheduling.WaitForTransitionRequest) (*scheduling.WaitForTransitionResponse, error) {
	projectID, actorID, err := parseIDs(req.GetProjectId(), req.GetSandboxId())
	if err != nil {
		return nil, err
	}
	transitionID, err := parseUUID("transition_id", req.GetTransitionId())
	if err != nil {
		return nil, err
	}
	if req.GetMaxWait().AsDuration() > maxWait {
		return nil, status.Error(codes.InvalidArgument, "max_wait: must be at most 5s")
	}

	result, err := s.store.WaitForTransition(ctx, projectID, actorID, transitionID, req.GetMaxWait().AsDuration())
	if err != nil && ctx.Err() != nil {
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	if err != nil {
		return nil, internalError(ctx, err)
	}

	switch result.Outcome {
	case store.WaitFinished:
		var failure *scheduling.Failure
		if result.Failure != nil {
			failure = scheduling.Failure_builder{
				Message: result.Failure.Message,
				Code:    result.Failure.Code,
			}.Build()
		}

		return scheduling.WaitForTransitionResponse_builder{
			Outcome: scheduling.WaitForTransitionOutcome_WAIT_FOR_TRANSITION_OUTCOME_FINISHED,
			Failure: failure,
		}.Build(), nil
	case store.WaitRestored:
		return scheduling.WaitForTransitionResponse_builder{Outcome: scheduling.WaitForTransitionOutcome_WAIT_FOR_TRANSITION_OUTCOME_RESTORED}.Build(), nil
	case store.WaitWaiting:
		return scheduling.WaitForTransitionResponse_builder{Outcome: scheduling.WaitForTransitionOutcome_WAIT_FOR_TRANSITION_OUTCOME_WAITING}.Build(), nil
	case store.WaitNotFound:
		return scheduling.WaitForTransitionResponse_builder{Outcome: scheduling.WaitForTransitionOutcome_WAIT_FOR_TRANSITION_OUTCOME_NOT_FOUND}.Build(), nil
	case store.WaitUnknown:
		return scheduling.WaitForTransitionResponse_builder{Outcome: scheduling.WaitForTransitionOutcome_WAIT_FOR_TRANSITION_OUTCOME_UNKNOWN}.Build(), nil
	default:
		return nil, internalError(ctx, fmt.Errorf("wait for transition: unknown outcome %d", result.Outcome))
	}
}

func parseIDs(projectID, sandboxID string) (uuid.UUID, uuid.UUID, error) {
	project, err := parseUUID("project_id", projectID)
	if err != nil {
		return uuid.Nil, uuid.Nil, err
	}
	actor, err := id.SandboxIDToUUID(sandboxID)
	if err != nil {
		return uuid.Nil, uuid.Nil, status.Error(codes.InvalidArgument, "sandbox_id: must be a current-format sandbox ID")
	}

	return project, actor, nil
}

func parseUUID(field, value string) (uuid.UUID, error) {
	parsed, err := uuid.Parse(value)
	if err != nil {
		return uuid.Nil, status.Error(codes.InvalidArgument, field+": must be a valid UUID")
	}

	return parsed, nil
}

func sandboxIDAndState(a *store.LiveActor) (string, scheduling.SandboxState, error) {
	sandboxID, ok := id.UUIDToSandboxID(a.ActorID)
	if !ok {
		return "", 0, fmt.Errorf("actor %s has no sandbox ID", a.ActorID)
	}

	switch a.State {
	case store.StateRunning:
		return sandboxID, scheduling.SandboxState_SANDBOX_STATE_RUNNING, nil
	case store.StatePausing:
		return sandboxID, scheduling.SandboxState_SANDBOX_STATE_PAUSING, nil
	case store.StateKilling:
		return sandboxID, scheduling.SandboxState_SANDBOX_STATE_KILLING, nil
	case store.StateSnapshotting:
		return sandboxID, scheduling.SandboxState_SANDBOX_STATE_SNAPSHOTTING, nil
	default:
		return "", 0, fmt.Errorf("actor %s has state %s, which is not live", a.ActorID, a.State)
	}
}

type statusError struct {
	cause error
}

func (e *statusError) Error() string              { return internalMessage }
func (e *statusError) GRPCStatus() *status.Status { return status.New(codes.Internal, internalMessage) }
func (e *statusError) Unwrap() error              { return e.cause }

func internalError(ctx context.Context, cause error) error {
	logger.L().Error(ctx, "scheduling request failed", zap.Error(cause))

	return &statusError{cause: cause}
}
