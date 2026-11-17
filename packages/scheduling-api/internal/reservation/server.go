package reservation

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/scheduling"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxstore/sandboxtypes"
)

const internalMessage = "internal error"

type Server struct {
	scheduling.UnimplementedReservationServiceServer

	reservations sandboxtypes.ReservationStorage
	maxWait      time.Duration
}

var _ scheduling.ReservationServiceServer = (*Server)(nil)

// NewServer's maxWait bounds one WaitForStart call.
func NewServer(reservations sandboxtypes.ReservationStorage, maxWait time.Duration) *Server {
	return &Server{reservations: reservations, maxWait: maxWait}
}

func (s *Server) Reserve(ctx context.Context, req *scheduling.ReserveRequest) (*scheduling.ReserveResponse, error) {
	teamID, err := parseIDs(req.GetTeamId(), req.GetSandboxId())
	if err != nil {
		return nil, err
	}

	reserved, err := s.reservations.Reserve(ctx, teamID, req.GetSandboxId(), int(req.GetLimit()))

	var outcome scheduling.ReserveOutcome
	switch {
	case err == nil && reserved:
		outcome = scheduling.ReserveOutcome_RESERVE_OUTCOME_RESERVED
	case err == nil:
		outcome = scheduling.ReserveOutcome_RESERVE_OUTCOME_ALREADY_PENDING
	case errors.Is(err, sandboxtypes.ErrAlreadyExists):
		outcome = scheduling.ReserveOutcome_RESERVE_OUTCOME_ALREADY_EXISTS
	case errors.Is(err, sandboxtypes.ErrSandboxKilled):
		outcome = scheduling.ReserveOutcome_RESERVE_OUTCOME_KILLED
	default:
		if _, ok := errors.AsType[*sandboxtypes.LimitExceededError](err); !ok {
			return nil, storeError(ctx, err, teamID, req.GetSandboxId())
		}
		outcome = scheduling.ReserveOutcome_RESERVE_OUTCOME_LIMIT_EXCEEDED
	}

	return scheduling.ReserveResponse_builder{Outcome: outcome}.Build(), nil
}

func (s *Server) FinishStart(ctx context.Context, req *scheduling.FinishStartRequest) (*scheduling.FinishStartResponse, error) {
	teamID, err := parseIDs(req.GetTeamId(), req.GetSandboxId())
	if err != nil {
		return nil, err
	}

	if err := s.reservations.FinishStart(ctx, teamID, req.GetSandboxId(), req.GetResult()); err != nil {
		return nil, storeError(ctx, err, teamID, req.GetSandboxId())
	}

	return scheduling.FinishStartResponse_builder{}.Build(), nil
}

func (s *Server) WaitForStart(ctx context.Context, req *scheduling.WaitForStartRequest) (*scheduling.WaitForStartResponse, error) {
	teamID, err := parseIDs(req.GetTeamId(), req.GetSandboxId())
	if err != nil {
		return nil, err
	}

	waitCtx, cancel := context.WithTimeout(ctx, s.maxWait)
	defer cancel()

	result, err := s.reservations.WaitForStart(waitCtx, teamID, req.GetSandboxId())
	if err == nil {
		return scheduling.WaitForStartResponse_builder{
			Outcome: scheduling.WaitForStartOutcome_WAIT_FOR_START_OUTCOME_FINISHED,
			Result:  result,
		}.Build(), nil
	}
	if _, ok := errors.AsType[*sandboxtypes.StartResultMissingError](err); ok {
		return scheduling.WaitForStartResponse_builder{
			Outcome: scheduling.WaitForStartOutcome_WAIT_FOR_START_OUTCOME_NO_RESULT,
		}.Build(), nil
	}
	// A Redis call cut by the wait's deadline can fail with a socket timeout
	// instead of context.DeadlineExceeded, so the expired wait context counts too.
	waitEnded := errors.Is(err, context.DeadlineExceeded) || errors.Is(waitCtx.Err(), context.DeadlineExceeded)
	if waitEnded && ctx.Err() == nil {
		return scheduling.WaitForStartResponse_builder{
			Outcome: scheduling.WaitForStartOutcome_WAIT_FOR_START_OUTCOME_WAITING,
		}.Build(), nil
	}

	return nil, storeError(ctx, err, teamID, req.GetSandboxId())
}

func (s *Server) Release(ctx context.Context, req *scheduling.ReleaseRequest) (*scheduling.ReleaseResponse, error) {
	teamID, err := parseIDs(req.GetTeamId(), req.GetSandboxId())
	if err != nil {
		return nil, err
	}

	if err := s.reservations.Release(ctx, teamID, req.GetSandboxId()); err != nil {
		return nil, storeError(ctx, err, teamID, req.GetSandboxId())
	}

	return scheduling.ReleaseResponse_builder{}.Build(), nil
}

func (s *Server) ClaimKill(ctx context.Context, req *scheduling.ClaimKillRequest) (*scheduling.ClaimKillResponse, error) {
	teamID, err := parseIDs(req.GetTeamId(), req.GetSandboxId())
	if err != nil {
		return nil, err
	}

	claimID, claimed, err := s.reservations.ClaimKill(ctx, teamID, req.GetSandboxId())
	if err != nil {
		return nil, storeError(ctx, err, teamID, req.GetSandboxId())
	}

	return scheduling.ClaimKillResponse_builder{Claimed: claimed, ClaimId: claimID}.Build(), nil
}

func (s *Server) ReleaseKillClaim(ctx context.Context, req *scheduling.ReleaseKillClaimRequest) (*scheduling.ReleaseKillClaimResponse, error) {
	teamID, err := parseIDs(req.GetTeamId(), req.GetSandboxId())
	if err != nil {
		return nil, err
	}
	if req.GetClaimId() == "" {
		return nil, status.Error(codes.InvalidArgument, "claim_id: must not be empty")
	}

	if err := s.reservations.ReleaseKillClaim(ctx, teamID, req.GetSandboxId(), req.GetClaimId()); err != nil {
		return nil, storeError(ctx, err, teamID, req.GetSandboxId())
	}

	return scheduling.ReleaseKillClaimResponse_builder{}.Build(), nil
}

// The store's key builders panic on an empty sandbox ID or one with a brace.
func parseIDs(teamID, sandboxID string) (uuid.UUID, error) {
	team, err := uuid.Parse(teamID)
	if err != nil {
		return uuid.Nil, status.Error(codes.InvalidArgument, "team_id: must be a valid UUID")
	}
	if sandboxID == "" || strings.ContainsAny(sandboxID, "{}") {
		return uuid.Nil, status.Error(codes.InvalidArgument, "sandbox_id: must not be empty or contain { or }")
	}

	return team, nil
}

func storeError(ctx context.Context, err error, teamID uuid.UUID, sandboxID string) error {
	if ctx.Err() != nil {
		return status.FromContextError(ctx.Err()).Err()
	}

	method, _ := grpc.Method(ctx)
	logger.L().Error(ctx, "reservation request failed",
		zap.String("method", method),
		logger.WithTeamID(teamID.String()),
		logger.WithSandboxID(sandboxID),
		zap.Error(err),
	)

	return status.Error(codes.Internal, internalMessage)
}
