package remote

import (
	"context"

	"github.com/google/uuid"
	"google.golang.org/grpc"

	reservationpb "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/scheduling/reservation"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxstore/sandboxtypes"
)

type ReservationStorage struct {
	client reservationpb.ReservationServiceClient
}

var _ sandboxtypes.ReservationStorage = (*ReservationStorage)(nil)

// NewReservationStorage is the start reservation store served by the reservation gRPC service.
func NewReservationStorage(conn grpc.ClientConnInterface) *ReservationStorage {
	return &ReservationStorage{client: reservationpb.NewReservationServiceClient(conn)}
}

func (s *ReservationStorage) Reserve(ctx context.Context, teamID uuid.UUID, sandboxID string, limit int) (bool, error) {
	const method = reservationpb.ReservationService_Reserve_FullMethodName
	resp, err := unary(ctx, method, s.client.Reserve, reservationpb.ReserveRequest_builder{
		TeamId: teamID.String(), SandboxId: sandboxID, Limit: int64(limit),
	}.Build())
	if err != nil {
		return false, err
	}

	switch resp.GetOutcome() {
	case reservationpb.ReserveOutcome_RESERVE_OUTCOME_RESERVED:
		return true, nil
	case reservationpb.ReserveOutcome_RESERVE_OUTCOME_ALREADY_PENDING:
		return false, nil
	case reservationpb.ReserveOutcome_RESERVE_OUTCOME_ALREADY_EXISTS:
		return false, sandboxtypes.ErrAlreadyExists
	case reservationpb.ReserveOutcome_RESERVE_OUTCOME_LIMIT_EXCEEDED:
		return false, &sandboxtypes.LimitExceededError{TeamID: teamID}
	case reservationpb.ReserveOutcome_RESERVE_OUTCOME_KILLED:
		return false, sandboxtypes.ErrSandboxKilled
	default:
		return false, unknownOutcome(method, resp.GetOutcome())
	}
}

func (s *ReservationStorage) FinishStart(ctx context.Context, teamID uuid.UUID, sandboxID string, result []byte) error {
	_, err := unary(ctx, reservationpb.ReservationService_FinishStart_FullMethodName, s.client.FinishStart, reservationpb.FinishStartRequest_builder{
		TeamId: teamID.String(), SandboxId: sandboxID, Result: result,
	}.Build())

	return err
}

func (s *ReservationStorage) WaitForStart(ctx context.Context, teamID uuid.UUID, sandboxID string) ([]byte, error) {
	const method = reservationpb.ReservationService_WaitForStart_FullMethodName
	for {
		resp, err := longPoll(ctx, method, s.client.WaitForStart, reservationpb.WaitForStartRequest_builder{
			TeamId: teamID.String(), SandboxId: sandboxID,
		}.Build())
		if err != nil {
			return nil, err
		}

		switch resp.GetOutcome() {
		case reservationpb.WaitForStartOutcome_WAIT_FOR_START_OUTCOME_WAITING:
			continue
		case reservationpb.WaitForStartOutcome_WAIT_FOR_START_OUTCOME_FINISHED:
			// The store returns a finished start's result non-nil, empty included.
			if resp.GetResult() == nil {
				return []byte{}, nil
			}

			return resp.GetResult(), nil
		case reservationpb.WaitForStartOutcome_WAIT_FOR_START_OUTCOME_NO_RESULT:
			return nil, &sandboxtypes.StartResultMissingError{SandboxID: sandboxID}
		default:
			return nil, unknownOutcome(method, resp.GetOutcome())
		}
	}
}

func (s *ReservationStorage) Release(ctx context.Context, teamID uuid.UUID, sandboxID string) error {
	_, err := unary(ctx, reservationpb.ReservationService_Release_FullMethodName, s.client.Release, reservationpb.ReleaseRequest_builder{
		TeamId: teamID.String(), SandboxId: sandboxID,
	}.Build())

	return err
}

func (s *ReservationStorage) ClaimKill(ctx context.Context, teamID uuid.UUID, sandboxID string) (string, bool, error) {
	resp, err := unary(ctx, reservationpb.ReservationService_ClaimKill_FullMethodName, s.client.ClaimKill, reservationpb.ClaimKillRequest_builder{
		TeamId: teamID.String(), SandboxId: sandboxID,
	}.Build())
	if err != nil {
		return "", false, err
	}

	return resp.GetClaimId(), resp.GetClaimed(), nil
}

func (s *ReservationStorage) ReleaseKillClaim(ctx context.Context, teamID uuid.UUID, sandboxID, claimID string) error {
	_, err := unary(ctx, reservationpb.ReservationService_ReleaseKillClaim_FullMethodName, s.client.ReleaseKillClaim, reservationpb.ReleaseKillClaimRequest_builder{
		TeamId: teamID.String(), SandboxId: sandboxID, ClaimId: claimID,
	}.Build())

	return err
}
