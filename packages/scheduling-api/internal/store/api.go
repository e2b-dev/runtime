package store

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	queries "github.com/e2b-dev/infra/packages/scheduling-api/internal/db/queries/gen"
)

var ErrConflict = errors.New("sandbox execution or state changed")

// The same transaction lock covers Reserve and ClaimSnapshotDeletion, including an absent run.
func lockAdmission(ctx context.Context, tx pgx.Tx, actorID uuid.UUID) error {
	return queries.New(tx).LockAdmission(ctx, actorID.String())
}

func (s *Store) ClaimSnapshotDeletion(ctx context.Context, projectID, actorID, claimID uuid.UUID) (bool, error) {
	claimed := false
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := lockAdmission(ctx, tx, actorID); err != nil {
			return err
		}
		q := s.queries.WithTx(tx)
		run, err := q.InspectReservation(ctx, actorID)
		if err != nil && !isNoRows(err) {
			return err
		}
		if err == nil && run.ProjectID != projectID {
			return ErrOtherProject
		}
		count, err := q.ClaimSnapshotDeletion(ctx, queries.ClaimSnapshotDeletionParams{ActorID: actorID, ProjectID: projectID, ClaimID: claimID})
		claimed = count == 1

		return err
	})

	return claimed, err
}

func (s *Store) ReleaseSnapshotDeletionClaim(ctx context.Context, projectID, actorID, claimID uuid.UUID) error {
	return s.queries.ReleaseSnapshotDeletionClaim(ctx, queries.ReleaseSnapshotDeletionClaimParams{ActorID: actorID, ProjectID: projectID, ClaimID: claimID})
}

type ReservationStatus struct {
	ReservationID uuid.UUID
	Pending       bool
	Actor         *LiveActor
	Failure       *Failure
	TransitionID  *uuid.UUID
}

func (s *Store) InspectReservation(ctx context.Context, projectID, actorID uuid.UUID) (ReservationStatus, error) {
	var result ReservationStatus
	err := pgx.BeginTxFunc(ctx, s.readPool, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}, func(tx pgx.Tx) error {
		q := s.queries.WithTx(tx)
		row, err := q.InspectAPIReservation(ctx, queries.InspectAPIReservationParams{ActorID: actorID, ProjectID: projectID})
		if isNoRows(err) {
			return nil
		}
		if err != nil {
			return err
		}
		result = ReservationStatus{ReservationID: row.ReservationID, Pending: row.Pending, TransitionID: row.TransitionID}
		if row.FailureMessage != nil {
			result.Failure = &Failure{Message: *row.FailureMessage}
			if row.FailureCode != nil {
				result.Failure.Code = *row.FailureCode
			}
		}
		result.Actor, err = get(ctx, q, projectID, actorID)

		return err
	})

	return result, err
}

func (s *Store) ListLive(ctx context.Context, projectID *uuid.UUID, states []string, expired bool, after uuid.UUID, limit int32) ([]*LiveActor, error) {
	rows, err := s.reads.ListLive(ctx, queries.ListLiveParams{ProjectID: projectID, States: states, ExpiredOnly: expired, AfterID: after, PageSize: limit})
	if err != nil {
		return nil, err
	}
	actors := make([]*LiveActor, 0, len(rows))
	for _, row := range rows {
		actor, err := actorFromRow(queries.GetLiveActorRow(row))
		if err != nil {
			return nil, err
		}
		actors = append(actors, actor)
	}

	return actors, nil
}

func (s *Store) UpdateDeadline(ctx context.Context, projectID, actorID uuid.UUID, executionID string, endTime, expectedEndTime time.Time) (*LiveActor, error) {
	var actor *LiveActor
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := s.queries.WithTx(tx)
		count, err := q.UpdateDeadline(ctx, queries.UpdateDeadlineParams{ActorID: actorID, ProjectID: projectID, ExecutionID: executionID, EndTime: endTime, ExpectedEndTime: expectedEndTime})
		if err != nil {
			return err
		}
		if count != 1 {
			return ErrConflict
		}
		actor, err = get(ctx, q, projectID, actorID)

		return err
	})

	return actor, err
}
