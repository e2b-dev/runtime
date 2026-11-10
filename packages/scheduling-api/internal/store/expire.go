package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/grpc/codes"

	queries "github.com/e2b-dev/infra/packages/scheduling-api/internal/db/queries/gen"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

const startTimeout = 90 * time.Second

var startTimeoutFailure = Failure{Message: "start timed out", Code: int32(codes.DeadlineExceeded)}

type ExpireResult struct {
	Ended  int
	Failed int
	// The actor ID to pass as after to continue, or uuid.Nil to start from
	// the first expired start.
	Next uuid.UUID
}

// ExpireStarts ends up to maxStarts starts that are past their start
// deadline, in actor ID order after the actor ID after, each like
// FinishStart with startTimeoutFailure. It changes only the database: a
// sandbox that a node created for such a start keeps running there.
//
// A start that fails to end does not stop the others, and Next moves past
// it. The error then reports how many failed and the first failure, together
// with the result. When ctx ends, ExpireStarts stops without counting a
// failure and returns ctx's error; Next stays before the start that it was
// ending, whose commit is unknown.
func (s *Store) ExpireStarts(ctx context.Context, after uuid.UUID, maxStarts int32) (ExpireResult, error) {
	return telemetry.Observe1(ctx, tracer, "store.ExpireStarts", func(ctx context.Context) (ExpireResult, error) {
		if maxStarts <= 0 {
			return ExpireResult{}, fmt.Errorf("expire starts: maxStarts must be positive, got %d", maxStarts)
		}
		var starts []queries.ListExpiredStartsRow
		err := pgx.BeginFunc(ctx, s.readPool, func(tx pgx.Tx) error {
			q := s.reads.WithTx(tx)
			if err := q.SetCleanupTimeouts(ctx); err != nil {
				return fmt.Errorf("set timeouts: %w", err)
			}
			var err error
			starts, err = q.ListExpiredStarts(ctx, queries.ListExpiredStartsParams{
				StartTimeout: interval(startTimeout),
				After:        after,
				MaxStarts:    maxStarts,
			})

			return err
		})
		if err != nil {
			return ExpireResult{Next: after}, fmt.Errorf("expire starts: list: %w", err)
		}

		result := ExpireResult{Next: after}
		done := 0
		var first error
		for _, start := range starts {
			if ctx.Err() != nil {
				break
			}
			expired, err := s.expireStart(ctx, start.ProjectID, start.ActorID, start.ReservationID)
			if err != nil && ctx.Err() != nil {
				break
			}
			if err != nil {
				result.Failed++
				if first == nil {
					first = err
				}
			} else if expired {
				result.Ended++
			}
			result.Next = start.ActorID
			done++
		}
		if done == len(starts) && len(starts) < int(maxStarts) {
			result.Next = uuid.Nil
		}

		switch {
		case first != nil:
			return result, fmt.Errorf("expire starts: %d of %d failed, first: %w", result.Failed, len(starts), first)
		case done < len(starts):
			return result, fmt.Errorf("expire starts: stopped after %d of %d: %w", done, len(starts), ctx.Err())
		default:
			return result, nil
		}
	})
}

// expireStart ends the start only if the actor still has this project and
// reservation, is starting, and is past the start deadline at the time of
// the update. A list result that a Put, a FinishStart or a new Reserve made
// stale changes nothing and returns false.
func (s *Store) expireStart(ctx context.Context, projectID, actorID, reservationID uuid.UUID) (bool, error) {
	expired := false
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := s.queries.WithTx(tx)
		if err := q.SetCleanupTimeouts(ctx); err != nil {
			return fmt.Errorf("set timeouts: %w", err)
		}
		slot, err := q.ExpireStart(ctx, queries.ExpireStartParams{
			FailureMessage: startTimeoutFailure.Message,
			FailureCode:    startTimeoutFailure.Code,
			ActorID:        actorID,
			ProjectID:      projectID,
			ReservationID:  reservationID,
			StartTimeout:   interval(startTimeout),
		})
		if isNoRows(err) {
			return errRollback
		}
		if err != nil {
			return fmt.Errorf("end start: %w", err)
		}
		released, err := q.ReleaseCounterSlot(ctx, queries.ReleaseCounterSlotParams{ProjectID: projectID, Slot: slot})
		if err != nil {
			return fmt.Errorf("release counter slot %d: %w", slot, err)
		}
		if released != 1 {
			return fmt.Errorf("release counter slot %d: the slot has no row", slot)
		}
		expired = true

		return nil
	})
	if err != nil && !errors.Is(err, errRollback) {
		return false, fmt.Errorf("expire start %s: %w", actorID, err)
	}

	return expired, nil
}

func interval(d time.Duration) pgtype.Interval {
	return pgtype.Interval{Microseconds: d.Microseconds(), Valid: true}
}
