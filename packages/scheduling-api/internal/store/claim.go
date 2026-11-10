package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	queries "github.com/e2b-dev/infra/packages/scheduling-api/internal/db/queries/gen"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

type ClaimRequest struct {
	ProjectID           uuid.UUID
	ActorID             uuid.UUID
	OldTransitionID     uuid.UUID
	NewTransitionID     uuid.UUID
	ExpectedExecutionID string
}

type ClaimOutcome int

const (
	ClaimClaimed ClaimOutcome = iota + 1
	ClaimNotFound
	ClaimConflict
	ClaimNotStale
	ClaimExecutionMismatch
	ClaimAlreadyFinished
	ClaimAlreadyRestored
)

type ClaimResult struct {
	Outcome ClaimOutcome
	// The active transition. Set only for ClaimClaimed (the new transition)
	// and ClaimConflict.
	TransitionID uuid.UUID
	Action       TransitionAction
	// Set only for ClaimClaimed: when the claim took the transition over.
	TransitionAt time.Time
	// Set only for ClaimClaimed, read with the claim.
	Actor *LiveActor
}

const claimAttempts = 3

// ClaimTransition makes NewTransitionID the owner of the stale pause or kill
// of OldTransitionID, if the running execution is ExpectedExecutionID. Only
// the transition ID and transition_at change. A repeat with the same new ID
// returns ClaimClaimed and does not move transition_at.
func (s *Store) ClaimTransition(ctx context.Context, r ClaimRequest) (ClaimResult, error) {
	return telemetry.Observe1(ctx, tracer, "store.ClaimTransition", func(ctx context.Context) (ClaimResult, error) {
		if r.OldTransitionID == r.NewTransitionID {
			return ClaimResult{}, fmt.Errorf("claim transition %s: the old and new transition IDs are equal", r.ActorID)
		}

		for range claimAttempts {
			var (
				claimed *LiveActor
				row     queries.ClaimTransitionRow
			)
			err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
				q := s.queries.WithTx(tx)
				var err error
				row, err = q.ClaimTransition(ctx, queries.ClaimTransitionParams{
					NewTransitionID:     r.NewTransitionID,
					ActorID:             r.ActorID,
					ProjectID:           r.ProjectID,
					OldTransitionID:     r.OldTransitionID,
					StaleAfter:          interval(staleTransitionAfter),
					ExpectedExecutionID: r.ExpectedExecutionID,
				})
				if isNoRows(err) {
					return nil
				}
				if err != nil {
					return fmt.Errorf("update run: %w", err)
				}
				claimed, err = get(ctx, q, r.ProjectID, r.ActorID)
				if err != nil {
					return err
				}
				if claimed == nil {
					return errors.New("an actor in a transition has no record")
				}

				return nil
			})
			if err != nil {
				return ClaimResult{}, fmt.Errorf("claim transition %s: %w", r.ActorID, err)
			}
			if claimed != nil {
				return ClaimResult{
					Outcome:      ClaimClaimed,
					TransitionID: r.NewTransitionID,
					Action:       TransitionAction(row.TransitionAction),
					TransitionAt: row.TransitionAt,
					Actor:        claimed,
				}, nil
			}

			run, actor, found, err := s.inspectClaim(ctx, r.ProjectID, r.ActorID)
			if err != nil {
				return ClaimResult{}, fmt.Errorf("claim transition %s: %w", r.ActorID, err)
			}
			if !found {
				return ClaimResult{Outcome: ClaimNotFound}, nil
			}
			state := run.State
			records := func(id uuid.UUID) bool { return run.TransitionID != nil && *run.TransitionID == id }
			active := state == StatePausing || state == StateKilling
			if active && actor == nil {
				return ClaimResult{}, fmt.Errorf("claim transition %s: an actor in a transition has no record", r.ActorID)
			}

			switch {
			case (records(r.OldTransitionID) || records(r.NewTransitionID)) && state == StateGone:
				return ClaimResult{Outcome: ClaimAlreadyFinished}, nil
			case (records(r.OldTransitionID) || records(r.NewTransitionID)) && state == StateRunning:
				return ClaimResult{Outcome: ClaimAlreadyRestored}, nil
			case !active:
				return ClaimResult{Outcome: ClaimNotFound}, nil
			case actor.ExecutionID != r.ExpectedExecutionID:
				return ClaimResult{Outcome: ClaimExecutionMismatch}, nil
			case records(r.NewTransitionID):
				return ClaimResult{
					Outcome:      ClaimClaimed,
					TransitionID: r.NewTransitionID,
					Action:       TransitionAction(*run.TransitionAction),
					TransitionAt: *run.TransitionAt,
					Actor:        actor,
				}, nil
			case records(r.OldTransitionID) && !run.Stale:
				return ClaimResult{Outcome: ClaimNotStale}, nil
			case records(r.OldTransitionID):
				// It became stale after the update read the clock.
				continue
			default:
				return ClaimResult{
					Outcome:      ClaimConflict,
					TransitionID: *run.TransitionID,
					Action:       TransitionAction(*run.TransitionAction),
				}, nil
			}
		}

		return ClaimResult{}, fmt.Errorf("claim transition %s: the actor changed during %d attempts", r.ActorID, claimAttempts)
	})
}

// inspectClaim reads the run and the live actor in one snapshot.
func (s *Store) inspectClaim(ctx context.Context, projectID, actorID uuid.UUID) (queries.InspectClaimRow, *LiveActor, bool, error) {
	var (
		run   queries.InspectClaimRow
		actor *LiveActor
		found bool
	)
	err := pgx.BeginTxFunc(ctx, s.readPool, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}, func(tx pgx.Tx) error {
		q := s.reads.WithTx(tx)
		var err error
		run, err = q.InspectClaim(ctx, queries.InspectClaimParams{StaleAfter: interval(staleTransitionAfter), ActorID: actorID, ProjectID: projectID})
		if isNoRows(err) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("inspect: %w", err)
		}
		found = true
		actor, err = get(ctx, q, projectID, actorID)

		return err
	})

	return run, actor, found, err
}
