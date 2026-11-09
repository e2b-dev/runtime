package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	queries "github.com/e2b-dev/infra/packages/scheduling-api/internal/db/queries/gen"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// TransitionAction is the transition_action code. A stored snapshot (3) has
// no constant: nothing begins one yet.
type TransitionAction int16

const (
	ActionPause TransitionAction = 1
	ActionKill  TransitionAction = 2
)

type TransitionRequest struct {
	ProjectID    uuid.UUID
	ActorID      uuid.UUID
	TransitionID uuid.UUID
	Action       TransitionAction
	// Empty means any execution.
	ExpectedExecutionID string
	RequireExpired      bool
}

type BeginOutcome int

const (
	BeginStarted BeginOutcome = iota + 1
	BeginJoined
	BeginConflict
	BeginNotFound
	BeginNotRunning
	BeginExecutionMismatch
	BeginNotExpired
	BeginAlreadyFinished
	BeginAlreadyRestored
	BeginActionMismatch
)

type BeginResult struct {
	Outcome BeginOutcome
	// The active transition. Set only for BeginStarted, BeginJoined and
	// BeginConflict.
	TransitionID uuid.UUID
	Action       TransitionAction
	// Set when the actor is live.
	Actor *LiveActor
}

type FinishTransitionOutcome int

const (
	FinishTransitionFinished FinishTransitionOutcome = iota + 1
	FinishTransitionNotFound
	FinishTransitionRestored
)

type RestoreOutcome int

const (
	RestoreRestored RestoreOutcome = iota + 1
	RestoreNotFound
	RestoreNotRestorable
	RestoreAlreadyFinished
)

type RestoreResult struct {
	Outcome RestoreOutcome
	// Set only for RestoreRestored.
	RefusedUntil time.Time
	RefusedSince time.Time
}

type WaitOutcome int

const (
	WaitFinished WaitOutcome = iota + 1
	WaitRestored
	WaitWaiting
	WaitNotFound
	WaitUnknown
)

type WaitResult struct {
	Outcome WaitOutcome
	// Set only for WaitFinished, when FinishTransition reported a failure.
	Failure *Failure
}

const beginAttempts = 3

func (s *Store) BeginTransition(ctx context.Context, r TransitionRequest) (BeginResult, error) {
	return telemetry.Observe1(ctx, tracer, "store.BeginTransition", func(ctx context.Context) (BeginResult, error) {
		var target State
		switch r.Action {
		case ActionPause:
			target = StatePausing
		case ActionKill:
			target = StateKilling
		default:
			return BeginResult{}, fmt.Errorf("begin transition %s: unknown action %d", r.ActorID, r.Action)
		}

		for range beginAttempts {
			var started *LiveActor
			err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
				q := s.queries.WithTx(tx)
				updated, err := q.BeginTransition(ctx, queries.BeginTransitionParams{
					State:               target,
					TransitionID:        r.TransitionID,
					TransitionAction:    int16(r.Action),
					ActorID:             r.ActorID,
					ProjectID:           r.ProjectID,
					ExpectedExecutionID: r.ExpectedExecutionID,
					RequireExpired:      r.RequireExpired,
				})
				if err != nil {
					return fmt.Errorf("update run: %w", err)
				}
				if updated == 0 {
					return nil
				}
				started, err = get(ctx, q, r.ProjectID, r.ActorID)
				if err != nil {
					return err
				}
				if started == nil {
					return errors.New("a running actor has no record")
				}

				return nil
			})
			if err != nil {
				return BeginResult{}, fmt.Errorf("begin transition %s: %w", r.ActorID, err)
			}
			if started != nil {
				return BeginResult{Outcome: BeginStarted, TransitionID: r.TransitionID, Action: r.Action, Actor: started}, nil
			}

			run, actor, found, err := s.inspectWithActor(ctx, r.ProjectID, r.ActorID)
			if err != nil {
				return BeginResult{}, fmt.Errorf("begin transition %s: %w", r.ActorID, err)
			}
			if !found {
				return BeginResult{Outcome: BeginNotFound}, nil
			}
			state := run.State
			live := isLive(state)
			if live && actor == nil {
				return BeginResult{}, fmt.Errorf("begin transition %s: a live actor has no record", r.ActorID)
			}

			switch {
			case run.TransitionID != nil && *run.TransitionID == r.TransitionID:
				action := TransitionAction(*run.TransitionAction)
				switch {
				case action != r.Action:
					return BeginResult{Outcome: BeginActionMismatch, Actor: actor}, nil
				case state == StatePausing || state == StateKilling:
					return BeginResult{Outcome: BeginStarted, TransitionID: r.TransitionID, Action: action, Actor: actor}, nil
				case state == StateRunning:
					return BeginResult{Outcome: BeginAlreadyRestored, Actor: actor}, nil
				case state == StateGone:
					return BeginResult{Outcome: BeginAlreadyFinished}, nil
				default:
					return BeginResult{}, fmt.Errorf("begin transition %s: transition %s in state %s", r.ActorID, r.TransitionID, state)
				}
			case state == StateGone:
				return BeginResult{Outcome: BeginNotFound}, nil
			case state == StateStarting:
				return BeginResult{Outcome: BeginNotRunning}, nil
			case r.ExpectedExecutionID != "" && actor.ExecutionID != r.ExpectedExecutionID:
				return BeginResult{Outcome: BeginExecutionMismatch, Actor: actor}, nil
			case state != StateRunning:
				outcome := BeginConflict
				if TransitionAction(*run.TransitionAction) == r.Action {
					outcome = BeginJoined
				}

				return BeginResult{
					Outcome:      outcome,
					TransitionID: *run.TransitionID,
					Action:       TransitionAction(*run.TransitionAction),
					Actor:        actor,
				}, nil
			case r.RequireExpired && !run.Expired:
				return BeginResult{Outcome: BeginNotExpired, Actor: actor}, nil
			}
		}

		return BeginResult{}, fmt.Errorf("begin transition %s: the actor changed during %d attempts", r.ActorID, beginAttempts)
	})
}

// inspectWithActor reads the run and the live actor in one snapshot, so that
// the actor belongs to the transition the run records.
func (s *Store) inspectWithActor(ctx context.Context, projectID, actorID uuid.UUID) (queries.InspectTransitionRow, *LiveActor, bool, error) {
	var (
		run   queries.InspectTransitionRow
		actor *LiveActor
		found bool
	)
	err := pgx.BeginTxFunc(ctx, s.readPool, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}, func(tx pgx.Tx) error {
		q := s.queries.WithTx(tx)
		var err error
		run, err = q.InspectTransition(ctx, queries.InspectTransitionParams{ActorID: actorID, ProjectID: projectID})
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

// FinishTransition ends a pause or a kill: the actor record is deleted, the
// run is gone with the failure, if any, and the counter slot of the run
// decreases by one, in one transaction.
func (s *Store) FinishTransition(ctx context.Context, projectID, actorID, transitionID uuid.UUID, failure *Failure) (FinishTransitionOutcome, error) {
	return telemetry.Observe1(ctx, tracer, "store.FinishTransition", func(ctx context.Context) (FinishTransitionOutcome, error) {
		params := queries.FinishTransitionParams{ActorID: actorID, ProjectID: projectID, TransitionID: transitionID}
		if failure != nil {
			params.FailureMessage = &failure.Message
			params.FailureCode = &failure.Code
		}

		finished := false
		err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
			q := s.queries.WithTx(tx)
			slot, err := q.FinishTransition(ctx, params)
			if isNoRows(err) {
				return errRollback
			}
			if err != nil {
				return fmt.Errorf("end transition: %w", err)
			}
			deleted, err := q.DeleteActor(ctx, queries.DeleteActorParams{ActorID: actorID, ProjectID: projectID})
			if err != nil {
				return fmt.Errorf("delete actor: %w", err)
			}
			if deleted != 1 {
				return errors.New("delete actor: the actor has no record")
			}
			released, err := q.ReleaseCounterSlot(ctx, queries.ReleaseCounterSlotParams{ProjectID: projectID, Slot: slot})
			if err != nil {
				return fmt.Errorf("release counter slot %d: %w", slot, err)
			}
			if released != 1 {
				return fmt.Errorf("release counter slot %d: the slot has no row", slot)
			}
			finished = true

			return nil
		})
		if err != nil && !errors.Is(err, errRollback) {
			return 0, fmt.Errorf("finish transition %s: %w", actorID, err)
		}
		if finished {
			return FinishTransitionFinished, nil
		}

		run, err := s.reads.InspectTransition(ctx, queries.InspectTransitionParams{ActorID: actorID, ProjectID: projectID})
		state := run.State
		switch {
		case isNoRows(err):
			return FinishTransitionNotFound, nil
		case err != nil:
			return 0, fmt.Errorf("finish transition %s: inspect: %w", actorID, err)
		case run.TransitionID == nil || *run.TransitionID != transitionID:
			return FinishTransitionNotFound, nil
		case state == StateGone:
			return FinishTransitionFinished, nil
		case state == StateRunning:
			return FinishTransitionRestored, nil
		default:
			return 0, fmt.Errorf("finish transition %s: transition %s in state %s did not end", actorID, transitionID, state)
		}
	})
}

// RestoreRunning ends a refused pause: the run is running again with a new
// retry window. The transition ID stays on the run, so that repeats and
// waiters can tell a restored pause from a replaced one.
func (s *Store) RestoreRunning(ctx context.Context, projectID, actorID, transitionID uuid.UUID, retryAfter time.Duration) (RestoreResult, error) {
	return telemetry.Observe1(ctx, tracer, "store.RestoreRunning", func(ctx context.Context) (RestoreResult, error) {
		restored, err := s.queries.RestoreRunning(ctx, queries.RestoreRunningParams{
			RetryAfter:   pgtype.Interval{Microseconds: retryAfter.Microseconds(), Valid: true},
			ActorID:      actorID,
			ProjectID:    projectID,
			TransitionID: transitionID,
		})
		if err == nil {
			return RestoreResult{Outcome: RestoreRestored, RefusedUntil: *restored.RefusedUntil, RefusedSince: *restored.RefusedSince}, nil
		}
		if !isNoRows(err) {
			return RestoreResult{}, fmt.Errorf("restore running %s: %w", actorID, err)
		}

		run, err := s.reads.InspectTransition(ctx, queries.InspectTransitionParams{ActorID: actorID, ProjectID: projectID})
		state := run.State
		switch {
		case isNoRows(err):
			return RestoreResult{Outcome: RestoreNotFound}, nil
		case err != nil:
			return RestoreResult{}, fmt.Errorf("restore running %s: inspect: %w", actorID, err)
		case run.TransitionID == nil || *run.TransitionID != transitionID:
			return RestoreResult{Outcome: RestoreNotFound}, nil
		case TransitionAction(*run.TransitionAction) != ActionPause:
			return RestoreResult{Outcome: RestoreNotRestorable}, nil
		case state == StateGone:
			return RestoreResult{Outcome: RestoreAlreadyFinished}, nil
		case state == StateRunning && run.RefusedUntil != nil && run.RefusedSince != nil:
			return RestoreResult{Outcome: RestoreRestored, RefusedUntil: *run.RefusedUntil, RefusedSince: *run.RefusedSince}, nil
		default:
			return RestoreResult{}, fmt.Errorf("restore running %s: transition %s in state %s was not restored", actorID, transitionID, state)
		}
	})
}
