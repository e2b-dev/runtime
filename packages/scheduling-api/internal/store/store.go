package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"

	queries "github.com/e2b-dev/infra/packages/scheduling-api/internal/db/queries/gen"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

var tracer = otel.Tracer("github.com/e2b-dev/infra/packages/scheduling-api/internal/store")

type State = queries.SchedulingActorState

const (
	StateStarting     = queries.SchedulingActorStateStarting
	StateRunning      = queries.SchedulingActorStateRunning
	StatePausing      = queries.SchedulingActorStatePausing
	StateKilling      = queries.SchedulingActorStateKilling
	StateSnapshotting = queries.SchedulingActorStateSnapshotting
	StateGone         = queries.SchedulingActorStateGone
)

func isLive(state State) bool {
	switch state {
	case StateRunning, StatePausing, StateKilling, StateSnapshotting:
		return true
	default:
		return false
	}
}

var (
	ErrOtherProject  = errors.New("actor ID is held by another project")
	ErrInvalidRecord = errors.New("record cannot be stored as jsonb")
	ErrKillClaim     = errors.New("sandbox snapshot is being deleted")
)

type Reservation struct {
	ProjectID     uuid.UUID
	ActorID       uuid.UUID
	ReservationID uuid.UUID
	Limit         int64
	EndTime       time.Time
}

type ReserveOutcome int

const (
	ReserveReserved ReserveOutcome = iota + 1
	ReserveAlreadyPending
	ReserveAlreadyRunning
	ReserveLimitExceeded
	ReserveFinished
)

type ReserveResult struct {
	ReservationID uuid.UUID
	Outcome       ReserveOutcome
	// Set only for ReserveAlreadyRunning.
	Actor *LiveActor
	// Set only for ReserveFinished, when FinishStart or the start timeout
	// ended the start.
	Failure *Failure
	// Set only for ReserveReserved and ReserveAlreadyPending: startTimeout
	// after reserved_at of the starting row.
	StartDeadline time.Time
}

type Actor struct {
	ProjectID               uuid.UUID
	ActorID                 uuid.UUID
	ExecutionID             string
	StartTime               time.Time
	MaxInstanceLength       time.Duration
	NodeID                  string
	ClusterID               uuid.UUID
	BaseTemplateID          string
	TemplateID              string
	Metadata                map[string]string
	AutoPause               bool
	AutoPauseFilesystemOnly bool
	// A JSON object.
	Record  []byte
	EndTime *time.Time
}

type LiveActor struct {
	Actor

	State            State
	EndTime          time.Time
	RefusedUntil     *time.Time
	RefusedSince     *time.Time
	EnvdRefusedUntil *time.Time
	EnvdRefusedSince *time.Time
}

type PutOutcome int

const (
	PutStored PutOutcome = iota + 1
	PutNotFound
)

type Failure struct {
	Message string
	Code    int32
}

type FinishStartOutcome int

const (
	FinishStartFinished FinishStartOutcome = iota + 1
	FinishStartNotFound
	FinishStartAlreadyStored
)

type Store struct {
	pool      *pgxpool.Pool
	readPool  *pgxpool.Pool
	queries   *queries.Queries
	reads     *queries.Queries
	pollDelay func(age time.Duration) time.Duration

	watchMu sync.Mutex
	watches map[watchKey]*transitionWatch
}

func New(writePool, readPool *pgxpool.Pool) *Store {
	return &Store{
		pool:      writePool,
		readPool:  readPool,
		queries:   queries.New(writePool),
		reads:     queries.New(readPool),
		pollDelay: transitionPollDelay,
		watches:   map[watchKey]*transitionWatch{},
	}
}

// Projects with a limit up to strictLimit count on slot 0 only, so their
// admission is exact. Larger projects spread their count over more slots to
// reduce lock waits, and admission compares the limit with a sum that can be
// stale for the other slots.
const (
	strictLimit      = 10_000
	mediumLimit      = 100_000
	mediumLimitSlots = 16
	largeLimitSlots  = 64
)

const reserveAttempts = 3

// counterSlot takes the slot from the last byte of the reservation ID, which
// is random in a UUIDv4 or UUIDv7. Both slot counts divide 256, so the slots
// are equally likely.
func counterSlot(limit int64, reservationID uuid.UUID) int16 {
	switch {
	case limit <= strictLimit:
		return 0
	case limit <= mediumLimit:
		return int16(reservationID[15] % mediumLimitSlots)
	default:
		return int16(reservationID[15] % largeLimitSlots)
	}
}

func (s *Store) Reserve(ctx context.Context, r Reservation) (ReserveResult, error) {
	return telemetry.Observe1(ctx, tracer, "store.Reserve", func(ctx context.Context) (ReserveResult, error) {
		slot := counterSlot(r.Limit, r.ReservationID)
		for range reserveAttempts {
			var reserved queries.ReserveRow
			err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
				if err := lockAdmission(ctx, tx, r.ActorID); err != nil {
					return err
				}
				claimed, err := s.queries.WithTx(tx).HasKillClaim(ctx, queries.HasKillClaimParams{ProjectID: r.ProjectID, ActorID: r.ActorID})
				if err != nil {
					return err
				}
				if claimed {
					return ErrKillClaim
				}
				reserved, err = s.queries.WithTx(tx).Reserve(ctx, queries.ReserveParams{
					ActorID:       r.ActorID,
					ProjectID:     r.ProjectID,
					ReservationID: r.ReservationID,
					CounterSlot:   slot,
					LiveLimit:     r.Limit,
					EndTime:       r.EndTime,
				})
				if err != nil {
					return fmt.Errorf("reserve statement: %w", err)
				}
				if !reserved.Inserted {
					return errRollback
				}

				return nil
			})
			if err != nil && !errors.Is(err, errRollback) {
				return ReserveResult{}, fmt.Errorf("reserve %s: %w", r.ActorID, err)
			}
			if reserved.Inserted {
				return ReserveResult{Outcome: ReserveReserved, StartDeadline: reserved.ReservedAt.Add(startTimeout)}, nil
			}

			run, err := s.reads.InspectReservation(ctx, r.ActorID)
			state := run.State
			switch {
			case isNoRows(err):
				if reserved.Free && !reserved.Bumped {
					return ReserveResult{Outcome: ReserveLimitExceeded}, nil
				}

				continue
			case err != nil:
				return ReserveResult{}, fmt.Errorf("reserve %s: inspect: %w", r.ActorID, err)
			case run.ProjectID != r.ProjectID:
				return ReserveResult{}, ErrOtherProject
			case state == StateGone && run.ReservationID == r.ReservationID:
				result := ReserveResult{Outcome: ReserveFinished}
				if !run.Transitions && run.FailureMessage != nil {
					result.Failure = &Failure{Message: *run.FailureMessage}
					if run.FailureCode != nil {
						result.Failure.Code = *run.FailureCode
					}
				}

				return result, nil
			case state == StateGone:
				if reserved.Free && !reserved.Bumped {
					return ReserveResult{Outcome: ReserveLimitExceeded}, nil
				}

				continue
			case state == StateStarting && run.ReservationID == r.ReservationID:
				return ReserveResult{Outcome: ReserveReserved, StartDeadline: run.ReservedAt.Add(startTimeout)}, nil
			case state == StateStarting:
				return ReserveResult{Outcome: ReserveAlreadyPending, StartDeadline: run.ReservedAt.Add(startTimeout), ReservationID: run.ReservationID}, nil
			}

			actor, err := get(ctx, s.reads, r.ProjectID, r.ActorID)
			if err != nil {
				return ReserveResult{}, fmt.Errorf("reserve %s: %w", r.ActorID, err)
			}
			if actor != nil {
				return ReserveResult{Outcome: ReserveAlreadyRunning, Actor: actor}, nil
			}
		}

		return ReserveResult{}, fmt.Errorf("reserve %s: the actor changed during %d attempts", r.ActorID, reserveAttempts)
	})
}

func (s *Store) Put(ctx context.Context, reservationID uuid.UUID, a Actor) (PutOutcome, error) {
	return telemetry.Observe1(ctx, tracer, "store.Put", func(ctx context.Context) (PutOutcome, error) {
		metadata := a.Metadata
		if metadata == nil {
			metadata = map[string]string{}
		}
		metadataJSON, err := json.Marshal(metadata)
		if err != nil {
			return 0, fmt.Errorf("put %s: encode metadata: %w", a.ActorID, err)
		}

		stored := false
		err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
			q := s.queries.WithTx(tx)
			updated, err := q.MarkRunning(ctx, queries.MarkRunningParams{
				ActorID:       a.ActorID,
				ProjectID:     a.ProjectID,
				ReservationID: reservationID,
				EndTime:       a.EndTime,
				MaxEndTime:    a.StartTime.Add(a.MaxInstanceLength),
			})
			if err != nil {
				return fmt.Errorf("update run: %w", err)
			}
			if updated == 0 {
				return errRollback
			}
			err = q.InsertActor(ctx, queries.InsertActorParams{
				ActorID:                 a.ActorID,
				ProjectID:               a.ProjectID,
				ExecutionID:             a.ExecutionID,
				StartTime:               a.StartTime,
				NodeID:                  a.NodeID,
				ClusterID:               a.ClusterID,
				BaseTemplateID:          a.BaseTemplateID,
				TemplateID:              a.TemplateID,
				MaxInstanceLength:       pgtype.Interval{Microseconds: a.MaxInstanceLength.Microseconds(), Valid: true},
				AutoPause:               a.AutoPause,
				AutoPauseFilesystemOnly: a.AutoPauseFilesystemOnly,
				Metadata:                metadataJSON,
				Record:                  a.Record,
			})
			if isJSONBInputError(err) {
				return fmt.Errorf("insert actor: %w: %w", ErrInvalidRecord, err)
			}
			if err != nil {
				return fmt.Errorf("insert actor: %w", err)
			}
			stored = true

			return nil
		})
		if err != nil && !errors.Is(err, errRollback) {
			return 0, fmt.Errorf("put %s: %w", a.ActorID, err)
		}
		if stored {
			return PutStored, nil
		}

		run, err := s.reads.InspectStart(ctx, queries.InspectStartParams{ActorID: a.ActorID, ProjectID: a.ProjectID})
		state := run.State
		switch {
		case isNoRows(err):
			return PutNotFound, nil
		case err != nil:
			return 0, fmt.Errorf("put %s: inspect: %w", a.ActorID, err)
		case run.ReservationID == reservationID && isLive(state):
			return PutStored, nil
		default:
			return PutNotFound, nil
		}
	})
}

func (s *Store) FinishStart(ctx context.Context, projectID, actorID, reservationID uuid.UUID, failure Failure) (FinishStartOutcome, error) {
	return telemetry.Observe1(ctx, tracer, "store.FinishStart", func(ctx context.Context) (FinishStartOutcome, error) {
		finished := false
		err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
			q := s.queries.WithTx(tx)
			slot, err := q.FinishStart(ctx, queries.FinishStartParams{
				FailureMessage: failure.Message,
				FailureCode:    failure.Code,
				ActorID:        actorID,
				ProjectID:      projectID,
				ReservationID:  reservationID,
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
			finished = true

			return nil
		})
		if err != nil && !errors.Is(err, errRollback) {
			return 0, fmt.Errorf("finish start %s: %w", actorID, err)
		}
		if finished {
			return FinishStartFinished, nil
		}

		run, err := s.reads.InspectStart(ctx, queries.InspectStartParams{ActorID: actorID, ProjectID: projectID})
		state := run.State
		switch {
		case isNoRows(err):
			return FinishStartNotFound, nil
		case err != nil:
			return 0, fmt.Errorf("finish start %s: inspect: %w", actorID, err)
		case run.ReservationID != reservationID:
			return FinishStartNotFound, nil
		case state == StateGone && !run.Transitions:
			return FinishStartFinished, nil
		case state != StateStarting:
			return FinishStartAlreadyStored, nil
		default:
			return 0, fmt.Errorf("finish start %s: the start changed concurrently", actorID)
		}
	})
}

// Get reads a live actor of the project. It returns nil when the project has
// no live actor with this ID: a starting or gone actor is not live.
func (s *Store) Get(ctx context.Context, projectID, actorID uuid.UUID) (*LiveActor, error) {
	return telemetry.Observe1(ctx, tracer, "store.Get", func(ctx context.Context) (*LiveActor, error) {
		actor, err := get(ctx, s.reads, projectID, actorID)
		if err != nil {
			return nil, fmt.Errorf("get %s: %w", actorID, err)
		}

		return actor, nil
	})
}

func get(ctx context.Context, q *queries.Queries, projectID, actorID uuid.UUID) (*LiveActor, error) {
	row, err := q.GetLiveActor(ctx, queries.GetLiveActorParams{ActorID: actorID, ProjectID: projectID})
	if isNoRows(err) {
		return nil, nil //nolint:nilnil // a missing actor is an outcome, not an error
	}
	if err != nil {
		return nil, fmt.Errorf("read actor: %w", err)
	}

	return actorFromRow(row)
}

func actorFromRow(row queries.GetLiveActorRow) (*LiveActor, error) {
	var metadata map[string]string
	if err := json.Unmarshal(row.Metadata, &metadata); err != nil {
		return nil, fmt.Errorf("decode metadata: %w", err)
	}

	return &LiveActor{
		Actor: Actor{
			ProjectID:               row.ProjectID,
			ActorID:                 row.ActorID,
			ExecutionID:             row.ExecutionID,
			StartTime:               row.StartTime,
			MaxInstanceLength:       time.Duration(row.MaxInstanceLength.Microseconds) * time.Microsecond,
			NodeID:                  row.NodeID,
			ClusterID:               row.ClusterID,
			BaseTemplateID:          row.BaseTemplateID,
			TemplateID:              row.TemplateID,
			Metadata:                metadata,
			AutoPause:               row.AutoPause,
			AutoPauseFilesystemOnly: row.AutoPauseFilesystemOnly,
			Record:                  row.Record,
		},
		State:            row.State,
		EndTime:          row.EndTime,
		RefusedUntil:     row.RefusedUntil,
		RefusedSince:     row.RefusedSince,
		EnvdRefusedUntil: row.EnvdRefusedUntil,
		EnvdRefusedSince: row.EnvdRefusedSince,
	}, nil
}

var errRollback = errors.New("rollback")

// isJSONBInputError reports the input errors of jsonb: a lone surrogate, a
// number out of the numeric range, invalid UTF-8, or \u0000. Validation
// rejects these in the other columns of the actor insert, so there they come
// from the record.
func isJSONBInputError(err error) bool {
	pgErr, ok := errors.AsType[*pgconn.PgError](err)
	if !ok {
		return false
	}
	switch pgErr.Code {
	case "22P02", // invalid_text_representation
		"22003", // numeric_value_out_of_range
		"22021", // character_not_in_repertoire
		"22P05": // untranslatable_character
		return true
	default:
		return false
	}
}

func isNoRows(err error) bool {
	return errors.Is(err, pgx.ErrNoRows) //nolint:forbidigo // dberrors is in packages/db, which this module does not depend on
}
