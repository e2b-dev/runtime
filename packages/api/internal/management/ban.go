package management

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"

	dashboardqueries "github.com/e2b-dev/infra/packages/db/pkg/dashboard/queries"
	"github.com/e2b-dev/infra/packages/db/pkg/dberrors"
	"github.com/e2b-dev/infra/packages/db/pkg/outbox"
	"github.com/e2b-dev/infra/packages/db/queries"
)

var ErrInvalidProjectBan = errors.New("invalid project ban state")

type JobInserter interface {
	InsertTx(ctx context.Context, tx pgx.Tx, args river.JobArgs, opts *river.InsertOpts) (*rivertype.JobInsertResult, error)
}

type ProjectBanProjection struct {
	ProjectID uuid.UUID
	Revision  int64
	Banned    bool
	DecidedAt time.Time
}

func (s *Service) ApplyProjectBan(ctx context.Context, jobs JobInserter, projection ProjectBanProjection) error {
	if projection.ProjectID == uuid.Nil || projection.Revision <= 0 {
		return ErrInvalidProjectBan
	}

	stored, err := s.applyProjectBan(ctx, jobs, projection)
	if err != nil {
		return err
	}
	if stored {
		s.applyLag.stored(ctx, projectionProjectBans, projection.DecidedAt)
	}

	if err := s.cache.InvalidateTeamCache(ctx, projection.ProjectID); err != nil {
		return fmt.Errorf("invalidate team cache after ban update: %w", err)
	}

	return nil
}

func (s *Service) applyProjectBan(ctx context.Context, jobs JobInserter, projection ProjectBanProjection) (bool, error) {
	txDB, tx, err := s.projectDB.WithTx(ctx)
	if err != nil {
		return false, fmt.Errorf("start project ban transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	wasBanned, err := txDB.LockManagedProjectBan(ctx, projection.ProjectID)
	if err != nil {
		if dberrors.IsNotFoundError(err) {
			return false, ErrProjectNotFound
		}

		return false, fmt.Errorf("lock project: %w", err)
	}

	applied, err := txDB.ApplyProjectBanProjection(ctx, queries.ApplyProjectBanProjectionParams{
		ProjectID: projection.ProjectID,
		Revision:  projection.Revision,
		DecidedAt: decidedAtParam(projection.DecidedAt),
	})
	if err != nil {
		return false, fmt.Errorf("advance project ban projection: %w", err)
	}
	if !applied {
		if err := tx.Commit(ctx); err != nil {
			return false, fmt.Errorf("commit stale project ban projection: %w", err)
		}

		return false, nil
	}

	if _, err := txDB.Dashboard.SetTeamBanned(ctx, dashboardqueries.SetTeamBannedParams{
		TeamID:   projection.ProjectID,
		IsBanned: projection.Banned,
	}); err != nil {
		return false, fmt.Errorf("set project ban state: %w", err)
	}

	if projection.Banned && !wasBanned {
		if _, err := jobs.InsertTx(ctx, tx, outbox.ContainBannedTeam{TeamID: projection.ProjectID}, nil); err != nil {
			return false, fmt.Errorf("enqueue banned team containment: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit project ban projection: %w", err)
	}

	return true, nil
}
