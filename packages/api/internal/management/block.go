package management

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	dashboardqueries "github.com/e2b-dev/infra/packages/db/pkg/dashboard/queries"
	"github.com/e2b-dev/infra/packages/db/pkg/dberrors"
	"github.com/e2b-dev/infra/packages/db/queries"
)

var ErrInvalidProjectBlock = errors.New("invalid project block state")

type ProjectBlockProjection struct {
	ProjectID uuid.UUID
	Revision  int64
	Blocked   bool
	Reason    string
	// Zero when the caller did not say.
	DecidedAt time.Time
}

func (s *Service) ApplyProjectBlock(ctx context.Context, projection ProjectBlockProjection) error {
	if projection.ProjectID == uuid.Nil || projection.Revision <= 0 {
		return ErrInvalidProjectBlock
	}

	stored, err := s.applyProjectBlock(ctx, projection)
	if err != nil {
		return err
	}
	if stored {
		s.applyLag.stored(ctx, projectionProjectBlocks, projection.DecidedAt)
	}

	// Duplicate deliveries must retry eviction after a committed write's cache failure.
	if err := s.cache.InvalidateTeamCache(ctx, projection.ProjectID); err != nil {
		return fmt.Errorf("invalidate team cache after block update: %w", err)
	}

	return nil
}

func (s *Service) applyProjectBlock(ctx context.Context, projection ProjectBlockProjection) (bool, error) {
	txDB, tx, err := s.projectDB.WithTx(ctx)
	if err != nil {
		return false, fmt.Errorf("start project block transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	if _, err := txDB.LockManagedProject(ctx, projection.ProjectID); err != nil {
		if dberrors.IsNotFoundError(err) {
			return false, ErrProjectNotFound
		}

		return false, fmt.Errorf("lock project: %w", err)
	}

	applied, err := txDB.ApplyProjectBlockProjection(ctx, queries.ApplyProjectBlockProjectionParams{
		ProjectID: projection.ProjectID,
		Revision:  projection.Revision,
		DecidedAt: decidedAtParam(projection.DecidedAt),
	})
	if err != nil {
		return false, fmt.Errorf("advance project block projection: %w", err)
	}
	if !applied {
		if err := tx.Commit(ctx); err != nil {
			return false, fmt.Errorf("commit stale project block projection: %w", err)
		}

		return false, nil
	}

	if _, err := txDB.Dashboard.SetTeamBlocked(ctx, dashboardqueries.SetTeamBlockedParams{
		TeamID:        projection.ProjectID,
		IsBlocked:     projection.Blocked,
		BlockedReason: blockedReason(projection),
	}); err != nil {
		return false, fmt.Errorf("set project block state: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit project block projection: %w", err)
	}

	return true, nil
}

func blockedReason(projection ProjectBlockProjection) *string {
	if !projection.Blocked {
		return nil
	}

	reason := strings.TrimSpace(projection.Reason)
	if reason == "" {
		return nil
	}

	return &reason
}
