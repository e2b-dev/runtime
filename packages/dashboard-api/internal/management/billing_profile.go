package management

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/db/pkg/dberrors"
	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

var ErrInvalidBillingProfile = errors.New("invalid billing profile")

// The facts arrive resolved: the caller owns the global billing state and this
// side stores what it is given. A team without a row has an unknown profile —
// readers must not default it.
type BillingProfileProjection struct {
	ProjectID uuid.UUID
	Revision  int64
	// Zero when the caller did not say.
	DecidedAt time.Time

	HasPaymentMethod bool
	Enterprise       bool
	// Empty when no plan is in force or the caller does not know.
	Plan string
}

// ApplyBillingProfile records a project's billing profile, behind the revision
// that decides whether this delivery is the newest one.
func (s *Service) ApplyBillingProfile(ctx context.Context, projection BillingProfileProjection) error {
	if projection.ProjectID == uuid.Nil || projection.Revision <= 0 {
		return ErrInvalidBillingProfile
	}

	stored, err := s.applyBillingProfile(ctx, projection)
	if err != nil {
		return err
	}
	if stored {
		s.applyLag.stored(ctx, projectionBillingProfiles, projection.DecidedAt)
	}

	// Logged rather than returned, as the sibling writes do: the row is
	// committed, and a caller told the delivery failed would repeat a revision
	// this side already has.
	if err := s.cache.InvalidateTeamCache(ctx, projection.ProjectID); err != nil {
		logger.L().Error(ctx, "invalidating team cache after billing profile update",
			logger.WithTeamID(projection.ProjectID.String()), zap.Error(err))
	}

	return nil
}

func (s *Service) applyBillingProfile(ctx context.Context, projection BillingProfileProjection) (bool, error) {
	txDB, tx, err := s.projectDB.WithTx(ctx)
	if err != nil {
		return false, fmt.Errorf("start billing profile transaction: %w", err)
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

	applied, err := txDB.ApplyBillingProfileProjection(ctx, queries.ApplyBillingProfileProjectionParams{
		ProjectID: projection.ProjectID,
		Revision:  projection.Revision,
		DecidedAt: decidedAtParam(projection.DecidedAt),
	})
	if err != nil {
		return false, fmt.Errorf("advance billing profile projection: %w", err)
	}
	if !applied {
		if err := tx.Commit(ctx); err != nil {
			return false, fmt.Errorf("commit stale billing profile projection: %w", err)
		}

		return false, nil
	}

	if err := txDB.UpsertTeamBillingProfile(ctx, queries.UpsertTeamBillingProfileParams{
		TeamID:           projection.ProjectID,
		HasPaymentMethod: projection.HasPaymentMethod,
		Enterprise:       projection.Enterprise,
		Plan:             planParam(projection.Plan),
	}); err != nil {
		return false, fmt.Errorf("upsert team billing profile: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit billing profile projection: %w", err)
	}

	return true, nil
}

func planParam(plan string) *string {
	trimmed := strings.TrimSpace(plan)
	if trimmed == "" {
		return nil
	}

	return &trimmed
}
