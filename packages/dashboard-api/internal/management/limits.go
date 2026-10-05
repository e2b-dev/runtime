package management

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/db/pkg/dberrors"
	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

var (
	ErrInvalidProjectLimits = errors.New("invalid project limits")

	// A pair the columns will not hold, currently a free disk allowance above
	// the ceiling. The caller sent values this side will not store, which is its
	// error to fix.
	ErrProjectLimitsRejected = errors.New("project limits violate a constraint")
)

// The values arrive absolute: the caller owns plans and add-ons and has already
// resolved them, which is what lets team_limits prefer this row over the tier it
// would otherwise compute from.
type ProjectLimitsProjection struct {
	ProjectID uuid.UUID
	Revision  int64
	// Zero when the caller did not say.
	DecidedAt time.Time

	MaxLengthHours           int64
	ConcurrentSandboxes      int64
	ConcurrentTemplateBuilds int64
	MaxVCPU                  int64
	MaxRAMMB                 int64
	DiskMB                   int64
	EventsTTLDays            int64
	DefaultFreeDiskSizeMB    int64
	MaxFreeDiskSizeMB        int64
	APITeamRPSList           int64
}

// ApplyProjectLimits records a project's effective limits, behind the revision
// that decides whether this delivery is the newest one.
//
// The eviction runs whether or not the delivery wrote. An accepted write whose
// eviction did not finish -- the process died, or one key of several failed --
// is retried by a caller carrying the same revision, which this side drops;
// skipping it there would leave the cache serving the old limits until the entry
// expires, with the retry that was meant to repair it reporting success.
func (s *Service) ApplyProjectLimits(ctx context.Context, projection ProjectLimitsProjection) error {
	if err := validateProjectLimitsProjection(projection); err != nil {
		return err
	}

	stored, err := s.applyProjectLimits(ctx, projection)
	if err != nil {
		return err
	}
	if stored {
		s.applyLag.stored(ctx, projectionProjectLimits, projection.DecidedAt)
	}

	// Logged rather than returned, as the sibling writes do: the row is
	// committed, and a caller told the delivery failed would repeat a revision
	// this side already has.
	if err := s.cache.InvalidateTeamCache(ctx, projection.ProjectID); err != nil {
		logger.L().Error(ctx, "invalidating team cache after limits update",
			logger.WithTeamID(projection.ProjectID.String()), zap.Error(err))
	}

	return nil
}

func (s *Service) applyProjectLimits(ctx context.Context, projection ProjectLimitsProjection) (bool, error) {
	txDB, tx, err := s.projectDB.WithTx(ctx)
	if err != nil {
		return false, fmt.Errorf("start project limits transaction: %w", err)
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

	applied, err := txDB.ApplyProjectLimitsProjection(ctx, queries.ApplyProjectLimitsProjectionParams{
		ProjectID: projection.ProjectID,
		Revision:  projection.Revision,
		DecidedAt: decidedAtParam(projection.DecidedAt),
	})
	if err != nil {
		return false, fmt.Errorf("advance project limits projection: %w", err)
	}
	if !applied {
		if err := tx.Commit(ctx); err != nil {
			return false, fmt.Errorf("commit stale project limits projection: %w", err)
		}

		return false, nil
	}

	// In the same transaction as the ledger above. A revision recorded without
	// its values would make every retry a duplicate this side drops, leaving the
	// project on the old limits for good.
	if err := txDB.UpsertProjectLimits(ctx, queries.UpsertProjectLimitsParams{
		TeamID:                   projection.ProjectID,
		MaxLengthHours:           projection.MaxLengthHours,
		ConcurrentSandboxes:      projection.ConcurrentSandboxes,
		ConcurrentTemplateBuilds: projection.ConcurrentTemplateBuilds,
		MaxVcpu:                  projection.MaxVCPU,
		MaxRamMb:                 projection.MaxRAMMB,
		DiskMb:                   projection.DiskMB,
		EventsTtlDays:            projection.EventsTTLDays,
		DefaultFreeDiskSizeMb:    projection.DefaultFreeDiskSizeMB,
		MaxFreeDiskSizeMb:        projection.MaxFreeDiskSizeMB,
		ApiTeamRpsList:           projection.APITeamRPSList,
	}); err != nil {
		if dberrors.IsCheckViolation(err) {
			return false, fmt.Errorf("%w: %w", ErrProjectLimitsRejected, err)
		}

		return false, fmt.Errorf("upsert project limits: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit project limits projection: %w", err)
	}

	return true, nil
}

// The floors are the management contract's minimums. Zero is a value only for
// the default free disk and the list rate, where it disables the limit.
func validateProjectLimitsProjection(projection ProjectLimitsProjection) error {
	if projection.ProjectID == uuid.Nil {
		return fmt.Errorf("%w: project_id is required", ErrInvalidProjectLimits)
	}

	for _, floored := range []struct {
		field string
		value int64
		floor int64
	}{
		{field: "revision", value: projection.Revision, floor: 1},
		{field: "max_sandbox_length_hours", value: projection.MaxLengthHours, floor: 1},
		{field: "concurrent_sandboxes", value: projection.ConcurrentSandboxes, floor: 1},
		{field: "concurrent_template_builds", value: projection.ConcurrentTemplateBuilds, floor: 1},
		{field: "max_vcpu", value: projection.MaxVCPU, floor: 1},
		{field: "max_ram_mb", value: projection.MaxRAMMB, floor: 1},
		{field: "disk_mb", value: projection.DiskMB, floor: 1},
		{field: "events_ttl_days", value: projection.EventsTTLDays, floor: 1},
		{field: "default_free_disk_size_mb", value: projection.DefaultFreeDiskSizeMB, floor: 0},
		{field: "max_free_disk_size_mb", value: projection.MaxFreeDiskSizeMB, floor: 1},
		{field: "api_team_rps_list", value: projection.APITeamRPSList, floor: 0},
	} {
		if floored.value < floored.floor {
			return fmt.Errorf("%w: %s must be at least %d, got %d",
				ErrInvalidProjectLimits, floored.field, floored.floor, floored.value)
		}
	}

	if projection.DefaultFreeDiskSizeMB > projection.MaxFreeDiskSizeMB {
		return fmt.Errorf("%w: default_free_disk_size_mb %d exceeds max_free_disk_size_mb %d",
			ErrInvalidProjectLimits, projection.DefaultFreeDiskSizeMB, projection.MaxFreeDiskSizeMB)
	}

	return nil
}
