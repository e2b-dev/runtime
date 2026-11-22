package orchestrator

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/api/internal/db/queries"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

const pausedExpiryWriteTimeout = 5 * time.Second

func pausedTTLSeconds(ttl *time.Duration) *int64 {
	if ttl == nil {
		return nil
	}
	seconds := int64(ttl.Seconds())

	return &seconds
}

// boundPausedExpiry runs before a resumed run is recorded. It sets the
// snapshot's expiry to the latest the run can end, StartTime plus
// MaxInstanceLength, plus the run's TTL, so a run end whose stamp fails
// leaves the sandbox expiring late rather than at the previous pause's
// expiry, which a run longer than its TTL has already passed. A run without a
// TTL clears the expiry.
func (o *Orchestrator) boundPausedExpiry(ctx context.Context, sbx sandbox.Sandbox) error {
	if sbx.PausedTTL == nil && !sbx.PausedTTLStored {
		return nil
	}

	var expiresAt *time.Time
	if sbx.PausedTTL != nil {
		expiresAt = new(sbx.StartTime.Add(sbx.MaxInstanceLength + *sbx.PausedTTL))
	}

	ctx, cancel := context.WithTimeout(ctx, pausedExpiryWriteTimeout)
	defer cancel()

	return o.sqlcDB.BoundSnapshotExpiry(ctx, queries.BoundSnapshotExpiryParams{
		ExpiresAt: expiresAt,
		SandboxID: sbx.SandboxID,
		TeamID:    sbx.TeamID,
	})
}

// stampPausedExpiry restarts the expiry clock of the sandbox's snapshot when
// RemoveSandbox ends one of its runs, with the TTL the run carried. A run
// that never had a TTL has nothing to stamp and makes no database call. It
// runs before the record is removed, so no resume reads the snapshot before
// its config carries the run's TTL. A run that ends without it, by a
// migration or a stale record's cleanup, or whose stamp fails, keeps the
// later expiry its resume set; a first run leaves the row without one.
func (o *Orchestrator) stampPausedExpiry(ctx context.Context, sbx sandbox.Sandbox) {
	if sbx.PausedTTL == nil && !sbx.PausedTTLStored {
		return
	}

	stampCtx, cancel := context.WithTimeout(ctx, pausedExpiryWriteTimeout)
	_, err := o.sqlcDB.StampSnapshotExpiry(stampCtx, queries.StampSnapshotExpiryParams{
		SandboxID:        sbx.SandboxID,
		TeamID:           sbx.TeamID,
		PausedTtlSeconds: pausedTTLSeconds(sbx.PausedTTL),
	})
	cancel()
	if err != nil {
		logger.L().Error(ctx, "Failed to stamp the paused sandbox expiry", zap.Error(err), logger.WithSandboxID(sbx.SandboxID))

		return
	}

	// Not bounded by the stamp's timeout: the delete waits out a cache writer
	// holding the entry's lock, which can take longer, and deletes without it
	// once its context ends, letting that writer restore the old config.
	o.snapshotCache.Invalidate(ctx, sbx.SandboxID)
}
