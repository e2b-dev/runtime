package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	sqlcdb "github.com/e2b-dev/infra/packages/api/internal/db/client"
	"github.com/e2b-dev/infra/packages/api/internal/db/queries"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator"
	"github.com/e2b-dev/infra/packages/api/internal/outbox"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
)

func (a *APIStore) ExpiredPausedSandboxes(ctx context.Context, asOf time.Time, after queries.ListExpiredPausedSnapshotsRow, limit int32) ([]queries.ListExpiredPausedSnapshotsRow, error) {
	return a.sqlcDB.ListExpiredPausedSnapshots(ctx, queries.ListExpiredPausedSnapshotsParams{
		AsOf:           asOf,
		AfterExpiresAt: after.ExpiresAt,
		AfterSandboxID: after.SandboxID,
		BatchSize:      limit,
	})
}

func (a *APIStore) PausedSandboxLive(ctx context.Context, teamID uuid.UUID, sandboxID string) (bool, error) {
	_, err := a.orchestrator.GetSandbox(ctx, teamID, sandboxID)
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, sandbox.ErrNotFound):
		return false, nil
	default:
		return false, err
	}
}

// ExpirePausedSandbox deletes an expired paused sandbox as
// DeleteSandboxesSandboxID does, except that it leaves a live one alone
// instead of killing it.
func (a *APIStore) ExpirePausedSandbox(ctx context.Context, teamID uuid.UUID, sandboxID string) (outbox.PausedExpiryOutcome, error) {
	// The deadline starts before the claim, so the claim, the store read
	// that follows it and the delete all end within the claim's lifetime.
	deleteCtx, cancel := context.WithTimeout(ctx, orchestrator.SnapshotDeleteTimeout)
	defer cancel()

	claimID, claimed, err := a.orchestrator.ClaimPausedSandbox(deleteCtx, teamID, sandboxID)
	if err != nil {
		return "", fmt.Errorf("claim sandbox: %w", err)
	}
	if !claimed {
		return outbox.PausedExpirySkippedLive, nil
	}

	templateID, aliasKeys, err := a.softDeleteEnv(deleteCtx, func(txDB *sqlcdb.Client) (string, error) {
		return txDB.SoftDeleteExpiredSnapshot(deleteCtx, queries.SoftDeleteExpiredSnapshotParams{TeamID: teamID, SandboxID: sandboxID})
	})
	if err != nil || templateID == "" {
		// The snapshot survived, so let resumes through again.
		a.orchestrator.ReleaseKillClaim(context.WithoutCancel(ctx), teamID, sandboxID, claimID)
		if err != nil {
			return "", err
		}

		return outbox.PausedExpirySkippedRefreshed, nil
	}

	a.invalidateDeletedSnapshot(ctx, sandboxID, templateID, aliasKeys)

	return outbox.PausedExpiryDeleted, nil
}

// pausedExpiryCursorKey holds where an unfinished expiry sweep stopped. It
// outlives a few runs, so an API without a sweep for that long starts over
// from the oldest, which is always safe.
const (
	pausedExpiryCursorKey = "paused-expiry:sweep-cursor"
	pausedExpiryCursorTTL = 6 * time.Hour
)

type pausedExpiryCursor struct {
	ExpiresAt time.Time `json:"expiresAt"`
	SandboxID string    `json:"sandboxID"`
}

func (a *APIStore) LoadSweepCursor(ctx context.Context) (*queries.ListExpiredPausedSnapshotsRow, error) {
	raw, err := a.redisClient.Get(ctx, pausedExpiryCursorKey).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var cursor pausedExpiryCursor
	if err := json.Unmarshal(raw, &cursor); err != nil {
		return nil, fmt.Errorf("decode sweep cursor: %w", err)
	}

	return &queries.ListExpiredPausedSnapshotsRow{ExpiresAt: cursor.ExpiresAt, SandboxID: cursor.SandboxID}, nil
}

func (a *APIStore) SaveSweepCursor(ctx context.Context, cursor *queries.ListExpiredPausedSnapshotsRow) error {
	if cursor == nil {
		return a.redisClient.Del(ctx, pausedExpiryCursorKey).Err()
	}

	raw, err := json.Marshal(pausedExpiryCursor{ExpiresAt: cursor.ExpiresAt, SandboxID: cursor.SandboxID})
	if err != nil {
		return err
	}

	return a.redisClient.Set(ctx, pausedExpiryCursorKey, raw, pausedExpiryCursorTTL).Err()
}
