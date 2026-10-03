package handlers

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/e2b-dev/infra/packages/api/internal/db"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/api/internal/utils"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

func (a *APIStore) deleteSnapshot(ctx context.Context, sandboxID string, teamID uuid.UUID) error {
	snapshot, err := a.throttledGetSnapshotBuilds(ctx, teamID, sandboxID)
	if err != nil {
		return err
	}

	aliasKeys, dbErr := a.softDeleteTemplate(ctx, teamID, snapshot.TemplateID)
	if dbErr != nil {
		return fmt.Errorf("error deleting template from db: %w", dbErr)
	}

	a.templateCache.InvalidateAllTags(context.WithoutCancel(ctx), snapshot.TemplateID)
	a.templateCache.InvalidateAliasesByTemplateID(context.WithoutCancel(ctx), snapshot.TemplateID, aliasKeys)
	a.snapshotCache.Invalidate(context.WithoutCancel(ctx), sandboxID)

	return nil
}

func (a *APIStore) DeleteSandboxesSandboxID(
	c *gin.Context,
	sandboxID string,
) {
	ctx := c.Request.Context()

	var err error
	sandboxID, err = utils.ShortID(sandboxID)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, "Invalid sandbox ID")

		return
	}

	teamID := auth.MustGetTeamID(c)

	telemetry.SetAttributes(ctx,
		telemetry.WithSandboxID(sandboxID),
		telemetry.WithTeamID(teamID.String()),
	)

	telemetry.ReportEvent(ctx, "killing sandbox")

	killedOrRemoved := false

	err = a.orchestrator.RemoveSandbox(ctx, teamID, sandboxID, sandbox.RemoveOpts{
		Action: sandbox.StateActionKill,
		Reason: sandbox.KillReasonRequest,
	})

	// runningRemoved records whether RemoveSandbox killed a running record. When
	// it did not (the sandbox is paused: only a snapshot exists, no running
	// record to lock), deleting the snapshot below races a concurrent resume,
	// whose publication (storage.Add) is a lockless SET+SADD with no
	// delete-intent check. Without a rendezvous the DELETE can soft-delete the
	// snapshot and return 204 while the resume republishes the sandbox as
	// running. We fence that window with a kill-claim on the reservation the
	// resume holds for its whole lifecycle.
	runningRemoved := false

	switch {
	case err == nil:
		killedOrRemoved = true
		runningRemoved = true
	case errors.Is(err, orchestrator.ErrSandboxNotFound):
		logger.L().Debug(ctx, "Running sandbox not found", logger.WithSandboxID(sandboxID))
	case errors.Is(err, orchestrator.ErrSandboxOperationFailed):
		a.sendAPIStoreError(c, http.StatusInternalServerError, fmt.Sprintf("Error killing sandbox: %s", err))

		return
	default:
		telemetry.ReportError(ctx, "error killing sandbox", err)
		a.sendAPIStoreError(c, http.StatusInternalServerError, fmt.Sprintf("Error killing sandbox: %s", err))

		return
	}

	// Paused sandbox: claim the ID against a concurrent resume before touching
	// the snapshot. If a resume is in flight (or already finished, so the
	// sandbox is running again), refuse with 409 and leave the snapshot intact —
	// the client retries the kill against the running sandbox through the locked
	// path above. An accepted kill (a claim) is irreversible: reserveScript
	// rejects any resume that starts after it.
	claimTaken := false
	if !runningRemoved {
		claimed, claimErr := a.orchestrator.ClaimPausedKill(ctx, teamID, sandboxID)
		if claimErr != nil {
			telemetry.ReportError(ctx, "error claiming paused sandbox for deletion", claimErr)
			a.sendAPIStoreError(c, http.StatusInternalServerError, fmt.Sprintf("Error killing sandbox: %s", claimErr))

			return
		}
		if !claimed {
			logger.L().Info(ctx, "Refusing to delete paused sandbox: a resume is in flight", logger.WithSandboxID(sandboxID))
			a.sendAPIStoreError(c, http.StatusConflict, fmt.Sprintf("Sandbox %s is resuming; retry the delete once it is running", sandboxID))

			return
		}
		claimTaken = true
	}

	// remove any snapshots when the sandbox is not running
	deleteSnapshotErr := a.deleteSnapshot(ctx, sandboxID, teamID)
	switch {
	case errors.Is(deleteSnapshotErr, db.ErrSnapshotNotFound):
		// no snapshot found, nothing to do
	case deleteSnapshotErr != nil:
		if claimTaken {
			// The snapshot survived, so drop the claim to unblock future resumes
			// of this ID rather than making them wait out the claim's TTL.
			a.orchestrator.ReleasePausedKillClaim(context.WithoutCancel(ctx), teamID, sandboxID)
		}
		telemetry.ReportError(ctx, "error deleting sandbox", deleteSnapshotErr)
		a.sendAPIStoreError(c, http.StatusInternalServerError, fmt.Sprintf("Error deleting sandbox: %s", deleteSnapshotErr))

		return
	default:
		killedOrRemoved = true
	}

	if killedOrRemoved {
		c.Status(http.StatusNoContent)
	} else {
		logger.L().Debug(ctx, "Sandbox not found for deletion", logger.WithSandboxID(sandboxID))
		a.sendAPIStoreError(c, http.StatusNotFound, utils.SandboxNotFoundMsg(sandboxID))
	}
}

// throttledGetSnapshotBuilds runs GetSnapshotBuilds gated by the snapshot build query semaphore.
func (a *APIStore) throttledGetSnapshotBuilds(ctx context.Context, teamID uuid.UUID, sandboxID string) (db.SnapshotBuilds, error) {
	if err := a.snapshotBuildQuerySem.Acquire(ctx, 1); err != nil {
		return db.SnapshotBuilds{}, err
	}
	defer a.snapshotBuildQuerySem.Release(1)

	return db.GetSnapshotBuilds(ctx, a.sqlcDB, teamID, sandboxID)
}
