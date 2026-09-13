package handlers

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"

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
	switch {
	case err == nil:
		killedOrRemoved = true
	case errors.Is(err, orchestrator.ErrSandboxNotFound):
		logger.L().Info(ctx, "Running sandbox not found, checking for snapshot",
			logger.WithSandboxID(sandboxID),
			logger.WithTeamID(teamID.String()),
		)
	case errors.Is(err, orchestrator.ErrSandboxOperationFailed):
		logger.L().Error(ctx, "Failed to kill sandbox on orchestrator node",
			logger.WithSandboxID(sandboxID),
			logger.WithTeamID(teamID.String()),
			zap.Error(err),
		)
		telemetry.ReportError(ctx, "error killing sandbox on node", err)
		a.sendAPIStoreError(c, http.StatusInternalServerError, fmt.Sprintf("Error killing sandbox: %s", err))

		return
	default:
		logger.L().Error(ctx, "Unexpected error removing sandbox",
			logger.WithSandboxID(sandboxID),
			logger.WithTeamID(teamID.String()),
			zap.Error(err),
		)
		telemetry.ReportError(ctx, "error killing sandbox", err)
		a.sendAPIStoreError(c, http.StatusInternalServerError, fmt.Sprintf("Error killing sandbox: %s", err))

		return
	}

	// remove any snapshots when the sandbox is not running
	deleteSnapshotErr := a.deleteSnapshot(ctx, sandboxID, teamID)
	switch {
	case errors.Is(deleteSnapshotErr, db.ErrSnapshotNotFound):
		// no snapshot found, nothing to do
	case deleteSnapshotErr != nil:
		logger.L().Error(ctx, "Failed to delete sandbox snapshot from DB",
			logger.WithSandboxID(sandboxID),
			logger.WithTeamID(teamID.String()),
			zap.Error(deleteSnapshotErr),
		)
		telemetry.ReportError(ctx, "error deleting sandbox snapshot", deleteSnapshotErr)
		a.sendAPIStoreError(c, http.StatusInternalServerError, fmt.Sprintf("Error deleting sandbox: %s", deleteSnapshotErr))

		return
	default:
		killedOrRemoved = true
	}

	if killedOrRemoved {
		c.Status(http.StatusNoContent)
	} else {
		logger.L().Info(ctx, "Sandbox not found: not running and no snapshot",
			logger.WithSandboxID(sandboxID),
			logger.WithTeamID(teamID.String()),
		)
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
