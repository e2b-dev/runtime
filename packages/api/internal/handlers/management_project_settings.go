package handlers

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/db/pkg/dberrors"
	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/ginutils"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

func (a *APIStore) ManagementSetProjectSettings(c *gin.Context, projectID uuid.UUID) {
	ctx := c.Request.Context()

	body, err := ginutils.ParseBody[api.ManagementProjectSettingsRequest](ctx, c)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, "Invalid request body")

		return
	}
	if projectID == uuid.Nil || body.Revision <= 0 {
		a.sendAPIStoreError(c, http.StatusBadRequest, "Invalid project settings revision")

		return
	}

	txDB, tx, err := a.sqlcDB.WithTx(ctx)
	if err != nil {
		telemetry.ReportCriticalError(ctx, "start project settings transaction failed", err, telemetry.WithTeamID(projectID.String()))
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error applying project settings")

		return
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	if _, err := txDB.LockManagedProject(ctx, projectID); err != nil {
		if dberrors.IsNotFoundError(err) {
			a.sendAPIStoreError(c, http.StatusNotFound, "Project not found")

			return
		}
		telemetry.ReportCriticalError(ctx, "lock project failed", err, telemetry.WithTeamID(projectID.String()))
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error applying project settings")

		return
	}
	deleted, err := txDB.IsProjectDeleted(ctx, projectID)
	if err != nil {
		telemetry.ReportCriticalError(ctx, "check project deletion failed", err, telemetry.WithTeamID(projectID.String()))
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error applying project settings")

		return
	}
	if deleted {
		a.sendAPIStoreError(c, http.StatusNotFound, "Project not found")

		return
	}

	if err := txDB.ApplyProjectSettingsProjection(ctx, queries.ApplyProjectSettingsProjectionParams{
		ProjectID:                        projectID,
		Revision:                         body.Revision,
		SandboxDefaultAllowPublicTraffic: body.Settings.SandboxSettings.DefaultAllowPublicTraffic,
	}); err != nil {
		telemetry.ReportCriticalError(ctx, "apply project settings projection failed", err, telemetry.WithTeamID(projectID.String()))
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error applying project settings")

		return
	}
	if err := tx.Commit(ctx); err != nil {
		telemetry.ReportCriticalError(ctx, "commit project settings projection failed", err, telemetry.WithTeamID(projectID.String()))
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error applying project settings")

		return
	}

	// Duplicate deliveries must retry eviction after a committed write's cache failure.
	if err := a.authService.InvalidateTeamCache(ctx, projectID); err != nil {
		telemetry.ReportCriticalError(ctx, "invalidate team cache after settings update failed", err, telemetry.WithTeamID(projectID.String()))
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error applying project settings")

		return
	}

	c.Status(http.StatusNoContent)
}
