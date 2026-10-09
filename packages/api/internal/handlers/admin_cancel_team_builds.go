package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

func (a *APIStore) PostAdminTeamsTeamIDBuildsCancel(c *gin.Context, teamID uuid.UUID) {
	ctx := c.Request.Context()
	ctx, span := tracer.Start(ctx, "cancel admin-team-builds")
	defer span.End()

	logger.L().Info(ctx, "Admin cancelling all builds for team", logger.WithTeamID(teamID.String()))

	cancelledCount, failedCount, err := a.templateManager.CancelTeamBuilds(ctx, teamID, "cancelled by admin")
	if err != nil {
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to get builds")

		return
	}

	logger.L().Info(ctx, "Completed cancelling team builds",
		logger.WithTeamID(teamID.String()),
		zap.Int("cancelled", cancelledCount),
		zap.Int("failed", failedCount),
	)

	c.JSON(http.StatusOK, api.AdminBuildCancelResult{
		CancelledCount: cancelledCount,
		FailedCount:    failedCount,
	})
}
