package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

func (a *APIStore) PostAdminTeamsTeamIDSandboxesKill(c *gin.Context, teamID uuid.UUID) {
	ctx := c.Request.Context()
	ctx, span := tracer.Start(ctx, "admin-kill-team-sandboxes")
	defer span.End()

	err := a.authService.InvalidateTeamCache(ctx, teamID)
	if err != nil {
		logger.L().Error(ctx, "Failed to invalidate auth cache for team",
			logger.WithTeamID(teamID.String()),
			zap.Error(err))
	}

	logger.L().Info(ctx, "Admin killing all sandboxes for team", logger.WithTeamID(teamID.String()))

	killedCount, failedCount, err := a.orchestrator.KillTeamSandboxes(ctx, teamID, sandbox.KillReasonAdmin)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to get sandboxes")

		return
	}

	// Invalidate auth cache for this team so subsequent requests re-check against DB
	if err := a.authService.InvalidateTeamCache(ctx, teamID); err != nil {
		logger.L().Error(ctx, "Failed to invalidate auth cache for team",
			logger.WithTeamID(teamID.String()),
			zap.Error(err))
	}

	logger.L().Info(ctx, "Completed killing team sandboxes",
		zap.String("teamID", teamID.String()),
		zap.Int("killed", killedCount),
		zap.Int("failed", failedCount),
	)

	c.JSON(http.StatusOK, api.AdminSandboxKillResult{
		KilledCount: killedCount,
		FailedCount: failedCount,
	})
}
