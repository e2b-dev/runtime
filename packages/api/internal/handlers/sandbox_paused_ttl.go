package handlers

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/utils"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/shared/pkg/ginutils"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

func (a *APIStore) PutSandboxesSandboxIDPausedRetention(c *gin.Context, sandboxID api.SandboxID) {
	ctx := c.Request.Context()

	sandboxID, err := utils.ShortID(sandboxID)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, "Invalid sandbox ID")

		return
	}

	telemetry.SetAttributes(ctx, telemetry.WithSandboxID(sandboxID))

	teamID := auth.MustGetTeamID(c)

	body, err := ginutils.ParseBody[api.SandboxPausedRetentionRequest](ctx, c)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("Error when parsing request: %s", err))
		telemetry.ReportCriticalError(ctx, "error when parsing request", err)

		return
	}

	if apiErr := a.orchestrator.SetPausedTTL(ctx, teamID, sandboxID, pausedTTLDuration(body.PausedRetentionSeconds)); apiErr != nil {
		telemetry.ReportErrorByCode(ctx, apiErr.Code, "error when setting the paused retention", apiErr.Err)
		a.sendAPIStoreError(c, apiErr.Code, apiErr.ClientMsg)

		return
	}

	c.Status(http.StatusNoContent)
}
