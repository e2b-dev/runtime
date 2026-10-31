package handlers

import (
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/utils"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/shared/pkg/ginutils"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

func (a *APIStore) PostSandboxesSandboxIDTimeout(
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

	telemetry.SetAttributes(ctx, telemetry.WithSandboxID(sandboxID))

	teamID := auth.MustGetTeamID(c)

	var duration time.Duration

	body, err := ginutils.ParseBody[api.SandboxTimeoutRequest](ctx, c)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("Error when parsing request: %s", err))

		telemetry.ReportCriticalError(ctx, "error when parsing request", err)

		return
	}

	if body.Timeout < 0 {
		duration = 0
	} else {
		duration = time.Duration(body.Timeout) * time.Second
	}

	_, apiErr := a.orchestrator.KeepAliveFor(ctx, teamID, sandboxID, duration, true)
	if apiErr != nil {
		if apiErr.Code == http.StatusNotFound {
			logger.L().Info(ctx, "sandbox not found for timeout update", logger.WithSandboxID(sandboxID))
		} else {
			telemetry.ReportError(ctx, "error when setting timeout", apiErr.Err)
		}
		a.sendAPIStoreError(c, apiErr.Code, apiErr.ClientMsg)

		return
	}

	c.Status(http.StatusNoContent)
}
