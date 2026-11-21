package handlers

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/api/internal/utils"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/shared/pkg/ginutils"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// timeoutOrchestrator is the slice of *orchestrator.Orchestrator the timeout
// handler consumes — an interface so the 404 branch (an already-expired
// sandbox is expected, not an error) is testable without a real orchestrator.
type timeoutOrchestrator interface {
	KeepAliveFor(ctx context.Context, teamID uuid.UUID, sandboxID string, duration time.Duration, allowShorter bool) (*sandbox.Sandbox, *api.APIError)
}

// timeoutBackend returns the timeout handler's orchestrator slice, overridable
// in tests via timeoutBackendOverride.
func (a *APIStore) timeoutBackend() timeoutOrchestrator {
	if a.timeoutBackendOverride != nil {
		return a.timeoutBackendOverride
	}

	return a.orchestrator
}

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

	_, apiErr := a.timeoutBackend().KeepAliveFor(ctx, teamID, sandboxID, duration, true)
	if apiErr != nil {
		// A 404 is expected: SDK cleanup calls race the sandbox's own
		// expiry, so the client cannot always know it is already gone. Log
		// it at INFO without a stack trace instead of ReportError (WARN +
		// span error), which would bury real failures.
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
