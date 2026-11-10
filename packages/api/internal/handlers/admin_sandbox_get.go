package handlers

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/utils"
	clickhouse "github.com/e2b-dev/infra/packages/clickhouse/pkg"
	"github.com/e2b-dev/infra/packages/shared/pkg/events"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

func (a *APIStore) GetAdminSandboxesSandboxID(c *gin.Context, sandboxID api.SandboxID) {
	ctx := c.Request.Context()

	id, err := utils.ShortID(sandboxID)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, "Invalid sandbox ID")

		return
	}

	lifecycle, err := a.sandboxLifecycles.QuerySandboxLifecycle(ctx, id)
	if errors.Is(err, clickhouse.ErrSandboxNotFound) {
		a.sendAPIStoreError(c, http.StatusNotFound, utils.SandboxNotFoundMsg(id))

		return
	}
	if err != nil {
		logger.L().Error(ctx, "Failed to read sandbox lifecycle", zap.Error(err), logger.WithSandboxID(id))
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to read sandbox lifecycle")

		return
	}

	state, err := adminSandboxState(lifecycle.EventType)
	if err != nil {
		logger.L().Error(ctx, "Failed to read sandbox lifecycle", zap.Error(err), logger.WithSandboxID(id))
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to read sandbox lifecycle")

		return
	}

	c.JSON(http.StatusOK, api.AdminSandbox{
		SandboxID: id,
		TeamID:    lifecycle.TeamID,
		State:     state,
	})
}

func adminSandboxState(eventType string) (api.AdminSandboxState, error) {
	switch eventType {
	case events.SandboxCreatedEvent, events.SandboxResumedEvent:
		return api.AdminSandboxStateRunning, nil
	case events.SandboxPausedEvent:
		return api.AdminSandboxStatePaused, nil
	case events.SandboxKilledEvent:
		return api.AdminSandboxStateKilled, nil
	default:
		return "", fmt.Errorf("no sandbox state for lifecycle event %q", eventType)
	}
}
