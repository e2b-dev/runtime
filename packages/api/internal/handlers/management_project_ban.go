package handlers

import (
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/management"
	"github.com/e2b-dev/infra/packages/shared/pkg/ginutils"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

func (a *APIStore) ManagementApplyProjectBan(c *gin.Context, projectID uuid.UUID) {
	ctx := c.Request.Context()
	attrs := []attribute.KeyValue{telemetry.WithTeamID(projectID.String())}

	body, err := ginutils.ParseBody[api.ManagementProjectBanRequest](ctx, c)
	if err != nil {
		telemetry.ReportErrorByCode(ctx, http.StatusBadRequest, "apply project ban failed",
			fmt.Errorf("parse project ban request: %w", err), attrs...)
		a.sendAPIStoreError(c, http.StatusBadRequest, "Invalid request body")

		return
	}

	if err := a.managementService.ApplyProjectBan(ctx, a.riverClient, management.ProjectBanProjection{
		ProjectID: projectID,
		Revision:  body.Revision,
		Banned:    body.Banned,
		DecidedAt: utils.DerefOrDefault(body.DecidedAt, time.Time{}),
	}); err != nil {
		a.sendProjectBanError(c, err, attrs...)

		return
	}

	c.Status(http.StatusNoContent)
}
