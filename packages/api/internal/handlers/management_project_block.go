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

func (a *APIStore) ManagementApplyProjectBlock(c *gin.Context, projectID uuid.UUID) {
	ctx := c.Request.Context()
	attrs := []attribute.KeyValue{telemetry.WithTeamID(projectID.String())}

	body, err := ginutils.ParseBody[api.ManagementProjectBlockRequest](ctx, c)
	if err != nil {
		telemetry.ReportErrorByCode(ctx, http.StatusBadRequest, "apply project block failed",
			fmt.Errorf("parse project block request: %w", err), attrs...)
		a.sendAPIStoreError(c, http.StatusBadRequest, "Invalid request body")

		return
	}

	if err := a.managementService.ApplyProjectBlock(ctx, management.ProjectBlockProjection{
		ProjectID: projectID,
		Revision:  body.Revision,
		Blocked:   body.Blocked,
		Reason:    utils.FromPtr(body.Reason),
		DecidedAt: utils.DerefOrDefault(body.DecidedAt, time.Time{}),
	}); err != nil {
		a.sendProjectBlockError(c, err, attrs...)

		return
	}

	c.Status(http.StatusNoContent)
}
