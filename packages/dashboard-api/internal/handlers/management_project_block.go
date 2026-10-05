package handlers

import (
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel/attribute"

	"github.com/e2b-dev/infra/packages/dashboard-api/internal/api"
	"github.com/e2b-dev/infra/packages/dashboard-api/internal/management"
	"github.com/e2b-dev/infra/packages/shared/pkg/ginutils"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

func (s *APIStore) ManagementApplyProjectBlock(c *gin.Context, projectID api.ProjectID) {
	ctx := c.Request.Context()
	attrs := []attribute.KeyValue{telemetry.WithTeamID(projectID.String())}

	body, err := ginutils.ParseBody[api.ManagementProjectBlockRequest](ctx, c)
	if err != nil {
		telemetry.ReportErrorByCode(ctx, http.StatusBadRequest, "apply project block failed",
			fmt.Errorf("parse project block request: %w", err), attrs...)
		s.sendAPIStoreError(c, http.StatusBadRequest, "Invalid request body")

		return
	}

	reason := ""
	if body.Reason != nil {
		reason = *body.Reason
	}

	if err := s.managementService.ApplyProjectBlock(ctx, management.ProjectBlockProjection{
		ProjectID: projectID,
		Revision:  body.Revision,
		Blocked:   body.Blocked,
		Reason:    reason,
		DecidedAt: utils.DerefOrDefault(body.DecidedAt, time.Time{}),
	}); err != nil {
		s.sendProjectBlockError(c, err, attrs...)

		return
	}

	c.Status(http.StatusNoContent)
}
