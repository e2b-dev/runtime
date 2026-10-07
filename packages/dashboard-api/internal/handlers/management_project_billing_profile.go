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

// ManagementUpsertProjectBillingProfile records a project's billing profile as
// resolved on the global billing plane.
//
// Idempotent for the same reason the limits route is: the revision decides
// which delivery writes, and a stale delivery is answered 204 — what it asked
// for is stored, and a newer answer is not this call's to undo.
func (s *APIStore) ManagementUpsertProjectBillingProfile(c *gin.Context, projectID api.ProjectID) {
	ctx := c.Request.Context()
	attrs := []attribute.KeyValue{telemetry.WithTeamID(projectID.String())}

	body, err := ginutils.ParseBody[api.ManagementProjectBillingProfile](ctx, c)
	if err != nil {
		telemetry.ReportErrorByCode(ctx, http.StatusBadRequest, "upsert project billing profile failed",
			fmt.Errorf("parse project billing profile request: %w", err), attrs...)
		s.sendAPIStoreError(c, http.StatusBadRequest, "Invalid request body")

		return
	}

	if err := s.managementService.ApplyBillingProfile(ctx, management.BillingProfileProjection{
		ProjectID:        projectID,
		Revision:         body.Revision,
		DecidedAt:        utils.DerefOrDefault(body.DecidedAt, time.Time{}),
		HasPaymentMethod: body.HasPaymentMethod,
		Enterprise:       body.Enterprise,
		Plan:             utils.DerefOrDefault(body.Plan, ""),
	}); err != nil {
		s.sendBillingProfileError(c, err, attrs...)

		return
	}

	c.Status(http.StatusNoContent)
}
