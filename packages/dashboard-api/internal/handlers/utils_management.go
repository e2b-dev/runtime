package handlers

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel/attribute"

	"github.com/e2b-dev/infra/packages/dashboard-api/internal/api"
	"github.com/e2b-dev/infra/packages/dashboard-api/internal/management"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// sendNotImplemented answers the management operations that are declared in
// the contract but not served.
func sendNotImplemented(c *gin.Context) {
	c.JSON(http.StatusNotImplemented, api.Error{
		Code:    http.StatusNotImplemented,
		Message: "operation is not implemented",
	})
}

func (s *APIStore) sendProjectMemberError(c *gin.Context, err error, attrs ...attribute.KeyValue) {
	ctx := c.Request.Context()

	switch {
	case errors.Is(err, management.ErrProjectNotFound):
		telemetry.ReportErrorByCode(ctx, http.StatusNotFound, "apply project member failed", err, attrs...)
		s.sendAPIStoreError(c, http.StatusNotFound, "Project not found")
	case errors.Is(err, management.ErrInvalidProjectMember),
		errors.Is(err, management.ErrDuplicateProjectIdentity):
		telemetry.ReportErrorByCode(ctx, http.StatusBadRequest, "apply project member failed", err, attrs...)
		s.sendAPIStoreError(c, http.StatusBadRequest, "Invalid project member")
	case errors.Is(err, management.ErrProjectIdentityOwnedByUser):
		telemetry.ReportErrorByCode(ctx, http.StatusConflict, "apply project member failed", err, attrs...)
		s.sendAPIStoreError(c, http.StatusConflict, "Identity is linked to another user")
	default:
		telemetry.ReportCriticalError(ctx, "apply project member failed", err, attrs...)
		s.sendAPIStoreError(c, http.StatusInternalServerError, "Error applying project member")
	}
}

func (s *APIStore) sendProjectBlockError(c *gin.Context, err error, attrs ...attribute.KeyValue) {
	ctx := c.Request.Context()

	switch {
	case errors.Is(err, management.ErrProjectNotFound):
		telemetry.ReportErrorByCode(ctx, http.StatusNotFound, "apply project block failed", err, attrs...)
		s.sendAPIStoreError(c, http.StatusNotFound, "Project not found")
	case errors.Is(err, management.ErrInvalidProjectBlock):
		telemetry.ReportErrorByCode(ctx, http.StatusBadRequest, "apply project block failed", err, attrs...)
		s.sendAPIStoreError(c, http.StatusBadRequest, "Invalid project block state")
	default:
		telemetry.ReportCriticalError(ctx, "apply project block failed", err, attrs...)
		s.sendAPIStoreError(c, http.StatusInternalServerError, "Error applying project block")
	}
}

// A cache eviction that failed is reported: the values are committed, so the
// caller retrying is what gets the stale entry cleared.
func (s *APIStore) sendProjectLimitsError(c *gin.Context, err error, attrs ...attribute.KeyValue) {
	ctx := c.Request.Context()

	switch {
	case errors.Is(err, management.ErrProjectNotFound):
		telemetry.ReportErrorByCode(ctx, http.StatusNotFound, "upsert project limits failed", err, attrs...)
		s.sendAPIStoreError(c, http.StatusNotFound, "Project not found")
	case errors.Is(err, management.ErrInvalidProjectLimits):
		telemetry.ReportErrorByCode(ctx, http.StatusBadRequest, "upsert project limits failed", err, attrs...)
		s.sendAPIStoreError(c, http.StatusBadRequest, "Invalid project limits")
	case errors.Is(err, management.ErrProjectLimitsRejected):
		telemetry.ReportErrorByCode(ctx, http.StatusBadRequest, "upsert project limits failed", err, attrs...)
		s.sendAPIStoreError(c, http.StatusBadRequest, "Limits violate a constraint")
	default:
		telemetry.ReportCriticalError(ctx, "upsert project limits failed", err, attrs...)
		s.sendAPIStoreError(c, http.StatusInternalServerError, "Error updating project limits")
	}
}
