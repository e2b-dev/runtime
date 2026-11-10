package handlers

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel/attribute"

	"github.com/e2b-dev/infra/packages/api/internal/management"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

func (a *APIStore) sendProjectMemberError(c *gin.Context, err error, attrs ...attribute.KeyValue) {
	ctx := c.Request.Context()

	switch {
	case errors.Is(err, management.ErrProjectNotFound):
		telemetry.ReportErrorByCode(ctx, http.StatusNotFound, "apply project member failed", err, attrs...)
		a.sendAPIStoreError(c, http.StatusNotFound, "Project not found")
	case errors.Is(err, management.ErrInvalidProjectMember),
		errors.Is(err, management.ErrDuplicateProjectIdentity):
		telemetry.ReportErrorByCode(ctx, http.StatusBadRequest, "apply project member failed", err, attrs...)
		a.sendAPIStoreError(c, http.StatusBadRequest, "Invalid project member")
	case errors.Is(err, management.ErrProjectIdentityOwnedByUser):
		telemetry.ReportErrorByCode(ctx, http.StatusConflict, "apply project member failed", err, attrs...)
		a.sendAPIStoreError(c, http.StatusConflict, "Identity is linked to another user")
	default:
		telemetry.ReportCriticalError(ctx, "apply project member failed", err, attrs...)
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error applying project member")
	}
}

func (a *APIStore) sendProjectLimitsError(c *gin.Context, err error, attrs ...attribute.KeyValue) {
	ctx := c.Request.Context()

	switch {
	case errors.Is(err, management.ErrProjectNotFound):
		telemetry.ReportErrorByCode(ctx, http.StatusNotFound, "upsert project limits failed", err, attrs...)
		a.sendAPIStoreError(c, http.StatusNotFound, "Project not found")
	case errors.Is(err, management.ErrInvalidProjectLimits):
		telemetry.ReportErrorByCode(ctx, http.StatusBadRequest, "upsert project limits failed", err, attrs...)
		a.sendAPIStoreError(c, http.StatusBadRequest, "Invalid project limits")
	case errors.Is(err, management.ErrProjectLimitsRejected):
		telemetry.ReportErrorByCode(ctx, http.StatusBadRequest, "upsert project limits failed", err, attrs...)
		a.sendAPIStoreError(c, http.StatusBadRequest, "Limits violate a constraint")
	default:
		telemetry.ReportCriticalError(ctx, "upsert project limits failed", err, attrs...)
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error updating project limits")
	}
}

func (a *APIStore) sendProjectBlockError(c *gin.Context, err error, attrs ...attribute.KeyValue) {
	ctx := c.Request.Context()

	switch {
	case errors.Is(err, management.ErrProjectNotFound):
		telemetry.ReportErrorByCode(ctx, http.StatusNotFound, "apply project block failed", err, attrs...)
		a.sendAPIStoreError(c, http.StatusNotFound, "Project not found")
	case errors.Is(err, management.ErrInvalidProjectBlock):
		telemetry.ReportErrorByCode(ctx, http.StatusBadRequest, "apply project block failed", err, attrs...)
		a.sendAPIStoreError(c, http.StatusBadRequest, "Invalid project block state")
	default:
		telemetry.ReportCriticalError(ctx, "apply project block failed", err, attrs...)
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error applying project block")
	}
}

func (a *APIStore) sendProjectBanError(c *gin.Context, err error, attrs ...attribute.KeyValue) {
	ctx := c.Request.Context()

	switch {
	case errors.Is(err, management.ErrProjectNotFound):
		telemetry.ReportErrorByCode(ctx, http.StatusNotFound, "apply project ban failed", err, attrs...)
		a.sendAPIStoreError(c, http.StatusNotFound, "Project not found")
	case errors.Is(err, management.ErrInvalidProjectBan):
		telemetry.ReportErrorByCode(ctx, http.StatusBadRequest, "apply project ban failed", err, attrs...)
		a.sendAPIStoreError(c, http.StatusBadRequest, "Invalid project ban state")
	default:
		telemetry.ReportCriticalError(ctx, "apply project ban failed", err, attrs...)
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error applying project ban")
	}
}
