package handlers

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/db/pkg/dberrors"
	"github.com/e2b-dev/infra/packages/db/queries"
)

func (a *APIStore) ManagementUpdateClusterTelemetry(c *gin.Context, clusterID api.ClusterID) {
	var body api.ManagementUpdateClusterTelemetryJSONRequestBody
	if err := c.ShouldBindJSON(&body); err != nil || strings.TrimSpace(body.TelemetryClientId) == "" {
		a.sendAPIStoreError(c, http.StatusBadRequest, "Telemetry client identity is required")

		return
	}
	result, err := a.sqlcDB.UpdateClusterTelemetryClient(c.Request.Context(), queries.UpdateClusterTelemetryClientParams{ClusterID: clusterID, TelemetryClientID: strings.TrimSpace(body.TelemetryClientId)})
	switch {
	case dberrors.IsUniqueConstraintViolation(err):
		a.sendAPIStoreError(c, http.StatusConflict, "Telemetry identity belongs to another cluster")
	case err != nil:
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to update telemetry identity")
	case !result.Found:
		a.sendAPIStoreError(c, http.StatusNotFound, "Cluster is absent or being deleted")
	case !result.Updated:
		a.sendAPIStoreError(c, http.StatusConflict, "Cluster already has a different telemetry identity")
	default:
		c.Status(http.StatusNoContent)
	}
}
