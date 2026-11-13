package handlers

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	clustersgrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/clusters/management"
)

func (a *APIStore) ManagementUpdateClusterTelemetry(c *gin.Context, clusterID api.ClusterID) {
	if !a.requireClusterService(c) {
		return
	}

	var body api.ManagementUpdateClusterTelemetryJSONRequestBody
	if err := c.ShouldBindJSON(&body); err != nil || strings.TrimSpace(body.TelemetryClientId) == "" {
		a.sendAPIStoreError(c, http.StatusBadRequest, "Telemetry client identity is required")

		return
	}
	if clusterID == consts.LocalClusterID {
		a.sendAPIStoreError(c, http.StatusNotFound, "Cluster is absent or being deleted")

		return
	}
	_, err := a.clusterService.UpdateCluster(c.Request.Context(), clustersgrpc.UpdateClusterRequest_builder{
		ClusterId:         clusterID.String(),
		TelemetryClientId: new(strings.TrimSpace(body.TelemetryClientId)),
	}.Build())
	switch {
	case status.Code(err) == codes.AlreadyExists:
		a.sendAPIStoreError(c, http.StatusConflict, "Telemetry identity belongs to another cluster")
	case status.Code(err) == codes.NotFound,
		clusterErrorReason(err) == clustersgrpc.ClusterErrorReason_CLUSTER_ERROR_REASON_DELETION_REQUESTED:
		a.sendAPIStoreError(c, http.StatusNotFound, "Cluster is absent or being deleted")
	case clusterErrorReason(err) == clustersgrpc.ClusterErrorReason_CLUSTER_ERROR_REASON_TELEMETRY_CLIENT_ASSIGNED:
		a.sendAPIStoreError(c, http.StatusConflict, "Cluster already has a different telemetry identity")
	case err != nil:
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to update telemetry identity")
	default:
		c.Status(http.StatusNoContent)
	}
}
