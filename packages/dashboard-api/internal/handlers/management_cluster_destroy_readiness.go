package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/dashboard-api/internal/api"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

func (s *APIStore) ManagementClusterDestroyReadiness(c *gin.Context, clusterID api.ClusterID) {
	ctx := c.Request.Context()
	// The cluster delete refuses a protected cluster, so refuse here too, before
	// the caller detaches teams or destroys infrastructure.
	protected, err := s.db.Dashboard.ClusterDeletionProtected(ctx, clusterID)
	if err != nil {
		logger.L().Error(ctx, "Failed to check cluster deletion protection", zap.Error(err), logger.WithClusterID(clusterID))
		s.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to check cluster destroy readiness")

		return
	}
	if protected {
		s.sendAPIStoreError(c, http.StatusConflict, "Cluster has deletion protection turned on.")

		return
	}

	shared, err := s.db.Dashboard.ClusterUsedByMultipleTeams(ctx, clusterID)
	if err != nil {
		logger.L().Error(ctx, "Failed to check cluster destroy readiness", zap.Error(err), logger.WithClusterID(clusterID))
		s.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to check cluster destroy readiness")

		return
	}
	if shared {
		s.sendAPIStoreError(c, http.StatusConflict, "Cluster is used by more than one team. Move the other teams and their templates off it before destroying the deployment.")

		return
	}

	c.Status(http.StatusNoContent)
}
