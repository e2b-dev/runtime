package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	clustersgrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/clusters/management"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/mtls"
)

func (a *APIStore) ManagementTelemetryNames(c *gin.Context) {
	ctx := c.Request.Context()

	peer, ok := mtls.PeerFromContext(ctx)
	if !ok || !peer.TLS || !peer.ChainVerified || a.config.TelemetryReaderID == "" || peer.ID != a.config.TelemetryReaderID {
		a.sendAPIStoreError(c, http.StatusUnauthorized, "A verified telemetry reader identity is required")

		return
	}

	teams, err := a.sqlcReadDB.ListTelemetryTeamNames(ctx)
	if err != nil {
		logger.L().Error(ctx, "Failed to read telemetry team names", zap.Error(err))
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to read telemetry names")

		return
	}
	names := api.TelemetryNames{Teams: make(map[string]api.TelemetryTeamName, len(teams)), Clusters: []api.TelemetryClusterName{}}
	for _, row := range teams {
		names.Teams[row.ID.String()] = api.TelemetryTeamName{Name: row.Name, ClusterId: new(row.ClusterID)}
	}

	// Without a cluster service there is no remote cluster to name.
	if a.clusterService != nil {
		resp, err := a.clusterService.ListClusters(ctx, clustersgrpc.ListClustersRequest_builder{}.Build())
		if err != nil {
			logger.L().Error(ctx, "Failed to read telemetry cluster names", zap.Error(err))
			a.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to read telemetry names")

			return
		}
		for _, cluster := range resp.GetClusters() {
			if cluster.HasDeletionRequestedAt() {
				continue
			}
			id, err := uuid.Parse(cluster.GetClusterId())
			if err != nil {
				logger.L().Error(ctx, "Failed to parse cluster ID", zap.Error(err), zap.String("cluster_id", cluster.GetClusterId()))
				a.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to read telemetry names")

				return
			}
			names.Clusters = append(names.Clusters, api.TelemetryClusterName{
				Id:       id,
				Name:     cluster.GetName(),
				ClientId: optionalOf(cluster.HasTelemetryClientId(), cluster.GetTelemetryClientId()),
			})
		}
	}

	c.JSON(http.StatusOK, names)
}
