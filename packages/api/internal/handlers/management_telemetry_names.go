package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

func (a *APIStore) ManagementTelemetryNames(c *gin.Context) {
	rows, err := a.sqlcReadDB.ListTelemetryNames(c.Request.Context())
	if err != nil {
		logger.L().Error(c.Request.Context(), "Failed to read telemetry names", zap.Error(err))
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to read telemetry names")

		return
	}
	names := api.TelemetryNames{Teams: map[string]api.TelemetryTeamName{}, Clusters: []api.TelemetryClusterName{}}
	for _, row := range rows {
		if row.Kind == "team" {
			team := api.TelemetryTeamName{Name: row.Name}
			if row.ClusterID != "" {
				team.ClusterId = new(uuid.MustParse(row.ClusterID))
			}
			names.Teams[row.ID] = team
		} else {
			cluster := api.TelemetryClusterName{Id: uuid.MustParse(row.ID), Name: row.Name}
			if row.TelemetryClientID != "" {
				cluster.ClientId = &row.TelemetryClientID
			}
			names.Clusters = append(names.Clusters, cluster)
		}
	}
	c.JSON(http.StatusOK, names)
}
