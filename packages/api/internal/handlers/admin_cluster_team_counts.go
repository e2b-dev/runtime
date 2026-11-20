package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

// AdminListClusterTeamCounts reads only the teams, so it answers without a
// cluster service.
func (a *APIStore) AdminListClusterTeamCounts(c *gin.Context) {
	ctx := c.Request.Context()

	// The operator surface reads the primary throughout, so a team just
	// assigned counts at once.
	counts, err := a.sqlcDB.CountLiveTeamsByCluster(ctx)
	if err != nil {
		logger.L().Error(ctx, "Failed to count cluster teams", zap.Error(err))
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to count cluster teams")

		return
	}

	items := make([]api.ClusterTeamCount, 0, len(counts))
	for _, row := range counts {
		items = append(items, api.ClusterTeamCount{ClusterId: row.ClusterID, TeamCount: row.TeamCount})
	}

	c.JSON(http.StatusOK, api.ClusterTeamCountList{Items: items})
}
