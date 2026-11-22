package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	sharedutils "github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

const adminClusterListDefaultLimit = 50

func (a *APIStore) AdminListClusterTeams(c *gin.Context, clusterID api.ClusterID, params api.AdminListClusterTeamsParams) {
	ctx := c.Request.Context()

	if !a.requireClusterService(c) {
		return
	}

	// A cluster without teams answers an empty page, an unknown one 404.
	cluster, err := a.lookUpCluster(ctx, &clusterID)
	if err != nil {
		logger.L().Error(ctx, "Failed to get cluster", zap.Error(err), logger.WithClusterID(clusterID))
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to list cluster teams")

		return
	}
	if cluster == nil {
		a.sendAPIStoreError(c, http.StatusNotFound, "Cluster not found")

		return
	}

	teams, err := a.sqlcDB.ListAdminClusterTeams(ctx, queries.ListAdminClusterTeamsParams{
		ClusterID:   clusterID,
		SortBy:      string(sharedutils.DerefOrDefault(params.SortBy, api.AdminListClusterTeamsParamsSortByCreatedAt)),
		Descending:  sharedutils.DerefOrDefault(params.SortOrder, api.AdminListClusterTeamsSortOrderDesc) == api.AdminListClusterTeamsSortOrderDesc,
		OffsetCount: int64(sharedutils.FromPtr(params.Offset)),
		PageSize:    int64(sharedutils.DerefOrDefault(params.Limit, adminClusterListDefaultLimit)),
	})
	if err != nil {
		logger.L().Error(ctx, "Failed to list cluster teams", zap.Error(err), logger.WithClusterID(clusterID))
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to list cluster teams")

		return
	}

	total, err := a.sqlcDB.CountAdminClusterTeams(ctx, clusterID)
	if err != nil {
		logger.L().Error(ctx, "Failed to count cluster teams", zap.Error(err), logger.WithClusterID(clusterID))
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to list cluster teams")

		return
	}

	c.JSON(http.StatusOK, api.ClusterTeamListResponse{Items: clusterTeamItems(teams), Total: total})
}

func clusterTeamItems(rows []queries.ListAdminClusterTeamsRow) []api.ClusterTeamListItem {
	items := make([]api.ClusterTeamListItem, 0, len(rows))
	for _, row := range rows {
		items = append(items, api.ClusterTeamListItem{
			Id:        row.ID,
			Email:     row.Email,
			Name:      &row.Name,
			Slug:      &row.Slug,
			IsBanned:  row.IsBanned,
			CreatedAt: row.CreatedAt,
		})
	}

	return items
}
