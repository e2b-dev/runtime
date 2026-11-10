package handlers

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/db/pkg/dberrors"
	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/apierrors"
	"github.com/e2b-dev/infra/packages/shared/pkg/ginutils"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	sharedutils "github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

const (
	adminClusterListDefaultLimit = 50

	// clusterDeletionRequestedConstraint refuses turning deletion protection
	// on once the cluster's deletion is recorded.
	clusterDeletionRequestedConstraint = "clusters_deletion_requested_unprotected"
)

func (a *APIStore) AdminListClusters(c *gin.Context, params api.AdminListClustersParams) {
	ctx := c.Request.Context()

	search := strings.TrimSpace(sharedutils.FromPtr(params.Search))
	filter := queries.CountAdminClustersParams{
		BySearch: search != "",
		Search:   likePatternEscaper.Replace(search),
	}

	// The operator surface reads the primary throughout, so a cluster just
	// created or updated reads back as written.
	rows, err := a.sqlcDB.ListAdminClusters(ctx, queries.ListAdminClustersParams{
		BySearch:    filter.BySearch,
		Search:      filter.Search,
		SortBy:      string(sharedutils.DerefOrDefault(params.SortBy, api.AdminListClustersParamsSortByEndpoint)),
		Descending:  sharedutils.DerefOrDefault(params.SortOrder, api.AdminListClustersSortOrderAsc) == api.AdminListClustersSortOrderDesc,
		OffsetCount: int64(sharedutils.FromPtr(params.Offset)),
		PageSize:    int64(sharedutils.DerefOrDefault(params.Limit, adminClusterListDefaultLimit)),
	})
	if err != nil {
		logger.L().Error(ctx, "Failed to list clusters", zap.Error(err))
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to list clusters")

		return
	}

	total, err := a.sqlcDB.CountAdminClusters(ctx, filter)
	if err != nil {
		logger.L().Error(ctx, "Failed to count clusters", zap.Error(err))
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to count clusters")

		return
	}

	items := make([]api.ClusterListItem, 0, len(rows))
	for _, row := range rows {
		items = append(items, api.ClusterListItem{
			Id:                 row.ID,
			Name:               row.Name,
			Endpoint:           row.Endpoint,
			SandboxProxyDomain: row.SandboxProxyDomain,
			TeamCount:          row.TeamCount,
		})
	}

	c.JSON(http.StatusOK, api.ClusterListResponse{Items: items, Total: total})
}

// AdminCreateCluster creates a protected cluster. A replay with the same
// cluster_id and configuration answers with the stored cluster.
func (a *APIStore) AdminCreateCluster(c *gin.Context) {
	ctx := c.Request.Context()

	body, err := ginutils.ParseBody[api.AdminClusterCreateRequest](ctx, c)
	if err != nil {
		apierrors.SendAPIError(c, &apierrors.APIError{Code: http.StatusBadRequest, ErrorCode: clusterRegistrationInvalidCode, ClientMsg: fmt.Sprintf("Error when parsing request: %s", err)})

		return
	}

	clusterID, err := a.createCluster(ctx, clusterRegistration{
		ClusterID:          body.ClusterId,
		Name:               body.Name,
		Endpoint:           body.Endpoint,
		EndpointTLS:        body.EndpointTls,
		Token:              body.Token,
		SandboxProxyDomain: body.SandboxProxyDomain,
		AuthOrgID:          body.AuthOrgId,
		DeletionProtection: true,
	})
	switch {
	case errors.Is(err, errInvalidClusterRegistration):
		apierrors.SendAPIError(c, &apierrors.APIError{Code: http.StatusBadRequest, ErrorCode: clusterRegistrationInvalidCode, ClientMsg: "name, endpoint and token are required"})

		return
	case dberrors.IsUniqueConstraintViolation(err), dberrors.IsNotFoundError(err):
		apierrors.SendAPIError(c, &apierrors.APIError{Code: http.StatusConflict, ErrorCode: clusterRegistrationConflictCode, ClientMsg: "Cluster ID or auth organization is already registered"})

		return
	case err != nil:
		logger.L().Error(ctx, "Failed to create cluster", zap.Error(err))
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to create cluster")

		return
	}

	cluster, err := a.sqlcDB.GetAdminCluster(ctx, clusterID)
	if err != nil {
		logger.L().Error(ctx, "Failed to read created cluster", zap.Error(err), logger.WithClusterID(clusterID))
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to read created cluster")

		return
	}

	c.JSON(http.StatusCreated, clusterDetail(cluster))
}

func (a *APIStore) AdminGetCluster(c *gin.Context, clusterID api.ClusterID, params api.AdminGetClusterParams) {
	ctx := c.Request.Context()

	cluster, err := a.sqlcDB.GetAdminCluster(ctx, clusterID)
	if dberrors.IsNotFoundError(err) {
		a.sendAPIStoreError(c, http.StatusNotFound, "Cluster not found")

		return
	}
	if err != nil {
		logger.L().Error(ctx, "Failed to get cluster", zap.Error(err), logger.WithClusterID(clusterID))
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to get cluster")

		return
	}

	teams, err := a.sqlcDB.ListAdminClusterTeams(ctx, queries.ListAdminClusterTeamsParams{
		ClusterID:   clusterID,
		SortBy:      string(api.AdminListClusterTeamsParamsSortByCreatedAt),
		Descending:  true,
		OffsetCount: int64(sharedutils.FromPtr(params.TeamsOffset)),
		PageSize:    int64(sharedutils.DerefOrDefault(params.TeamsLimit, adminClusterListDefaultLimit)),
	})
	if err != nil {
		logger.L().Error(ctx, "Failed to list cluster teams", zap.Error(err), logger.WithClusterID(clusterID))
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to get cluster")

		return
	}

	teamCount, err := a.sqlcDB.CountAdminClusterTeams(ctx, clusterID)
	if err != nil {
		logger.L().Error(ctx, "Failed to count cluster teams", zap.Error(err), logger.WithClusterID(clusterID))
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to get cluster")

		return
	}

	templateCount, err := a.sqlcDB.CountAdminClusterTemplates(ctx, clusterID)
	if err != nil {
		logger.L().Error(ctx, "Failed to count cluster templates", zap.Error(err), logger.WithClusterID(clusterID))
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to get cluster")

		return
	}

	c.JSON(http.StatusOK, api.ClusterDetailWithTeams{
		Cluster:       clusterDetail(cluster),
		Teams:         api.ClusterTeamListResponse{Items: clusterTeamItems(teams), Total: teamCount},
		TeamCount:     teamCount,
		TemplateCount: templateCount,
	})
}

// AdminUpdateCluster writes only the properties present in the body. The
// generated request type reads an absent property and an explicit null alike,
// and an explicit null clears sandboxProxyDomain or authOrgId while an absent
// one leaves it alone, so presence is read off the raw body.
func (a *APIStore) AdminUpdateCluster(c *gin.Context, clusterID api.ClusterID) {
	ctx := c.Request.Context()

	raw, err := c.GetRawData()
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("Error when reading request: %s", err))

		return
	}
	var body api.ClusterUpdateRequest
	if err := json.Unmarshal(raw, &body); err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("Error when parsing request: %s", err))

		return
	}
	var present map[string]json.RawMessage
	if err := json.Unmarshal(raw, &present); err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("Error when parsing request: %s", err))

		return
	}
	isPresent := func(key string) bool {
		_, ok := present[key]

		return ok
	}

	update := queries.UpdateAdminClusterParams{
		ClusterID:             clusterID,
		SetName:               isPresent("name"),
		Name:                  strings.TrimSpace(sharedutils.FromPtr(body.Name)),
		SetEndpoint:           isPresent("endpoint"),
		Endpoint:              strings.TrimSpace(sharedutils.FromPtr(body.Endpoint)),
		SetEndpointTls:        isPresent("endpointTls"),
		EndpointTls:           sharedutils.FromPtr(body.EndpointTls),
		SetSandboxProxyDomain: isPresent("sandboxProxyDomain"),
		SandboxProxyDomain:    trimmedOptional(body.SandboxProxyDomain),
		SetAuthOrgID:          isPresent("authOrgId"),
		AuthOrgID:             trimmedOptional(body.AuthOrgId),
		SetDeletionProtection: isPresent("deletionProtection"),
		DeletionProtection:    sharedutils.FromPtr(body.DeletionProtection),
	}
	if update.SetName && update.Name == "" {
		a.sendAPIStoreError(c, http.StatusBadRequest, "name must not be blank")

		return
	}
	if update.SetEndpoint && update.Endpoint == "" {
		a.sendAPIStoreError(c, http.StatusBadRequest, "endpoint must not be blank")

		return
	}
	if update.SetEndpointTls && body.EndpointTls == nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, "endpointTls must not be null")

		return
	}
	if update.SetDeletionProtection && body.DeletionProtection == nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, "deletionProtection must not be null")

		return
	}
	if !update.SetName && !update.SetEndpoint && !update.SetEndpointTls &&
		!update.SetSandboxProxyDomain && !update.SetAuthOrgID && !update.SetDeletionProtection {
		a.sendAPIStoreError(c, http.StatusBadRequest, "No updatable field supplied")

		return
	}

	cluster, err := a.sqlcDB.UpdateAdminCluster(ctx, update)
	switch {
	case dberrors.IsNotFoundError(err):
		a.sendAPIStoreError(c, http.StatusNotFound, "Cluster not found")

		return
	case dberrors.IsUniqueConstraintViolation(err):
		a.sendAPIStoreError(c, http.StatusConflict, "A cluster with that auth org ID already exists")

		return
	case dberrors.ConstraintName(err) == clusterDeletionRequestedConstraint:
		a.sendAPIStoreError(c, http.StatusConflict, "This cluster is being deleted, so its deletion protection cannot be turned on")

		return
	case err != nil:
		logger.L().Error(ctx, "Failed to update cluster", zap.Error(err), logger.WithClusterID(clusterID))
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to update cluster")

		return
	}

	c.JSON(http.StatusOK, clusterDetail(queries.GetAdminClusterRow(cluster)))
}

// AdminDeleteCluster is the management delete; both callers get the same
// refusals.
func (a *APIStore) AdminDeleteCluster(c *gin.Context, clusterID api.ClusterID) {
	a.ManagementDeleteCluster(c, clusterID)
}

func (a *APIStore) AdminListClusterTeams(c *gin.Context, clusterID api.ClusterID, params api.AdminListClusterTeamsParams) {
	ctx := c.Request.Context()

	// A cluster without teams answers an empty page, an unknown one 404.
	if _, err := a.sqlcDB.GetAdminCluster(ctx, clusterID); err != nil {
		if dberrors.IsNotFoundError(err) {
			a.sendAPIStoreError(c, http.StatusNotFound, "Cluster not found")

			return
		}
		logger.L().Error(ctx, "Failed to get cluster", zap.Error(err), logger.WithClusterID(clusterID))
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to list cluster teams")

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

func (a *APIStore) AdminRotateClusterToken(c *gin.Context, clusterID api.ClusterID) {
	ctx := c.Request.Context()

	body, err := ginutils.ParseBody[api.ClusterTokenRotateRequest](ctx, c)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("Error when parsing request: %s", err))

		return
	}
	token := strings.TrimSpace(body.Token)
	if token == "" {
		a.sendAPIStoreError(c, http.StatusBadRequest, "token must not be blank")

		return
	}

	rows, err := a.sqlcDB.UpdateAdminClusterToken(ctx, queries.UpdateAdminClusterTokenParams{ClusterID: clusterID, Token: token})
	if err != nil {
		logger.L().Error(ctx, "Failed to rotate cluster token", zap.Error(err), logger.WithClusterID(clusterID))
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to rotate cluster token")

		return
	}
	if rows == 0 {
		a.sendAPIStoreError(c, http.StatusNotFound, "Cluster not found")

		return
	}

	c.Status(http.StatusNoContent)
}

func clusterDetail(row queries.GetAdminClusterRow) api.ClusterDetail {
	return api.ClusterDetail{
		Id:                 row.ID,
		Name:               row.Name,
		Endpoint:           row.Endpoint,
		EndpointTls:        row.EndpointTls,
		SandboxProxyDomain: row.SandboxProxyDomain,
		AuthOrgId:          row.AuthOrgID,
		DeletionProtection: row.DeletionProtection,
	}
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
