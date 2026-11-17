package handlers

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/apierrors"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	"github.com/e2b-dev/infra/packages/shared/pkg/ginutils"
	clustersgrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/clusters/management"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	sharedutils "github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

const adminClusterListDefaultLimit = 50

// AdminListClusters filters, sorts and pages the clusters in memory: the
// cluster service lists every cluster, and the team counts come from the
// teams table.
func (a *APIStore) AdminListClusters(c *gin.Context, params api.AdminListClustersParams) {
	ctx := c.Request.Context()

	if !a.requireClusterService(c) {
		return
	}

	resp, err := a.clusterService.ListClusters(ctx, clustersgrpc.ListClustersRequest_builder{}.Build())
	if err != nil {
		logger.L().Error(ctx, "Failed to list clusters", zap.Error(err))
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to list clusters")

		return
	}

	// The operator surface reads the primary throughout, so a team just
	// assigned counts at once.
	counts, err := a.sqlcDB.CountLiveTeamsByCluster(ctx)
	if err != nil {
		logger.L().Error(ctx, "Failed to count cluster teams", zap.Error(err))
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to count clusters")

		return
	}
	teamCounts := make(map[uuid.UUID]int64, len(counts))
	for _, row := range counts {
		teamCounts[row.ClusterID] = row.TeamCount
	}

	search := strings.ToLower(strings.TrimSpace(sharedutils.FromPtr(params.Search)))
	items := make([]api.ClusterListItem, 0, len(resp.GetClusters()))
	for _, cluster := range resp.GetClusters() {
		if search != "" &&
			!strings.Contains(strings.ToLower(cluster.GetName()), search) &&
			!strings.Contains(strings.ToLower(cluster.GetEndpoint()), search) {
			continue
		}
		id, err := uuid.Parse(cluster.GetClusterId())
		if err != nil {
			logger.L().Error(ctx, "Failed to parse cluster ID", zap.Error(err), zap.String("cluster_id", cluster.GetClusterId()))
			a.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to list clusters")

			return
		}
		items = append(items, api.ClusterListItem{
			Id:                 id,
			Name:               cluster.GetName(),
			Endpoint:           cluster.GetEndpoint(),
			SandboxProxyDomain: optionalOf(cluster.HasSandboxProxyDomain(), cluster.GetSandboxProxyDomain()),
			TeamCount:          teamCounts[id],
		})
	}

	sortBy := sharedutils.DerefOrDefault(params.SortBy, api.AdminListClustersParamsSortByEndpoint)
	descending := sharedutils.DerefOrDefault(params.SortOrder, api.AdminListClustersSortOrderAsc) == api.AdminListClustersSortOrderDesc
	slices.SortStableFunc(items, func(x, y api.ClusterListItem) int {
		var order int
		switch sortBy {
		case api.AdminListClustersParamsSortByName:
			order = compareText(x.Name, y.Name)
		case api.AdminListClustersParamsSortByTeamCount:
			order = cmp.Compare(x.TeamCount, y.TeamCount)
		default:
			order = compareText(x.Endpoint, y.Endpoint)
		}
		if descending {
			order = -order
		}
		// Team counts collide often, so that sort breaks ties on the endpoint.
		if order == 0 && sortBy == api.AdminListClustersParamsSortByTeamCount {
			order = compareText(x.Endpoint, y.Endpoint)
		}
		if order == 0 {
			order = strings.Compare(x.Id.String(), y.Id.String())
		}

		return order
	})

	total := int64(len(items))
	offset := min(int(sharedutils.FromPtr(params.Offset)), len(items))
	end := min(offset+int(sharedutils.DerefOrDefault(params.Limit, adminClusterListDefaultLimit)), len(items))

	c.JSON(http.StatusOK, api.ClusterListResponse{Items: items[offset:end], Total: total})
}

// compareText orders case-insensitively, then by the exact text so the order
// is total.
func compareText(x, y string) int {
	if order := strings.Compare(strings.ToLower(x), strings.ToLower(y)); order != 0 {
		return order
	}

	return strings.Compare(x, y)
}

// AdminCreateCluster creates a protected cluster. A replay with the same
// cluster_id and configuration answers with the stored cluster.
func (a *APIStore) AdminCreateCluster(c *gin.Context) {
	ctx := c.Request.Context()

	if !a.requireClusterService(c) {
		return
	}

	body, err := ginutils.ParseBody[api.AdminClusterCreateRequest](ctx, c)
	if err != nil {
		apierrors.SendAPIError(c, &apierrors.APIError{Code: http.StatusBadRequest, ErrorCode: clusterRegistrationInvalidCode, ClientMsg: fmt.Sprintf("Error when parsing request: %s", err)})

		return
	}

	cluster, err := a.createCluster(ctx, clusterRegistration{
		ClusterID:          body.ClusterId,
		Name:               body.Name,
		Endpoint:           body.Endpoint,
		EndpointTLS:        body.EndpointTls,
		Token:              body.Token,
		SandboxProxyDomain: body.SandboxProxyDomain,
		AuthOrgID:          body.AuthOrgId,
		TelemetryClientID:  body.TelemetryClientId,
		DeletionProtection: true,
	})
	switch {
	case errors.Is(err, errInvalidClusterRegistration):
		apierrors.SendAPIError(c, &apierrors.APIError{Code: http.StatusBadRequest, ErrorCode: clusterRegistrationInvalidCode, ClientMsg: "name, endpoint and token are required"})

		return
	case errors.Is(err, errClusterRegistrationConflict):
		apierrors.SendAPIError(c, &apierrors.APIError{Code: http.StatusConflict, ErrorCode: clusterRegistrationConflictCode, ClientMsg: "Cluster ID or auth organization is already registered"})

		return
	case err != nil:
		logger.L().Error(ctx, "Failed to create cluster", zap.Error(err))
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to create cluster")

		return
	}

	detail, err := clusterDetail(cluster)
	if err != nil {
		logger.L().Error(ctx, "Failed to read created cluster", zap.Error(err))
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to read created cluster")

		return
	}

	c.JSON(http.StatusCreated, detail)
}

func (a *APIStore) AdminGetClusterMetadata(c *gin.Context, clusterID api.ClusterID) {
	detail, ok := a.adminClusterDetail(c, clusterID, "Failed to get cluster")
	if !ok {
		return
	}

	c.JSON(http.StatusOK, detail)
}

func (a *APIStore) AdminGetCluster(c *gin.Context, clusterID api.ClusterID, params api.AdminGetClusterParams) {
	ctx := c.Request.Context()

	detail, ok := a.adminClusterDetail(c, clusterID, "Failed to get cluster")
	if !ok {
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
		Cluster:       detail,
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

	if !a.requireClusterService(c) {
		return
	}

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

	update := clustersgrpc.UpdateClusterRequest_builder{ClusterId: clusterID.String()}
	if isPresent("name") {
		name := strings.TrimSpace(sharedutils.FromPtr(body.Name))
		if name == "" {
			a.sendAPIStoreError(c, http.StatusBadRequest, "name must not be blank")

			return
		}
		update.Name = &name
	}
	if isPresent("endpoint") {
		endpoint := strings.TrimSpace(sharedutils.FromPtr(body.Endpoint))
		if endpoint == "" {
			a.sendAPIStoreError(c, http.StatusBadRequest, "endpoint must not be blank")

			return
		}
		update.Endpoint = &endpoint
	}
	if isPresent("endpointTls") {
		if body.EndpointTls == nil {
			a.sendAPIStoreError(c, http.StatusBadRequest, "endpointTls must not be null")

			return
		}
		update.EndpointTls = body.EndpointTls
	}
	if isPresent("sandboxProxyDomain") {
		update.SandboxProxyDomain = clustersgrpc.NullableStringUpdate_builder{Value: trimmedOptional(body.SandboxProxyDomain)}.Build()
	}
	if isPresent("authOrgId") {
		update.AuthOrgId = clustersgrpc.NullableStringUpdate_builder{Value: trimmedOptional(body.AuthOrgId)}.Build()
	}
	if isPresent("deletionProtection") {
		if body.DeletionProtection == nil {
			a.sendAPIStoreError(c, http.StatusBadRequest, "deletionProtection must not be null")

			return
		}
		update.DeletionProtection = body.DeletionProtection
	}
	if update.Name == nil && update.Endpoint == nil && update.EndpointTls == nil &&
		update.SandboxProxyDomain == nil && update.AuthOrgId == nil && update.DeletionProtection == nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, "No updatable field supplied")

		return
	}

	if clusterID == consts.LocalClusterID {
		a.sendAPIStoreError(c, http.StatusNotFound, "Cluster not found")

		return
	}

	resp, err := a.clusterService.UpdateCluster(ctx, update.Build())
	switch {
	case status.Code(err) == codes.NotFound:
		a.sendAPIStoreError(c, http.StatusNotFound, "Cluster not found")

		return
	case status.Code(err) == codes.AlreadyExists:
		a.sendAPIStoreError(c, http.StatusConflict, "A cluster with that auth org ID already exists")

		return
	case clusterErrorReason(err) == clustersgrpc.ClusterErrorReason_CLUSTER_ERROR_REASON_DELETION_REQUESTED:
		a.sendAPIStoreError(c, http.StatusConflict, "This cluster is being deleted, so its deletion protection cannot be turned on")

		return
	case err != nil:
		logger.L().Error(ctx, "Failed to update cluster", zap.Error(err), logger.WithClusterID(clusterID))
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to update cluster")

		return
	}

	detail, err := clusterDetail(resp.GetCluster())
	if err != nil {
		logger.L().Error(ctx, "Failed to read updated cluster", zap.Error(err), logger.WithClusterID(clusterID))
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to update cluster")

		return
	}

	c.JSON(http.StatusOK, detail)
}

// AdminDeleteCluster is the management delete; both callers get the same
// refusals.
func (a *APIStore) AdminDeleteCluster(c *gin.Context, clusterID api.ClusterID) {
	a.ManagementDeleteCluster(c, clusterID)
}

func (a *APIStore) AdminListClusterTeams(c *gin.Context, clusterID api.ClusterID, params api.AdminListClusterTeamsParams) {
	ctx := c.Request.Context()

	// A cluster without teams answers an empty page, an unknown one 404.
	if _, ok := a.adminClusterDetail(c, clusterID, "Failed to list cluster teams"); !ok {
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

	if !a.requireClusterService(c) {
		return
	}

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

	if clusterID == consts.LocalClusterID {
		a.sendAPIStoreError(c, http.StatusNotFound, "Cluster not found")

		return
	}

	_, err = a.clusterService.UpdateCluster(ctx, clustersgrpc.UpdateClusterRequest_builder{ClusterId: clusterID.String(), Token: &token}.Build())
	switch {
	case status.Code(err) == codes.NotFound:
		a.sendAPIStoreError(c, http.StatusNotFound, "Cluster not found")
	case err != nil:
		logger.L().Error(ctx, "Failed to rotate cluster token", zap.Error(err), logger.WithClusterID(clusterID))
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to rotate cluster token")
	default:
		c.Status(http.StatusNoContent)
	}
}

// adminClusterDetail reads a cluster for an admin route, answering 503
// without a cluster service, 404 for an unknown cluster and failMsg for a
// failed read.
func (a *APIStore) adminClusterDetail(c *gin.Context, clusterID api.ClusterID, failMsg string) (api.ClusterDetail, bool) {
	ctx := c.Request.Context()

	if !a.requireClusterService(c) {
		return api.ClusterDetail{}, false
	}

	cluster, err := a.lookUpCluster(ctx, &clusterID)
	if err != nil {
		logger.L().Error(ctx, "Failed to get cluster", zap.Error(err), logger.WithClusterID(clusterID))
		a.sendAPIStoreError(c, http.StatusInternalServerError, failMsg)

		return api.ClusterDetail{}, false
	}
	if cluster == nil {
		a.sendAPIStoreError(c, http.StatusNotFound, "Cluster not found")

		return api.ClusterDetail{}, false
	}

	detail, err := clusterDetail(cluster)
	if err != nil {
		logger.L().Error(ctx, "Failed to read cluster", zap.Error(err), logger.WithClusterID(clusterID))
		a.sendAPIStoreError(c, http.StatusInternalServerError, failMsg)

		return api.ClusterDetail{}, false
	}

	return detail, true
}

// clusterDetail leaves the token out on purpose: no admin read returns it.
func clusterDetail(cluster *clustersgrpc.Cluster) (api.ClusterDetail, error) {
	id, err := uuid.Parse(cluster.GetClusterId())
	if err != nil {
		return api.ClusterDetail{}, fmt.Errorf("parse cluster ID %q: %w", cluster.GetClusterId(), err)
	}

	return api.ClusterDetail{
		Id:                 id,
		Name:               cluster.GetName(),
		Endpoint:           cluster.GetEndpoint(),
		EndpointTls:        cluster.GetEndpointTls(),
		SandboxProxyDomain: optionalOf(cluster.HasSandboxProxyDomain(), cluster.GetSandboxProxyDomain()),
		AuthOrgId:          optionalOf(cluster.HasAuthOrgId(), cluster.GetAuthOrgId()),
		DeletionProtection: cluster.GetDeletionProtection(),
	}, nil
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
