package handlers

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	clustersgrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/clusters/management"
)

// The metadata route serves the cluster record alone: the store has no
// database, so a read of teams or templates would fail the request.
func TestAdminClusterMetadataReadsOnlyTheClusterRecord(t *testing.T) {
	t.Parallel()

	clusterID, missing := uuid.New(), uuid.New()
	domain := "sandbox.example.test"
	store, clusterService := newClusterServiceStore(t)
	clusterService.EXPECT().GetCluster(mock.Anything, mock.MatchedBy(func(req *clustersgrpc.GetClusterRequest) bool {
		return req.GetClusterId() == clusterID.String()
	})).Return(clustersgrpc.GetClusterResponse_builder{Cluster: clustersgrpc.Cluster_builder{
		ClusterId:          clusterID.String(),
		Name:               "metadata",
		Endpoint:           "metadata.example.test",
		EndpointTls:        true,
		Token:              "private-token",
		SandboxProxyDomain: &domain,
	}.Build()}.Build(), nil).Once()
	clusterService.EXPECT().GetCluster(mock.Anything, mock.MatchedBy(func(req *clustersgrpc.GetClusterRequest) bool {
		return req.GetClusterId() == missing.String()
	})).Return(nil, status.Error(codes.NotFound, "cluster not found")).Once()

	response := callAdmin(t, store, http.MethodGet, "/unstable/admin/clusters/"+clusterID.String()+"/metadata", "")
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	var detail api.ClusterDetail
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &detail))
	require.Equal(t, api.ClusterDetail{Id: clusterID, Name: "metadata", Endpoint: "metadata.example.test", EndpointTls: true, SandboxProxyDomain: &domain}, detail)
	require.NotContains(t, response.Body.String(), "private-token")

	absent := callAdmin(t, store, http.MethodGet, "/unstable/admin/clusters/"+missing.String()+"/metadata", "")
	require.Equal(t, http.StatusNotFound, absent.Code, absent.Body.String())
}
