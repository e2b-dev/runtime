package handlers

import (
	"errors"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	clustersgrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/clusters/management"
)

func callManagementUpdateClusterTelemetry(t *testing.T, store *APIStore, clusterID uuid.UUID, body string) int {
	t.Helper()

	return callManagement(t, store, http.MethodPut, "/unstable/management/clusters/"+clusterID.String()+"/telemetry", body).Code
}

// The update carries the telemetry identity alone, so the cluster's other
// settings stay as stored.
func TestManagementUpdateClusterTelemetrySendsOnlyTheIdentity(t *testing.T) {
	t.Parallel()

	clusterID := uuid.New()
	want := clustersgrpc.UpdateClusterRequest_builder{ClusterId: clusterID.String(), TelemetryClientId: new("client_saved")}.Build()
	store, clusterService := newClusterServiceStore(t)
	clusterService.EXPECT().UpdateCluster(mock.Anything, mock.MatchedBy(func(req *clustersgrpc.UpdateClusterRequest) bool {
		return proto.Equal(want, req)
	})).Return(clustersgrpc.UpdateClusterResponse_builder{Cluster: clustersgrpc.Cluster_builder{ClusterId: clusterID.String()}.Build()}.Build(), nil).Once()

	require.Equal(t, http.StatusNoContent, callManagementUpdateClusterTelemetry(t, store, clusterID, `{"telemetry_client_id":" client_saved "}`))
}

func TestManagementUpdateClusterTelemetryReportsARefusal(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		answer error
		want   int
	}{
		"another cluster holds the identity": {status.Error(codes.AlreadyExists, "a cluster with this telemetry client exists"), http.StatusConflict},
		"the cluster has another identity": {
			clusterRefusal(clustersgrpc.ClusterErrorReason_CLUSTER_ERROR_REASON_TELEMETRY_CLIENT_ASSIGNED), http.StatusConflict,
		},
		"an absent cluster": {status.Error(codes.NotFound, "cluster not found"), http.StatusNotFound},
		"a cluster being deleted": {
			clusterRefusal(clustersgrpc.ClusterErrorReason_CLUSTER_ERROR_REASON_DELETION_REQUESTED), http.StatusNotFound,
		},
		"an unavailable cluster service": {errors.New("connection refused"), http.StatusInternalServerError},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			store, clusterService := newClusterServiceStore(t)
			clusterService.EXPECT().UpdateCluster(mock.Anything, mock.Anything).Return(nil, tc.answer).Once()

			require.Equal(t, tc.want, callManagementUpdateClusterTelemetry(t, store, uuid.New(), `{"telemetry_client_id":"client_saved"}`))
		})
	}
}

func TestManagementUpdateClusterTelemetryRequiresAnIdentity(t *testing.T) {
	t.Parallel()

	// The cluster service is never asked.
	store, _ := newClusterServiceStore(t)

	require.Equal(t, http.StatusBadRequest, callManagementUpdateClusterTelemetry(t, store, uuid.New(), `{"telemetry_client_id":" "}`))
}
