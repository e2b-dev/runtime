package handlers

import (
	"context"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	sqlcdb "github.com/e2b-dev/infra/packages/api/internal/db/client"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	clustersgrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/clusters/management"
)

// requireClusterService answers 503 and reports false when no cluster
// service is configured.
func (a *APIStore) requireClusterService(c *gin.Context) bool {
	if a.clusterService != nil {
		return true
	}

	a.sendAPIStoreError(c, http.StatusServiceUnavailable, "Cluster management is not available")

	return false
}

// clusterErrorReason reads the reason a FAILED_PRECONDITION status carries,
// and UNSPECIFIED for any other error.
func clusterErrorReason(err error) clustersgrpc.ClusterErrorReason {
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.FailedPrecondition {
		return clustersgrpc.ClusterErrorReason_CLUSTER_ERROR_REASON_UNSPECIFIED
	}
	for _, detail := range st.Details() {
		if detail, ok := detail.(*clustersgrpc.ClusterErrorDetail); ok {
			return detail.GetReason()
		}
	}

	return clustersgrpc.ClusterErrorReason_CLUSTER_ERROR_REASON_UNSPECIFIED
}

// lookUpCluster reads a team's cluster. It returns nil for the local
// cluster, an unknown cluster, or no configured cluster service.
func (a *APIStore) lookUpCluster(ctx context.Context, clusterID *uuid.UUID) (*clustersgrpc.Cluster, error) {
	// The local cluster has no record, and the cluster service refuses its nil
	// ID outright.
	if clusterID == nil || *clusterID == consts.LocalClusterID || a.clusterService == nil {
		return nil, nil
	}

	resp, err := a.clusterService.GetCluster(ctx, clustersgrpc.GetClusterRequest_builder{ClusterId: clusterID.String()}.Build())
	if status.Code(err) == codes.NotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return resp.GetCluster(), nil
}

// refuseLocalOnlyWithRemoteClusters refuses running without a cluster service
// while a live team is assigned to a remote cluster: the pool would hold the
// local cluster alone, and that team's sandboxes would fail with nothing else
// amiss.
func refuseLocalOnlyWithRemoteClusters(ctx context.Context, db *sqlcdb.Client) error {
	assigned, err := db.CountLiveTeamsByCluster(ctx)
	if err != nil {
		return fmt.Errorf("list the remote clusters teams are assigned to: %w", err)
	}
	if len(assigned) > 0 {
		return fmt.Errorf("teams are assigned to %d remote clusters, which only the cluster service can serve", len(assigned))
	}

	return nil
}
