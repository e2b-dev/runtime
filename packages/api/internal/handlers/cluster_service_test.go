package handlers

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/e2b-dev/infra/packages/db/pkg/dberrors"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	clustersgrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/clusters/management"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/clusters/management/clienttests"
)

// newClusterServiceStore returns a store whose cluster routes reach only the
// returned cluster service, for the routes that read and write no table.
func newClusterServiceStore(t *testing.T) (*APIStore, *clienttests.MockClusterServiceClient) {
	t.Helper()

	clusterService := clienttests.NewMockClusterServiceClient(t)

	return &APIStore{clusterService: clusterService}, clusterService
}

// clusterRefusal is the FAILED_PRECONDITION the cluster service answers with.
func clusterRefusal(reason clustersgrpc.ClusterErrorReason) error {
	st, err := status.New(codes.FailedPrecondition, reason.String()).WithDetails(clustersgrpc.ClusterErrorDetail_builder{Reason: reason}.Build())
	if err != nil {
		return status.Error(codes.FailedPrecondition, reason.String())
	}

	return st.Err()
}

// clusterServiceFromDB answers the cluster service's reads and deletes from
// the clusters stored in the test database, which the service shares with the
// API. A delete removes the row, refused as IN_USE while a team or template
// still references it.
func clusterServiceFromDB(t *testing.T, db *testutils.Database) *clienttests.MockClusterServiceClient {
	t.Helper()

	clusterService := clienttests.NewMockClusterServiceClient(t)
	clusterService.EXPECT().GetCluster(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *clustersgrpc.GetClusterRequest, _ ...grpc.CallOption) (*clustersgrpc.GetClusterResponse, error) {
			clusters, err := storedClusters(ctx, db, req.GetClusterId())
			if err != nil {
				return nil, err
			}
			if len(clusters) == 0 {
				return nil, status.Error(codes.NotFound, "cluster not found")
			}

			return clustersgrpc.GetClusterResponse_builder{Cluster: clusters[0]}.Build(), nil
		}).Maybe()
	clusterService.EXPECT().ListClusters(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, _ *clustersgrpc.ListClustersRequest, _ ...grpc.CallOption) (*clustersgrpc.ListClustersResponse, error) {
			clusters, err := storedClusters(ctx, db, "")
			if err != nil {
				return nil, err
			}

			return clustersgrpc.ListClustersResponse_builder{Clusters: clusters}.Build(), nil
		}).Maybe()
	clusterService.EXPECT().DeleteCluster(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *clustersgrpc.DeleteClusterRequest, _ ...grpc.CallOption) (*clustersgrpc.DeleteClusterResponse, error) {
			err := db.SqlcClient.TestsRawSQL(ctx, `DELETE FROM public.clusters WHERE id = $1::uuid`, req.GetClusterId())
			if dberrors.IsForeignKeyViolation(err) {
				return nil, clusterRefusal(clustersgrpc.ClusterErrorReason_CLUSTER_ERROR_REASON_IN_USE)
			}
			if err != nil {
				return nil, err
			}

			return clustersgrpc.DeleteClusterResponse_builder{}.Build(), nil
		}).Maybe()

	return clusterService
}

// storedClusters reads one stored cluster, or every one for an empty ID.
func storedClusters(ctx context.Context, db *testutils.Database, clusterID string) ([]*clustersgrpc.Cluster, error) {
	clusters := []*clustersgrpc.Cluster{}
	err := db.SqlcClient.TestsRawSQLQuery(ctx,
		`SELECT id::text, name, endpoint, endpoint_tls, token, sandbox_proxy_domain, auth_org_id,
			deletion_protection, deletion_requested_at, telemetry_client_id
		FROM public.clusters WHERE $1::text = '' OR id::text = $1::text ORDER BY id`,
		func(rows pgx.Rows) error {
			for rows.Next() {
				var (
					id, name, endpoint, token                      string
					endpointTLS, deletionProtection                bool
					sandboxProxyDomain, authOrgID, telemetryClient *string
					deletionRequestedAt                            *time.Time
				)
				if err := rows.Scan(&id, &name, &endpoint, &endpointTLS, &token, &sandboxProxyDomain, &authOrgID,
					&deletionProtection, &deletionRequestedAt, &telemetryClient); err != nil {
					return err
				}
				cluster := clustersgrpc.Cluster_builder{
					ClusterId:          id,
					Name:               name,
					Endpoint:           endpoint,
					EndpointTls:        endpointTLS,
					Token:              token,
					SandboxProxyDomain: sandboxProxyDomain,
					AuthOrgId:          authOrgID,
					DeletionProtection: deletionProtection,
					TelemetryClientId:  telemetryClient,
				}
				if deletionRequestedAt != nil {
					cluster.DeletionRequestedAt = timestamppb.New(*deletionRequestedAt)
				}
				clusters = append(clusters, cluster.Build())
			}

			return rows.Err()
		}, clusterID)

	return clusters, err
}

// An API without a cluster service keeps only the local cluster, so it must
// not start while a team still depends on a remote one.
func TestLocalOnlyIsRefusedWhileATeamIsAssignedToARemoteCluster(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	teamID, deletedTeamID := testutils.CreateTestTeam(t, db), testutils.CreateTestTeam(t, db)
	require.NoError(t, refuseLocalOnlyWithRemoteClusters(t.Context(), db.SqlcClient))

	// A deleted team keeps its cluster_id but runs no sandboxes.
	clusterID := uuid.New()
	insertCluster(t, db, clusterID, false)
	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(),
		`UPDATE public.teams SET cluster_id = $2, deleted_at = now(), is_blocked = true WHERE id = $1`, deletedTeamID, clusterID))
	require.NoError(t, refuseLocalOnlyWithRemoteClusters(t.Context(), db.SqlcClient))

	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(), `UPDATE public.teams SET cluster_id = $2 WHERE id = $1`, teamID, clusterID))
	require.Error(t, refuseLocalOnlyWithRemoteClusters(t.Context(), db.SqlcClient))
}

// Without a cluster service the pool never loads a remote cluster, so an
// assignment to one would leave the team unable to run sandboxes.
func TestRemoteClusterAssignmentsAnswer503WithoutAClusterService(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store, _ := newManagementStore(t, db)
	store.clusterService = nil
	clusterID := uuid.New()
	insertCluster(t, db, clusterID, false)
	projectID := testutils.CreateTestTeam(t, db)
	project := newProjectFixture()
	upsert := project.request()
	upsert.ClusterId = &clusterID

	assigned := callManagementAssignProjectCluster(t, store, projectID, clusterID)
	upserted := callUpsertProject(t, store, project.id, upsert)

	require.Equal(t, http.StatusServiceUnavailable, assigned.Code, assigned.Body.String())
	require.Equal(t, http.StatusServiceUnavailable, upserted.Code, upserted.Body.String())
	require.Nil(t, teamCluster(t, db, projectID))
}
