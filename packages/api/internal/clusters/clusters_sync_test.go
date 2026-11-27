package clusters

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/api/internal/db/pkg/testutils"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	clustersgrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/clusters/management"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/clusters/management/clienttests"
)

// The pool connects to the clusters a team references; a cluster no team uses
// has nothing to schedule.
func TestAssignedRemoteClustersAreTheStoredClustersATeamUses(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	ctx := t.Context()
	assigned, unassigned := uuid.New(), uuid.New()
	teamID := testutils.CreateTestTeam(t, db)
	require.NoError(t, db.SqlcClient.TestsRawSQL(ctx, `UPDATE public.teams SET cluster_id = $2 WHERE id = $1`, teamID, assigned))
	domain := "sandbox.example.test"
	clusterService := clienttests.NewMockClusterServiceClient(t)
	clusterService.EXPECT().ListClusters(mock.Anything, mock.Anything).Return(clustersgrpc.ListClustersResponse_builder{Clusters: []*clustersgrpc.Cluster{
		clustersgrpc.Cluster_builder{ClusterId: assigned.String(), Endpoint: "edge.example.test:443", EndpointTls: true, Token: "edge-token", SandboxProxyDomain: &domain}.Build(),
		clustersgrpc.Cluster_builder{ClusterId: unassigned.String(), Endpoint: "idle.example.test:443", Token: "idle-token"}.Build(),
	}}.Build(), nil).Once()

	records, err := assignedRemoteClusters(ctx, db.SqlcClient, clusterService)

	require.NoError(t, err)
	require.Equal(t, []Record{{ID: assigned, Endpoint: "edge.example.test:443", EndpointTls: true, Token: "edge-token", SandboxProxyDomain: &domain}}, records)
}

// The startup fallback recognizes a failed list by errClusterList.
func TestAssignedRemoteClustersMarksAFailedClusterList(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	clusterID := uuid.New()
	teamID := testutils.CreateTestTeam(t, db)
	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(), `UPDATE public.teams SET cluster_id = $2 WHERE id = $1`, teamID, clusterID))
	clusterService := clienttests.NewMockClusterServiceClient(t)
	clusterService.EXPECT().ListClusters(mock.Anything, mock.Anything).Return(nil, status.Error(codes.Unavailable, "connection refused")).Once()

	_, err := assignedRemoteClusters(t.Context(), db.SqlcClient, clusterService)

	require.ErrorIs(t, err, errClusterList)
}

func TestAssignedRemoteClustersSkipsTheListWithoutAnAssignment(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	// No expectation: a call fails the test.
	clusterService := clienttests.NewMockClusterServiceClient(t)

	records, err := assignedRemoteClusters(t.Context(), db.SqlcClient, clusterService)

	require.NoError(t, err)
	require.Empty(t, records)
}

func TestSourceListWithoutAClusterServiceIsTheLocalCluster(t *testing.T) {
	t.Parallel()

	local := Record{ID: consts.LocalClusterID}
	records, err := clustersSyncStore{local: &local}.SourceList(t.Context())

	require.NoError(t, err)
	require.Equal(t, []Record{local}, records)
}

func TestStartupSourceStartsWithTheLocalClusterWhenTheClusterServiceKeepsFailing(t *testing.T) {
	t.Parallel()

	local := Record{ID: consts.LocalClusterID}
	source := &startupSource{
		list:  func(context.Context) ([]Record, error) { return nil, fmt.Errorf("%w: unavailable", errClusterList) },
		local: &local,
	}

	for range startupAttempts - 1 {
		_, err := source.inventory(t.Context())
		require.ErrorIs(t, err, errClusterList)
	}
	records, err := source.inventory(t.Context())

	require.NoError(t, err)
	require.Equal(t, []Record{local}, records)
}

// The API cannot serve without its database, so only the cluster service's
// failure is waited out.
func TestStartupSourceKeepsWaitingForTheDatabase(t *testing.T) {
	t.Parallel()

	databaseErr := errors.New("list assigned cluster IDs: connection refused")
	local := Record{ID: consts.LocalClusterID}
	source := &startupSource{
		list:  func(context.Context) ([]Record, error) { return nil, databaseErr },
		local: &local,
	}

	for range 2 * startupAttempts {
		_, err := source.inventory(t.Context())
		require.ErrorIs(t, err, databaseErr)
	}
}
