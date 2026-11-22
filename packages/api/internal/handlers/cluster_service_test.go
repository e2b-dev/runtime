package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/e2b-dev/infra/packages/api/internal/db/pkg/testutils"
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

// clusterRecords is what the cluster service holds for one test database,
// the records the API reaches through clusterServiceFor.
type clusterRecords struct {
	mu   sync.Mutex
	byID map[uuid.UUID]*clustersgrpc.Cluster
}

// testClusterRecords maps each test database's connection string, unique per
// test, to its records.
var testClusterRecords sync.Map

func recordsOf(db *testutils.Database) *clusterRecords {
	records, _ := testClusterRecords.LoadOrStore(db.ConnStr(), &clusterRecords{byID: map[uuid.UUID]*clustersgrpc.Cluster{}})

	return records.(*clusterRecords)
}

func (r *clusterRecords) put(cluster *clustersgrpc.Cluster) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.byID[uuid.MustParse(cluster.GetClusterId())] = cluster
}

func (r *clusterRecords) get(clusterID uuid.UUID) (*clustersgrpc.Cluster, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	cluster, ok := r.byID[clusterID]

	return cluster, ok
}

func (r *clusterRecords) list() []*clustersgrpc.Cluster {
	r.mu.Lock()
	defer r.mu.Unlock()
	clusters := make([]*clustersgrpc.Cluster, 0, len(r.byID))
	for _, cluster := range r.byID {
		clusters = append(clusters, cluster)
	}
	slices.SortFunc(clusters, func(a, b *clustersgrpc.Cluster) int { return strings.Compare(a.GetClusterId(), b.GetClusterId()) })

	return clusters
}

// requestDeletion records the deletion of a present, unprotected cluster, as
// the cluster service does.
func (r *clusterRecords) requestDeletion(clusterID uuid.UUID) (*clustersgrpc.Cluster, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	cluster, ok := r.byID[clusterID]
	switch {
	case !ok:
		return nil, status.Error(codes.NotFound, "cluster not found")
	case cluster.GetDeletionProtection():
		return nil, clusterRefusal(clustersgrpc.ClusterErrorReason_CLUSTER_ERROR_REASON_DELETION_PROTECTED)
	case !cluster.HasDeletionRequestedAt():
		cluster = proto.CloneOf(cluster)
		cluster.SetDeletionRequestedAt(timestamppb.Now())
		r.byID[clusterID] = cluster
	}

	return cluster, nil
}

func (r *clusterRecords) delete(clusterID uuid.UUID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.byID, clusterID)
}

// clusterServiceFor answers the cluster service's reads and deletes from the
// records kept for db.
func clusterServiceFor(t *testing.T, db *testutils.Database) *clienttests.MockClusterServiceClient {
	t.Helper()

	records := recordsOf(db)
	clusterService := clienttests.NewMockClusterServiceClient(t)
	clusterService.EXPECT().CheckProjectCreation(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, req *clustersgrpc.CheckProjectCreationRequest, _ ...grpc.CallOption) (*clustersgrpc.CheckProjectCreationResponse, error) {
			cluster, ok := records.get(uuid.MustParse(req.GetClusterId()))
			switch {
			case !ok:
				return nil, status.Error(codes.NotFound, "cluster not found")
			case cluster.HasDeletionRequestedAt():
				return nil, clusterRefusal(clustersgrpc.ClusterErrorReason_CLUSTER_ERROR_REASON_DELETION_REQUESTED)
			default:
				return clustersgrpc.CheckProjectCreationResponse_builder{}.Build(), nil
			}
		}).Maybe()
	clusterService.EXPECT().GetCluster(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, req *clustersgrpc.GetClusterRequest, _ ...grpc.CallOption) (*clustersgrpc.GetClusterResponse, error) {
			cluster, ok := records.get(uuid.MustParse(req.GetClusterId()))
			if !ok {
				return nil, status.Error(codes.NotFound, "cluster not found")
			}

			return clustersgrpc.GetClusterResponse_builder{Cluster: cluster}.Build(), nil
		}).Maybe()
	clusterService.EXPECT().ListClusters(mock.Anything, mock.Anything).RunAndReturn(
		func(context.Context, *clustersgrpc.ListClustersRequest, ...grpc.CallOption) (*clustersgrpc.ListClustersResponse, error) {
			return clustersgrpc.ListClustersResponse_builder{Clusters: records.list()}.Build(), nil
		}).Maybe()
	clusterService.EXPECT().UpdateCluster(mock.Anything, mock.MatchedBy(isDeletionRequest)).RunAndReturn(
		func(_ context.Context, req *clustersgrpc.UpdateClusterRequest, _ ...grpc.CallOption) (*clustersgrpc.UpdateClusterResponse, error) {
			cluster, err := records.requestDeletion(uuid.MustParse(req.GetClusterId()))
			if err != nil {
				return nil, err
			}

			return clustersgrpc.UpdateClusterResponse_builder{Cluster: cluster}.Build(), nil
		}).Maybe()
	clusterService.EXPECT().DeleteCluster(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, req *clustersgrpc.DeleteClusterRequest, _ ...grpc.CallOption) (*clustersgrpc.DeleteClusterResponse, error) {
			records.delete(uuid.MustParse(req.GetClusterId()))

			return clustersgrpc.DeleteClusterResponse_builder{}.Build(), nil
		}).Maybe()

	return clusterService
}

// isDeletionRequest matches an UpdateCluster that only records a deletion,
// leaving other updates to the expectations a test sets.
func isDeletionRequest(req *clustersgrpc.UpdateClusterRequest) bool {
	return req.GetRequestDeletion() && proto.Equal(req, clustersgrpc.UpdateClusterRequest_builder{ClusterId: req.GetClusterId(), RequestDeletion: true}.Build())
}

// insertCluster records a cluster named and addressed by its ID.
func insertCluster(t *testing.T, db *testutils.Database, clusterID uuid.UUID, protected bool) {
	t.Helper()

	recordsOf(db).put(clustersgrpc.Cluster_builder{
		ClusterId:          clusterID.String(),
		Name:               clusterID.String(),
		Endpoint:           clusterID.String(),
		EndpointTls:        true,
		Token:              "token",
		DeletionProtection: protected,
	}.Build())
}

func clusterExists(t *testing.T, db *testutils.Database, clusterID uuid.UUID) bool {
	t.Helper()

	_, ok := recordsOf(db).get(clusterID)

	return ok
}

// recordedClustersJSON is the records kept for db, as JSON.
func recordedClustersJSON(t *testing.T, db *testutils.Database) string {
	t.Helper()

	clusters := recordsOf(db).list()
	encoded := make([]json.RawMessage, 0, len(clusters))
	for _, cluster := range clusters {
		raw, err := protojson.Marshal(cluster)
		require.NoError(t, err)
		encoded = append(encoded, raw)
	}
	out, err := json.Marshal(encoded)
	require.NoError(t, err)

	return string(out)
}

// markClusterDeleting records the cluster's deletion as the cluster service
// does on a deletion request.
func markClusterDeleting(t *testing.T, db *testutils.Database, clusterID uuid.UUID) {
	t.Helper()

	records := recordsOf(db)
	cluster, ok := records.get(clusterID)
	require.True(t, ok, "the cluster is recorded")
	deleting, _ := proto.Clone(cluster).(*clustersgrpc.Cluster)
	deleting.SetDeletionProtection(false)
	deleting.SetDeletionRequestedAt(timestamppb.Now())
	records.put(deleting)
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
func TestLocalOnlyProjectCannotBeCreatedOnOrMovedToARemoteCluster(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store, _ := newManagementStore(t, db)
	withClusterService(t, db, store, nil)
	clusterID := uuid.New()
	insertCluster(t, db, clusterID, false)
	project := newProjectFixture()
	upsert := project.request()
	upsert.ClusterId = &clusterID

	upserted := callUpsertProject(t, store, project.id, upsert)

	require.Equal(t, http.StatusServiceUnavailable, upserted.Code, upserted.Body.String())
	require.Equal(t, http.StatusCreated, callUpsertProject(t, store, project.id, project.request()).Code)
	require.Equal(t, http.StatusConflict, callUpsertProject(t, store, project.id, upsert).Code)
}
