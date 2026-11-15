package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/e2b-dev/infra/packages/api/internal/clusterassign"
	"github.com/e2b-dev/infra/packages/api/internal/management"
	sqlcdb "github.com/e2b-dev/infra/packages/db/client"
	"github.com/e2b-dev/infra/packages/db/pkg/pool"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	clustersgrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/clusters/management"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/clusters/management/clienttests"
)

// clusterState is what a cluster service holding one cluster answers about it.
type clusterState int

const (
	clusterPresent clusterState = iota
	clusterDeleting
	clusterAbsent
)

// singleClusterService answers GetCluster for one cluster from a state the
// test changes, independently of the clusters table.
type singleClusterService struct {
	*clienttests.MockClusterServiceClient

	mu    sync.Mutex
	state clusterState
}

func newSingleClusterService(t *testing.T, clusterID uuid.UUID, state clusterState) *singleClusterService {
	t.Helper()

	s := &singleClusterService{MockClusterServiceClient: clienttests.NewMockClusterServiceClient(t), state: state}
	s.EXPECT().GetCluster(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, _ *clustersgrpc.GetClusterRequest, _ ...grpc.CallOption) (*clustersgrpc.GetClusterResponse, error) {
			s.mu.Lock()
			defer s.mu.Unlock()

			cluster := clustersgrpc.Cluster_builder{ClusterId: clusterID.String(), Name: "cluster", Endpoint: "edge.example.test:443", Token: "token"}
			switch s.state {
			case clusterAbsent:
				return nil, status.Error(codes.NotFound, "cluster not found")
			case clusterDeleting:
				cluster.DeletionRequestedAt = timestamppb.Now()
			case clusterPresent:
			}

			return clustersgrpc.GetClusterResponse_builder{Cluster: cluster.Build()}.Build(), nil
		}).Maybe()
	s.EXPECT().UpdateCluster(mock.Anything, mock.MatchedBy(isDeletionRequest)).RunAndReturn(
		func(context.Context, *clustersgrpc.UpdateClusterRequest, ...grpc.CallOption) (*clustersgrpc.UpdateClusterResponse, error) {
			s.mu.Lock()
			defer s.mu.Unlock()
			if s.state == clusterAbsent {
				return nil, status.Error(codes.NotFound, "cluster not found")
			}
			s.state = clusterDeleting

			return clustersgrpc.UpdateClusterResponse_builder{Cluster: clustersgrpc.Cluster_builder{ClusterId: clusterID.String()}.Build()}.Build(), nil
		}).Maybe()

	return s
}

func (s *singleClusterService) set(state clusterState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = state
}

// withClusterService points both the assignment route and the project upsert
// at clusterService.
func withClusterService(t *testing.T, db *testutils.Database, store *APIStore, clusterService clustersgrpc.ClusterServiceClient) {
	t.Helper()

	store.clusterService = clusterService
	store.managementService = management.NewService(db.AuthDB, db.SqlcClient, store.authService, clusterService, noop.NewMeterProvider())
}

// Each case keeps a clusters row the database accepts, so only the cluster
// service can refuse the assignment.
func TestANewAssignmentChecksTheClusterService(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		state   clusterState
		current bool
		want    int
		code    string
	}{
		"a cluster the service does not know":                             {state: clusterAbsent, want: http.StatusNotFound, code: clusterAssignmentClusterNotFoundCode},
		"a cluster being deleted":                                         {state: clusterDeleting, want: http.StatusConflict, code: clusterAssignmentClusterDeletingCode},
		"the team's current cluster, being deleted, is no new assignment": {state: clusterDeleting, current: true, want: http.StatusNoContent},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			db := testutils.SetupDatabase(t)
			store, _ := newManagementStore(t, db)
			clusterID, team := uuid.New(), testutils.CreateTestTeam(t, db)
			insertCluster(t, db, clusterID, false)
			if tc.current {
				require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(), `UPDATE public.teams SET cluster_id = $1 WHERE id = $2`, clusterID, team))
			}
			withClusterService(t, db, store, newSingleClusterService(t, clusterID, tc.state))

			response := callManagementAssignProjectCluster(t, store, team, clusterID)
			require.Equal(t, tc.want, response.Code, response.Body.String())
			if tc.code != "" {
				requireClusterErrorCode(t, response, tc.code)
				require.Nil(t, teamCluster(t, db, team))
			}
		})
	}
}

func TestUpsertProjectChecksANewClusterWithTheClusterService(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		state clusterState
		want  int
		body  string
	}{
		"a cluster the service does not know": {state: clusterAbsent, want: http.StatusNotFound, body: "Cluster not found"},
		"a cluster being deleted":             {state: clusterDeleting, want: http.StatusConflict, body: "Cluster is being deleted"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			db := testutils.SetupDatabase(t)
			store, _ := newManagementStore(t, db)
			clusterID := createCluster(t, db)
			withClusterService(t, db, store, newSingleClusterService(t, clusterID, tc.state))
			project := newProjectFixture()
			request := project.request()
			request.ClusterId = &clusterID

			response := callUpsertProject(t, store, project.id, request)
			require.Equal(t, tc.want, response.Code, response.Body.String())
			require.Contains(t, response.Body.String(), tc.body)
		})
	}

	t.Run("keeping the current cluster while it is deleted", func(t *testing.T) {
		t.Parallel()

		db := testutils.SetupDatabase(t)
		store, _ := newManagementStore(t, db)
		clusterID := createCluster(t, db)
		clusterService := newSingleClusterService(t, clusterID, clusterPresent)
		withClusterService(t, db, store, clusterService)
		project := newProjectFixture()
		request := project.request()
		request.ClusterId = &clusterID
		require.Equal(t, http.StatusCreated, callUpsertProject(t, store, project.id, request).Code)

		clusterService.set(clusterDeleting)
		response := callUpsertProject(t, store, project.id, request)
		require.Equal(t, http.StatusOK, response.Code, response.Body.String())
		require.Equal(t, &clusterID, teamCluster(t, db, project.id))
	})
}

// An assignment holding the lock may have read the cluster before its
// deletion was recorded. The caller lists the cluster's teams once the
// request returns, so the request must wait for that assignment to commit.
func TestADeletionRequestWaitsForAnAssignmentInFlight(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	ctx := t.Context()
	clusterID := uuid.New()
	store, clusterService := newClusterServiceStore(t)
	store.sqlcDB = db.SqlcClient
	clusterService.EXPECT().UpdateCluster(mock.Anything, mock.Anything).
		Return(clustersgrpc.UpdateClusterResponse_builder{Cluster: clustersgrpc.Cluster_builder{ClusterId: clusterID.String()}.Build()}.Build(), nil).Once()

	assigning, err := pgx.Connect(ctx, db.ConnStr())
	require.NoError(t, err)
	t.Cleanup(func() { _ = assigning.Close(context.WithoutCancel(ctx)) })
	tx, err := assigning.Begin(ctx)
	require.NoError(t, err)
	require.NoError(t, clusterassign.Lock(ctx, tx, clusterID))
	var assigningPID int
	require.NoError(t, tx.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&assigningPID))

	status := make(chan int, 1)
	go func() { status <- callManagementRequestClusterDeletion(t, store, clusterID) }()
	waitForBlockedBy(t, db, assigningPID)
	require.NoError(t, tx.Commit(ctx))

	require.Equal(t, http.StatusNoContent, <-status)
}

// The delete counts the cluster's teams and then removes the cluster from the
// cluster service. An assignment in between would hold a cluster that is gone.
func TestAnAssignmentWaitsForAClusterDeleteInFlight(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store, _ := newManagementStore(t, db)
	clusterID, team := uuid.New(), testutils.CreateTestTeam(t, db)
	clusterService := newSingleClusterService(t, clusterID, clusterPresent)
	entered, release := make(chan struct{}), make(chan struct{})
	clusterService.EXPECT().DeleteCluster(mock.Anything, mock.Anything).RunAndReturn(
		func(context.Context, *clustersgrpc.DeleteClusterRequest, ...grpc.CallOption) (*clustersgrpc.DeleteClusterResponse, error) {
			close(entered)
			<-release
			clusterService.set(clusterAbsent)

			return clustersgrpc.DeleteClusterResponse_builder{}.Build(), nil
		}).Once()
	withClusterService(t, db, store, clusterService)

	deleted := make(chan int, 1)
	go func() { deleted <- callManagementDeleteCluster(t, store, clusterID).Code }()
	<-entered
	assigned := make(chan int, 1)
	go func() { assigned <- callManagementAssignProjectCluster(t, store, team, clusterID).Code }()
	require.Eventually(t, func() bool {
		var waiting bool
		err := db.SqlcClient.TestsRawSQLQuery(t.Context(), `
			SELECT EXISTS (
			    SELECT FROM pg_locks
			    WHERE locktype = 'advisory' AND NOT granted
			      AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
			)`, func(rows pgx.Rows) error {
			if !rows.Next() {
				return rows.Err()
			}

			return rows.Scan(&waiting)
		})

		return err == nil && waiting
	}, 10*time.Second, 10*time.Millisecond, "the assignment waits for the delete")
	close(release)

	require.Equal(t, http.StatusNoContent, <-deleted)
	require.Equal(t, http.StatusNotFound, <-assigned)
	require.Nil(t, teamCluster(t, db, team))
}

// An assignment that read the cluster before the delete took the lock
// commits while holding it. The delete counts the cluster's teams once it has
// the lock, and refuses without removing the cluster.
func TestAClusterDeleteCountsTeamsUnderTheLock(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	ctx := t.Context()
	store, _ := newManagementStore(t, db)
	clusterID, team := uuid.New(), testutils.CreateTestTeam(t, db)
	insertCluster(t, db, clusterID, false)
	// No DeleteCluster expectation: a call fails the test.
	withClusterService(t, db, store, newSingleClusterService(t, clusterID, clusterPresent))

	assigning, err := pgx.Connect(ctx, db.ConnStr())
	require.NoError(t, err)
	t.Cleanup(func() { _ = assigning.Close(context.WithoutCancel(ctx)) })
	tx, err := assigning.Begin(ctx)
	require.NoError(t, err)
	require.NoError(t, clusterassign.Lock(ctx, tx, clusterID))
	var assigningPID int
	require.NoError(t, tx.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&assigningPID))

	deleted := make(chan *httptest.ResponseRecorder, 1)
	go func() { deleted <- callManagementDeleteCluster(t, store, clusterID) }()
	waitForBlockedBy(t, db, assigningPID)
	_, err = tx.Exec(ctx, `UPDATE public.teams SET cluster_id = $1 WHERE id = $2`, clusterID, team)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	response := <-deleted
	require.Equal(t, http.StatusConflict, response.Code, response.Body.String())
	require.Equal(t, &clusterID, teamCluster(t, db, team))
}

// Assignments waiting on the lock each hold a connection, so a delete that
// needed a second one while holding the lock could wait on them forever.
func TestAClusterDeleteHoldsOneConnectionAtATime(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store, _ := newManagementStore(t, db)
	single, err := sqlcdb.NewClient(t.Context(), db.ConnStr(), pool.WithMaxConnections(1))
	require.NoError(t, err)
	t.Cleanup(func() { _ = single.Close() })
	store.sqlcDB = single
	clusterID, team := uuid.New(), testutils.CreateTestTeam(t, db)
	insertCluster(t, db, clusterID, false)
	template := testutils.CreateTestTemplate(t, db, team)
	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(), `UPDATE public.envs SET cluster_id = $2 WHERE id = $1`, template, clusterID))

	deleted := make(chan int, 1)
	go func() { deleted <- callManagementDeleteCluster(t, store, clusterID).Code }()
	select {
	case code := <-deleted:
		require.Equal(t, http.StatusNoContent, code)
	case <-time.After(10 * time.Second):
		require.Fail(t, "the delete did not finish on a pool of one connection")
	}
}

func templateDeleted(t *testing.T, db *testutils.Database, templateID string) bool {
	t.Helper()

	var deleted bool
	require.NoError(t, db.SqlcClient.TestsRawSQLQuery(t.Context(), `SELECT deleted_at IS NOT NULL FROM public.envs WHERE id = $1`,
		func(rows pgx.Rows) error {
			require.True(t, rows.Next())

			return rows.Scan(&deleted)
		}, templateID))

	return deleted
}

// A sandbox still running on the cluster can record a template while the
// cluster service removes the record. Once the record is gone, nothing would
// release it.
func TestAClusterDeleteReleasesATemplateRecordedWhileTheServiceRemovesIt(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store, _ := newManagementStore(t, db)
	clusterID, owner := uuid.New(), testutils.CreateTestTeam(t, db)
	late := testutils.CreateTestTemplate(t, db, owner)
	clusterService := newSingleClusterService(t, clusterID, clusterPresent)
	clusterService.EXPECT().DeleteCluster(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, _ *clustersgrpc.DeleteClusterRequest, _ ...grpc.CallOption) (*clustersgrpc.DeleteClusterResponse, error) {
			require.NoError(t, db.SqlcClient.TestsRawSQL(ctx, `UPDATE public.envs SET cluster_id = $2 WHERE id = $1`, late, clusterID))
			clusterService.set(clusterAbsent)

			return clustersgrpc.DeleteClusterResponse_builder{}.Build(), nil
		}).Once()
	withClusterService(t, db, store, clusterService)

	response := callManagementDeleteCluster(t, store, clusterID)

	require.Equal(t, http.StatusNoContent, response.Code, response.Body.String())
	require.True(t, templateDeleted(t, db, late))
}

// A delete whose commit failed after the cluster service removed the record
// is retried against an absent cluster, and must still release its templates.
func TestDeletingAnAbsentClusterReleasesItsTemplates(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store, _ := newManagementStore(t, db)
	clusterID, owner := uuid.New(), testutils.CreateTestTeam(t, db)
	templateID := testutils.CreateTestTemplate(t, db, owner)
	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(), `UPDATE public.envs SET cluster_id = $2 WHERE id = $1`, templateID, clusterID))
	withClusterService(t, db, store, newSingleClusterService(t, clusterID, clusterAbsent))

	response := callManagementDeleteCluster(t, store, clusterID)

	require.Equal(t, http.StatusNoContent, response.Code, response.Body.String())
	require.True(t, templateDeleted(t, db, templateID))
}

// Ending the delete when the caller leaves could leave the record removed
// and the templates still live.
func TestAClusterDeleteFinishesWhenTheCallerLeaves(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store, _ := newManagementStore(t, db)
	clusterID, owner := uuid.New(), testutils.CreateTestTeam(t, db)
	templateID := testutils.CreateTestTemplate(t, db, owner)
	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(), `UPDATE public.envs SET cluster_id = $2 WHERE id = $1`, templateID, clusterID))
	request, leave := context.WithCancel(t.Context())
	clusterService := newSingleClusterService(t, clusterID, clusterPresent)
	clusterService.EXPECT().DeleteCluster(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, _ *clustersgrpc.DeleteClusterRequest, _ ...grpc.CallOption) (*clustersgrpc.DeleteClusterResponse, error) {
			leave()
			if err := ctx.Err(); err != nil {
				return nil, status.FromContextError(err).Err()
			}
			clusterService.set(clusterAbsent)

			return clustersgrpc.DeleteClusterResponse_builder{}.Build(), nil
		}).Once()
	withClusterService(t, db, store, clusterService)

	require.NoError(t, store.deleteCluster(request, clusterID))
	require.True(t, templateDeleted(t, db, templateID))
}
