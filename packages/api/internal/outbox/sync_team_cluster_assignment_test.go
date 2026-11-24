package outbox

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivertest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	dbmodule "github.com/e2b-dev/infra/packages/db"
	"github.com/e2b-dev/infra/packages/db/pkg/outbox"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	clustersgrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/clusters/management"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/clusters/management/clienttests"
	"github.com/e2b-dev/infra/packages/shared/pkg/sharedriver"
)

// recordedAssignments is the cluster the cluster service holds for each
// project, and how many times it was asked to change one.
type recordedAssignments struct {
	mu        sync.Mutex
	byProject map[uuid.UUID]uuid.UUID
	writes    int
	// assignErr fails every AssignProject.
	assignErr error
	// lookedUp, when set, receives each lookup as it arrives.
	lookedUp chan struct{}
	// onAssign, when set, runs as an AssignProject arrives.
	onAssign func()
	// lookupErr fails every LookupProjectAssignments.
	lookupErr error
}

func (r *recordedAssignments) of(projectID uuid.UUID) *uuid.UUID {
	r.mu.Lock()
	defer r.mu.Unlock()
	clusterID, ok := r.byProject[projectID]
	if !ok {
		return nil
	}

	return &clusterID
}

// client answers the assignment calls from r, as the cluster service does.
func (r *recordedAssignments) client(t *testing.T) *clienttests.MockClusterServiceClient {
	t.Helper()

	if r.byProject == nil {
		r.byProject = map[uuid.UUID]uuid.UUID{}
	}
	client := clienttests.NewMockClusterServiceClient(t)
	client.EXPECT().LookupProjectAssignments(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, req *clustersgrpc.LookupProjectAssignmentsRequest, _ ...grpc.CallOption) (*clustersgrpc.LookupProjectAssignmentsResponse, error) {
			if r.lookedUp != nil {
				r.lookedUp <- struct{}{}
			}
			r.mu.Lock()
			defer r.mu.Unlock()
			if r.lookupErr != nil {
				return nil, r.lookupErr
			}
			var assignments []*clustersgrpc.ProjectAssignment
			for _, projectID := range req.GetProjectIds() {
				if clusterID, ok := r.byProject[uuid.MustParse(projectID)]; ok {
					assignments = append(assignments, clustersgrpc.ProjectAssignment_builder{ProjectId: projectID, ClusterId: clusterID.String()}.Build())
				}
			}

			return clustersgrpc.LookupProjectAssignmentsResponse_builder{Assignments: assignments}.Build(), nil
		}).Maybe()
	client.EXPECT().AssignProject(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, req *clustersgrpc.AssignProjectRequest, _ ...grpc.CallOption) (*clustersgrpc.AssignProjectResponse, error) {
			if r.onAssign != nil {
				r.onAssign()
			}
			r.mu.Lock()
			defer r.mu.Unlock()
			if r.assignErr != nil {
				return nil, r.assignErr
			}
			r.writes++
			r.byProject[uuid.MustParse(req.GetProjectId())] = uuid.MustParse(req.GetClusterId())

			return clustersgrpc.AssignProjectResponse_builder{}.Build(), nil
		}).Maybe()
	client.EXPECT().DetachProject(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, req *clustersgrpc.DetachProjectRequest, _ ...grpc.CallOption) (*clustersgrpc.DetachProjectResponse, error) {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.writes++
			projectID := uuid.MustParse(req.GetProjectId())
			if r.byProject[projectID] == uuid.MustParse(req.GetClusterId()) {
				delete(r.byProject, projectID)
			}

			return clustersgrpc.DetachProjectResponse_builder{}.Build(), nil
		}).Maybe()

	return client
}

// teamClusterSync returns a function that works one sync job for the team.
func teamClusterSync(t *testing.T, db *testutils.Database, clusters clustersgrpc.ClusterServiceClient, teamID uuid.UUID) func() (*rivertest.WorkResult, error) {
	t.Helper()

	worker := rivertest.NewWorker(t, riverpgxv5.New(nil), &river.Config{Schema: dbmodule.RiverSchema}, &teamClusterAssignmentWorker{
		db:       db.SqlcClient,
		clusters: clusters,
		steps:    sharedriver.NewSteps(nil, nil, sharedriver.StepsConfig{MetricPrefix: "api"}).Job(outbox.SyncTeamClusterAssignment{}.Kind()),
	})
	// River's transaction gets a pool of its own. On the worker's it would
	// hold a connection the worker's own transaction then waits for, and
	// parallel tests exhaust the pool.
	riverPool, err := pgxpool.New(t.Context(), db.ConnStr())
	require.NoError(t, err)
	t.Cleanup(riverPool.Close)
	tx, err := riverPool.Begin(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback(context.WithoutCancel(t.Context())) })

	return func() (*rivertest.WorkResult, error) {
		return worker.Work(t.Context(), t, tx, outbox.SyncTeamClusterAssignment{TeamID: teamID}, nil)
	}
}

func workTeamClusterSync(t *testing.T, db *testutils.Database, clusters clustersgrpc.ClusterServiceClient, teamID uuid.UUID) (*rivertest.WorkResult, error) {
	t.Helper()

	return teamClusterSync(t, db, clusters, teamID)()
}

func teamOnCluster(t *testing.T, db *testutils.Database, clusterID *uuid.UUID) uuid.UUID {
	t.Helper()

	teamID := testutils.CreateTestTeam(t, db)
	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(), `UPDATE public.teams SET cluster_id = $2 WHERE id = $1`, teamID, clusterID))

	return teamID
}

func TestSyncTeamClusterAssignmentWorker(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	cluster, other := uuid.New(), uuid.New()

	deletedTeam := teamOnCluster(t, db, &cluster)
	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(), `UPDATE public.teams SET deleted_at = now(), is_blocked = true WHERE id = $1`, deletedTeam))

	tests := map[string]struct {
		teamID     uuid.UUID
		recorded   *uuid.UUID
		want       *uuid.UUID
		wantWrites int
	}{
		"a team on a cluster is recorded on it":                      {teamID: teamOnCluster(t, db, &cluster), want: &cluster, wantWrites: 1},
		"a team recorded on another cluster is moved":                {teamID: teamOnCluster(t, db, &cluster), recorded: &other, want: &cluster, wantWrites: 1},
		"a team already recorded on its cluster is left alone":       {teamID: teamOnCluster(t, db, &cluster), recorded: &cluster, want: &cluster},
		"a team off its cluster has the record cleared":              {teamID: teamOnCluster(t, db, nil), recorded: &cluster, wantWrites: 1},
		"a team on no cluster with nothing recorded is left alone":   {teamID: teamOnCluster(t, db, nil)},
		"a deleted team whose row keeps a cluster has none recorded": {teamID: deletedTeam, recorded: &cluster, wantWrites: 1},
		"a team whose row is gone has the record cleared":            {teamID: uuid.New(), recorded: &cluster, wantWrites: 1},
		"a deleted team with nothing recorded is not assigned again": {teamID: deletedTeam},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			records := &recordedAssignments{byProject: map[uuid.UUID]uuid.UUID{}}
			if tt.recorded != nil {
				records.byProject[tt.teamID] = *tt.recorded
			}

			result, err := workTeamClusterSync(t, db, records.client(t), tt.teamID)
			require.NoError(t, err)
			require.Equal(t, river.EventKindJobCompleted, result.EventKind)
			assert.Equal(t, tt.want, records.of(tt.teamID))
			assert.Equal(t, tt.wantWrites, records.writes)
		})
	}
}

// clusterRefusal is the FAILED_PRECONDITION the cluster service answers with.
func clusterRefusal(t *testing.T, reason clustersgrpc.ClusterErrorReason) error {
	t.Helper()

	st, err := status.New(codes.FailedPrecondition, reason.String()).WithDetails(clustersgrpc.ClusterErrorDetail_builder{Reason: reason}.Build())
	require.NoError(t, err)

	return st.Err()
}

func TestSyncTeamClusterAssignmentWhenTheClusterServiceDoesNotRecordTheTeam(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	cluster := uuid.New()

	// A day of retries would not change either answer, and the discarded job
	// would be kept for good.
	for name, refusal := range map[string]error{
		"a released project cannot be mirrored again":                       clusterRefusal(t, clustersgrpc.ClusterErrorReason_CLUSTER_ERROR_REASON_PROJECT_PLACEMENT_CONFLICT),
		"a cluster being deleted takes no new team and the sync ends":       clusterRefusal(t, clustersgrpc.ClusterErrorReason_CLUSTER_ERROR_REASON_DELETION_REQUESTED),
		"a cluster the cluster service does not know ends the sync as well": status.Error(codes.NotFound, "cluster not found"),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			teamID := teamOnCluster(t, db, &cluster)
			records := &recordedAssignments{assignErr: refusal}

			result, err := workTeamClusterSync(t, db, records.client(t), teamID)
			require.NoError(t, err)
			require.Equal(t, river.EventKindJobCompleted, result.EventKind)
			assert.Nil(t, records.of(teamID))
		})
	}

	t.Run("any other failure retries", func(t *testing.T) {
		t.Parallel()
		unavailable := status.Error(codes.Unavailable, "cluster service unavailable")
		records := &recordedAssignments{assignErr: unavailable}

		result, err := workTeamClusterSync(t, db, records.client(t), teamOnCluster(t, db, &cluster))
		require.ErrorIs(t, err, unavailable)
		require.Equal(t, river.EventKindJobFailed, result.EventKind)
	})
}

// A session lost mid-call releases the team's lock early, so a later job may
// have read before this one wrote. Only a failed attempt has both read again.
func TestSyncTeamClusterAssignmentFailsWhenItsSessionIsLostWhileTheClusterServiceAnswers(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	cluster := uuid.New()
	records := &recordedAssignments{onAssign: func() {
		assert.NoError(t, db.SqlcClient.TestsRawSQL(context.WithoutCancel(t.Context()), `
			SELECT pg_terminate_backend(pid) FROM pg_locks
			WHERE locktype = 'advisory' AND pid <> pg_backend_pid()
			  AND database = (SELECT oid FROM pg_database WHERE datname = current_database())`))
	}}

	result, err := workTeamClusterSync(t, db, records.client(t), teamOnCluster(t, db, &cluster))
	require.Error(t, err)
	require.Equal(t, river.EventKindJobFailed, result.EventKind)
}

func TestSyncTeamClusterAssignmentDoesNothingWithoutAClusterService(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	cluster := uuid.New()

	result, err := workTeamClusterSync(t, db, nil, teamOnCluster(t, db, &cluster))
	require.NoError(t, err)
	require.Equal(t, river.EventKindJobCompleted, result.EventKind)
}

// Two syncs of one team must not interleave: the one that read the older
// cluster could otherwise write it after the one that read the newer.
func TestSyncTeamClusterAssignmentWaitsForAnotherSyncOfTheTeam(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	cluster := uuid.New()
	teamID := teamOnCluster(t, db, &cluster)
	records := &recordedAssignments{lookedUp: make(chan struct{}, 1)}
	clusters := records.client(t)

	earlier, earlierTx, err := db.SqlcClient.WithTx(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = earlierTx.Rollback(context.WithoutCancel(t.Context())) })
	require.NoError(t, earlier.LockTeamClusterSync(t.Context(), teamID))

	work := teamClusterSync(t, db, clusters, teamID)
	done := make(chan error, 1)
	go func() {
		_, err := work()
		done <- err
	}()

	select {
	case <-records.lookedUp:
		require.Fail(t, "the sync reached the cluster service while another held the team")
	case <-time.After(300 * time.Millisecond):
	}

	require.NoError(t, earlierTx.Rollback(t.Context()))
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		require.Fail(t, "the sync did not run once the team was released")
	}
	assert.Equal(t, &cluster, records.of(teamID))
}
