package outbox

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivertest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	teammocks "github.com/e2b-dev/infra/packages/api/internal/team/mocks"
	dbmodule "github.com/e2b-dev/infra/packages/db"
	"github.com/e2b-dev/infra/packages/db/pkg/outbox"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	clustersgrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/clusters/management"
	"github.com/e2b-dev/infra/packages/shared/pkg/sharedriver"
)

func workTeamClusterSweep(t *testing.T, db *testutils.Database, clusters clustersgrpc.ClusterServiceClient, pageSize int32) (*rivertest.WorkResult, error) {
	t.Helper()

	jobs, err := river.NewClient(riverpgxv5.New(db.SqlcClient.Pool()), &river.Config{Schema: dbmodule.RiverSchema})
	require.NoError(t, err)
	worker := rivertest.NewWorker(t, riverpgxv5.New(nil), &river.Config{Schema: dbmodule.RiverSchema}, &teamClusterSweepWorker{
		db:       db.SqlcClient,
		clusters: clusters,
		jobs:     jobs,
		pageSize: pageSize,
		steps:    sharedriver.NewSteps(nil, nil, sharedriver.StepsConfig{MetricPrefix: "api"}).Job(outbox.SweepTeamClusterAssignments{}.Kind()),
	})
	riverPool, err := pgxpool.New(t.Context(), db.ConnStr())
	require.NoError(t, err)
	t.Cleanup(riverPool.Close)
	tx, err := riverPool.Begin(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback(context.WithoutCancel(t.Context())) })

	return worker.Work(t.Context(), t, tx, outbox.SweepTeamClusterAssignments{}, nil)
}

func teamClusterSyncs(t *testing.T, db *testutils.Database, teamID uuid.UUID) int {
	t.Helper()

	var jobs int
	require.NoError(t, db.SqlcClient.TestsRawSQLQuery(t.Context(),
		`SELECT count(*) FROM river.river_job WHERE kind = $1 AND args->>'team_id' = $2`,
		func(rows pgx.Rows) error {
			require.True(t, rows.Next())

			return rows.Scan(&jobs)
		}, outbox.SyncTeamClusterAssignment{}.Kind(), teamID.String()))

	return jobs
}

func TestSweepEnqueuesASyncForEachLiveTeamTheClusterServiceDoesNotRecord(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	cluster, other := uuid.New(), uuid.New()
	unrecorded := []uuid.UUID{teamOnCluster(t, db, &cluster), teamOnCluster(t, db, &cluster), teamOnCluster(t, db, &cluster)}
	moved, recorded := teamOnCluster(t, db, &cluster), teamOnCluster(t, db, &cluster)
	local := teamOnCluster(t, db, nil)
	deleted := teamOnCluster(t, db, &cluster)
	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(), `UPDATE public.teams SET deleted_at = now(), is_blocked = true WHERE id = $1`, deleted))
	records := &recordedAssignments{byProject: map[uuid.UUID]uuid.UUID{moved: other, recorded: cluster}}

	// Four teams need a sync and a page holds two, so some lie past the first.
	result, err := workTeamClusterSweep(t, db, records.client(t), 2)
	require.NoError(t, err)
	require.Equal(t, river.EventKindJobCompleted, result.EventKind)

	for _, teamID := range unrecorded {
		assert.Equal(t, 1, teamClusterSyncs(t, db, teamID))
	}
	assert.Equal(t, 1, teamClusterSyncs(t, db, moved), "a team recorded on another cluster")
	assert.Zero(t, teamClusterSyncs(t, db, recorded), "a team recorded on its cluster")
	assert.Zero(t, teamClusterSyncs(t, db, local), "a team on no cluster")
	assert.Zero(t, teamClusterSyncs(t, db, deleted), "a deleted team whose row keeps a cluster")
	assert.Zero(t, records.writes, "the sweep leaves the writes to the syncs")
}

// A team the cluster service keeps refusing stays unrecorded, and would
// gather another retrying sync with every sweep.
func TestSweepKeepsOneOpenSyncPerTeam(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	cluster := uuid.New()
	teamID := teamOnCluster(t, db, &cluster)
	clusters := (&recordedAssignments{}).client(t)

	for range 2 {
		_, err := workTeamClusterSweep(t, db, clusters, teamClusterSweepPage)
		require.NoError(t, err)
	}
	assert.Equal(t, 1, teamClusterSyncs(t, db, teamID))

	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(),
		`UPDATE river.river_job SET state = 'completed', finalized_at = now() WHERE args->>'team_id' = $1`, teamID.String()))
	_, err := workTeamClusterSweep(t, db, clusters, teamClusterSweepPage)
	require.NoError(t, err)
	assert.Equal(t, 2, teamClusterSyncs(t, db, teamID), "a finished sync does not stand in for the next one")
}

func TestSweepFailsWhenTheClusterServiceCannotBeAsked(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	cluster := uuid.New()
	teamID := teamOnCluster(t, db, &cluster)
	unavailable := status.Error(codes.Unavailable, "cluster service unavailable")

	result, err := workTeamClusterSweep(t, db, (&recordedAssignments{lookupErr: unavailable}).client(t), teamClusterSweepPage)
	require.ErrorIs(t, err, unavailable)
	require.Equal(t, river.EventKindJobFailed, result.EventKind)
	assert.Zero(t, teamClusterSyncs(t, db, teamID))
}

// The outbox starts a sweep itself, and the sync it enqueues records a team
// that was on its cluster before any write enqueued one.
func TestTheOutboxRecordsATeamAssignedBeforeItStarted(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	cluster := uuid.New()
	teamID := teamOnCluster(t, db, &cluster)
	records := &recordedAssignments{}
	_, container := newContainmentMocks(t)
	outboxRiver, err := New(Dependencies{
		Pool:        db.SqlcClient.Pool(),
		Sandboxes:   &teardownRecorder{},
		Containment: container,
		TeamCache:   teammocks.NewMockTeamCache(t),
		Teams:       db.AuthDb,
		DB:          db.SqlcClient,
		Clusters:    records.client(t),
		Config:      Config{MaxWorkers: 2, BacklogInterval: time.Second},
	})
	require.NoError(t, err)

	require.NoError(t, outboxRiver.Start(t.Context()))
	t.Cleanup(func() { require.NoError(t, outboxRiver.Stop(context.WithoutCancel(t.Context()))) })

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, &cluster, records.of(teamID))
	}, 30*time.Second, 100*time.Millisecond)
}
