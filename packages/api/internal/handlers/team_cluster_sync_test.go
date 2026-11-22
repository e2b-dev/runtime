package handlers

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/db/pkg/outbox"
	"github.com/e2b-dev/infra/packages/api/internal/db/pkg/testutils"
	"github.com/e2b-dev/infra/packages/api/internal/management"
)

func teamClusterSyncJobs(t *testing.T, db *testutils.Database, teamID uuid.UUID) int {
	t.Helper()

	var jobs int
	queryRow(t, db, `SELECT count(*) FROM river.river_job WHERE kind = $1 AND args->>'team_id' = $2`,
		[]any{outbox.SyncTeamClusterAssignment{}.Kind(), teamID.String()}, &jobs)

	return jobs
}

// The cluster service learns a team's cluster only from these jobs, so a
// write that enqueues none leaves it holding the old one.
func TestEveryWriteOfATeamsClusterEnqueuesItsSync(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store, _ := newManagementStore(t, db)
	clusterID := uuid.New()
	insertCluster(t, db, clusterID, false)

	t.Run("a project deletion", func(t *testing.T) {
		t.Parallel()
		teamID := testutils.CreateTestTeam(t, db)

		require.Equal(t, http.StatusNoContent, callDeleteProject(t, store, teamID).Code)
		assert.Equal(t, 1, teamClusterSyncJobs(t, db, teamID))
	})

	t.Run("a project deletion without a cluster service enqueues none", func(t *testing.T) {
		t.Parallel()
		localOnly, _ := newManagementStore(t, db)
		localOnly.clusterService = nil
		teamID := testutils.CreateTestTeam(t, db)

		require.Equal(t, http.StatusNoContent, callDeleteProject(t, localOnly, teamID).Code)
		assert.Zero(t, teamClusterSyncJobs(t, db, teamID))
	})

	t.Run("a project upsert that leaves the project on a cluster", func(t *testing.T) {
		t.Parallel()
		project := newProjectFixture()

		local := newProjectFixture()
		require.Equal(t, http.StatusCreated, callUpsertProject(t, store, local.id, local.request()).Code)
		assert.Zero(t, teamClusterSyncJobs(t, db, local.id), "a project on the local cluster has nothing to copy")

		onCluster := project.request()
		onCluster.ClusterId = &clusterID
		require.Equal(t, http.StatusCreated, callUpsertProject(t, store, project.id, onCluster).Code)
		assert.Equal(t, 1, teamClusterSyncJobs(t, db, project.id))

		// A retry cannot tell whether the first attempt's job was enqueued.
		require.Equal(t, http.StatusOK, callUpsertProject(t, store, project.id, project.request()).Code)
		assert.Equal(t, 2, teamClusterSyncJobs(t, db, project.id))
	})
}

type failingEnqueuer struct{ err error }

func (f failingEnqueuer) Insert(context.Context, river.JobArgs, *river.InsertOpts) (*rivertype.JobInsertResult, error) {
	return nil, f.err
}

// The upsert enqueues after its commit, so only an error makes the caller
// retry and the job get enqueued.
func TestUpsertProjectFailsWhenItsClusterSyncCannotBeEnqueued(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store, _ := newManagementStore(t, db)
	clusterID := uuid.New()
	insertCluster(t, db, clusterID, false)
	project := newProjectFixture()
	unavailable := errors.New("river unavailable")

	_, _, err := store.managementService.UpsertProject(t.Context(), failingEnqueuer{err: unavailable}, management.Project{
		ClusterID: &clusterID, ID: project.id, Name: "Acme", Slug: project.slug, Email: "ops@acme.test",
	})
	require.ErrorIs(t, err, unavailable)
}
