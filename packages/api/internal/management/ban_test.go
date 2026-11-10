package management

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivertype"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	dbmodule "github.com/e2b-dev/infra/packages/db"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
)

func TestApplyProjectBanBansTheTeamAndEnqueuesItsContainment(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	service, cache := newService(db)
	jobs := newJobInserter(t, db)
	teamID := testutils.CreateTestTeam(t, db)

	require.NoError(t, service.ApplyProjectBan(t.Context(), jobs, ProjectBanProjection{ProjectID: teamID, Revision: 1, Banned: true}))

	require.True(t, banState(t, db, teamID))
	require.EqualValues(t, 1, *banLedgerRevision(t, db, teamID))
	require.Equal(t, 1, containmentJobs(t, db, teamID))
	require.Equal(t, []uuid.UUID{teamID}, cache.teams)
}

func TestApplyProjectBanHonorsTheNewestRevision(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	service, cache := newService(db)
	jobs := newJobInserter(t, db)
	teamID := testutils.CreateTestTeam(t, db)

	require.NoError(t, service.ApplyProjectBan(t.Context(), jobs, ProjectBanProjection{ProjectID: teamID, Revision: 2, Banned: true}))
	cache.reset()

	for _, stale := range []ProjectBanProjection{
		{ProjectID: teamID, Revision: 1, Banned: false},
		{ProjectID: teamID, Revision: 2, Banned: false},
		{ProjectID: teamID, Revision: 2, Banned: true},
	} {
		require.NoError(t, service.ApplyProjectBan(t.Context(), jobs, stale),
			"a superseded delivery is satisfied, not an error the caller should retry")
	}

	require.True(t, banState(t, db, teamID))
	require.EqualValues(t, 2, *banLedgerRevision(t, db, teamID))
	require.Equal(t, 1, containmentJobs(t, db, teamID), "a dropped delivery enqueues nothing")
	require.Equal(t, []uuid.UUID{teamID, teamID, teamID}, cache.teams)
}

func TestApplyProjectBanContainsOnlyABanThatTurnsOn(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	service, _ := newService(db)
	jobs := newJobInserter(t, db)
	teamID := testutils.CreateTestTeam(t, db)

	steps := []struct {
		projection ProjectBanProjection
		banned     bool
		jobs       int
	}{
		{ProjectBanProjection{ProjectID: teamID, Revision: 1, Banned: true}, true, 1},
		{ProjectBanProjection{ProjectID: teamID, Revision: 2, Banned: true}, true, 1},
		{ProjectBanProjection{ProjectID: teamID, Revision: 3, Banned: false}, false, 1},
		{ProjectBanProjection{ProjectID: teamID, Revision: 4, Banned: false}, false, 1},
		{ProjectBanProjection{ProjectID: teamID, Revision: 5, Banned: true}, true, 2},
	}
	for _, step := range steps {
		require.NoError(t, service.ApplyProjectBan(t.Context(), jobs, step.projection))
		require.Equal(t, step.banned, banState(t, db, teamID), "revision %d", step.projection.Revision)
		require.Equal(t, step.jobs, containmentJobs(t, db, teamID), "revision %d", step.projection.Revision)
	}
}

func TestApplyProjectBanDoesNotContainATeamAlreadyBanned(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	service, _ := newService(db)
	jobs := newJobInserter(t, db)
	teamID := testutils.CreateTestTeam(t, db)
	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(), "UPDATE public.teams SET is_banned = true WHERE id = $1", teamID))

	require.NoError(t, service.ApplyProjectBan(t.Context(), jobs, ProjectBanProjection{ProjectID: teamID, Revision: 1, Banned: true}))

	require.EqualValues(t, 1, *banLedgerRevision(t, db, teamID))
	require.Zero(t, containmentJobs(t, db, teamID))
}

func TestApplyProjectBanRetriesFailedCacheInvalidation(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	cache := &failingBlockCache{err: errors.New("cache unavailable")}
	service := NewService(db.AuthDB, db.SqlcClient, cache, noop.NewMeterProvider())
	jobs := newJobInserter(t, db)
	teamID := testutils.CreateTestTeam(t, db)
	projection := ProjectBanProjection{ProjectID: teamID, Revision: 1, Banned: true}

	require.ErrorIs(t, service.ApplyProjectBan(t.Context(), jobs, projection), cache.err)
	require.True(t, banState(t, db, teamID))

	cache.err = nil
	require.NoError(t, service.ApplyProjectBan(t.Context(), jobs, projection))
	require.Equal(t, 2, cache.calls)
	require.EqualValues(t, 1, *banLedgerRevision(t, db, teamID))
	require.Equal(t, 1, containmentJobs(t, db, teamID))
}

func TestApplyProjectBanThatCannotEnqueueItsContainmentStoresNothing(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	service, cache := newService(db)
	teamID := testutils.CreateTestTeam(t, db)
	projection := ProjectBanProjection{ProjectID: teamID, Revision: 1, Banned: true}
	unavailable := errors.New("queue unavailable")

	require.ErrorIs(t, service.ApplyProjectBan(t.Context(), failingJobInserter{err: unavailable}, projection), unavailable)
	require.False(t, banState(t, db, teamID))
	require.Nil(t, banLedgerRevision(t, db, teamID))
	require.Empty(t, cache.teams)

	require.NoError(t, service.ApplyProjectBan(t.Context(), newJobInserter(t, db), projection))
	require.True(t, banState(t, db, teamID))
	require.Equal(t, 1, containmentJobs(t, db, teamID))
}

func TestApplyProjectBanReportsAnUnknownProject(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	service, cache := newService(db)
	projectID := uuid.New()

	err := service.ApplyProjectBan(t.Context(), newJobInserter(t, db), ProjectBanProjection{ProjectID: projectID, Revision: 1, Banned: true})

	require.ErrorIs(t, err, ErrProjectNotFound)
	require.Empty(t, cache.teams)
	require.Zero(t, containmentJobs(t, db, projectID))
}

func TestApplyProjectBanRefusesAnUnfenceableDelivery(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	service, cache := newService(db)
	jobs := newJobInserter(t, db)
	teamID := testutils.CreateTestTeam(t, db)

	for _, projection := range []ProjectBanProjection{
		{ProjectID: uuid.Nil, Revision: 1, Banned: true},
		{ProjectID: teamID, Revision: 0, Banned: true},
		{ProjectID: teamID, Revision: -1, Banned: true},
	} {
		require.ErrorIs(t, service.ApplyProjectBan(t.Context(), jobs, projection), ErrInvalidProjectBan)
	}

	require.False(t, banState(t, db, teamID))
	require.Nil(t, banLedgerRevision(t, db, teamID))
	require.Empty(t, cache.teams)
}

func TestApplyProjectBanMeasuresOnlyTheRevisionItStores(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	service, reader := newMeasuredService(db)
	jobs := newJobInserter(t, db)
	teamID := testutils.CreateTestTeam(t, db)

	require.NoError(t, service.ApplyProjectBan(t.Context(), jobs, ProjectBanProjection{
		ProjectID: teamID, Revision: 2, Banned: true, DecidedAt: time.Now().Add(-90 * time.Second),
	}))
	require.NoError(t, service.ApplyProjectBan(t.Context(), jobs, ProjectBanProjection{
		ProjectID: teamID, Revision: 1, Banned: false, DecidedAt: time.Now(),
	}))

	seen := collectApplyLag(t, reader)
	require.EqualValues(t, 1, seen.lag[projectionProjectBans].Count, "an overtaken delivery applied nothing")
	require.InDelta(t, 105, seen.lag[projectionProjectBans].Sum, 15, "one point, 90s old when sent")
}

func newJobInserter(t *testing.T, db *testutils.Database) *river.Client[pgx.Tx] {
	t.Helper()

	client, err := river.NewClient(riverpgxv5.New(db.SqlcClient.Pool()), &river.Config{Schema: dbmodule.RiverSchema})
	require.NoError(t, err)

	return client
}

type failingJobInserter struct {
	err error
}

func (f failingJobInserter) InsertTx(context.Context, pgx.Tx, river.JobArgs, *river.InsertOpts) (*rivertype.JobInsertResult, error) {
	return nil, f.err
}

func banState(t *testing.T, db *testutils.Database, teamID uuid.UUID) bool {
	t.Helper()

	var banned bool
	require.NoError(t, db.SqlcClient.TestsRawSQLQuery(t.Context(),
		"SELECT is_banned FROM public.teams WHERE id = $1",
		func(rows pgx.Rows) error {
			require.True(t, rows.Next())

			return rows.Scan(&banned)
		}, teamID))

	return banned
}

func banLedgerRevision(t *testing.T, db *testutils.Database, teamID uuid.UUID) *int64 {
	t.Helper()

	var revision *int64
	require.NoError(t, db.SqlcClient.TestsRawSQLQuery(t.Context(),
		"SELECT revision FROM projection.project_bans WHERE project_id = $1",
		func(rows pgx.Rows) error {
			if !rows.Next() {
				return nil
			}

			return rows.Scan(&revision)
		}, teamID))

	return revision
}

func containmentJobs(t *testing.T, db *testutils.Database, teamID uuid.UUID) int {
	t.Helper()

	var jobs int
	require.NoError(t, db.SqlcClient.TestsRawSQLQuery(t.Context(),
		"SELECT count(*) FROM river.river_job WHERE kind = 'contain_banned_team' AND args->>'team_id' = $1",
		func(rows pgx.Rows) error {
			require.True(t, rows.Next())

			return rows.Scan(&jobs)
		}, teamID.String()))

	return jobs
}
