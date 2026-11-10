package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivertest"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/api/internal/team"
	teammocks "github.com/e2b-dev/infra/packages/api/internal/team/mocks"
	template_manager "github.com/e2b-dev/infra/packages/api/internal/template-manager"
	dbmodule "github.com/e2b-dev/infra/packages/db"
	"github.com/e2b-dev/infra/packages/db/pkg/outbox"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	"github.com/e2b-dev/infra/packages/shared/pkg/sharedriver"
)

type containmentMocks struct {
	sandboxes *teammocks.MockSandboxKiller
	builds    *teammocks.MockBuildCanceller
}

func newContainmentMocks(t *testing.T) (containmentMocks, *team.Service) {
	t.Helper()

	mocks := containmentMocks{
		sandboxes: teammocks.NewMockSandboxKiller(t),
		builds:    teammocks.NewMockBuildCanceller(t),
	}

	return mocks, team.NewService(nil, teammocks.NewMockTeamCache(t), mocks.sandboxes, mocks.builds, time.Minute)
}

func bannedTeam(t *testing.T, db *testutils.Database) uuid.UUID {
	t.Helper()

	teamID := testutils.CreateTestTeam(t, db)
	require.NoError(t, db.AuthDb.TestsRawSQL(t.Context(), `UPDATE teams SET is_banned = true WHERE id = $1`, teamID))

	return teamID
}

func workContainment(t *testing.T, db *testutils.Database, cache teamCache, container teamContainer, settle time.Duration, teamID uuid.UUID, opts *river.InsertOpts) (*rivertest.WorkResult, error) {
	t.Helper()

	worker := rivertest.NewWorker(t, riverpgxv5.New(nil), &river.Config{Schema: dbmodule.RiverSchema}, &bannedTeamContainmentWorker{
		teams:     db.AuthDb,
		cache:     cache,
		container: container,
		settle:    settle,
		steps:     sharedriver.NewSteps(nil, nil, sharedriver.StepsConfig{MetricPrefix: "api"}).Job(outbox.ContainBannedTeam{}.Kind()),
	})
	tx, err := db.SqlcClient.Pool().Begin(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback(context.WithoutCancel(t.Context())) })

	return worker.Work(t.Context(), t, tx, outbox.ContainBannedTeam{TeamID: teamID}, opts)
}

func recordedEviction(t *testing.T, result *rivertest.WorkResult) time.Time {
	t.Helper()

	var progress containmentProgress
	require.NoError(t, json.Unmarshal(result.Job.Output(), &progress))

	return progress.EvictedAt
}

func TestContainBannedTeamWorker(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)

	t.Run("a banned team has its sandboxes killed and its builds cancelled", func(t *testing.T) {
		t.Parallel()

		mocks, container := newContainmentMocks(t)
		cache := teammocks.NewMockTeamCache(t)
		teamID := bannedTeam(t, db)
		evict := cache.EXPECT().InvalidateTeamCache(mock.Anything, teamID).Return(nil).Once()
		kill := mocks.sandboxes.EXPECT().KillTeamSandboxes(mock.Anything, teamID, sandbox.KillReasonAdmin).Return(2, 0, nil).Once()
		cancel := mocks.builds.EXPECT().CancelTeamBuilds(mock.Anything, teamID, template_manager.CancelledByAdminReason).Return(1, 0, nil).Once()
		kill.NotBefore(evict)
		cancel.NotBefore(evict)

		result, err := workContainment(t, db, cache, container, 0, teamID, nil)
		require.NoError(t, err)
		require.Equal(t, river.EventKindJobCompleted, result.EventKind)
	})

	t.Run("a sweep a settle delay after the recorded eviction completes the job", func(t *testing.T) {
		t.Parallel()

		mocks, container := newContainmentMocks(t)
		cache := teammocks.NewMockTeamCache(t)
		teamID := bannedTeam(t, db)
		cache.EXPECT().InvalidateTeamCache(mock.Anything, teamID).Return(nil).Once()
		mocks.sandboxes.EXPECT().KillTeamSandboxes(mock.Anything, teamID, mock.Anything).Return(0, 0, nil).Once()
		mocks.builds.EXPECT().CancelTeamBuilds(mock.Anything, teamID, mock.Anything).Return(0, 0, nil).Once()
		evictedAt := time.Now().Add(-2 * time.Hour).UTC().Truncate(time.Second)
		metadata, err := json.Marshal(map[string]containmentProgress{"output": {EvictedAt: evictedAt}})
		require.NoError(t, err)

		result, err := workContainment(t, db, cache, container, time.Hour, teamID, &river.InsertOpts{Metadata: metadata})
		require.NoError(t, err)
		require.Equal(t, river.EventKindJobCompleted, result.EventKind)
		require.True(t, recordedEviction(t, result).Equal(evictedAt), "a later eviction must not restart the window")
	})

	t.Run("the first eviction starts the settle window and the job sweeps again after it", func(t *testing.T) {
		t.Parallel()

		mocks, container := newContainmentMocks(t)
		cache := teammocks.NewMockTeamCache(t)
		teamID := bannedTeam(t, db)
		cache.EXPECT().InvalidateTeamCache(mock.Anything, teamID).Return(nil).Once()
		mocks.sandboxes.EXPECT().KillTeamSandboxes(mock.Anything, teamID, mock.Anything).Return(0, 0, nil).Once()
		mocks.builds.EXPECT().CancelTeamBuilds(mock.Anything, teamID, mock.Anything).Return(0, 0, nil).Once()

		result, err := workContainment(t, db, cache, container, time.Hour, teamID, nil)
		require.NoError(t, err)
		require.Equal(t, river.EventKindJobSnoozed, result.EventKind)
		require.WithinDuration(t, time.Now(), recordedEviction(t, result), time.Minute)
	})

	t.Run("a sweep that started inside the window sweeps again even when it ends after it", func(t *testing.T) {
		t.Parallel()

		mocks, container := newContainmentMocks(t)
		cache := teammocks.NewMockTeamCache(t)
		teamID := bannedTeam(t, db)
		cache.EXPECT().InvalidateTeamCache(mock.Anything, teamID).Return(nil).Once()
		mocks.sandboxes.EXPECT().KillTeamSandboxes(mock.Anything, teamID, mock.Anything).RunAndReturn(
			func(context.Context, uuid.UUID, sandbox.KillReason) (int, int, error) {
				time.Sleep(200 * time.Millisecond)

				return 3, 0, nil
			}).Once()
		mocks.builds.EXPECT().CancelTeamBuilds(mock.Anything, teamID, mock.Anything).Return(0, 0, nil).Once()

		result, err := workContainment(t, db, cache, container, 50*time.Millisecond, teamID, nil)
		require.NoError(t, err)
		require.Equal(t, river.EventKindJobSnoozed, result.EventKind)
	})

	t.Run("a failed cache eviction fails the attempt before anything is contained", func(t *testing.T) {
		t.Parallel()

		_, container := newContainmentMocks(t)
		cache := teammocks.NewMockTeamCache(t)
		teamID := bannedTeam(t, db)
		unavailable := errors.New("redis unavailable")
		cache.EXPECT().InvalidateTeamCache(mock.Anything, teamID).Return(unavailable).Once()

		result, err := workContainment(t, db, cache, container, 0, teamID, nil)
		require.ErrorIs(t, err, unavailable)
		require.Equal(t, river.EventKindJobFailed, result.EventKind)
		require.Nil(t, result.Job.Output(), "a failed eviction must not start the settle window")
	})

	t.Run("a team unbanned before the job ran keeps its workloads", func(t *testing.T) {
		t.Parallel()

		_, container := newContainmentMocks(t)

		result, err := workContainment(t, db, teammocks.NewMockTeamCache(t), container, 0, testutils.CreateTestTeam(t, db), nil)
		require.NoError(t, err)
		require.Equal(t, river.EventKindJobCompleted, result.EventKind)
	})

	t.Run("a team whose row is gone has nothing to contain", func(t *testing.T) {
		t.Parallel()

		_, container := newContainmentMocks(t)

		result, err := workContainment(t, db, teammocks.NewMockTeamCache(t), container, 0, uuid.New(), nil)
		require.NoError(t, err)
		require.Equal(t, river.EventKindJobCompleted, result.EventKind)
	})

	t.Run("a sandbox that failed to die fails the attempt so the job retries", func(t *testing.T) {
		t.Parallel()

		mocks, container := newContainmentMocks(t)
		cache := teammocks.NewMockTeamCache(t)
		teamID := bannedTeam(t, db)
		cache.EXPECT().InvalidateTeamCache(mock.Anything, teamID).Return(nil).Once()
		mocks.sandboxes.EXPECT().KillTeamSandboxes(mock.Anything, teamID, mock.Anything).Return(1, 1, nil).Once()
		mocks.builds.EXPECT().CancelTeamBuilds(mock.Anything, teamID, mock.Anything).Return(0, 0, nil).Once()

		result, err := workContainment(t, db, cache, container, 0, teamID, nil)
		require.ErrorIs(t, err, errContainmentIncomplete)
		require.Equal(t, river.EventKindJobFailed, result.EventKind)
	})

	t.Run("builds that cannot be listed fail the attempt so the job retries", func(t *testing.T) {
		t.Parallel()

		mocks, container := newContainmentMocks(t)
		cache := teammocks.NewMockTeamCache(t)
		teamID := bannedTeam(t, db)
		unavailable := errors.New("database unavailable")
		cache.EXPECT().InvalidateTeamCache(mock.Anything, teamID).Return(nil).Once()
		mocks.sandboxes.EXPECT().KillTeamSandboxes(mock.Anything, teamID, mock.Anything).Return(0, 0, nil).Once()
		mocks.builds.EXPECT().CancelTeamBuilds(mock.Anything, teamID, mock.Anything).Return(0, 0, unavailable).Once()

		result, err := workContainment(t, db, cache, container, 0, teamID, nil)
		require.ErrorIs(t, err, unavailable)
		require.Equal(t, river.EventKindJobFailed, result.EventKind)
	})
}

func TestContainBannedTeamRunsThroughTheOutboxClient(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	mocks, container := newContainmentMocks(t)
	cache := teammocks.NewMockTeamCache(t)
	outboxRiver, err := New(Dependencies{
		Pool:        db.SqlcClient.Pool(),
		Sandboxes:   &teardownRecorder{},
		Containment: container,
		TeamCache:   cache,
		Teams:       db.AuthDb,
		Config:      Config{MaxWorkers: 1, BacklogInterval: time.Second},
	})
	require.NoError(t, err)

	snoozed, cancelSubscription := outboxRiver.client.Subscribe(river.EventKindJobSnoozed)
	defer cancelSubscription()

	require.NoError(t, outboxRiver.Start(t.Context()))
	t.Cleanup(func() { require.NoError(t, outboxRiver.Stop(context.WithoutCancel(t.Context()))) })

	teamID := bannedTeam(t, db)
	cache.EXPECT().InvalidateTeamCache(mock.Anything, teamID).Return(nil).Once()
	mocks.sandboxes.EXPECT().KillTeamSandboxes(mock.Anything, teamID, mock.Anything).Return(1, 0, nil).Once()
	mocks.builds.EXPECT().CancelTeamBuilds(mock.Anything, teamID, mock.Anything).Return(0, 0, nil).Once()

	inserted, err := outboxRiver.Client().Insert(t.Context(), outbox.ContainBannedTeam{TeamID: teamID}, nil)
	require.NoError(t, err)

	select {
	case event := <-snoozed:
		require.Equal(t, inserted.Job.ID, event.Job.ID)
	case <-time.After(30 * time.Second):
		t.Fatal("the containment job did not run")
	}
}
