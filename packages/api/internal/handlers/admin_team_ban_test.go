package handlers

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/api/internal/team"
	teammocks "github.com/e2b-dev/infra/packages/api/internal/team/mocks"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
)

type teamBanMocks struct {
	cache     *teammocks.MockTeamCache
	sandboxes *teammocks.MockSandboxKiller
	builds    *teammocks.MockBuildCanceller
}

// testBanRequestBudget is short so a test that waits out the budget stays fast,
// and long enough that the deletion check and ban write it also covers finish
// inside it under a loaded race-enabled run.
const testBanRequestBudget = time.Second

func newTeamBanStore(t *testing.T, db *testutils.Database) (*APIStore, teamBanMocks) {
	t.Helper()

	mocks := teamBanMocks{
		cache:     teammocks.NewMockTeamCache(t),
		sandboxes: teammocks.NewMockSandboxKiller(t),
		builds:    teammocks.NewMockBuildCanceller(t),
	}

	return &APIStore{
		sqlcDB:      db.SqlcClient,
		teamService: team.NewService(db.SqlcClient, mocks.cache, mocks.sandboxes, mocks.builds, testBanRequestBudget),
	}, mocks
}

func TestBanTeamAnswersWithWhatContainmentDid(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)

	stopped := containmentStep{done: 2, failed: 1}
	for name, tc := range map[string]struct {
		alreadyBanned bool
		kill          containmentStep
		cancel        containmentStep
		want          string
	}{
		"a team whose workloads all stop": {
			kill:   stopped,
			cancel: stopped,
			want:   `{"teamID":%q,"sandboxesKilled":2,"sandboxesFailedToKill":1,"buildsCancelled":2,"buildsFailedToCancel":1}`,
		},
		"an already banned team": {
			alreadyBanned: true,
			kill:          stopped,
			cancel:        stopped,
			want:          `{"teamID":%q,"sandboxesKilled":2,"sandboxesFailedToKill":1,"buildsCancelled":2,"buildsFailedToCancel":1}`,
		},
		"a team whose sandboxes cannot be listed": {
			kill:   containmentStep{err: errors.New("orchestrator unavailable")},
			cancel: stopped,
			want:   `{"teamID":%q,"sandboxesKilled":0,"sandboxesFailedToKill":0,"sandboxKillError":"Failed to kill the team's sandboxes","buildsCancelled":2,"buildsFailedToCancel":1}`,
		},
		"a team whose builds cannot be listed": {
			kill:   stopped,
			cancel: containmentStep{err: errors.New("database unavailable")},
			want:   `{"teamID":%q,"sandboxesKilled":2,"sandboxesFailedToKill":1,"buildsCancelled":0,"buildsFailedToCancel":0,"buildCancelError":"Failed to cancel the team's builds"}`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			store, mocks := newTeamBanStore(t, db)
			teamID := testutils.CreateTestTeam(t, db)
			if tc.alreadyBanned {
				require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(), "UPDATE public.teams SET is_banned = true WHERE id = $1", teamID))
			}
			mocks.cache.EXPECT().InvalidateTeamCache(mock.Anything, teamID).Return(nil)
			mocks.sandboxes.EXPECT().KillTeamSandboxes(mock.Anything, teamID, mock.Anything).Return(tc.kill.done, tc.kill.failed, tc.kill.err)
			mocks.builds.EXPECT().CancelTeamBuilds(mock.Anything, teamID, mock.Anything).Return(tc.cancel.done, tc.cancel.failed, tc.cancel.err)

			response := callManagement(t, store, http.MethodPut, "/unstable/admin/teams/"+teamID.String()+"/ban", "")

			require.Equal(t, http.StatusOK, response.Code, response.Body.String())
			require.JSONEq(t, fmt.Sprintf(tc.want, teamID), response.Body.String())
			require.True(t, teamIsBanned(t, db, teamID))
		})
	}
}

func TestBanTeamFailsWhenTheCacheKeepsTheTeam(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store, mocks := newTeamBanStore(t, db)
	teamID := testutils.CreateTestTeam(t, db)
	mocks.cache.EXPECT().InvalidateTeamCache(mock.Anything, teamID).Return(errors.New("cache unavailable"))

	response := callManagement(t, store, http.MethodPut, "/unstable/admin/teams/"+teamID.String()+"/ban", "")

	require.Equal(t, http.StatusInternalServerError, response.Code)
	require.JSONEq(t, `{"code":500,"message":"Failed to ban team"}`, response.Body.String())
}

func TestUnbanTeamClearsTheBan(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store, mocks := newTeamBanStore(t, db)
	teamID := testutils.CreateTestTeam(t, db)
	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(), "UPDATE public.teams SET is_banned = true WHERE id = $1", teamID))
	mocks.cache.EXPECT().InvalidateTeamCache(mock.Anything, teamID).Return(nil)

	response := callManagement(t, store, http.MethodDelete, "/unstable/admin/teams/"+teamID.String()+"/ban", "")

	require.Equal(t, http.StatusNoContent, response.Code, response.Body.String())
	require.False(t, teamIsBanned(t, db, teamID))
}

// A deleted project keeps its team row. The mocks expect nothing, so reaching
// the ban itself fails the test.
func TestBanAndUnbanAnswerNotFoundForATeamThatIsGone(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)

	for name, tc := range map[string]struct {
		method  string
		deleted bool
	}{
		"banning an unknown team":   {method: http.MethodPut},
		"unbanning an unknown team": {method: http.MethodDelete},
		"banning a deleted team":    {method: http.MethodPut, deleted: true},
		"unbanning a deleted team":  {method: http.MethodDelete, deleted: true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			store, _ := newTeamBanStore(t, db)
			teamID := uuid.New()
			if tc.deleted {
				teamID = testutils.CreateTestTeam(t, db)
				markProjectDeleted(t, db, teamID)
			}

			response := callManagement(t, store, tc.method, "/unstable/admin/teams/"+teamID.String()+"/ban", "")

			require.Equal(t, http.StatusNotFound, response.Code)
			require.JSONEq(t, `{"code":404,"message":"Team not found"}`, response.Body.String())
		})
	}
}

func TestBanTeamsSortsEveryTeamIntoBannedNotFoundOrFailed(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store, mocks := newTeamBanStore(t, db)
	banned := testutils.CreateTestTeam(t, db)
	unknown := uuid.New()
	deleted := testutils.CreateTestTeam(t, db)
	markProjectDeleted(t, db, deleted)
	failing := testutils.CreateTestTeam(t, db)
	mocks.cache.EXPECT().InvalidateTeamCache(mock.Anything, banned).Return(nil)
	mocks.cache.EXPECT().InvalidateTeamCache(mock.Anything, failing).Return(errors.New("cache unavailable"))
	mocks.sandboxes.EXPECT().KillTeamSandboxes(mock.Anything, banned, mock.Anything).Return(1, 0, nil)
	mocks.builds.EXPECT().CancelTeamBuilds(mock.Anything, banned, mock.Anything).Return(0, 0, nil)

	response := callManagement(t, store, http.MethodPost, "/unstable/admin/teams/bans",
		fmt.Sprintf(`{"teamIDs":[%q,%q,%q,%q]}`, banned, unknown, deleted, failing))

	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	require.JSONEq(t, fmt.Sprintf(`{
		"banned": [{"teamID":%q,"sandboxesKilled":1,"sandboxesFailedToKill":0,"buildsCancelled":0,"buildsFailedToCancel":0}],
		"notFound": [%q,%q],
		"failed": [{"teamID":%q,"error":"Failed to ban team"}]
	}`, banned, deleted, unknown, failing), response.Body.String())
}

// containmentStep is what a kill or cancel returns. One that fails as a whole
// counts nothing, as the real ones do.
type containmentStep struct {
	done   int
	failed int
	err    error
}

func TestBanTeamCancelsBuildsWhileAKillUsesUpTheBudget(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store, mocks := newTeamBanStore(t, db)
	teamID := testutils.CreateTestTeam(t, db)
	mocks.cache.EXPECT().InvalidateTeamCache(mock.Anything, teamID).Return(nil)
	mocks.sandboxes.EXPECT().KillTeamSandboxes(mock.Anything, teamID, mock.Anything).RunAndReturn(killUntilTheBudgetRunsOut)
	mocks.builds.EXPECT().CancelTeamBuilds(mock.Anything, teamID, mock.Anything).RunAndReturn(cancelTwoBuildsWithinTheBudget)

	response := callManagement(t, store, http.MethodPut, "/unstable/admin/teams/"+teamID.String()+"/ban", "")

	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	require.JSONEq(t, fmt.Sprintf(`{"teamID":%q,"sandboxesKilled":1,"sandboxesFailedToKill":1,"sandboxKillError":"Failed to kill the team's sandboxes",
		"buildsCancelled":2,"buildsFailedToCancel":0}`, teamID), response.Body.String(),
		"the builds are cancelled while the kill waits out the budget, not after it")
	require.True(t, teamIsBanned(t, db, teamID))
}

// More teams than are banned at once, so a slow containment of the first ones
// would otherwise hold back the later bans.
func TestBanTeamsStoresEveryBanBeforeContainingAny(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store, mocks := newTeamBanStore(t, db)
	teamIDs := make([]uuid.UUID, 7)
	for i := range teamIDs {
		teamIDs[i] = testutils.CreateTestTeam(t, db)
	}
	var containedBeforeAllBanned atomic.Bool
	mocks.cache.EXPECT().InvalidateTeamCache(mock.Anything, mock.Anything).Return(nil).Times(len(teamIDs))
	mocks.sandboxes.EXPECT().KillTeamSandboxes(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, teamID uuid.UUID, reason sandbox.KillReason) (int, int, error) {
			// The later teams start after the budget ran out, so the check
			// cannot share its deadline.
			if banned, err := countBanned(context.WithoutCancel(ctx), db, teamIDs); err != nil || banned != len(teamIDs) {
				containedBeforeAllBanned.Store(true)
			}

			return killUntilTheBudgetRunsOut(ctx, teamID, reason)
		}).Times(len(teamIDs))
	mocks.builds.EXPECT().CancelTeamBuilds(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(cancelTwoBuildsWithinTheBudget).Times(len(teamIDs))

	quoted := make([]string, len(teamIDs))
	for i, teamID := range teamIDs {
		quoted[i] = fmt.Sprintf("%q", teamID)
	}
	response := callManagement(t, store, http.MethodPost, "/unstable/admin/teams/bans", `{"teamIDs":[`+strings.Join(quoted, ",")+`]}`)

	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	require.False(t, containedBeforeAllBanned.Load(), "a team was contained before every ban was stored")
	banned, err := countBanned(t.Context(), db, teamIDs)
	require.NoError(t, err)
	require.Equal(t, len(teamIDs), banned)
}

func TestBanTeamsStoresTheBansOfACallerThatHungUp(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store, mocks := newTeamBanStore(t, db)
	teamIDs := []uuid.UUID{testutils.CreateTestTeam(t, db), testutils.CreateTestTeam(t, db)}
	mocks.cache.EXPECT().InvalidateTeamCache(mock.Anything, mock.Anything).Return(nil).Times(2)
	mocks.sandboxes.EXPECT().KillTeamSandboxes(mock.Anything, mock.Anything, mock.Anything).Return(0, 0, nil).Times(2)
	mocks.builds.EXPECT().CancelTeamBuilds(mock.Anything, mock.Anything, mock.Anything).Return(0, 0, nil).Times(2)

	ctx, hangUp := context.WithCancel(t.Context())
	hangUp()
	router := gin.New()
	api.RegisterHandlers(router, store)
	request := httptest.NewRequestWithContext(ctx, http.MethodPost, "/unstable/admin/teams/bans",
		strings.NewReader(fmt.Sprintf(`{"teamIDs":[%q,%q]}`, teamIDs[0], teamIDs[1])))
	request.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(httptest.NewRecorder(), request)

	banned, err := countBanned(t.Context(), db, teamIDs)
	require.NoError(t, err)
	require.Equal(t, len(teamIDs), banned)
}

func TestBanTeamFailsAWriteThatOutlastsTheBudget(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store, _ := newTeamBanStore(t, db)
	teamID := testutils.CreateTestTeam(t, db)
	holdTeamRowLock(t, db, teamID)

	response := callManagement(t, store, http.MethodPut, "/unstable/admin/teams/"+teamID.String()+"/ban", "")

	require.Equal(t, http.StatusInternalServerError, response.Code, response.Body.String())
	require.JSONEq(t, `{"code":500,"message":"Failed to ban team"}`, response.Body.String())
}

func TestBanTeamsFailsATeamWhoseWriteOutlastsTheBudgetAndBansTheRest(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store, mocks := newTeamBanStore(t, db)
	stuck := testutils.CreateTestTeam(t, db)
	banned := testutils.CreateTestTeam(t, db)
	holdTeamRowLock(t, db, stuck)
	mocks.cache.EXPECT().InvalidateTeamCache(mock.Anything, banned).Return(nil)
	mocks.sandboxes.EXPECT().KillTeamSandboxes(mock.Anything, banned, mock.Anything).RunAndReturn(killUntilTheBudgetRunsOut)
	mocks.builds.EXPECT().CancelTeamBuilds(mock.Anything, banned, mock.Anything).RunAndReturn(cancelTwoBuildsWithinTheBudget)

	response := callManagement(t, store, http.MethodPost, "/unstable/admin/teams/bans",
		fmt.Sprintf(`{"teamIDs":[%q,%q]}`, stuck, banned))

	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	require.JSONEq(t, fmt.Sprintf(`{
		"banned": [{"teamID":%q,"sandboxesKilled":1,"sandboxesFailedToKill":1,"sandboxKillError":"Failed to kill the team's sandboxes",
			"buildsCancelled":0,"buildsFailedToCancel":0,"buildCancelError":"Failed to cancel the team's builds"}],
		"notFound": [],
		"failed": [{"teamID":%q,"error":"Failed to ban team"}]
	}`, banned, stuck), response.Body.String(), "the stuck write spent the budget, so the banned team's containment is reported as cut short")
	require.True(t, teamIsBanned(t, db, banned))
}

// holdTeamRowLock keeps the team's row locked until the test ends, so a ban
// write to it blocks until its context ends.
func holdTeamRowLock(t *testing.T, db *testutils.Database, teamID uuid.UUID) {
	t.Helper()

	tx, err := db.SqlcClient.Pool().Begin(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = tx.Rollback(context.WithoutCancel(t.Context()))
	})
	_, err = tx.Exec(t.Context(), "SELECT 1 FROM public.teams WHERE id = $1 FOR UPDATE", teamID)
	require.NoError(t, err)
}

// killUntilTheBudgetRunsOut kills one sandbox and fails one, then waits out
// the budget, as a team with more sandboxes than the budget allows does.
func killUntilTheBudgetRunsOut(ctx context.Context, _ uuid.UUID, _ sandbox.KillReason) (int, int, error) {
	<-ctx.Done()

	return 1, 1, nil
}

// cancelTwoBuildsWithinTheBudget cancels two builds, or fails to list them once
// the budget is spent, as the real query does.
func cancelTwoBuildsWithinTheBudget(ctx context.Context, _ uuid.UUID, _ string) (int, int, error) {
	if err := ctx.Err(); err != nil {
		return 0, 0, err
	}

	return 2, 0, nil
}

func countBanned(ctx context.Context, db *testutils.Database, teamIDs []uuid.UUID) (int, error) {
	var banned int
	err := db.SqlcClient.TestsRawSQLQuery(ctx,
		"SELECT count(*) FROM public.teams WHERE id = ANY($1) AND is_banned",
		func(rows pgx.Rows) error {
			rows.Next()

			return rows.Scan(&banned)
		}, teamIDs)

	return banned, err
}

func teamIsBanned(t *testing.T, db *testutils.Database, teamID uuid.UUID) bool {
	t.Helper()

	team, err := db.AuthDB.GetTeamByTeamID(t.Context(), teamID)
	require.NoError(t, err)

	return team.IsBanned
}
