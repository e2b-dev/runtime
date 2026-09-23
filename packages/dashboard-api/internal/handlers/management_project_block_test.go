package handlers

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/dashboard-api/internal/management"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
)

func TestApplyProjectBlockIsVisibleToTheAuthPath(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	teamID := testutils.CreateTestTeam(t, db)
	store, auth := newBlockStore(db)

	recorder := callApplyProjectBlock(t, store, teamID, `{"revision": 1, "blocked": true, "reason": "credit_exhausted"}`)
	require.Equal(t, http.StatusNoContent, recorder.Code, recorder.Body.String())

	blocked, reason := readTeamBlockState(t, db, teamID)
	require.True(t, blocked)
	require.Equal(t, "credit_exhausted", *reason)

	require.Equal(t, []uuid.UUID{teamID}, auth.invalidated)
}

func TestApplyProjectBlockRejectsARevisionThatIsNotPositive(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	teamID := testutils.CreateTestTeam(t, db)
	store, auth := newBlockStore(db)

	recorder := callApplyProjectBlock(t, store, teamID, `{"revision": 0, "blocked": true}`)
	require.Equal(t, http.StatusBadRequest, recorder.Code)
	require.Empty(t, auth.invalidated)
}

func TestApplyProjectBlockRejectsAnUnknownProject(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store, auth := newBlockStore(db)

	recorder := callApplyProjectBlock(t, store, uuid.New(), `{"revision": 1, "blocked": true}`)
	require.Equal(t, http.StatusNotFound, recorder.Code)
	require.Empty(t, auth.invalidated, "nothing was written, so nothing should be invalidated")
}

func TestApplyProjectBlockRejectsAMalformedBody(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	teamID := testutils.CreateTestTeam(t, db)
	store, auth := newBlockStore(db)

	recorder := callApplyProjectBlock(t, store, teamID, `{"revision":`)
	require.Equal(t, http.StatusBadRequest, recorder.Code)
	require.Empty(t, auth.invalidated)
}

func newBlockStore(db *testutils.Database) (*APIStore, *recordingCacheAuthService) {
	auth := &recordingCacheAuthService{}

	return &APIStore{
		db:                db.SqlcClient,
		authService:       auth,
		managementService: management.NewService(db.AuthDB, db.SqlcClient, auth),
	}, auth
}

func callApplyProjectBlock(t *testing.T, store *APIStore, teamID uuid.UUID, body string) *httptest.ResponseRecorder {
	t.Helper()

	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Request = httptest.NewRequestWithContext(t.Context(), http.MethodPut,
		"/v1/management/projects/"+teamID.String()+"/block", strings.NewReader(body))
	ginCtx.Request.Header.Set("Content-Type", "application/json")

	store.ManagementApplyProjectBlock(ginCtx, teamID)
	ginCtx.Writer.WriteHeaderNow()

	return recorder
}

func readTeamBlockState(t *testing.T, db *testutils.Database, teamID uuid.UUID) (bool, *string) {
	t.Helper()

	var blocked bool
	var reason *string
	require.NoError(t, db.SqlcClient.TestsRawSQLQuery(t.Context(),
		"SELECT is_blocked, blocked_reason FROM public.teams WHERE id = $1",
		func(rows pgx.Rows) error {
			rows.Next()

			return rows.Scan(&blocked, &reason)
		}, teamID))

	return blocked, reason
}
