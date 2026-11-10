package handlers

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
)

func TestApplyProjectBlockIsVisibleToTheAuthPath(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	teamID := testutils.CreateTestTeam(t, db)
	store, auth := newManagementStore(t, db)

	recorder := callApplyProjectBlock(t, store, teamID, `{"revision": 1, "blocked": true, "reason": "credit_exhausted"}`)
	require.Equal(t, http.StatusNoContent, recorder.Code, recorder.Body.String())

	blocked, reason := readTeamBlockState(t, db, teamID)
	require.True(t, blocked)
	require.Equal(t, "credit_exhausted", *reason)

	require.Equal(t, []uuid.UUID{teamID}, *auth.invalidatedTeamIDs)
}

func TestApplyProjectBlockRecordsWhenItsRevisionWasDecided(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	teamID := testutils.CreateTestTeam(t, db)
	store, _ := newManagementStore(t, db)

	body := `{"revision": 2, "blocked": true, "decided_at": "2026-09-29T08:00:00.123456789Z"}`
	require.Equal(t, http.StatusNoContent, callApplyProjectBlock(t, store, teamID, body).Code)
	require.Equal(t, time.Date(2026, 9, 29, 8, 0, 0, 123456000, time.UTC), *readBlockDecidedAt(t, db, teamID),
		"stored to the microsecond Postgres keeps")
}

func TestApplyProjectBlockRefusesWithoutWritingAnything(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)

	for name, tc := range map[string]struct {
		knownProject bool
		body         string
		status       int
	}{
		"a revision that is not positive": {knownProject: true, body: `{"revision": 0, "blocked": true}`, status: http.StatusBadRequest},
		"a malformed body":                {knownProject: true, body: `{"revision":`, status: http.StatusBadRequest},
		"an unknown project":              {knownProject: false, body: `{"revision": 1, "blocked": true}`, status: http.StatusNotFound},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			store, auth := newManagementStore(t, db)
			projectID := uuid.New()
			if tc.knownProject {
				projectID = testutils.CreateTestTeam(t, db)
			}

			recorder := callApplyProjectBlock(t, store, projectID, tc.body)

			require.Equal(t, tc.status, recorder.Code, recorder.Body.String())
			require.Empty(t, *auth.invalidatedTeamIDs)
		})
	}
}

func callApplyProjectBlock(t *testing.T, store *APIStore, teamID uuid.UUID, body string) *httptest.ResponseRecorder {
	t.Helper()

	return callManagement(t, store, http.MethodPut, "/unstable/management/projects/"+teamID.String()+"/block", body)
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

func readBlockDecidedAt(t *testing.T, db *testutils.Database, teamID uuid.UUID) *time.Time {
	t.Helper()

	var decidedAt *time.Time
	require.NoError(t, db.SqlcClient.TestsRawSQLQuery(t.Context(),
		"SELECT decided_at FROM projection.project_blocks WHERE project_id = $1",
		func(rows pgx.Rows) error {
			require.True(t, rows.Next(), "no project_blocks ledger row")

			return rows.Scan(&decidedAt)
		}, teamID))
	if decidedAt != nil {
		utc := decidedAt.UTC()
		decidedAt = &utc
	}

	return decidedAt
}
