package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/e2b-dev/infra/packages/api/internal/management"
	sharedauth "github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
	redisutils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

func TestApplyProjectBanRefusesTheTeamsKeysAndEnqueuesItsContainment(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	ctx := t.Context()
	auth, err := sharedauth.NewAuthService(ctx, redisutils.SetupInstance(t), db.AuthDB, sharedauth.ProviderConfig{}, http.DefaultClient)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, auth.Close(context.WithoutCancel(ctx))) })
	store, _ := newManagementStore(t, db)
	store.managementService = management.NewService(db.AuthDB, db.SqlcClient, auth, noop.NewMeterProvider())
	teamID := testutils.CreateTestTeam(t, db)
	key := addProjectAPIKey(t, db, teamID)
	ginCtx, _ := gin.CreateTestContext(httptest.NewRecorder())
	ginCtx.Request = httptest.NewRequestWithContext(ctx, http.MethodGet, "/", nil)
	_, apiErr := auth.ValidateAPIKey(ctx, ginCtx, key.PrefixedRawValue)
	require.Nil(t, apiErr)

	recorder := callApplyProjectBan(t, store, teamID, `{"revision": 1, "banned": true}`)
	require.Equal(t, http.StatusNoContent, recorder.Code, recorder.Body.String())

	_, apiErr = auth.ValidateAPIKey(ctx, ginCtx, key.PrefixedRawValue)
	require.NotNil(t, apiErr)
	require.Equal(t, http.StatusForbidden, apiErr.Code)
	require.Equal(t, 1, containmentJobs(t, db, teamID))
}

func TestApplyProjectBanRecordsWhenItsRevisionWasDecided(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	teamID := testutils.CreateTestTeam(t, db)
	store, _ := newManagementStore(t, db)

	body := `{"revision": 2, "banned": true, "decided_at": "2026-09-29T08:00:00.123456789Z"}`
	require.Equal(t, http.StatusNoContent, callApplyProjectBan(t, store, teamID, body).Code)
	require.Equal(t, time.Date(2026, 9, 29, 8, 0, 0, 123456000, time.UTC), *readBanDecidedAt(t, db, teamID),
		"stored to the microsecond Postgres keeps")
}

func TestApplyProjectBanRefusesWithoutWritingAnything(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)

	for name, tc := range map[string]struct {
		knownProject bool
		body         string
		status       int
	}{
		"a revision that is not positive": {knownProject: true, body: `{"revision": 0, "banned": true}`, status: http.StatusBadRequest},
		"a malformed body":                {knownProject: true, body: `{"revision":`, status: http.StatusBadRequest},
		"an unknown project":              {knownProject: false, body: `{"revision": 1, "banned": true}`, status: http.StatusNotFound},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			store, auth := newManagementStore(t, db)
			projectID := uuid.New()
			if tc.knownProject {
				projectID = testutils.CreateTestTeam(t, db)
			}

			recorder := callApplyProjectBan(t, store, projectID, tc.body)

			require.Equal(t, tc.status, recorder.Code, recorder.Body.String())
			require.Empty(t, *auth.invalidatedTeamIDs)
			require.Zero(t, containmentJobs(t, db, projectID))
		})
	}
}

func callApplyProjectBan(t *testing.T, store *APIStore, teamID uuid.UUID, body string) *httptest.ResponseRecorder {
	t.Helper()

	return callManagement(t, store, http.MethodPut, "/unstable/management/projects/"+teamID.String()+"/ban", body)
}

func containmentJobs(t *testing.T, db *testutils.Database, projectID uuid.UUID) int {
	t.Helper()

	var jobs int
	queryRow(t, db, `SELECT count(*) FROM river.river_job WHERE kind = 'contain_banned_team' AND args->>'team_id' = $1`,
		[]any{projectID.String()}, &jobs)

	return jobs
}

func readBanDecidedAt(t *testing.T, db *testutils.Database, teamID uuid.UUID) *time.Time {
	t.Helper()

	var decidedAt *time.Time
	require.NoError(t, db.SqlcClient.TestsRawSQLQuery(t.Context(),
		"SELECT decided_at FROM projection.project_bans WHERE project_id = $1",
		func(rows pgx.Rows) error {
			require.True(t, rows.Next(), "no project_bans ledger row")

			return rows.Scan(&decidedAt)
		}, teamID))
	if decidedAt != nil {
		utc := decidedAt.UTC()
		decidedAt = &utc
	}

	return decidedAt
}
