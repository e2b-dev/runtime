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
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/e2b-dev/infra/packages/dashboard-api/internal/management"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
)

func TestApplyProjectMember(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	projectID := testutils.CreateTestTeam(t, db)
	store := newMembershipStore(db)
	userID := uuid.New()

	require.Equal(t, http.StatusNoContent, callApplyProjectMember(t, store, projectID, userID,
		`{"revision":1,"present":true,"is_default":true,"identities":[{"issuer":"https://issuer.test","subject":"subject"}]}`).Code)
	require.Equal(t, []uuid.UUID{userID}, handlerTeamMembers(t, db, projectID))
	require.True(t, handlerTeamMemberIsDefault(t, db, projectID, userID))

	require.Equal(t, http.StatusNoContent, callApplyProjectMember(t, store, projectID, userID,
		`{"revision":2,"present":false}`).Code)
	require.Empty(t, handlerTeamMembers(t, db, projectID))
}

func TestApplyProjectMemberReportsAnUnknownProject(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	store := newMembershipStore(db)

	recorder := callApplyProjectMember(t, store, uuid.New(), uuid.New(), `{"revision":1,"present":false}`)

	require.Equal(t, http.StatusNotFound, recorder.Code, recorder.Body.String())
}

func TestApplyProjectMemberRejectsInvalidInput(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	projectID := testutils.CreateTestTeam(t, db)
	store := newMembershipStore(db)

	recorder := callApplyProjectMember(t, store, projectID, uuid.New(), `{"revision":1,"present":true,"identities":[{"issuer":"https://issuer.test","subject":"first"},{"issuer":"https://issuer.test","subject":"second"}]}`)

	require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
}

func TestApplyProjectMemberRejectsAnIdentityLinkedToAnotherUser(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	projectID := testutils.CreateTestTeam(t, db)
	store := newMembershipStore(db)
	owner, requested := uuid.New(), uuid.New()
	body := `{"revision":1,"present":true,"identities":[{"issuer":"https://issuer.test","subject":"subject"}]}`

	require.Equal(t, http.StatusNoContent, callApplyProjectMember(t, store, projectID, owner, body).Code)
	recorder := callApplyProjectMember(t, store, projectID, requested, body)

	require.Equal(t, http.StatusConflict, recorder.Code, recorder.Body.String())
}

func newMembershipStore(db *testutils.Database) *APIStore {
	auth := &recordingCacheAuthService{}

	return &APIStore{
		authDB:            db.AuthDB,
		authService:       auth,
		managementService: management.NewService(db.AuthDB, db.SqlcClient, auth, noop.NewMeterProvider()),
	}
}

func callApplyProjectMember(t *testing.T, store *APIStore, projectID, userID uuid.UUID, body string) *httptest.ResponseRecorder {
	t.Helper()

	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Request = httptest.NewRequestWithContext(t.Context(), http.MethodPut,
		"/v1/management/projects/"+projectID.String()+"/members/"+userID.String(), strings.NewReader(body))
	ginCtx.Request.Header.Set("Content-Type", "application/json")

	store.ManagementApplyProjectMember(ginCtx, projectID, userID)
	ginCtx.Writer.WriteHeaderNow()

	return recorder
}

func handlerTeamMembers(t *testing.T, db *testutils.Database, projectID uuid.UUID) []uuid.UUID {
	t.Helper()

	var members []uuid.UUID
	err := db.AuthDB.TestsRawSQLQuery(t.Context(),
		"SELECT user_id FROM public.users_teams WHERE team_id = $1", func(rows pgx.Rows) error {
			for rows.Next() {
				var userID uuid.UUID
				if err := rows.Scan(&userID); err != nil {
					return err
				}
				members = append(members, userID)
			}

			return nil
		}, projectID)
	require.NoError(t, err)

	return members
}

func handlerTeamMemberIsDefault(t *testing.T, db *testutils.Database, projectID, userID uuid.UUID) bool {
	t.Helper()

	var isDefault bool
	err := db.AuthDB.TestsRawSQLQuery(t.Context(),
		"SELECT is_default FROM public.users_teams WHERE team_id = $1 AND user_id = $2", func(rows pgx.Rows) error {
			rows.Next()

			return rows.Scan(&isDefault)
		}, projectID, userID)
	require.NoError(t, err)

	return isDefault
}
