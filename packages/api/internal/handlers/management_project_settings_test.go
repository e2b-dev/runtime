package handlers

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/gin-gonic/gin"
	jose "github.com/go-jose/go-jose/v4"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	middleware "github.com/oapi-codegen/gin-middleware"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/utils"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	authtypes "github.com/e2b-dev/infra/packages/auth/pkg/types"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
)

func TestProjectSettingsApplyOnlyNewerRevisions(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	teamID := testutils.CreateTestTeam(t, db)
	store, auth := newProjectSettingsStore(db)
	row, err := db.AuthDB.GetTeamByTeamID(t.Context(), teamID)
	require.NoError(t, err)
	require.Nil(t, authtypes.NewTeam(row).ProjectSettings.SandboxSettings.DefaultAllowPublicTraffic)

	for _, tc := range []struct {
		revision     int64
		settings     string
		wantRevision int64
		wantDefault  *bool
	}{
		{1, `{"sandboxSettings":{"defaultAllowPublicTraffic":false}}`, 1, new(false)},
		{3, `{"sandboxSettings":{"defaultAllowPublicTraffic":true}}`, 3, new(true)},
		{2, `{"sandboxSettings":{"defaultAllowPublicTraffic":false}}`, 3, new(true)},
		{3, `{"sandboxSettings":{}}`, 3, new(true)},
		{4, `{"sandboxSettings":{}}`, 4, nil},
		{3, `{"sandboxSettings":{"defaultAllowPublicTraffic":false}}`, 4, nil},
	} {
		body := fmt.Sprintf(`{"revision":%d,"settings":%s}`, tc.revision, tc.settings)
		response := callProjectSettings(t, store, teamID, body)
		require.Equal(t, http.StatusNoContent, response.Code, response.Body.String())
		require.Empty(t, response.Body.String())
		require.Equal(t, tc.wantRevision, *readProjectSettingsRevision(t, db, teamID))

		require.Equal(t, tc.wantDefault, readTeamDefaultAllowPublicTraffic(t, db, teamID))

		row, err := db.AuthDB.GetTeamByTeamID(t.Context(), teamID)
		require.NoError(t, err)
		cached, err := json.Marshal(authtypes.NewTeam(row))
		require.NoError(t, err)
		var team authtypes.Team
		require.NoError(t, json.Unmarshal(cached, &team))
		require.Equal(t, tc.wantDefault, team.ProjectSettings.SandboxSettings.DefaultAllowPublicTraffic, "sandbox creation must see the projected settings through the auth cache")
	}
	require.Equal(t, []uuid.UUID{teamID, teamID, teamID, teamID, teamID, teamID}, *auth.invalidatedTeamIDs)
}

func TestProjectSettingsRetryFailedCacheEviction(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	teamID := testutils.CreateTestTeam(t, db)
	store, auth := newProjectSettingsStore(db)
	auth.invalidateTeamErr = errors.New("cache unavailable")

	response := callProjectSettings(t, store, teamID, `{"revision":1,"settings":{"sandboxSettings":{"defaultAllowPublicTraffic":false}}}`)
	require.Equal(t, http.StatusInternalServerError, response.Code, response.Body.String())
	require.Equal(t, new(false), readTeamDefaultAllowPublicTraffic(t, db, teamID))
	require.EqualValues(t, 1, *readProjectSettingsRevision(t, db, teamID))

	auth.invalidateTeamErr = nil
	retry := callProjectSettings(t, store, teamID, `{"revision":1,"settings":{"sandboxSettings":{"defaultAllowPublicTraffic":true}}}`)
	require.Equal(t, http.StatusNoContent, retry.Code, retry.Body.String())
	require.Equal(t, new(false), readTeamDefaultAllowPublicTraffic(t, db, teamID), "a duplicate must retry eviction without rewriting settings")
	require.Equal(t, []uuid.UUID{teamID, teamID}, *auth.invalidatedTeamIDs)
}

func TestProjectSettingsRollBackTheRevisionWhenTheWriteFails(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	teamID := testutils.CreateTestTeam(t, db)
	store, auth := newProjectSettingsStore(db)
	initial := callProjectSettings(t, store, teamID, `{"revision":1,"settings":{"sandboxSettings":{"defaultAllowPublicTraffic":true}}}`)
	require.Equal(t, http.StatusNoContent, initial.Code, initial.Body.String())
	*auth.invalidatedTeamIDs = nil
	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(),
		`ALTER TABLE projection.project_settings ADD CONSTRAINT reject_settings_test CHECK (sandbox_default_allow_public_traffic IS DISTINCT FROM false)`))

	body := `{"revision":2,"settings":{"sandboxSettings":{"defaultAllowPublicTraffic":false}}}`
	response := callProjectSettings(t, store, teamID, body)
	require.Equal(t, http.StatusInternalServerError, response.Code, response.Body.String())
	require.EqualValues(t, 1, *readProjectSettingsRevision(t, db, teamID))
	require.Equal(t, new(true), readTeamDefaultAllowPublicTraffic(t, db, teamID))
	require.Empty(t, *auth.invalidatedTeamIDs)

	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(), `ALTER TABLE projection.project_settings DROP CONSTRAINT reject_settings_test`))
	retry := callProjectSettings(t, store, teamID, body)
	require.Equal(t, http.StatusNoContent, retry.Code, retry.Body.String())
	require.Equal(t, new(false), readTeamDefaultAllowPublicTraffic(t, db, teamID))
	require.EqualValues(t, 2, *readProjectSettingsRevision(t, db, teamID))
}

func TestProjectSettingsRejectInvalidDeliveries(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	teamID := testutils.CreateTestTeam(t, db)
	store, auth := newProjectSettingsStore(db)

	for _, tc := range []struct {
		projectID uuid.UUID
		body      string
		status    int
	}{
		{uuid.New(), `{"revision":1,"settings":{"sandboxSettings":{}}}`, http.StatusNotFound},
		{teamID, `{"revision":0,"settings":{"sandboxSettings":{}}}`, http.StatusBadRequest},
		{teamID, `{"revision":-1,"settings":{"sandboxSettings":{}}}`, http.StatusBadRequest},
		{uuid.Nil, `{"revision":1,"settings":{"sandboxSettings":{}}}`, http.StatusBadRequest},
		{teamID, `{"revision":`, http.StatusBadRequest},
	} {
		response := callProjectSettings(t, store, tc.projectID, tc.body)
		require.Equal(t, tc.status, response.Code, response.Body.String())
		require.Nil(t, readProjectSettingsRevision(t, db, tc.projectID))
	}
	require.Empty(t, *auth.invalidatedTeamIDs)
}

func TestProjectSettingsContract(t *testing.T) {
	t.Parallel()

	swagger, err := api.GetSwagger()
	require.NoError(t, err)
	path := swagger.Paths.Find("/unstable/management/projects/{projectID}/settings")
	require.NotNil(t, path)
	require.Nil(t, path.Get)
	require.NotNil(t, path.Put.Responses.Status(http.StatusNoContent))
	schema := swagger.Components.Schemas["ManagementProjectSettingsRequest"].Value

	for _, body := range []string{
		`{"revision":1,"settings":{"sandboxSettings":{}}}`,
		`{"revision":1,"settings":{"sandboxSettings":{"defaultAllowPublicTraffic":false}}}`,
		`{"revision":1,"settings":{"sandboxSettings":{"defaultAllowPublicTraffic":true}}}`,
	} {
		var value map[string]any
		require.NoError(t, json.Unmarshal([]byte(body), &value))
		require.NoError(t, schema.VisitJSON(value), body)
	}
	for _, body := range []string{
		`{"settings":{"sandboxSettings":{}}}`,
		`{"revision":0,"settings":{"sandboxSettings":{}}}`,
		`{"revision":1}`,
		`{"revision":1,"settings":{}}`,
		`{"revision":1,"settings":{"sandboxSettings":null}}`,
		`{"revision":1,"settings":{"sandboxSettings":{"defaultAllowPublicTraffic":null}}}`,
		`{"revision":1,"settings":{"sandboxSettings":{"defaultAllowPublicTraffic":"false"}}}`,
	} {
		var value map[string]any
		require.NoError(t, json.Unmarshal([]byte(body), &value))
		require.Error(t, schema.VisitJSON(value), body)
	}
}

func newProjectSettingsStore(db *testutils.Database) (*APIStore, *fakeAPIKeyAuthService) {
	var invalidated []uuid.UUID
	auth := &fakeAPIKeyAuthService{invalidatedTeamIDs: &invalidated}

	return &APIStore{sqlcDB: db.SqlcClient, authService: auth}, auth
}

func callProjectSettings(t *testing.T, store *APIStore, projectID uuid.UUID, body string) *httptest.ResponseRecorder {
	t.Helper()

	router := gin.New()
	api.RegisterHandlers(router, store)
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPut,
		"/unstable/management/projects/"+projectID.String()+"/settings", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)

	return recorder
}

func readTeamDefaultAllowPublicTraffic(t *testing.T, db *testutils.Database, teamID uuid.UUID) *bool {
	t.Helper()

	var value *bool
	require.NoError(t, db.SqlcClient.TestsRawSQLQuery(t.Context(),
		"SELECT sandbox_default_allow_public_traffic FROM projection.project_settings WHERE project_id = $1",
		func(rows pgx.Rows) error {
			if !rows.Next() {
				return nil
			}

			return rows.Scan(&value)
		}, teamID))

	return value
}

func readProjectSettingsRevision(t *testing.T, db *testutils.Database, teamID uuid.UUID) *int64 {
	t.Helper()

	var revision *int64
	require.NoError(t, db.SqlcClient.TestsRawSQLQuery(t.Context(),
		"SELECT revision FROM projection.project_settings WHERE project_id = $1",
		func(rows pgx.Rows) error {
			if !rows.Next() {
				return nil
			}

			return rows.Scan(&revision)
		}, teamID))

	return revision
}

func TestProjectSettingsRejectDeletedProject(t *testing.T) {
	t.Parallel()

	db := testutils.SetupDatabase(t)
	projectID := testutils.CreateTestTeam(t, db)
	store, auth := newProjectSettingsStore(db)
	body := `{"revision":1,"settings":{"sandboxSettings":{"defaultAllowPublicTraffic":false}}}`
	require.Equal(t, http.StatusNoContent, callProjectSettings(t, store, projectID, body).Code)
	*auth.invalidatedTeamIDs = nil
	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(),
		`UPDATE public.teams SET deleted_at = now(), is_blocked = true, blocked_reason = 'Project deletion' WHERE id = $1`, projectID))

	response := callProjectSettings(t, store, projectID, `{"revision":2,"settings":{"sandboxSettings":{"defaultAllowPublicTraffic":true}}}`)
	require.Equal(t, http.StatusNotFound, response.Code, response.Body.String())
	require.EqualValues(t, 1, *readProjectSettingsRevision(t, db, projectID))
	require.Equal(t, new(false), readTeamDefaultAllowPublicTraffic(t, db, projectID))
	require.Empty(t, *auth.invalidatedTeamIDs)
}

func TestProjectSettingsRouteRequiresServiceJWT(t *testing.T) {
	t.Parallel()

	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	keys := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{Key: publicKey, KeyID: "settings-test", Algorithm: "EdDSA", Use: "sig"}}})
	}))
	t.Cleanup(keys.Close)
	var config auth.ProviderConfig
	require.NoError(t, json.Unmarshal([]byte(`{"jwt":[{"issuer":{"url":"`+keys.URL+`"},"audiences":["test-control-plane"]}]}`), &config))
	verifier, err := auth.NewJWKSVerifier(t.Context(), config, keys.Client())
	require.NoError(t, err)
	token := jwt.NewWithClaims(jwt.SigningMethodEdDSA, jwt.MapClaims{"iss": keys.URL, "aud": "test-control-plane", "exp": int64(4102444800)})
	token.Header["kid"] = "settings-test"
	signed, err := token.SignedString(privateKey)
	require.NoError(t, err)
	_, otherKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	wrongSignature, err := token.SignedString(otherKey)
	require.NoError(t, err)

	db := testutils.SetupDatabase(t)
	for name, bearer := range map[string]string{
		"missing": "", "static admin token": "test-admin-token", "wrong signature": wrongSignature, "service JWT": signed,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			projectID := testutils.CreateTestTeam(t, db)
			store, cache := newProjectSettingsStore(db)
			swagger, err := api.GetSwagger()
			require.NoError(t, err)
			swagger.Servers = nil
			router := gin.New()
			router.Use(middleware.OapiRequestValidatorWithOptions(swagger, &middleware.Options{
				ErrorHandler: func(c *gin.Context, message string, status int) {
					utils.ErrorHandler(c, message, max(c.Writer.Status(), status))
				},
				Options: openapi3filter.Options{AuthenticationFunc: auth.CreateAuthenticationFunc([]auth.Authenticator{
					auth.NewAdminJWTAuthenticator(verifier),
					auth.NewAdminApiKeyAuthenticator("test-admin-token"),
				}, nil)},
				SilenceServersWarning: true,
			}))
			api.RegisterHandlers(router, store)
			request := httptest.NewRequestWithContext(t.Context(), http.MethodPut,
				"/unstable/management/projects/"+projectID.String()+"/settings", strings.NewReader(`{"revision":1,"settings":{"sandboxSettings":{"defaultAllowPublicTraffic":false}}}`))
			request.Header.Set("Content-Type", "application/json")
			if name == "static admin token" {
				request.Header.Set("X-Admin-Token", bearer)
			} else if bearer != "" {
				request.Header.Set("Authorization", "Bearer "+bearer)
			}
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, request)

			if name == "service JWT" {
				require.Equal(t, http.StatusNoContent, recorder.Code, recorder.Body.String())
				require.Equal(t, new(false), readTeamDefaultAllowPublicTraffic(t, db, projectID))
			} else {
				require.Equal(t, http.StatusUnauthorized, recorder.Code, recorder.Body.String())
				require.Nil(t, readProjectSettingsRevision(t, db, projectID))
				require.Empty(t, *cache.invalidatedTeamIDs)
			}
		})
	}
}
