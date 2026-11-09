package handlers

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/gin-gonic/gin"
	jose "github.com/go-jose/go-jose/v4"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	middleware "github.com/oapi-codegen/gin-middleware"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/utils"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/db/pkg/testutils"
)

func TestTelemetryNamesExcludeCredentialsAndDeletedRecords(t *testing.T) {
	t.Parallel()
	db := testutils.SetupDatabase(t)
	teamID := insertAdminTestTeam(t, db, adminTestTeam{name: "Team name", createdAt: time.Now()})
	clusterID := uuid.New()
	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(), `INSERT INTO public.clusters(id,name,endpoint,token,telemetry_client_id,deletion_protection) VALUES ($1,'Cluster name','private.example:443','private-token','client_test',false)`, clusterID))
	store := &APIStore{sqlcReadDB: db.SqlcClient}
	read := func() api.TelemetryNames {
		recorder := httptest.NewRecorder()
		ctx, _ := gin.CreateTestContext(recorder)
		ctx.Request = httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/unstable/management/telemetry/names", nil)
		store.ManagementTelemetryNames(ctx)
		require.Equal(t, http.StatusOK, recorder.Code)
		assert.NotContains(t, recorder.Body.String(), "private-token")
		assert.NotContains(t, recorder.Body.String(), "private.example")
		var names api.TelemetryNames
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &names))

		return names
	}
	require.NotContains(t, read().Teams, teamID.String())
	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(), `UPDATE public.teams SET cluster_id=$1 WHERE id=$2`, clusterID, teamID))
	names := read()
	assert.Equal(t, "Team name", names.Teams[teamID.String()].Name)
	require.Len(t, names.Clusters, 1)
	assert.Equal(t, clusterID, names.Clusters[0].Id)
	assert.Equal(t, "Cluster name", names.Clusters[0].Name)
	require.NotNil(t, names.Clusters[0].ClientId)
	assert.Equal(t, "client_test", *names.Clusters[0].ClientId)
	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(), `UPDATE public.teams SET name='Renamed' WHERE id=$1`, teamID))
	assert.Equal(t, "Renamed", read().Teams[teamID.String()].Name)
	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(), `UPDATE public.teams SET deleted_at=now(),is_blocked=true WHERE id=$1`, teamID))
	require.NoError(t, db.SqlcClient.TestsRawSQL(t.Context(), `UPDATE public.clusters SET deletion_requested_at=now() WHERE id=$1`, clusterID))
	names = read()
	assert.NotContains(t, names.Teams, teamID.String())
	assert.Empty(t, names.Clusters)
}

func TestTelemetryNamesRequiresServiceJWT(t *testing.T) {
	t.Parallel()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	keys := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{Key: publicKey, KeyID: "service", Algorithm: "EdDSA", Use: "sig"}}})
	}))
	t.Cleanup(keys.Close)
	var config auth.ProviderConfig
	require.NoError(t, json.Unmarshal([]byte(`{"jwt":[{"issuer":{"url":"`+keys.URL+`","audiences":["regional-api"]}}]}`), &config))
	verifier, err := auth.NewJWKSVerifier(t.Context(), config, keys.Client())
	require.NoError(t, err)
	telemetryVerifier, err := auth.NewTelemetryJWKSVerifier(t.Context(), config, keys.Client())
	require.NoError(t, err)
	sign := func(audience, scope string) string {
		token := jwt.NewWithClaims(jwt.SigningMethodEdDSA, jwt.MapClaims{"iss": keys.URL, "aud": audience, "scope": scope, "exp": time.Now().Add(time.Minute).Unix()})
		token.Header["kid"] = "service"
		signed, err := token.SignedString(privateKey)
		require.NoError(t, err)

		return signed
	}
	swagger, err := api.GetSwagger()
	require.NoError(t, err)
	swagger.Servers = nil
	router := gin.New()
	router.Use(middleware.OapiRequestValidatorWithOptions(swagger, &middleware.Options{
		ErrorHandler: func(c *gin.Context, message string, status int) {
			utils.ErrorHandler(c, message, max(c.Writer.Status(), status))
		},
		Options: openapi3filter.Options{AuthenticationFunc: auth.CreateAuthenticationFunc([]auth.Authenticator{auth.NewAdminJWTAuthenticator(verifier), auth.NewTelemetryJWTAuthenticator(telemetryVerifier), auth.NewAdminApiKeyAuthenticator("admin-key")}, nil)},
	}))
	router.GET("/unstable/management/telemetry/names", func(c *gin.Context) { c.Status(http.StatusNoContent) })
	for _, tc := range []struct {
		name, token, apiKey string
		status              int
	}{
		{"service JWT", sign("regional-api:telemetry-names", "telemetry:read"), "", http.StatusNoContent},
		{"wrong audience", sign("other-api", "telemetry:read"), "", http.StatusUnauthorized},
		{"admin JWT", sign("regional-api", ""), "", http.StatusUnauthorized},
		{"missing JWT", "", "", http.StatusUnauthorized},
		{"admin key alone", "", "admin-key", http.StatusUnauthorized},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/unstable/management/telemetry/names", nil)
			if tc.token != "" {
				req.Header.Set("Authorization", "Bearer "+tc.token)
			}
			req.Header.Set("X-Admin-Token", tc.apiKey)
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, req)
			assert.Equal(t, tc.status, recorder.Code)
		})
	}
}
