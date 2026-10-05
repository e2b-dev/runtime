package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	jose "github.com/go-jose/go-jose/v4"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sharedauth "github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/dashboard-api/internal/api"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

type managementReadinessStore struct {
	api.ServerInterface

	clusterID uuid.UUID
}

func (s *managementReadinessStore) ManagementClusterDestroyReadiness(c *gin.Context, clusterID api.ClusterID) {
	s.clusterID = clusterID
	c.Status(http.StatusNoContent)
}

func TestManagementClusterReadinessRequiresServiceJWT(t *testing.T) {
	t.Parallel()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	keys := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		assert.NoError(t, json.NewEncoder(w).Encode(jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{Key: publicKey, KeyID: "management", Algorithm: "EdDSA", Use: "sig"}}}))
	}))
	t.Cleanup(keys.Close)
	verifier, err := sharedauth.NewJWKSVerifier(t.Context(), sharedauth.ProviderConfig{JWT: []sharedauth.JWTConfig{{Issuer: sharedauth.JWTIssuer{URL: keys.URL, Audiences: []string{"regional"}}}}}, keys.Client())
	require.NoError(t, err)
	swagger, err := api.GetSpec()
	require.NoError(t, err)
	swagger.Servers = nil
	authenticate := sharedauth.CreateAuthenticationFunc([]sharedauth.Authenticator{sharedauth.NewAdminJWTAuthenticator(verifier), sharedauth.NewAdminApiKeyAuthenticator("admin-key")}, nil)
	clusterID := uuid.New()
	token := jwt.NewWithClaims(jwt.SigningMethodEdDSA, jwt.MapClaims{"iss": keys.URL, "aud": "regional", "exp": time.Now().Add(time.Minute).Unix()})
	token.Header["kid"] = "management"
	signed, err := token.SignedString(privateKey)
	require.NoError(t, err)
	for _, tc := range []struct {
		name, authorization, adminKey string
		want                          int
	}{
		{"missing credential", "", "", http.StatusUnauthorized},
		{"invalid signature", "Bearer invalid", "", http.StatusUnauthorized},
		{"admin key is not management auth", "", "admin-key", http.StatusUnauthorized},
		{"signed service token", "Bearer " + signed, "", http.StatusNoContent},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := &managementReadinessStore{}
			server := newHTTPServer(0, logger.NewNopLogger(), telemetry.NewNoopClient(), swagger, authenticate, store)
			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/v1/management/clusters/"+clusterID.String()+"/destroy-readiness", nil)
			req.Header.Set("Authorization", tc.authorization)
			req.Header.Set(sharedauth.HeaderAdminToken, tc.adminKey)
			response := httptest.NewRecorder()
			server.Handler.ServeHTTP(response, req)
			require.Equal(t, tc.want, response.Code, response.Body.String())
			if tc.want == http.StatusNoContent {
				require.Equal(t, clusterID, store.clusterID)
			} else {
				require.Equal(t, uuid.Nil, store.clusterID)
			}
		})
	}
	require.Nil(t, swagger.Paths.Value("/admin/clusters/{clusterID}/destroy-readiness"))
}
