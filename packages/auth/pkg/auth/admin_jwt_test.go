package auth_test

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
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
)

func TestAdminJWTAuthenticatorRetainsVerifiedIssuer(t *testing.T) {
	t.Parallel()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	keys := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{Key: publicKey, KeyID: "service-test", Algorithm: "EdDSA", Use: "sig"}}})
	}))
	t.Cleanup(keys.Close)
	var config auth.ProviderConfig
	require.NoError(t, json.Unmarshal([]byte(`{"jwt":[{"issuer":{"url":"`+keys.URL+`"}}]}`), &config))
	verifier, err := auth.NewJWKSVerifier(t.Context(), config, keys.Client())
	require.NoError(t, err)
	sign := func(issuer string, expires time.Time, key ed25519.PrivateKey) string {
		token := jwt.NewWithClaims(jwt.SigningMethodEdDSA, jwt.MapClaims{"iss": issuer, "exp": expires.Unix()})
		token.Header["kid"] = "service-test"
		signed, err := token.SignedString(key)
		require.NoError(t, err)

		return signed
	}
	_, otherKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	for _, tc := range []struct {
		name, token string
		valid       bool
	}{
		{"verified", sign(keys.URL, time.Now().Add(time.Minute), privateKey), true},
		{"missing", "", false},
		{"malformed", "invalid", false},
		{"foreign issuer", sign("https://foreign.example", time.Now().Add(time.Minute), privateKey), false},
		{"wrong signature", sign(keys.URL, time.Now().Add(time.Minute), otherKey), false},
		{"expired", sign(keys.URL, time.Now().Add(-time.Minute), privateKey), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
			if tc.token != "" {
				request.Header.Set("Authorization", "Bearer "+tc.token)
			}
			request.Header.Set("X-E2B-Actor-Service-Issuer", "https://spoofed.example")
			authenticator := auth.NewAdminJWTAuthenticator(verifier)
			require.Equal(t, "AdminJWTAuth", authenticator.SecuritySchemeName())
			err := authenticator.Authenticate(t.Context(), c, &openapi3filter.AuthenticationInput{RequestValidationInput: &openapi3filter.RequestValidationInput{Request: request}})
			issuer, ok := auth.GetServiceIssuer(c)
			if tc.valid {
				require.NoError(t, err)
				require.True(t, ok)
				require.Equal(t, keys.URL, issuer)
			} else {
				require.Error(t, err)
				require.Equal(t, http.StatusUnauthorized, c.Writer.Status())
				require.False(t, ok)
				require.Empty(t, issuer)
			}
		})
	}
}
