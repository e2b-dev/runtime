package token

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	jose "github.com/go-jose/go-jose/v4"
	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/auth/pkg/auth/internal/token/jwks"
)

const (
	adminTestKeyID    = "workspace-key"
	adminTestAudience = "fx1"
)

func newAdminTestVerifier(t *testing.T) (*JWKSVerifier, ed25519.PrivateKey, string) {
	t.Helper()

	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	server := jwks.NewTestServer(t, publicKey, adminTestKeyID, jose.EdDSA, "https://unexpected.example.com")

	verifier, err := NewJWKSVerifier(t.Context(), ProviderConfig{
		JWT: []jwks.Config{{
			Issuer: jwks.Issuer{
				URL:       server.URL,
				Audiences: []string{adminTestAudience},
			},
		}},
	}, server.Client())
	require.NoError(t, err)

	return verifier, privateKey, server.URL
}

func signAdminToken(t *testing.T, privateKey ed25519.PrivateKey, claims jwt.MapClaims) string {
	t.Helper()

	token := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims)
	token.Header["kid"] = adminTestKeyID
	signed, err := token.SignedString(privateKey)
	require.NoError(t, err)

	return signed
}

func TestJWKSVerifier(t *testing.T) {
	t.Parallel()

	verifier, privateKey, issuer := newAdminTestVerifier(t)

	baseClaims := func() jwt.MapClaims {
		return jwt.MapClaims{
			"iss": issuer,
			"aud": adminTestAudience,
			"exp": time.Now().Add(5 * time.Minute).Unix(),
		}
	}

	t.Run("valid token", func(t *testing.T) {
		t.Parallel()

		_, err := verifier.Verify(t.Context(), signAdminToken(t, privateKey, baseClaims()))
		require.NoError(t, err)
	})

	t.Run("expired token", func(t *testing.T) {
		t.Parallel()

		claims := baseClaims()
		claims["exp"] = time.Now().Add(-5 * time.Minute).Unix()
		_, err := verifier.Verify(t.Context(), signAdminToken(t, privateKey, claims))
		require.Error(t, err)
	})

	t.Run("wrong audience", func(t *testing.T) {
		t.Parallel()

		claims := baseClaims()
		claims["aud"] = "other"
		_, err := verifier.Verify(t.Context(), signAdminToken(t, privateKey, claims))
		require.Error(t, err)
	})

	t.Run("missing audience", func(t *testing.T) {
		t.Parallel()

		claims := baseClaims()
		delete(claims, "aud")
		_, err := verifier.Verify(t.Context(), signAdminToken(t, privateKey, claims))
		require.Error(t, err)
	})
}

func TestJWKSVerifierWithoutAudienceRestriction(t *testing.T) {
	t.Parallel()

	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	server := jwks.NewTestServer(t, publicKey, adminTestKeyID, jose.EdDSA, "https://unexpected.example.com")
	verifier, err := NewJWKSVerifier(t.Context(), ProviderConfig{
		JWT: []jwks.Config{{
			Issuer: jwks.Issuer{URL: server.URL},
		}},
	}, server.Client())
	require.NoError(t, err)

	_, err = verifier.Verify(t.Context(), signAdminToken(t, privateKey, jwt.MapClaims{
		"iss": server.URL,
		"exp": time.Now().Add(5 * time.Minute).Unix(),
	}))
	require.NoError(t, err)
}

func TestJWKSVerifierDisabled(t *testing.T) {
	t.Parallel()

	verifier, err := NewJWKSVerifier(t.Context(), ProviderConfig{}, nil)
	require.NoError(t, err)
	require.Nil(t, verifier)

	_, err = verifier.Verify(t.Context(), "any-token")
	require.ErrorContains(t, err, "not configured")
}

func TestJWKSVerifierRejectsNonEdDSA(t *testing.T) {
	t.Parallel()

	verifier, _, issuer := newAdminTestVerifier(t)

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"iss": issuer,
		"aud": adminTestAudience,
		"exp": time.Now().Add(5 * time.Minute).Unix(),
	})
	token.Header["kid"] = adminTestKeyID
	signed, err := token.SignedString([]byte("shared-secret"))
	require.NoError(t, err)

	_, err = verifier.Verify(t.Context(), signed)
	require.Error(t, err)
}

func TestJWKSVerifierES256(t *testing.T) {
	t.Parallel()

	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	server := jwks.NewTestServer(t, &privateKey.PublicKey, adminTestKeyID, jose.ES256, "https://unexpected.example.com")
	issuer := server.URL

	verifier, err := NewJWKSVerifier(t.Context(), ProviderConfig{
		JWT: []jwks.Config{{
			Issuer: jwks.Issuer{
				URL:       issuer,
				Audiences: []string{adminTestAudience},
			},
		}},
	}, server.Client())
	require.NoError(t, err)

	token := jwt.NewWithClaims(jwt.SigningMethodES256, jwt.MapClaims{
		"iss": issuer,
		"aud": adminTestAudience,
		"exp": time.Now().Add(5 * time.Minute).Unix(),
	})
	token.Header["kid"] = adminTestKeyID
	signed, err := token.SignedString(privateKey)
	require.NoError(t, err)

	_, err = verifier.Verify(t.Context(), signed)
	require.NoError(t, err)
}

func TestJWKSVerifierRejectsJWKSKeyWithoutAlgorithm(t *testing.T) {
	t.Parallel()

	publicKey, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	server := jwks.NewTestServer(t, publicKey, adminTestKeyID, "", "https://unexpected.example.com")

	verifier, err := NewJWKSVerifier(t.Context(), ProviderConfig{
		JWT: []jwks.Config{{
			Issuer: jwks.Issuer{
				URL:       server.URL,
				Audiences: []string{adminTestAudience},
			},
		}},
	}, server.Client())
	require.Nil(t, verifier)
	require.ErrorContains(t, err, "missing alg")
}

func TestJWKSVerifierRequiresKeysAtStartupByDefault(t *testing.T) {
	t.Parallel()

	_, err := NewJWKSVerifier(t.Context(), unreachableIssuer(nil), http.DefaultClient)
	require.Error(t, err)
}

func TestJWKSVerifierLoadsKeysAfterAnOptionalStartup(t *testing.T) {
	t.Parallel()

	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	body, err := json.Marshal(jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{
		Key:       publicKey,
		KeyID:     adminTestKeyID,
		Algorithm: string(jose.EdDSA),
		Use:       "sig",
	}}})
	require.NoError(t, err)

	var ready atomic.Bool
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if !ready.Load() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)

			return
		}
		requests.Add(1)
		_, _ = w.Write(body)
	}))
	t.Cleanup(server.Close)

	optional := false
	verifier, err := NewJWKSVerifier(t.Context(), ProviderConfig{
		JWT: []jwks.Config{{
			RequiredAtStartup: &optional,
			Issuer: jwks.Issuer{
				URL:       server.URL,
				Audiences: []string{adminTestAudience},
			},
		}},
	}, server.Client())
	require.NoError(t, err)

	signed := signAdminToken(t, privateKey, jwt.MapClaims{
		"iss": server.URL,
		"aud": adminTestAudience,
		"exp": time.Now().Add(5 * time.Minute).Unix(),
	})
	_, err = verifier.Verify(t.Context(), signed)
	require.Error(t, err)

	ready.Store(true)
	require.Eventually(t, func() bool {
		_, err := verifier.Verify(t.Context(), signed)

		return err == nil
	}, 5*time.Second, 100*time.Millisecond)

	settled := requests.Load()
	time.Sleep(2500 * time.Millisecond)
	require.Equal(t, settled, requests.Load())
}

func TestJWKSVerifierStartsWhenStartupFetchIsOptional(t *testing.T) {
	t.Parallel()

	optional := false
	verifier, err := NewJWKSVerifier(t.Context(), unreachableIssuer(&optional), http.DefaultClient)
	require.NoError(t, err)

	_, err = verifier.Verify(t.Context(), "not-a-token")
	require.Error(t, err)
}

func TestProviderConfigKeepsRequiredAtStartup(t *testing.T) {
	t.Parallel()

	config, err := ParseProviderConfig(`{"jwt":[{"requiredAtStartup":false,"issuer":{"url":"https://issuer.example","audiences":["test-audience"]}}]}`)
	require.NoError(t, err)
	require.NotNil(t, config.JWT[0].RequiredAtStartup)
	require.False(t, *config.JWT[0].RequiredAtStartup)

	omitted, err := ParseProviderConfig(`{"jwt":[{"issuer":{"url":"https://issuer.example","audiences":["test-audience"]}}]}`)
	require.NoError(t, err)
	require.Nil(t, omitted.JWT[0].RequiredAtStartup)
}

func unreachableIssuer(requiredAtStartup *bool) ProviderConfig {
	return ProviderConfig{
		JWT: []jwks.Config{{
			RequiredAtStartup: requiredAtStartup,
			Issuer: jwks.Issuer{
				URL:       "http://127.0.0.1:1",
				Audiences: []string{adminTestAudience},
			},
		}},
	}
}
