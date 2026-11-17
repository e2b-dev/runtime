package jwks

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	jose "github.com/go-jose/go-jose/v4"
	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVerifierRefreshesValidMethodsWithJWKS(t *testing.T) {
	t.Parallel()

	rsaPrivateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	_, edPrivateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	const (
		rsaKeyID = "rsa-key"
		edKeyID  = "ed-key"
		audience = "test-audience"
	)

	var keySet atomic.Value
	keySet.Store(jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{
		Key:       &rsaPrivateKey.PublicKey,
		KeyID:     rsaKeyID,
		Algorithm: jwt.SigningMethodRS256.Alg(),
		Use:       "sig",
	}}})

	mux := http.NewServeMux()
	server := httptest.NewTLSServer(mux)
	t.Cleanup(server.Close)
	mux.HandleFunc(defaultJWKSPath, func(w http.ResponseWriter, _ *http.Request) {
		assert.NoError(t, json.NewEncoder(w).Encode(keySet.Load().(jose.JSONWebKeySet)), "encode JWKS")
	})

	verifier, err := NewVerifierFromIssuerJWKS(t.Context(), Config{
		Issuer: Issuer{
			URL:                 server.URL,
			Audiences:           []string{audience},
			AudienceMatchPolicy: AudienceMatchAny,
		},
		CacheDuration: 10 * time.Millisecond,
	}, server.Client())
	require.NoError(t, err)

	rsaToken := signedTestToken(t, jwt.SigningMethodRS256, rsaPrivateKey, rsaKeyID, server.URL, audience)
	_, err = verifier.Verify(t.Context(), rsaToken)
	require.NoError(t, err)

	keySet.Store(jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{
		Key:       edPrivateKey.Public().(ed25519.PublicKey),
		KeyID:     edKeyID,
		Algorithm: jwt.SigningMethodEdDSA.Alg(),
		Use:       "sig",
	}}})
	edToken := signedTestToken(t, jwt.SigningMethodEdDSA, edPrivateKey, edKeyID, server.URL, audience)

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		_, verifyErr := verifier.Verify(t.Context(), edToken)
		assert.NoError(collect, verifyErr)
	}, time.Second, 10*time.Millisecond)
}

func signedTestToken(t *testing.T, method jwt.SigningMethod, privateKey any, keyID, issuer, audience string) string {
	t.Helper()

	token := jwt.NewWithClaims(method, jwt.MapClaims{
		"iss": issuer,
		"aud": audience,
		"exp": time.Now().Add(time.Minute).Unix(),
	})
	token.Header["kid"] = keyID
	signed, err := token.SignedString(privateKey)
	require.NoError(t, err)

	return signed
}
