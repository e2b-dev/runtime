// Package resumetoken implements the regional registry's short-lived resume credentials.
package resumetoken

import (
	"crypto"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
	gojwt "github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

const (
	issuer   = "urn:e2b:byoc-registry:resume"
	audience = "e2b:sandbox-resume"
	Lifetime = 15 * time.Minute
)

var errInvalid = errors.New("invalid regional resume token")

func NewSigner(raw string) (jose.Signer, error) {
	var key jose.JSONWebKey
	if json.Unmarshal([]byte(raw), &key) != nil || key.KeyID == "" || !key.Valid() {
		return nil, errors.New("invalid resume signing JWK")
	}
	priv, ok := key.Key.(ed25519.PrivateKey)
	if !ok {
		return nil, errors.New("expected Ed25519 resume signing key")
	}

	return jose.NewSigner(jose.SigningKey{Algorithm: jose.EdDSA, Key: priv}, (&jose.SignerOptions{}).WithType("JWT").WithHeader("kid", key.KeyID))
}

func claims(clusterID string, expiry time.Time) map[string]any {
	return map[string]any{
		"iss": issuer, "sub": clusterID, "aud": audience,
		"iat": time.Now().Unix(), "exp": expiry.Unix(),
	}
}

func Issue(signer jose.Signer, clusterID string, expiry time.Time) (string, error) {
	return jwt.Signed(signer).Claims(claims(clusterID, expiry)).Serialize()
}

// ES256SigningInput returns the encoded header and claims for an external signer.
func ES256SigningInput(keyID, clusterID string, expiry time.Time) (string, error) {
	token := gojwt.NewWithClaims(gojwt.SigningMethodES256, gojwt.MapClaims(claims(clusterID, expiry)))
	token.Header["kid"] = keyID

	return token.SigningString()
}

type Verifier map[string]crypto.PublicKey

func NewVerifier(raw string) (Verifier, error) {
	var set jose.JSONWebKeySet
	if json.Unmarshal([]byte(raw), &set) != nil || len(set.Keys) == 0 {
		return nil, errors.New("invalid resume verification JWKS")
	}
	keys := make(Verifier, len(set.Keys))
	for _, key := range set.Keys {
		pub, ok := key.Key.(ed25519.PublicKey)
		if !ok || !key.Valid() || key.KeyID == "" || keys[key.KeyID] != nil {
			return nil, errors.New("expected distinct public Ed25519 resume verification keys")
		}
		keys[key.KeyID] = pub
	}

	return keys, nil
}

// Verify distinguishes regional tokens from legacy tokens using the untrusted issuer
// solely for routing. Only verified claims identify a cluster.
func (v Verifier) Verify(raw string) (string, bool, error) {
	t, err := jwt.ParseSigned(raw, []jose.SignatureAlgorithm{jose.EdDSA, jose.ES256})
	if err != nil {
		// Other token formats still go through the legacy verifier.
		return "", false, nil //nolint:nilerr
	}
	var claims jwt.Claims
	if t.UnsafeClaimsWithoutVerification(&claims) != nil {
		return "", true, errInvalid
	}
	if claims.Issuer != issuer {
		return "", false, nil
	}
	key := v[t.Headers[0].KeyID]
	if key == nil || t.Claims(key, &claims) != nil || claims.Expiry == nil || claims.IssuedAt == nil {
		return "", true, errInvalid
	}
	lifetime := claims.Expiry.Time().Sub(claims.IssuedAt.Time())
	expected := jwt.Expected{Issuer: issuer, AnyAudience: jwt.Audience{audience}, Time: time.Now()}
	if lifetime <= 0 || lifetime > Lifetime || len(claims.Audience) != 1 || claims.ValidateWithLeeway(expected, 0) != nil {
		return "", true, errInvalid
	}
	id, err := uuid.Parse(claims.Subject)
	if err != nil || id == uuid.Nil {
		return "", true, errInvalid
	}

	return id.String(), true, nil
}
