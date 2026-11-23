package resumetoken

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestRegionalResumeCredential(t *testing.T) {
	t.Parallel()
	cluster := uuid.NewString()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	key := jose.JSONWebKey{Key: private, KeyID: "current"}
	raw, err := json.Marshal(key)
	require.NoError(t, err)
	signer, err := NewSigner(string(raw))
	require.NoError(t, err)
	verify := Verifier{"current": public}
	rawBad, err := json.Marshal(jose.JSONWebKeySet{Keys: []jose.JSONWebKey{key}})
	require.NoError(t, err)
	_, err = NewVerifier(string(rawBad))
	require.Error(t, err)
	rawGood, err := json.Marshal(jose.JSONWebKeySet{Keys: []jose.JSONWebKey{key.Public()}})
	require.NoError(t, err)
	keyset, err := NewVerifier(string(rawGood))
	require.NoError(t, err)
	rawToken, err := Issue(signer, cluster, time.Now().Add(Lifetime))
	require.NoError(t, err)
	got, regional, err := keyset.Verify(rawToken)
	require.NoError(t, err)
	require.True(t, regional)
	require.Equal(t, cluster, got)
	for _, c := range []jwt.Claims{
		{Issuer: issuer, Subject: cluster, Audience: jwt.Audience{"e2b:flag-read"}, IssuedAt: jwt.NewNumericDate(time.Now()), Expiry: jwt.NewNumericDate(time.Now().Add(Lifetime))},
		{Issuer: issuer, Subject: cluster, Audience: jwt.Audience{audience}, IssuedAt: jwt.NewNumericDate(time.Now().Add(-Lifetime)), Expiry: jwt.NewNumericDate(time.Now().Add(-time.Second))},
		{Issuer: issuer, Subject: cluster, Audience: jwt.Audience{audience}, IssuedAt: jwt.NewNumericDate(time.Now()), Expiry: jwt.NewNumericDate(time.Now().Add(time.Hour))},
	} {
		raw, err := jwt.Signed(signer).Claims(c).Serialize()
		require.NoError(t, err)
		_, regional, err := verify.Verify(raw)
		require.True(t, regional)
		require.Error(t, err)
	}
	other, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	_, _, err = (Verifier{"current": other}).Verify(rawToken)
	require.Error(t, err)
}
