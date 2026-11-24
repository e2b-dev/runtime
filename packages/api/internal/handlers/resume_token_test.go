package handlers

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/json"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/oauth"
	"github.com/e2b-dev/infra/packages/shared/pkg/resumetoken"
)

type legacyVerifierFunc func(context.Context, string) (oauth.Claims, error)

func (f legacyVerifierFunc) VerifyClaims(ctx context.Context, token string) (oauth.Claims, error) {
	return f(ctx, token)
}

func TestResumeVerifierKeepsLegacyAndClusterClaimsSeparate(t *testing.T) {
	t.Parallel()
	_, private, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	key := jose.JSONWebKey{Key: private, KeyID: "k1"}
	public, err := json.Marshal(jose.JSONWebKeySet{Keys: []jose.JSONWebKey{key.Public()}})
	require.NoError(t, err)
	legacy := legacyVerifierFunc(func(_ context.Context, raw string) (oauth.Claims, error) {
		require.Equal(t, "legacy", raw)

		return oauth.Claims{OrgID: "test-org"}, nil
	})
	verify, err := NewRegionalResumeVerifier(t.Context(), nil, string(public), legacy)
	require.NoError(t, err)
	claims, err := verify.VerifyClaims(t.Context(), "legacy")
	require.NoError(t, err)
	require.Equal(t, "test-org", claims.OrgID)
	require.Empty(t, claims.ClusterID)

	raw, err := json.Marshal(key)
	require.NoError(t, err)
	signer, err := resumetoken.NewSigner(string(raw))
	require.NoError(t, err)
	cluster := uuid.New()
	token, err := resumetoken.Issue(signer, cluster.String(), time.Now().Add(resumetoken.Lifetime))
	require.NoError(t, err)
	claims, err = verify.VerifyClaims(t.Context(), token)
	require.NoError(t, err)
	require.NoError(t, oauth.RequireScopeClaims(claims, oauth.RequiredScope))
	service := &SandboxService{}
	other := uuid.New()
	require.True(t, service.orgOwnsCluster(claims, &cluster))
	require.False(t, service.orgOwnsCluster(claims, &other))
}

func TestKMSResumeKeyOverlap(t *testing.T) {
	t.Parallel()
	keys := resumetoken.Verifier{}
	tokens := make(map[string]string)
	cluster := uuid.NewString()
	load := func(_ context.Context, version string) (*ecdsa.PublicKey, string, error) {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		require.NoError(t, err)
		signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.ES256, Key: key}, (&jose.SignerOptions{}).WithHeader("kid", version))
		require.NoError(t, err)
		tokens[version], err = resumetoken.Issue(signer, cluster, time.Now().Add(resumetoken.Lifetime))
		require.NoError(t, err)

		return &key.PublicKey, version, nil
	}
	require.NoError(t, addKMSResumeKeys(t.Context(), keys, []string{"old", "new"}, load))
	for _, version := range []string{"old", "new"} {
		id, regional, err := keys.Verify(tokens[version])
		require.NoError(t, err)
		require.True(t, regional)
		require.Equal(t, cluster, id)
	}
	require.Error(t, addKMSResumeKeys(t.Context(), keys, []string{"old"}, load))
}
