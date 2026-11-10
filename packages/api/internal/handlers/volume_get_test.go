package handlers

import (
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/cfg"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
	dbtypes "github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/db/queries"
)

func TestGenerateVolumeContentToken_SetsTokidHeader(t *testing.T) {
	t.Parallel()

	secret := []byte("super-secret-key")

	// Minimal APIStore with the VolumesToken config set for signing
	config := cfg.VolumesTokenConfig{
		Enabled:        true,
		SigningKeyName: "key-v1",
		Issuer:         "test-issuer",
		SigningMethod:  jwt.SigningMethodHS256,
		SigningKey:     secret,
		// not required for the header test, but set to a small value
		Duration: time.Hour,
	}

	// Arrange a team and volume
	teamID := uuid.New()
	clusterID := uuid.New()
	team := &types.Team{Team: &dbtypes.Team{ID: teamID, ClusterID: &clusterID}}

	volID := uuid.New()
	volume := queries.Volume{
		ID:         volID,
		TeamID:     teamID,
		Name:       "test-volume",
		VolumeType: "persistent",
	}

	// Act: generate token
	audience := "https://api.custom.example.com"
	tokenStr, apiErr := generateVolumeContentToken(config, volume, team, audience)
	require.Nil(t, apiErr)
	require.NotEmpty(t, tokenStr)

	// Parse token to inspect headers and claims
	parsed, err := jwt.ParseWithClaims(tokenStr, jwt.MapClaims{}, func(token *jwt.Token) (any, error) {
		// ensure the expected alg is used
		assert.Equal(t, jwt.SigningMethodHS256.Alg(), token.Method.Alg())

		return secret, nil
	})
	require.NoError(t, err)
	require.NotNil(t, parsed)
	require.True(t, parsed.Valid)

	// Assert: custom header `tokid` exists and matches `jti` claim
	tokidVal, ok := parsed.Header["tokid"].(string)
	require.True(t, ok, "expected custom header 'tokid' to be present and a string")
	require.Equal(t, config.SigningKeyName, tokidVal)

	// Assert: the audience claim is the origin we passed, not the cluster ID.
	claims, ok := parsed.Claims.(jwt.MapClaims)
	require.True(t, ok)
	aud, err := claims.GetAudience()
	require.NoError(t, err)
	require.Equal(t, jwt.ClaimStrings{audience}, aud)
}
