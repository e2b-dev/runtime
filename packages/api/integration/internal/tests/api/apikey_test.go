package api

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/integration/internal/setup"
	"github.com/e2b-dev/infra/packages/api/integration/internal/utils"
)

func TestAPIKeyLastUsedUpdated(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	c := setup.GetAPIClient()
	db := setup.GetTestDBClient(t)
	teamID := uuid.MustParse(setup.TeamID)

	// The last used is updated only once a minute.
	expectedLastUsed := time.Now().Add(-2 * time.Minute)
	_, err := c.GetSandboxesWithResponse(ctx, nil, setup.WithAPIKey())
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		apiKeys, err := db.AuthDb.GetTeamAPIKeysWithCreator(ctx, teamID)
		require.NoError(t, err)

		for _, key := range apiKeys {
			if !utils.MatchesAPIKeyMask(setup.APIKey, key.ApiKeyPrefix, key.ApiKeyMaskPrefix, key.ApiKeyMaskSuffix) {
				continue
			}

			return key.LastUsed != nil && !key.LastUsed.Before(expectedLastUsed)
		}

		return false
	}, 10*time.Second, 50*time.Millisecond, "Expected API key last used to be updated")
}
