package utils

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	authqueries "github.com/e2b-dev/infra/packages/db/pkg/auth/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/keys"
	"github.com/e2b-dev/infra/tests/integration/internal/api"
	"github.com/e2b-dev/infra/tests/integration/internal/setup"
)

func CreateTeam(t *testing.T, db *setup.Database, teamName string) uuid.UUID {
	t.Helper()

	return CreateTeamWithUser(t, db, teamName, "")
}

func CreateTeamWithUser(
	t *testing.T,
	db *setup.Database,
	teamName, userID string,
) uuid.UUID {
	t.Helper()

	teamID := uuid.New()
	slug := fmt.Sprintf("test-%s", teamID.String()[:8])

	err := db.AuthDb.TestsRawSQL(t.Context(), `
INSERT INTO teams (id, email, name, is_blocked, slug)
VALUES ($1, $2, $3, $4, $5)
`, teamID, fmt.Sprintf("test-integration-%s@e2b.dev", teamID), teamName, false, slug)
	require.NoError(t, err)

	if userID != "" {
		AddUserToTeam(t, db, teamID, userID)
	}

	t.Cleanup(func() {
		db.AuthDb.TestsRawSQL(t.Context(), `
DELETE FROM teams WHERE id = $1
`, teamID)
	})

	return teamID
}

func AddUserToTeam(t *testing.T, db *setup.Database, teamID uuid.UUID, userID string) {
	t.Helper()

	userUUID, err := uuid.Parse(userID)
	require.NoError(t, err)

	err = db.AuthDb.TestsRawSQL(t.Context(), `
INSERT INTO users_teams (user_id, team_id, is_default)
VALUES ($1, $2, $3)
`, userUUID, teamID, false)
	require.NoError(t, err)

	t.Cleanup(func() {
		db.AuthDb.TestsRawSQL(t.Context(), `
DELETE FROM users_teams WHERE user_id = $1 and team_id = $2
`, userUUID, teamID)
	})
}

func CreateAPIKey(t *testing.T, ctx context.Context, _ *api.ClientWithResponses, userID string, teamID uuid.UUID) string {
	t.Helper()

	db := setup.GetTestDBClient(t) //nolint:contextcheck // Test DB setup is bound to t.Context; request ctx is for the API key row lifecycle.
	apiKey, err := keys.GenerateKey(keys.ApiKeyPrefix)
	require.NoError(t, err)

	var createdBy *uuid.UUID
	if userID != "" {
		parsedUserID, err := uuid.Parse(userID)
		require.NoError(t, err)
		createdBy = &parsedUserID
	}

	created, err := db.AuthDb.CreateTeamAPIKey(ctx, authqueries.CreateTeamAPIKeyParams{
		TeamID:           teamID,
		CreatedBy:        createdBy,
		ApiKeyHash:       apiKey.HashedValue,
		ApiKeyPrefix:     apiKey.Masked.Prefix,
		ApiKeyLength:     int32(apiKey.Masked.ValueLength),
		ApiKeyMaskPrefix: apiKey.Masked.MaskedValuePrefix,
		ApiKeyMaskSuffix: apiKey.Masked.MaskedValueSuffix,
		Name:             uuid.New().String(),
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		_, _ = db.AuthDb.DeleteTeamAPIKey(ctx, authqueries.DeleteTeamAPIKeyParams{
			ID:     created.ID,
			TeamID: teamID,
		})
	})

	return apiKey.PrefixedRawValue
}

func MatchesAPIKeyMask(apiKey string, prefix string, maskPrefix string, maskSuffix string) bool {
	return strings.HasPrefix(apiKey, prefix+maskPrefix) && strings.HasSuffix(apiKey, maskSuffix)
}
