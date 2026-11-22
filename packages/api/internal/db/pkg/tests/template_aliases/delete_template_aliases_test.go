package aliases

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/db/pkg/testutils"
)

func TestDeleteTemplateAliases_Success(t *testing.T) {
	t.Parallel()
	// Setup test database with migrations
	client := testutils.SetupDatabase(t)
	ctx := t.Context()

	// Create a test team first (required by foreign key constraint)
	teamID := testutils.CreateTestTeam(t, client)
	// Create a base env (required by foreign key constraint on snapshots table)
	templateID, _ := testutils.CreateTestTemplateWithAlias(t, client, teamID)

	// Execute UpsertSnapshot for a new snapshot
	result, err := client.SqlcClient.DeleteOtherTemplateAliases(ctx, templateID)
	require.NoError(t, err, "Failed to create new snapshot")
	require.Len(t, result, 1, "Expected 1 deleted alias")
}

func TestDeleteTemplateAliases_NoAlias(t *testing.T) {
	t.Parallel()
	// Setup test database with migrations
	client := testutils.SetupDatabase(t)
	ctx := t.Context()

	// Create a test team first (required by foreign key constraint)
	teamID := testutils.CreateTestTeam(t, client)
	// Create a base env (required by foreign key constraint on snapshots table)
	_, _ = testutils.CreateTestTemplateWithAlias(t, client, teamID)
	anotherTemplateID := testutils.CreateTestTemplate(t, client, teamID)

	// Execute UpsertSnapshot for a new snapshot
	result, err := client.SqlcClient.DeleteOtherTemplateAliases(ctx, anotherTemplateID)
	require.NoError(t, err, "Failed to create new snapshot")
	assert.Empty(t, result, "Expected no deleted aliases")
}
