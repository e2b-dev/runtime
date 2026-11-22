package api_templates

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/integration/internal/api"
	"github.com/e2b-dev/infra/packages/api/integration/internal/setup"
	testutils "github.com/e2b-dev/infra/packages/api/integration/internal/utils"
)

func TestUpdateTemplateVisibilityToPublicWithAPIKey(t *testing.T) {
	t.Parallel()
	// Create a test template (metadata only, no build needed)
	template := testutils.RequestTemplateWithoutBuild(t, "test-update-public-api-key", setup.WithAPIKey())

	c := setup.GetAPIClient()

	// Update template to public
	updateResp, err := c.PatchV2TemplatesTemplateIDWithResponse(
		t.Context(),
		template.TemplateID,
		api.TemplateUpdateRequest{
			Public: new(true),
		},
		setup.WithAPIKey(),
	)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, updateResp.StatusCode())

	// Verify the update by fetching the template
	getResp, err := c.GetTemplatesWithResponse(t.Context(), nil, setup.WithAPIKey())
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, getResp.StatusCode())
	require.NotNil(t, getResp.JSON200)

	// Find our template in the list
	templates := *getResp.JSON200
	var found bool
	for _, tmpl := range templates {
		if tmpl.TemplateID == template.TemplateID {
			assert.True(t, tmpl.Public, "Template should be public")
			found = true

			break
		}
	}
	assert.True(t, found, "Template should be in the list")
}

func TestUpdateTemplateVisibilityToPrivateWithAPIKey(t *testing.T) {
	t.Parallel()
	// Create a test template (metadata only, no build needed)
	template := testutils.RequestTemplateWithoutBuild(t, "test-update-private-api-key", setup.WithAPIKey())

	c := setup.GetAPIClient()

	// First make it public
	_, err := c.PatchV2TemplatesTemplateIDWithResponse(
		t.Context(),
		template.TemplateID,
		api.TemplateUpdateRequest{
			Public: new(true),
		},
		setup.WithAPIKey(),
	)
	require.NoError(t, err)

	// Then update template back to private
	updateResp, err := c.PatchV2TemplatesTemplateIDWithResponse(
		t.Context(),
		template.TemplateID,
		api.TemplateUpdateRequest{
			Public: new(false),
		},
		setup.WithAPIKey(),
	)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, updateResp.StatusCode())

	// Verify the update by fetching the template
	getResp, err := c.GetTemplatesWithResponse(t.Context(), nil, setup.WithAPIKey())
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, getResp.StatusCode())
	require.NotNil(t, getResp.JSON200)

	// Find our template in the list
	templates := *getResp.JSON200
	var found bool
	for _, tmpl := range templates {
		if tmpl.TemplateID == template.TemplateID {
			assert.False(t, tmpl.Public, "Template should be private")
			found = true

			break
		}
	}
	assert.True(t, found, "Template should be in the list")
}

func TestUpdateTemplateWithInvalidAPIKey(t *testing.T) {
	t.Parallel()
	// Create a test template with valid API key
	template := testutils.RequestTemplateWithoutBuild(t, "test-update-invalid-key", setup.WithAPIKey())

	c := setup.GetAPIClient()

	// Try to update with invalid API key
	updateResp, err := c.PatchV2TemplatesTemplateIDWithResponse(
		t.Context(),
		template.TemplateID,
		api.TemplateUpdateRequest{
			Public: new(true),
		},
		setup.WithAPIKey("invalid-api-key"),
	)
	require.NoError(t, err)
	require.Equal(t, http.StatusUnauthorized, updateResp.StatusCode())
	require.NotNil(t, updateResp.JSON401)
}

func TestUpdateNonExistentTemplateWithAPIKey(t *testing.T) {
	t.Parallel()

	c := setup.GetAPIClient()

	// Try to update a non-existent template
	nonExistentTemplateID := "non-existent-template-id"
	updateResp, err := c.PatchV2TemplatesTemplateIDWithResponse(
		t.Context(),
		nonExistentTemplateID,
		api.TemplateUpdateRequest{
			Public: new(true),
		},
		setup.WithAPIKey(),
	)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, updateResp.StatusCode(),
		"Expected 404, got %d", updateResp.StatusCode())
}

func TestUpdateTemplateWithAPIKey(t *testing.T) {
	t.Parallel()
	// Create a test template with API key first
	template := testutils.RequestTemplateWithoutBuild(t, "test-update-api-key", setup.WithAPIKey())

	c := setup.GetAPIClient()

	// Update template using API key authentication.
	updateResp, err := c.PatchV2TemplatesTemplateIDWithResponse(
		t.Context(),
		template.TemplateID,
		api.TemplateUpdateRequest{
			Public: new(true),
		},
		setup.WithAPIKey(),
		setup.WithTeamID(),
	)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, updateResp.StatusCode())

	// Verify the update by fetching the template
	getResp, err := c.GetTemplatesWithResponse(
		t.Context(),
		&api.GetTemplatesParams{
			TeamID: new(setup.TeamID),
		},
		setup.WithAPIKey(),
		setup.WithTeamID(),
	)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, getResp.StatusCode())
	require.NotNil(t, getResp.JSON200)

	// Find our template in the list
	templates := *getResp.JSON200
	var found bool
	for _, tmpl := range templates {
		if tmpl.TemplateID == template.TemplateID {
			assert.True(t, tmpl.Public, "Template should be public")
			found = true

			break
		}
	}
	assert.True(t, found, "Template should be in the list")
}

func TestUpdateTemplateNotOwnedByTeam(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	db := setup.GetTestDBClient(t)
	c := setup.GetAPIClient()

	// Create second team
	user1ID := testutils.CreateUser(t, db)
	team2ID := testutils.CreateTeamWithUser(t, db, "test-team-template-update-2", user1ID.String())
	team2APIKey := testutils.CreateAPIKey(t, ctx, c, user1ID.String(), team2ID)

	// Create a template
	template := testutils.RequestTemplateWithoutBuild(t, "test-update-template-cross-team", setup.WithAPIKey())
	team1TemplateID := template.TemplateID

	// Try to update team1's template using team2's API key - should fail
	updateResp, err := c.PatchV2TemplatesTemplateIDWithResponse(
		ctx,
		team1TemplateID,
		api.TemplateUpdateRequest{
			Public: new(true),
		},
		setup.WithAPIKey(team2APIKey),
	)
	require.NoError(t, err)
	// Should return template doesn't belong to team2
	assert.Equal(t, http.StatusForbidden, updateResp.StatusCode(),
		"Expected 403 when trying to update another team's template, got %d", updateResp.StatusCode())

	// Verify that team1 can still update their own template
	updateResp2, err := c.PatchV2TemplatesTemplateIDWithResponse(
		ctx,
		team1TemplateID,
		api.TemplateUpdateRequest{
			Public: new(true),
		},
		setup.WithAPIKey(),
	)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, updateResp2.StatusCode(),
		"Team1 should be able to update their own template")

	// Verify the update worked by listing templates with team1's API key
	getResp, err := c.GetTemplatesWithResponse(ctx, nil, setup.WithAPIKey())
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, getResp.StatusCode())
	require.NotNil(t, getResp.JSON200)

	// Find the template and verify it's public
	templates := *getResp.JSON200
	var found bool
	for _, tmpl := range templates {
		if tmpl.TemplateID == team1TemplateID {
			assert.True(t, tmpl.Public, "Template should be public after update")
			found = true

			break
		}
	}
	assert.True(t, found, "Template should be in team1's list")
}
