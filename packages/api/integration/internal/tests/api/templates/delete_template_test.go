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

func TestDeleteTemplate(t *testing.T) {
	t.Parallel()
	alias := "test-to-delete"
	res := buildTemplate(t, alias, api.TemplateBuildStartV2{
		Force:     new(ForceBaseBuild),
		FromImage: new("ubuntu:22.04"),
		Steps: new([]api.TemplateStep{
			{
				Type:  "RUN",
				Force: new(true),
				Args:  new([]string{"echo 'Hello, World!'"}),
			},
		}),
	}, defaultBuildLogHandler(t))

	require.True(t, res)

	c := setup.GetAPIClient()
	deleteRes, err := c.DeleteTemplatesTemplateIDWithResponse(
		t.Context(),
		alias,
		setup.WithAPIKey(),
	)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNoContent, deleteRes.StatusCode())
}

func TestDeleteTemplateFromAnotherTeamAPIKey(t *testing.T) {
	t.Parallel()
	alias := "test-to-delete-another-team-api-key"

	db := setup.GetTestDBClient(t)
	userID := testutils.CreateUser(t, db)
	teamID := testutils.CreateTeamWithUser(t, db, "foreign-team", userID.String())
	apiKey := testutils.CreateAPIKey(t, t.Context(), setup.GetAPIClient(), userID.String(), teamID)

	// The foreign team's lookup never sees the template regardless of build
	// state, so requesting the template (no build) is enough for this test.
	testutils.RequestTemplateWithoutBuild(t, alias, setup.WithAPIKey())

	c := setup.GetAPIClient()
	deleteRes, err := c.DeleteTemplatesTemplateIDWithResponse(
		t.Context(),
		alias,
		setup.WithAPIKey(apiKey),
	)
	require.NoError(t, err)
	// With namespace-scoped templates, the foreign team's lookup searches in their own namespace
	// and doesn't find the template (which exists in the original team's namespace), so 404 is returned.
	assert.Equal(t, http.StatusNotFound, deleteRes.StatusCode())
}
