package api_templates

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/integration/internal/setup"
)

func TestInvalidBuildStatus(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()

	resp, err := c.GetTemplatesTemplateIDBuildsBuildIDStatusWithResponse(
		t.Context(),
		"non-existing",
		"also-non-existing",
		nil,
		setup.WithAPIKey(),
	)
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode())
}
