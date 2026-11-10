package sandboxes

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/tests/integration/internal/api"
	"github.com/e2b-dev/infra/tests/integration/internal/setup"
)

// TestSandboxCreate_CpuCountAboveTemplateBound verifies that a cpuCount the
// template cannot run is refused up front with a named error code, for both
// fixed-size and resizable test templates. Requires resizable-templates enabled.
func TestSandboxCreate_CpuCountAboveTemplateBound(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()

	tmpl, err := c.GetTemplatesTemplateIDWithResponse(t.Context(), setup.SandboxTemplateID, &api.GetTemplatesTemplateIDParams{}, setup.WithAPIKey())
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, tmpl.StatusCode())
	require.NotNil(t, tmpl.JSON200)

	bound := tmpl.JSON200.CpuCount
	wantCode := "sandbox_cpu_count_not_resizable"
	if tmpl.JSON200.MaxCpuCount != nil {
		bound = *tmpl.JSON200.MaxCpuCount
		wantCode = "sandbox_cpu_count_out_of_range"
	}

	timeout := int32(30)
	resp, err := c.PostSandboxesWithResponse(t.Context(), api.NewSandbox{
		TemplateID: setup.SandboxTemplateID,
		Timeout:    &timeout,
		CpuCount:   new(bound + 2),
	}, setup.WithAPIKey())
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode(), string(resp.Body))
	require.NotNil(t, resp.JSON400)
	require.NotNil(t, resp.JSON400.ErrorCode)
	assert.Equal(t, wantCode, *resp.JSON400.ErrorCode)
}
