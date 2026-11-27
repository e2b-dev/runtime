package sandboxes

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/integration/internal/api"
	"github.com/e2b-dev/infra/packages/api/integration/internal/setup"
	"github.com/e2b-dev/infra/packages/api/integration/internal/utils"
)

func TestSandboxKill(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()

	t.Run("kill a non-existing sandbox", func(t *testing.T) {
		t.Parallel()
		killSandboxResponse, err := c.DeleteSandboxesSandboxIDWithResponse(t.Context(), "non-existing", setup.WithAPIKey())

		require.NoError(t, err)
		assert.Equal(t, http.StatusNotFound, killSandboxResponse.StatusCode())
	})

	t.Run("start and kill a sandbox", func(t *testing.T) {
		t.Parallel()

		utils.AcquireSandboxSlot(t)

		// create a new samdbox
		createSandboxResponse, err := c.PostSandboxesWithResponse(t.Context(), api.NewSandbox{
			TemplateID: setup.SandboxTemplateID,
		}, setup.WithAPIKey())

		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, createSandboxResponse.StatusCode())

		sandboxID := createSandboxResponse.JSON201.SandboxID

		// kill the sandbox
		killSandboxResponse, err := c.DeleteSandboxesSandboxIDWithResponse(t.Context(), sandboxID, setup.WithAPIKey())

		require.NoError(t, err)
		assert.Equal(t, http.StatusNoContent, killSandboxResponse.StatusCode())

		// list all sandboxes and check that the sandbox is not in the list
		listSandboxesResponse, err := c.GetSandboxesWithResponse(t.Context(), &api.GetSandboxesParams{}, setup.WithAPIKey())

		require.NoError(t, err)
		require.Equal(t, http.StatusOK, listSandboxesResponse.StatusCode())

		runningSandboxes := listSandboxesResponse.JSON200
		require.NotNil(t, runningSandboxes)
		assert.NotContains(t, *runningSandboxes, sandboxID)
	})

	t.Run("start and kill a paused sandbox", func(t *testing.T) {
		t.Parallel()

		utils.AcquireSandboxSlot(t)

		// create a new sandbox
		createSandboxResponse, err := c.PostSandboxesWithResponse(t.Context(), api.NewSandbox{
			TemplateID: setup.SandboxTemplateID,
		}, setup.WithAPIKey())

		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, createSandboxResponse.StatusCode())

		sandboxID := createSandboxResponse.JSON201.SandboxID

		// pause the sandbox
		pauseSandboxResponse, err := c.PostSandboxesSandboxIDPauseWithResponse(t.Context(), sandboxID, api.PostSandboxesSandboxIDPauseJSONRequestBody{}, setup.WithAPIKey())

		require.NoError(t, err)
		require.Equal(t, http.StatusNoContent, pauseSandboxResponse.StatusCode())

		// kill the sandbox
		killSandboxResponse, err := c.DeleteSandboxesSandboxIDWithResponse(t.Context(), sandboxID, setup.WithAPIKey())

		require.NoError(t, err)
		require.Equal(t, http.StatusNoContent, killSandboxResponse.StatusCode())

		// list all sandboxes and check that the sandbox is not in the list
		listSandboxesResponse, err := c.GetSandboxesWithResponse(t.Context(), &api.GetSandboxesParams{}, setup.WithAPIKey())

		require.NoError(t, err)
		require.Equal(t, http.StatusOK, listSandboxesResponse.StatusCode())

		runningSandboxes := listSandboxesResponse.JSON200
		require.NotNil(t, runningSandboxes)
		assert.NotContains(t, *runningSandboxes, sandboxID)
	})

	t.Run("start and kill a subsequently paused sandbox", func(t *testing.T) {
		t.Parallel()

		utils.AcquireSandboxSlot(t)

		// create a new sandbox
		createSandboxResponse, err := c.PostSandboxesWithResponse(t.Context(), api.NewSandbox{
			TemplateID: setup.SandboxTemplateID,
		}, setup.WithAPIKey())

		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, createSandboxResponse.StatusCode())

		sandboxID := createSandboxResponse.JSON201.SandboxID

		// pause the sandbox
		pauseSandboxResponse, err := c.PostSandboxesSandboxIDPauseWithResponse(t.Context(), sandboxID, api.PostSandboxesSandboxIDPauseJSONRequestBody{}, setup.WithAPIKey())

		require.NoError(t, err)
		require.Equal(t, http.StatusNoContent, pauseSandboxResponse.StatusCode())

		// resume the sandbox
		timeout := int32(1000)
		resumeSandboxResponse, err := c.PostSandboxesSandboxIDResumeWithResponse(t.Context(), sandboxID, api.PostSandboxesSandboxIDResumeJSONRequestBody{
			Timeout: &timeout,
		}, setup.WithAPIKey())

		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, resumeSandboxResponse.StatusCode())

		// kill the sandbox
		killSandboxResponse, err := c.DeleteSandboxesSandboxIDWithResponse(t.Context(), sandboxID, setup.WithAPIKey())

		require.NoError(t, err)
		require.Equal(t, http.StatusNoContent, killSandboxResponse.StatusCode())

		// list all sandboxes and check that the sandbox is not in the list
		listSandboxesResponse, err := c.GetSandboxesWithResponse(t.Context(), &api.GetSandboxesParams{}, setup.WithAPIKey())

		require.NoError(t, err)
		require.Equal(t, http.StatusOK, listSandboxesResponse.StatusCode())

		runningSandboxes := listSandboxesResponse.JSON200
		require.NotNil(t, runningSandboxes)
		assert.NotContains(t, *runningSandboxes, sandboxID)
	})
}
