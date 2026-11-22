package sandboxes

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/e2b-dev/infra/packages/api/integration/internal/api"
	"github.com/e2b-dev/infra/packages/api/integration/internal/setup"
	"github.com/e2b-dev/infra/packages/api/integration/internal/utils"
)

func TestSandboxPause(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()

	t.Run("regular pause", func(t *testing.T) {
		t.Parallel()
		// Create a sandbox with auto-pause disabled
		sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithAutoPause(false))
		sbxId := sbx.SandboxID

		// Set timeout to 0 to force sandbox to be stopped
		resp, err := c.PostSandboxesSandboxIDPauseWithResponse(t.Context(), sbxId, api.PostSandboxesSandboxIDPauseJSONRequestBody{}, setup.WithAPIKey())
		require.NoError(t, err)
		require.Equal(t, http.StatusNoContent, resp.StatusCode())

		res, err := c.GetSandboxesSandboxIDWithResponse(t.Context(), sbxId, setup.WithAPIKey())
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, res.StatusCode())
		require.NotNil(t, res.JSON200)
		assert.Equal(t, api.Paused, res.JSON200.State)

		// Resume the sandbox with auto-pause enabled
		sbxResume, err := c.PostSandboxesSandboxIDResumeWithResponse(t.Context(), sbxId, api.PostSandboxesSandboxIDResumeJSONRequestBody{}, setup.WithAPIKey())
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, sbxResume.StatusCode())
		require.NotNil(t, sbxResume.JSON201)
		assert.Equal(t, sbxResume.JSON201.SandboxID, sbxId)
	})

	t.Run("test concurrent pauses", func(t *testing.T) {
		t.Parallel()
		sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithAutoPause(true))
		sbxId := sbx.SandboxID

		// Pause the sandbox
		wg := errgroup.Group{}
		for range 5 {
			wg.Go(func() error {
				pauseResp, err := c.PostSandboxesSandboxIDPauseWithResponse(t.Context(), sbxId, api.PostSandboxesSandboxIDPauseJSONRequestBody{}, setup.WithAPIKey())
				require.NoError(t, err)
				require.Equal(t, http.StatusNoContent, pauseResp.StatusCode())

				return nil
			})
		}

		err := wg.Wait()
		require.NoError(t, err)

		res, err := c.GetSandboxesSandboxIDWithResponse(t.Context(), sbxId, setup.WithAPIKey())
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, res.StatusCode())
		require.NotNil(t, res.JSON200)
		assert.Equal(t, api.Paused, res.JSON200.State)
	})

	t.Run("pause killed sandbox", func(t *testing.T) {
		t.Parallel()
		// Create a sandbox with auto-pause disabled
		sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithAutoPause(true))
		sbxId := sbx.SandboxID

		// Kill the sandbox
		killResp, err := c.DeleteSandboxesSandboxIDWithResponse(t.Context(), sbxId, setup.WithAPIKey())
		require.NoError(t, err)
		require.Equal(t, http.StatusNoContent, killResp.StatusCode())

		pauseResp, err := c.PostSandboxesSandboxIDPauseWithResponse(t.Context(), sbxId, api.PostSandboxesSandboxIDPauseJSONRequestBody{}, setup.WithAPIKey())
		require.NoError(t, err)
		require.Equal(t, http.StatusNotFound, pauseResp.StatusCode())
	})

	t.Run("pause already paused sandbox", func(t *testing.T) {
		t.Parallel()
		sbx := utils.SetupSandboxWithCleanup(t, c)
		sbxId := sbx.SandboxID

		// Pause the sandbox
		resp, err := c.PostSandboxesSandboxIDPauseWithResponse(t.Context(), sbxId, api.PostSandboxesSandboxIDPauseJSONRequestBody{}, setup.WithAPIKey())
		require.NoError(t, err)
		require.Equal(t, http.StatusNoContent, resp.StatusCode())

		// Try to pause the sandbox again
		resp, err = c.PostSandboxesSandboxIDPauseWithResponse(t.Context(), sbxId, api.PostSandboxesSandboxIDPauseJSONRequestBody{}, setup.WithAPIKey())
		require.NoError(t, err)
		require.Equal(t, http.StatusConflict, resp.StatusCode())
	})
}
