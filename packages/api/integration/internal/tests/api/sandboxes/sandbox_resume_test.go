package sandboxes

import (
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/e2b-dev/infra/packages/api/integration/internal/api"
	"github.com/e2b-dev/infra/packages/api/integration/internal/setup"
	"github.com/e2b-dev/infra/packages/api/integration/internal/utils"
)

func TestSandboxResume(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()

	t.Run("regular resume", func(t *testing.T) {
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
		assert.Equal(t, sbx.TemplateID, sbxResume.JSON201.TemplateID)
	})

	t.Run("concurrent resumes", func(t *testing.T) {
		t.Parallel()
		sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithAutoPause(true))
		sbxId := sbx.SandboxID

		pauseSandbox(t, c, sbxId)

		// Pause the sandbox
		wg := errgroup.Group{}
		resumed := atomic.Bool{}
		for range 5 {
			wg.Go(func() error {
				resumeResp, err := c.PostSandboxesSandboxIDResumeWithResponse(t.Context(), sbxId, api.PostSandboxesSandboxIDResumeJSONRequestBody{}, setup.WithAPIKey())
				require.NoError(t, err)
				require.Contains(t, []int{http.StatusCreated, http.StatusConflict}, resumeResp.StatusCode())

				if resumeResp.StatusCode() == http.StatusCreated {
					resumed.Store(true)
				}

				return nil
			})
		}

		err := wg.Wait()
		require.NoError(t, err)

		res, err := c.GetSandboxesSandboxIDWithResponse(t.Context(), sbxId, setup.WithAPIKey())
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, res.StatusCode())
		require.NotNil(t, res.JSON200)
		assert.Equal(t, api.Running, res.JSON200.State)
		assert.Equal(t, sbx.TemplateID, res.JSON200.TemplateID)

		assert.True(t, resumed.Load(), "at least one resume should succeed")
	})

	t.Run("resume killed sandbox", func(t *testing.T) {
		t.Parallel()
		// Create a sandbox with auto-pause disabled
		sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithAutoPause(true))
		sbxId := sbx.SandboxID

		// Kill the sandbox
		killResp, err := c.DeleteSandboxesSandboxIDWithResponse(t.Context(), sbxId, setup.WithAPIKey())
		require.NoError(t, err)
		require.Equal(t, http.StatusNoContent, killResp.StatusCode())

		// Try to resume the sandbox
		sbxResume, err := c.PostSandboxesSandboxIDResumeWithResponse(t.Context(), sbxId, api.PostSandboxesSandboxIDResumeJSONRequestBody{}, setup.WithAPIKey())
		require.NoError(t, err)
		require.Equal(t, http.StatusNotFound, sbxResume.StatusCode())
	})

	t.Run("resume killed sandbox", func(t *testing.T) {
		t.Parallel()
		c := setup.GetAPIClient()

		// Create a sandbox with auto-pause disabled
		sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithAutoPause(true))
		sbxId := sbx.SandboxID

		// Pause the sandbox
		resp, err := c.PostSandboxesSandboxIDPauseWithResponse(t.Context(), sbxId, api.PostSandboxesSandboxIDPauseJSONRequestBody{}, setup.WithAPIKey())
		require.NoError(t, err)
		require.Equal(t, http.StatusNoContent, resp.StatusCode())

		// Kill the sandbox
		killResp, err := c.DeleteSandboxesSandboxIDWithResponse(t.Context(), sbxId, setup.WithAPIKey())
		require.NoError(t, err)
		require.Equal(t, http.StatusNoContent, killResp.StatusCode())

		// Try to kill the sandbox
		sbxResume, err := c.PostSandboxesSandboxIDResumeWithResponse(t.Context(), sbxId, api.PostSandboxesSandboxIDResumeJSONRequestBody{}, setup.WithAPIKey())
		require.NoError(t, err)
		require.Equal(t, http.StatusNotFound, sbxResume.StatusCode())
	})

	t.Run("concurrent resumes - not returning early", func(t *testing.T) {
		t.Parallel()
		c := setup.GetAPIClient()

		// Create a sandbox with auto-pause disabled
		sbx := utils.SetupSandboxWithCleanup(t, c)
		sbxId := sbx.SandboxID

		// Pause the sandbox
		resp, err := c.PostSandboxesSandboxIDPauseWithResponse(t.Context(), sbxId, api.PostSandboxesSandboxIDPauseJSONRequestBody{}, setup.WithAPIKey())
		require.NoError(t, err)
		require.Equal(t, http.StatusNoContent, resp.StatusCode())

		wg := errgroup.Group{}
		for range 5 {
			wg.Go(func() error {
				// Try to resume the sandbox
				sbxResume, err := c.PostSandboxesSandboxIDResumeWithResponse(t.Context(), sbxId, api.PostSandboxesSandboxIDResumeJSONRequestBody{}, setup.WithAPIKey())
				if err != nil {
					return fmt.Errorf("resume sandbox - %w", err)
				}

				// Try to check the status of the sandbox
				sbxState, err := c.GetSandboxesSandboxIDWithResponse(t.Context(), sbxId, setup.WithAPIKey())
				if err != nil {
					return fmt.Errorf("get sandbox - %w", err)
				}

				if sbxState.StatusCode() != http.StatusOK {
					return fmt.Errorf("get sandbox - unexpected status code: %d", sbxState.StatusCode())
				}

				if sbxState.JSON200.State != api.Running {
					return fmt.Errorf("get sandbox - unexpected state: %s", sbxState.JSON200.State)
				}

				if sbxResume.StatusCode() != http.StatusCreated {
					return fmt.Errorf("resume sandbox - unexpected status code: %d", sbxResume.StatusCode())
				}

				return nil
			})
		}

		err = wg.Wait()
		require.NoError(t, err)
	})
}

func TestSandboxResume_CrossTeamAccess_Paused(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()
	db := setup.GetTestDBClient(t)

	// Create a sandbox with the default team's API key and pause it
	sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithAutoPause(false))
	pauseSandbox(t, c, sbx.SandboxID)

	// Create a second team with a different API key
	foreignUserID := utils.CreateUser(t, db)
	foreignTeamID := utils.CreateTeamWithUser(t, db, "foreign-team-resume", foreignUserID.String())
	foreignAPIKey := utils.CreateAPIKey(t, t.Context(), c, foreignUserID.String(), foreignTeamID)

	// Try to resume the first team's sandbox using the second team's API key
	resumeResp, err := c.PostSandboxesSandboxIDResumeWithResponse(t.Context(), sbx.SandboxID, api.PostSandboxesSandboxIDResumeJSONRequestBody{}, setup.WithAPIKey(foreignAPIKey))
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, resumeResp.StatusCode(), "Should return 404 Not Found when trying to resume a paused sandbox owned by a different team")
}

func TestSandboxResume_CrossTeamAccess_Running(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()
	db := setup.GetTestDBClient(t)

	// Create a sandbox with the default team's API key and pause it
	sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithAutoPause(false))

	// Create a second team with a different API key
	foreignUserID := utils.CreateUser(t, db)
	foreignTeamID := utils.CreateTeamWithUser(t, db, "foreign-team-resume", foreignUserID.String())
	foreignAPIKey := utils.CreateAPIKey(t, t.Context(), c, foreignUserID.String(), foreignTeamID)

	// Try to resume the first team's sandbox using the second team's API key
	resumeResp, err := c.PostSandboxesSandboxIDResumeWithResponse(t.Context(), sbx.SandboxID, api.PostSandboxesSandboxIDResumeJSONRequestBody{}, setup.WithAPIKey(foreignAPIKey))
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, resumeResp.StatusCode(), "Should return 404 Not Found when trying to resume a sandbox owned by a different team")
}
