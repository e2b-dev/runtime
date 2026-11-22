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

func TestSandboxFork(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()

	// The checkpoint plus fork boots can outlast the default 30s sandbox
	// timeout on a loaded runner, which would expire the original mid-test.
	sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithAutoPause(false), utils.WithTimeout(300))
	sbxID := sbx.SandboxID

	// Forks default to a 15s timeout; give them room so the state
	// assertions below don't race their expiration on a loaded runner.
	forkTimeout := int32(300)
	forkResp, err := c.PostSandboxesSandboxIDForkWithResponse(t.Context(), sbxID, api.PostSandboxesSandboxIDForkJSONRequestBody{Timeout: &forkTimeout}, setup.WithAPIKey())
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, forkResp.StatusCode())
	require.NotNil(t, forkResp.JSON201)
	require.Len(t, *forkResp.JSON201, 1)

	result := (*forkResp.JSON201)[0]
	require.Nil(t, result.Error)
	require.NotNil(t, result.Sandbox)

	forked := result.Sandbox
	t.Cleanup(func() {
		utils.TeardownSandbox(t, c, forked.SandboxID)
	})

	assert.NotEqual(t, sbxID, forked.SandboxID)
	assert.Equal(t, sbx.TemplateID, forked.TemplateID)

	// The original sandbox should keep running under its original ID.
	origRes, err := c.GetSandboxesSandboxIDWithResponse(t.Context(), sbxID, setup.WithAPIKey())
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, origRes.StatusCode())
	require.NotNil(t, origRes.JSON200)
	assert.Equal(t, api.Running, origRes.JSON200.State)

	// The forked sandbox should also be running.
	forkedRes, err := c.GetSandboxesSandboxIDWithResponse(t.Context(), forked.SandboxID, setup.WithAPIKey())
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, forkedRes.StatusCode())
	require.NotNil(t, forkedRes.JSON200)
	assert.Equal(t, api.Running, forkedRes.JSON200.State)
}

func TestSandboxFork_Multiple(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()

	// The checkpoint plus fork boots can outlast the default 30s sandbox
	// timeout on a loaded runner, which would expire the original mid-test.
	sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithAutoPause(false), utils.WithTimeout(300))

	count := int32(2)
	// Forks default to a 15s timeout; give them room so the state
	// assertions below don't race their expiration on a loaded runner.
	forkTimeout := int32(300)
	forkResp, err := c.PostSandboxesSandboxIDForkWithResponse(t.Context(), sbx.SandboxID, api.PostSandboxesSandboxIDForkJSONRequestBody{Count: &count, Timeout: &forkTimeout}, setup.WithAPIKey())
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, forkResp.StatusCode())
	require.NotNil(t, forkResp.JSON201)

	forks := *forkResp.JSON201
	require.Len(t, forks, 2)

	seen := map[string]bool{sbx.SandboxID: true}
	for _, result := range forks {
		require.Nil(t, result.Error)
		require.NotNil(t, result.Sandbox)

		forked := result.Sandbox
		t.Cleanup(func() {
			utils.TeardownSandbox(t, c, forked.SandboxID)
		})

		assert.False(t, seen[forked.SandboxID], "fork IDs should be unique")
		seen[forked.SandboxID] = true
		assert.Equal(t, sbx.TemplateID, forked.TemplateID)

		res, err := c.GetSandboxesSandboxIDWithResponse(t.Context(), forked.SandboxID, setup.WithAPIKey())
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, res.StatusCode())
		require.NotNil(t, res.JSON200)
		assert.Equal(t, api.Running, res.JSON200.State)
	}

	// The original sandbox should keep running under its original ID.
	origRes, err := c.GetSandboxesSandboxIDWithResponse(t.Context(), sbx.SandboxID, setup.WithAPIKey())
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, origRes.StatusCode())
	require.NotNil(t, origRes.JSON200)
	assert.Equal(t, api.Running, origRes.JSON200.State)
}

func TestSandboxFork_InvalidCount(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()

	sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithAutoPause(false))

	count := int32(0)
	forkResp, err := c.PostSandboxesSandboxIDForkWithResponse(t.Context(), sbx.SandboxID, api.PostSandboxesSandboxIDForkJSONRequestBody{Count: &count}, setup.WithAPIKey())
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, forkResp.StatusCode())
}

func TestSandboxFork_AlreadyPaused(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()

	sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithAutoPause(false))
	pauseSandbox(t, c, sbx.SandboxID)

	forkResp, err := c.PostSandboxesSandboxIDForkWithResponse(t.Context(), sbx.SandboxID, api.PostSandboxesSandboxIDForkJSONRequestBody{}, setup.WithAPIKey())
	require.NoError(t, err)
	assert.Equal(t, http.StatusConflict, forkResp.StatusCode())
}

func TestSandboxFork_NotFound(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()

	sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithAutoPause(false))

	killResp, err := c.DeleteSandboxesSandboxIDWithResponse(t.Context(), sbx.SandboxID, setup.WithAPIKey())
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, killResp.StatusCode())

	forkResp, err := c.PostSandboxesSandboxIDForkWithResponse(t.Context(), sbx.SandboxID, api.PostSandboxesSandboxIDForkJSONRequestBody{}, setup.WithAPIKey())
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, forkResp.StatusCode())
}

func TestSandboxFork_CrossTeamAccess(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()
	db := setup.GetTestDBClient(t)

	sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithAutoPause(false))

	foreignUserID := utils.CreateUser(t, db)
	foreignTeamID := utils.CreateTeamWithUser(t, db, "foreign-team-fork", foreignUserID.String())
	foreignAPIKey := utils.CreateAPIKey(t, t.Context(), c, foreignUserID.String(), foreignTeamID)

	forkResp, err := c.PostSandboxesSandboxIDForkWithResponse(t.Context(), sbx.SandboxID, api.PostSandboxesSandboxIDForkJSONRequestBody{}, setup.WithAPIKey(foreignAPIKey))
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, forkResp.StatusCode(), "Should return 404 Not Found when trying to fork a sandbox owned by a different team")
}
