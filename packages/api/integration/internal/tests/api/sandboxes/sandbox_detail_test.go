package sandboxes

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/e2b-dev/infra/packages/api/integration/internal/api"
	"github.com/e2b-dev/infra/packages/api/integration/internal/setup"
	"github.com/e2b-dev/infra/packages/api/integration/internal/utils"
)

func TestSandboxDetailRunning(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()

	// Create a sandbox for testing
	sbx := utils.SetupSandboxWithCleanup(t, c)

	// Test basic list functionality
	response, err := c.GetSandboxesSandboxIDWithResponse(t.Context(), sbx.SandboxID, setup.WithAPIKey())
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode())

	returnedSbx := response.JSON200
	assert.Equal(t, sbx.SandboxID, returnedSbx.SandboxID)
	assert.Equal(t, sbx.TemplateID, returnedSbx.TemplateID)
	require.NotNil(t, returnedSbx.Alias)
	assert.Equal(t, "base", *returnedSbx.Alias)
}

func TestSandboxDetailReturnsLifecycleAndNetworkConfig(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()

	allowInternetAccess := true
	allowOut := []string{"8.8.8.8"}
	denyOut := []string{"10.0.0.0/8"}

	sbx := utils.SetupSandboxWithCleanup(
		t,
		c,
		utils.WithAutoPause(true),
		utils.WithAutoResume(true),
		utils.WithAllowInternetAccess(allowInternetAccess),
		utils.WithNetwork(&api.SandboxNetworkConfig{
			AllowOut: &allowOut,
			DenyOut:  &denyOut,
		}),
	)

	assertDetail := func(t *testing.T, expectedState api.SandboxState) {
		t.Helper()

		response, err := c.GetSandboxesSandboxIDWithResponse(t.Context(), sbx.SandboxID, setup.WithAPIKey())
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, response.StatusCode())
		require.NotNil(t, response.JSON200)

		returnedSbx := response.JSON200
		assert.Equal(t, sbx.SandboxID, returnedSbx.SandboxID)
		assert.Equal(t, sbx.TemplateID, returnedSbx.TemplateID)
		assert.Equal(t, expectedState, returnedSbx.State)

		require.NotNil(t, returnedSbx.AllowInternetAccess)
		assert.Equal(t, allowInternetAccess, *returnedSbx.AllowInternetAccess)

		require.NotNil(t, returnedSbx.Lifecycle)
		assert.True(t, returnedSbx.Lifecycle.AutoResume)
		assert.Equal(t, api.Pause, returnedSbx.Lifecycle.OnTimeout)

		require.NotNil(t, returnedSbx.Network)
		require.NotNil(t, returnedSbx.Network.AllowOut)
		assert.Equal(t, allowOut, *returnedSbx.Network.AllowOut)
		require.NotNil(t, returnedSbx.Network.DenyOut)
		assert.Equal(t, denyOut, *returnedSbx.Network.DenyOut)
	}

	assertDetail(t, api.Running)

	pauseSandbox(t, c, sbx.SandboxID)
	assertDetail(t, api.Paused)
}

func TestSandboxDetailPaused(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()

	sbx := utils.SetupSandboxWithCleanup(t, c)
	sandboxID := sbx.SandboxID
	pauseSandbox(t, c, sandboxID)

	response, err := c.GetSandboxesSandboxIDWithResponse(t.Context(), sandboxID, setup.WithAPIKey())

	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode())
	returnedSbx := response.JSON200
	assert.Equal(t, sbx.SandboxID, returnedSbx.SandboxID)
	assert.Equal(t, sbx.TemplateID, returnedSbx.TemplateID)
	require.NotNil(t, returnedSbx.Alias)
	assert.Equal(t, "base", *returnedSbx.Alias)
}

func TestSandboxDetailPausingSandbox(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()

	sbx := utils.SetupSandboxWithCleanup(t, c)
	sandboxID := sbx.SandboxID

	wg := errgroup.Group{}
	wg.Go(func() error {
		pauseSandboxResponse, err := c.PostSandboxesSandboxIDPauseWithResponse(t.Context(), sandboxID, api.PostSandboxesSandboxIDPauseJSONRequestBody{}, setup.WithAPIKey())
		if err != nil {
			return err
		}

		if pauseSandboxResponse.StatusCode() != http.StatusNoContent {
			return fmt.Errorf("expected status code %d, got %d", http.StatusNoContent, pauseSandboxResponse.StatusCode())
		}

		return nil
	})

	require.Eventually(t, func() bool {
		detailResponse, err := c.GetSandboxesSandboxIDWithResponse(t.Context(), sandboxID, setup.WithAPIKey())
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, detailResponse.StatusCode())
		require.NotNil(t, detailResponse.JSON200)

		return detailResponse.JSON200.State == api.Paused
	}, 10*time.Second, 100*time.Millisecond, "Sandbox did not reach paused state in time")

	err := wg.Wait()
	require.NoError(t, err)
}
