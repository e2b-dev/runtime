package api

import (
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/integration/internal/api"
	"github.com/e2b-dev/infra/packages/api/integration/internal/setup"
	"github.com/e2b-dev/infra/packages/api/integration/internal/utils"
)

// TestSandboxNoAutoResumeFilesystemOnly verifies that incoming proxy traffic
// does NOT auto-resume a filesystem-only snapshot, even with auto-resume
// enabled. Resuming a disk-only snapshot cold-boots (reboots) the guest and
// loses in-memory state, so doing it implicitly on a stray request would be
// surprising; the API refuses (FailedPrecondition) and the caller must resume
// explicitly. The sandbox must stay paused and the proxy request must not 200.
func TestSandboxNoAutoResumeFilesystemOnly(t *testing.T) {
	t.Parallel()

	c := setup.GetAPIClient()
	ctx := t.Context()

	sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithAutoPause(true), utils.WithAutoResume(true))
	envdClient := setup.GetEnvdClient(t, ctx)

	// Start an HTTP server inside the sandbox and confirm it's reachable.
	port := 8000
	startHTTPServerInSandbox(t, ctx, sbx, envdClient, port)

	proxyURL, err := url.Parse(setup.EnvdProxy)
	require.NoError(t, err)

	client := &http.Client{Timeout: 5 * time.Second}
	resp := utils.WaitForStatus(t, client, sbx, proxyURL, port, nil, http.StatusOK)
	require.NotNil(t, resp)
	require.NoError(t, resp.Body.Close())

	// Pause as a filesystem-only snapshot (memory:false).
	memory := false
	pauseResp, err := c.PostSandboxesSandboxIDPauseWithResponse(ctx, sbx.SandboxID,
		api.PostSandboxesSandboxIDPauseJSONRequestBody{Memory: &memory}, setup.WithAPIKey())
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, pauseResp.StatusCode())

	res, err := c.GetSandboxesSandboxIDWithResponse(ctx, sbx.SandboxID, setup.WithAPIKey())
	require.NoError(t, err)
	require.NotNil(t, res.JSON200, "expected 200 response, got status %d", res.StatusCode())
	require.Equal(t, api.Paused, res.JSON200.State)

	// A proxy request must NOT auto-resume the disk-only snapshot.
	resumeClient := &http.Client{Timeout: 15 * time.Second}
	req := utils.NewRequest(sbx, proxyURL, port, nil)
	resp, err = resumeClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.NotEqual(t, http.StatusOK, resp.StatusCode,
		"filesystem-only snapshot must not be auto-resumed by proxy traffic")

	// The sandbox must remain paused.
	res, err = c.GetSandboxesSandboxIDWithResponse(ctx, sbx.SandboxID, setup.WithAPIKey())
	require.NoError(t, err)
	require.NotNil(t, res.JSON200, "expected 200 response, got status %d", res.StatusCode())
	require.Equal(t, api.Paused, res.JSON200.State, "sandbox should remain paused")
}
