package api

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/envd/process"
	"github.com/e2b-dev/infra/tests/integration/internal/api"
	"github.com/e2b-dev/infra/tests/integration/internal/setup"
	"github.com/e2b-dev/infra/tests/integration/internal/utils"
)

func startHTTPServerInSandbox(t *testing.T, ctx context.Context, sbx *api.Sandbox, envdClient *setup.EnvdClient, port int) {
	t.Helper()

	serverCtx, serverCancel := context.WithCancel(ctx)
	serverReq := connect.NewRequest(&process.StartRequest{
		Process: &process.ProcessConfig{
			Cmd:  "python",
			Args: []string{"-m", "http.server", fmt.Sprintf("%d", port)},
		},
	})
	setup.SetSandboxHeader(t, serverReq.Header(), sbx)
	setup.SetUserHeader(t, serverReq.Header(), "user")
	serverStream, err := envdClient.ProcessClient.Start(serverCtx, serverReq)
	require.NoError(t, err)

	t.Cleanup(func() {
		serverCancel()
		if streamErr := serverStream.Close(); streamErr != nil {
			t.Logf("Error closing server stream: %v", streamErr)
		}
	})

	// Wait for server to start.
	time.Sleep(time.Second)
}

func TestSandboxAutoResumeViaExec(t *testing.T) {
	t.Parallel()

	c := setup.GetAPIClient()
	ctx := t.Context()

	sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithAutoPause(true), utils.WithAutoResume(true))
	envdClient := setup.GetEnvdClient(t, ctx)

	// Run ls before pausing.
	err := utils.ExecCommand(t, ctx, sbx, envdClient, "ls")
	require.NoError(t, err)

	// Pause the sandbox.
	pauseResp, err := c.PostSandboxesSandboxIDPauseWithResponse(ctx, sbx.SandboxID, api.PostSandboxesSandboxIDPauseJSONRequestBody{}, setup.WithAPIKey())
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, pauseResp.StatusCode())

	res, err := c.GetSandboxesSandboxIDWithResponse(ctx, sbx.SandboxID, setup.WithAPIKey())
	require.NoError(t, err)
	require.NotNil(t, res.JSON200, "expected 200 response, got status %d", res.StatusCode())
	require.Equal(t, api.Paused, res.JSON200.State)
	// Run ls again — this should trigger auto-resume.
	err = utils.ExecCommand(t, ctx, sbx, envdClient, "ls")
	require.NoError(t, err)

	// Verify the sandbox is running again.
	res, err = c.GetSandboxesSandboxIDWithResponse(ctx, sbx.SandboxID, setup.WithAPIKey())
	require.NoError(t, err)
	require.NotNil(t, res.JSON200, "expected 200 response, got status %d", res.StatusCode())
	require.Equal(t, api.Running, res.JSON200.State, "sandbox should be running after auto-resume")
}

func TestSandboxAutoResumeViaProxy(t *testing.T) {
	t.Parallel()

	c := setup.GetAPIClient()
	ctx := t.Context()

	sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithAutoPause(true), utils.WithAutoResume(true))
	envdClient := setup.GetEnvdClient(t, ctx)

	// Start an HTTP server inside the sandbox.
	port := 8000
	startHTTPServerInSandbox(t, ctx, sbx, envdClient, port)

	proxyURL, err := url.Parse(setup.EnvdProxy)
	require.NoError(t, err)

	// Verify the server is accessible before pausing.
	client := &http.Client{Timeout: 5 * time.Second}
	resp := utils.WaitForStatus(t, client, sbx, proxyURL, port, nil, http.StatusOK)
	require.NotNil(t, resp)
	require.NoError(t, resp.Body.Close())

	// Pause the sandbox.
	pauseResp, err := c.PostSandboxesSandboxIDPauseWithResponse(ctx, sbx.SandboxID, api.PostSandboxesSandboxIDPauseJSONRequestBody{}, setup.WithAPIKey())
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, pauseResp.StatusCode())

	// Verify sandbox is paused.
	res, err := c.GetSandboxesSandboxIDWithResponse(ctx, sbx.SandboxID, setup.WithAPIKey())
	require.NoError(t, err)
	require.NotNil(t, res.JSON200, "expected 200 response, got status %d", res.StatusCode())
	require.Equal(t, api.Paused, res.JSON200.State)

	// Make a proxy request to trigger auto-resume. The single request must
	// cover the whole snapshot resume, which can take a while under load.
	resumeClient := &http.Client{Timeout: 60 * time.Second}
	req := utils.NewRequest(sbx, proxyURL, port, nil)
	resp, err = resumeClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "expected server response after auto-resume")

	// Verify the sandbox is running — it must be since the server responded.
	res, err = c.GetSandboxesSandboxIDWithResponse(ctx, sbx.SandboxID, setup.WithAPIKey())
	require.NoError(t, err)
	require.NotNil(t, res.JSON200, "expected 200 response, got status %d", res.StatusCode())
	require.Equal(t, api.Running, res.JSON200.State, "sandbox should be running after auto-resume")
}

func TestSandboxNoAutoResumeViaProxyWithoutFlag(t *testing.T) {
	t.Parallel()

	c := setup.GetAPIClient()
	ctx := t.Context()

	sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithAutoPause(true))
	envdClient := setup.GetEnvdClient(t, ctx)

	// Start an HTTP server inside the sandbox.
	port := 8000
	startHTTPServerInSandbox(t, ctx, sbx, envdClient, port)

	proxyURL, err := url.Parse(setup.EnvdProxy)
	require.NoError(t, err)

	// Verify the server is accessible before pausing.
	client := &http.Client{Timeout: 5 * time.Second}
	resp := utils.WaitForStatus(t, client, sbx, proxyURL, port, nil, http.StatusOK)
	require.NotNil(t, resp)
	require.NoError(t, resp.Body.Close())

	// Pause the sandbox.
	pauseResp, err := c.PostSandboxesSandboxIDPauseWithResponse(ctx, sbx.SandboxID, api.PostSandboxesSandboxIDPauseJSONRequestBody{}, setup.WithAPIKey())
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, pauseResp.StatusCode())

	// Verify sandbox is paused.
	res, err := c.GetSandboxesSandboxIDWithResponse(ctx, sbx.SandboxID, setup.WithAPIKey())
	require.NoError(t, err)
	require.NotNil(t, res.JSON200, "expected 200 response, got status %d", res.StatusCode())
	require.Equal(t, api.Paused, res.JSON200.State)

	// Make a proxy request. It should not auto-resume without the flag.
	resumeClient := &http.Client{Timeout: 10 * time.Second}
	req := utils.NewRequest(sbx, proxyURL, port, nil)
	resp, err = resumeClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.NotEqual(t, http.StatusOK, resp.StatusCode, "sandbox without auto-resume should not serve proxy traffic after pause")

	// Verify the sandbox is still paused.
	res, err = c.GetSandboxesSandboxIDWithResponse(ctx, sbx.SandboxID, setup.WithAPIKey())
	require.NoError(t, err)
	require.NotNil(t, res.JSON200, "expected 200 response, got status %d", res.StatusCode())
	require.Equal(t, api.Paused, res.JSON200.State, "sandbox should still be paused without auto-resume")
}

func TestSandboxAutoResumeWithoutExplicitTimeoutUsesMinimumTimeout(t *testing.T) {
	t.Parallel()

	c := setup.GetAPIClient()
	ctx := t.Context()

	autoPause := true
	autoResume := api.SandboxAutoResumeConfig{Enabled: true}

	// Create sandbox without passing timeout so API default is used.
	utils.AcquireSandboxSlot(t)
	createResp, err := c.PostSandboxesWithResponse(ctx, api.NewSandbox{
		TemplateID: setup.SandboxTemplateID,
		AutoPause:  &autoPause,
		AutoResume: &autoResume,
	}, setup.WithAPIKey())
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, createResp.StatusCode())
	require.NotNil(t, createResp.JSON201)

	sbx := createResp.JSON201
	t.Cleanup(func() {
		utils.TeardownSandbox(t, c, sbx.SandboxID)
	})

	envdClient := setup.GetEnvdClient(t, ctx)

	// Run ls before pausing.
	err = utils.ExecCommand(t, ctx, sbx, envdClient, "ls")
	require.NoError(t, err)

	// Pause the sandbox.
	pauseResp, err := c.PostSandboxesSandboxIDPauseWithResponse(ctx, sbx.SandboxID, api.PostSandboxesSandboxIDPauseJSONRequestBody{}, setup.WithAPIKey())
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, pauseResp.StatusCode())

	// Run ls again — this should trigger auto-resume.
	err = utils.ExecCommand(t, ctx, sbx, envdClient, "ls")
	require.NoError(t, err)

	// Verify auto-resume applies a minimum timeout (~300s) instead of 15s API default.
	res, err := c.GetSandboxesSandboxIDWithResponse(ctx, sbx.SandboxID, setup.WithAPIKey())
	require.NoError(t, err)
	require.NotNil(t, res.JSON200, "expected 200 response, got status %d", res.StatusCode())
	require.Equal(t, api.Running, res.JSON200.State, "sandbox should be running after auto-resume")
	resumedDuration := res.JSON200.EndAt.Sub(res.JSON200.StartedAt)
	require.InDelta(t, 300, resumedDuration.Seconds(), 30, "expected resumed timeout near 300s minimum, got %s", resumedDuration)
}

func TestSandboxAutoResumeUsesInitialTimeoutNotUpdatedTimeout(t *testing.T) {
	t.Parallel()

	c := setup.GetAPIClient()
	ctx := t.Context()

	// Create sandbox with explicit starting timeout above the (now) 300s floor.
	sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithTimeout(420), utils.WithAutoPause(true), utils.WithAutoResume(true))
	envdClient := setup.GetEnvdClient(t, ctx)

	// Capture initial end time and update current timeout to a larger value.
	res, err := c.GetSandboxesSandboxIDWithResponse(ctx, sbx.SandboxID, setup.WithAPIKey())
	require.NoError(t, err)
	require.NotNil(t, res.JSON200, "expected 200 response, got status %d", res.StatusCode())
	require.Equal(t, api.Running, res.JSON200.State)
	initialEndAt := res.JSON200.EndAt

	timeoutResp, err := c.PostSandboxesSandboxIDTimeoutWithResponse(ctx, sbx.SandboxID, api.PostSandboxesSandboxIDTimeoutJSONRequestBody{
		Timeout: 600,
	}, setup.WithAPIKey())
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, timeoutResp.StatusCode())

	res, err = c.GetSandboxesSandboxIDWithResponse(ctx, sbx.SandboxID, setup.WithAPIKey())
	require.NoError(t, err)
	require.NotNil(t, res.JSON200, "expected 200 response, got status %d", res.StatusCode())
	require.Equal(t, api.Running, res.JSON200.State)
	require.True(t, res.JSON200.EndAt.After(initialEndAt), "endAt should move forward after timeout update")

	// Run ls before pausing.
	err = utils.ExecCommand(t, ctx, sbx, envdClient, "ls")
	require.NoError(t, err)

	// Pause the sandbox.
	pauseResp, err := c.PostSandboxesSandboxIDPauseWithResponse(ctx, sbx.SandboxID, api.PostSandboxesSandboxIDPauseJSONRequestBody{}, setup.WithAPIKey())
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, pauseResp.StatusCode())

	// Run ls again — this should trigger auto-resume.
	err = utils.ExecCommand(t, ctx, sbx, envdClient, "ls")
	require.NoError(t, err)

	// Verify auto-resume uses original starting timeout (420s), not updated timeout (600s).
	res, err = c.GetSandboxesSandboxIDWithResponse(ctx, sbx.SandboxID, setup.WithAPIKey())
	require.NoError(t, err)
	require.NotNil(t, res.JSON200, "expected 200 response, got status %d", res.StatusCode())
	require.Equal(t, api.Running, res.JSON200.State, "sandbox should be running after auto-resume")
	resumedDuration := res.JSON200.EndAt.Sub(res.JSON200.StartedAt)
	require.InDelta(t, 420, resumedDuration.Seconds(), 45, "expected timeout ~420s (starting timeout), got %s", resumedDuration)
	require.Less(t, resumedDuration.Seconds(), 540.0, "resumed timeout should not follow updated 600s timeout")
}

func TestSandboxNoAutoResumeWithoutFlag(t *testing.T) {
	t.Parallel()

	c := setup.GetAPIClient()
	ctx := t.Context()

	sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithAutoPause(true))
	envdClient := setup.GetEnvdClient(t, ctx)

	// Run ls before pausing.
	err := utils.ExecCommand(t, ctx, sbx, envdClient, "ls")
	require.NoError(t, err)

	// Pause the sandbox.
	pauseResp, err := c.PostSandboxesSandboxIDPauseWithResponse(ctx, sbx.SandboxID, api.PostSandboxesSandboxIDPauseJSONRequestBody{}, setup.WithAPIKey())
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, pauseResp.StatusCode())

	// Verify sandbox is paused.
	res, err := c.GetSandboxesSandboxIDWithResponse(ctx, sbx.SandboxID, setup.WithAPIKey())
	require.NoError(t, err)
	require.NotNil(t, res.JSON200, "expected 200 response, got status %d", res.StatusCode())
	require.Equal(t, api.Paused, res.JSON200.State)

	// Attempt to exec — should fail since auto-resume is not enabled.
	err = utils.ExecCommand(t, ctx, sbx, envdClient, "ls")
	require.Error(t, err)

	// Verify the sandbox is still paused.
	res, err = c.GetSandboxesSandboxIDWithResponse(ctx, sbx.SandboxID, setup.WithAPIKey())
	require.NoError(t, err)
	require.NotNil(t, res.JSON200, "expected 200 response, got status %d", res.StatusCode())
	require.Equal(t, api.Paused, res.JSON200.State, "sandbox should still be paused without auto-resume")
}
