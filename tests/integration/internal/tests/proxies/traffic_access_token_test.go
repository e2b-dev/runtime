package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/envd/process"
	proxygrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/proxy"
	"github.com/e2b-dev/infra/tests/integration/internal/api"
	"github.com/e2b-dev/infra/tests/integration/internal/setup"
	"github.com/e2b-dev/infra/tests/integration/internal/utils"
)

func TestSandboxWithEnabledTrafficAccessTokenButMissingHeader(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()

	sbxNetAllowPublic := false
	sbxNet := &api.SandboxNetworkConfig{
		AllowPublicTraffic: &sbxNetAllowPublic,
	}
	sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithNetwork(sbxNet))
	require.NotNil(t, sbx.TrafficAccessToken)
	require.NotNil(t, sbx.EnvdAccessToken)

	url, err := url.Parse(setup.EnvdProxy)
	require.NoError(t, err)

	client := &http.Client{
		Timeout: 1000 * time.Second,
	}

	sbx.TrafficAccessToken = nil // Simulate missing header

	port := 8080
	req := utils.NewRequest(sbx, url, port, nil)
	resp, err := client.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusForbidden, resp.StatusCode)
	assert.Equal(t, "application/json; charset=utf-8", resp.Header.Get("Content-Type"))

	// Parse error response
	var errorResp struct {
		Message   string `json:"message"`
		SandboxID string `json:"sandboxId"`
	}
	err = json.NewDecoder(resp.Body).Decode(&errorResp)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, "Sandbox is secured with traffic access token. Token header 'e2b-traffic-access-token' is missing", errorResp.Message)
	assert.Equal(t, sbx.SandboxID, errorResp.SandboxID)

	// Pretend to be a browser
	headers := &http.Header{"User-Agent": []string{"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"}}
	req = utils.NewRequest(sbx, url, port, headers)
	resp, err = client.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusForbidden, resp.StatusCode)
	assert.Equal(t, "text/html; charset=utf-8", resp.Header.Get("Content-Type"))
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	assert.True(t, strings.HasPrefix(string(body), "<!DOCTYPE html>"))
	assert.Contains(t, string(body), "Missing traffic access token")
	assert.Contains(t, string(body), sbx.SandboxID)
	assert.True(t, strings.HasSuffix(strings.TrimSpace(string(body)), "</html>"))
}

func TestSandboxWithEnabledTrafficAccessTokenButInvalidHeader(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()

	sbxNetAllowPublic := false
	sbxNet := &api.SandboxNetworkConfig{
		AllowPublicTraffic: &sbxNetAllowPublic,
	}
	sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithNetwork(sbxNet))
	require.NotNil(t, sbx.TrafficAccessToken)
	require.NotNil(t, sbx.EnvdAccessToken)

	url, err := url.Parse(setup.EnvdProxy)
	require.NoError(t, err)

	client := &http.Client{
		Timeout: 1000 * time.Second,
	}

	// Simulate invalid header
	invalidTrafficAccessToken := "abcd"
	sbx.TrafficAccessToken = &invalidTrafficAccessToken

	port := 8080
	req := utils.NewRequest(sbx, url, port, nil)
	resp, err := client.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusForbidden, resp.StatusCode)
	assert.Equal(t, "application/json; charset=utf-8", resp.Header.Get("Content-Type"))

	// Parse error response
	var errorResp struct {
		Message   string `json:"message"`
		SandboxID string `json:"sandboxId"`
	}
	err = json.NewDecoder(resp.Body).Decode(&errorResp)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, "Sandbox is secured with traffic access token. Provided token in header 'e2b-traffic-access-token' is invalid", errorResp.Message)
	assert.Equal(t, sbx.SandboxID, errorResp.SandboxID)

	// Pretend to be a browser
	headers := &http.Header{"User-Agent": []string{"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"}}
	req = utils.NewRequest(sbx, url, port, headers)
	resp, err = client.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusForbidden, resp.StatusCode)
	assert.Equal(t, "text/html; charset=utf-8", resp.Header.Get("Content-Type"))
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	assert.True(t, strings.HasPrefix(string(body), "<!DOCTYPE html>"))
	assert.Contains(t, string(body), "Invalid traffic access token")
	assert.Contains(t, string(body), sbx.SandboxID)
	assert.True(t, strings.HasSuffix(strings.TrimSpace(string(body)), "</html>"))
}

func TestSandboxWithEnabledTrafficAccessToken(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()

	sbxNetAllowPublic := false
	sbxNet := &api.SandboxNetworkConfig{
		AllowPublicTraffic: &sbxNetAllowPublic,
	}
	sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithNetwork(sbxNet))
	require.NotNil(t, sbx.TrafficAccessToken)
	require.NotNil(t, sbx.EnvdAccessToken)

	url, err := url.Parse(setup.EnvdProxy)
	require.NoError(t, err)

	client := &http.Client{
		Timeout: 1000 * time.Second,
	}

	port := 8080
	req := utils.NewRequest(sbx, url, port, nil)
	resp, err := client.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusBadGateway, resp.StatusCode)

	assert.Equal(t, "application/json; charset=utf-8", resp.Header.Get("Content-Type"))

	// Parse error response
	var errorResp struct {
		Message   string `json:"message"`
		SandboxID string `json:"sandboxId"`
		Port      int    `json:"port"`
	}
	err = json.NewDecoder(resp.Body).Decode(&errorResp)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, "The sandbox is running but port is not open", errorResp.Message)
	assert.Equal(t, sbx.SandboxID, errorResp.SandboxID)
	assert.Equal(t, port, errorResp.Port)
}

func TestEnvdPortIsNotAffectedByTrafficAccessToken(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()

	sbxNetAllowPublic := false
	sbxNet := &api.SandboxNetworkConfig{
		AllowPublicTraffic: &sbxNetAllowPublic,
	}
	sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithNetwork(sbxNet))
	require.NotNil(t, sbx.TrafficAccessToken)
	require.NotNil(t, sbx.EnvdAccessToken)

	url, err := url.Parse(setup.EnvdProxy)
	require.NoError(t, err)
	envdHealthURL := *url
	envdHealthURL.Path = "/health"

	client := &http.Client{
		Timeout: 1000 * time.Second,
	}

	headers := &http.Header{proxygrpc.MetadataEnvdHTTPAccessToken: []string{*sbx.EnvdAccessToken}}
	req := utils.NewRequest(sbx, &envdHealthURL, int(consts.DefaultEnvdServerPort), headers)
	resp, err := client.Do(req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusNoContent, resp.StatusCode)
}

func TestSandboxWithTrafficAccessTokenAutoResumeViaProxy(t *testing.T) {
	t.Parallel()

	c := setup.GetAPIClient()
	ctx := t.Context()

	sbxNetAllowPublic := false
	sbxNet := &api.SandboxNetworkConfig{
		AllowPublicTraffic: &sbxNetAllowPublic,
	}

	sbx := utils.SetupSandboxWithCleanup(
		t,
		c,
		utils.WithNetwork(sbxNet),
		utils.WithAutoPause(true),
		utils.WithAutoResume(true),
	)
	require.NotNil(t, sbx.TrafficAccessToken)
	require.NotNil(t, sbx.EnvdAccessToken)

	envdClient := setup.GetEnvdClient(t, ctx)
	serverCtx, serverCancel := context.WithCancel(ctx)
	port := 8080
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
	defer func() {
		serverCancel()
		if streamErr := serverStream.Close(); streamErr != nil {
			t.Logf("Error closing server stream: %v", streamErr)
		}
	}()

	proxyURL, err := url.Parse(setup.EnvdProxy)
	require.NoError(t, err)

	client := &http.Client{Timeout: 10 * time.Second}

	sbxWithoutToken := *sbx
	sbxWithoutToken.TrafficAccessToken = nil

	// Valid traffic token allows access (wait for python server to start).
	resp := utils.WaitForStatus(t, client, sbx, proxyURL, port, nil, http.StatusOK)
	require.NotNil(t, resp)
	require.NoError(t, resp.Body.Close())

	// Pause sandbox.
	pauseResp, err := c.PostSandboxesSandboxIDPauseWithResponse(ctx, sbx.SandboxID, api.PostSandboxesSandboxIDPauseJSONRequestBody{}, setup.WithAPIKey())
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, pauseResp.StatusCode())

	res, err := c.GetSandboxesSandboxIDWithResponse(ctx, sbx.SandboxID, setup.WithAPIKey())
	require.NoError(t, err)
	require.NotNil(t, res.JSON200, "expected 200 response, got status %d", res.StatusCode())
	require.Equal(t, api.Paused, res.JSON200.State)

	// While paused, missing token must not auto-resume and request should fail.
	req := utils.NewRequest(&sbxWithoutToken, proxyURL, port, nil)
	resp, err = client.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusForbidden, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	res, err = c.GetSandboxesSandboxIDWithResponse(ctx, sbx.SandboxID, setup.WithAPIKey())
	require.NoError(t, err)
	require.NotNil(t, res.JSON200, "expected 200 response, got status %d", res.StatusCode())
	require.Equal(t, api.Paused, res.JSON200.State)

	// Valid token request should auto-resume and succeed.
	resp = utils.WaitForStatus(t, client, sbx, proxyURL, port, nil, http.StatusOK)
	require.NotNil(t, resp)
	require.NoError(t, resp.Body.Close())

	res, err = c.GetSandboxesSandboxIDWithResponse(ctx, sbx.SandboxID, setup.WithAPIKey())
	require.NoError(t, err)
	require.NotNil(t, res.JSON200, "expected 200 response, got status %d", res.StatusCode())
	require.Equal(t, api.Running, res.JSON200.State)
}

func TestEnvdAccessTokenAutoResumeViaProxy(t *testing.T) {
	t.Parallel()

	c := setup.GetAPIClient()
	ctx := t.Context()

	sbx := utils.SetupSandboxWithCleanup(
		t,
		c,
		utils.WithAutoPause(true),
		utils.WithAutoResume(true),
	)
	require.NotNil(t, sbx.EnvdAccessToken)

	proxyURL, err := url.Parse(setup.EnvdProxy)
	require.NoError(t, err)
	envdHealthURL := *proxyURL
	envdHealthURL.Path = "/health"

	// The auto-resume request below must cover the whole snapshot resume,
	// which can take longer than 10s under load.
	client := &http.Client{Timeout: 60 * time.Second}
	envdPort := int(consts.DefaultEnvdServerPort)

	// Verify envd is reachable with valid access token while running.
	headers := &http.Header{proxygrpc.MetadataEnvdHTTPAccessToken: []string{*sbx.EnvdAccessToken}}
	req := utils.NewRequest(sbx, &envdHealthURL, envdPort, headers)
	resp, err := client.Do(req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, http.StatusNoContent, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	// Pause sandbox.
	pauseResp, err := c.PostSandboxesSandboxIDPauseWithResponse(ctx, sbx.SandboxID, api.PostSandboxesSandboxIDPauseJSONRequestBody{}, setup.WithAPIKey())
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, pauseResp.StatusCode())

	res, err := c.GetSandboxesSandboxIDWithResponse(ctx, sbx.SandboxID, setup.WithAPIKey())
	require.NoError(t, err)
	require.NotNil(t, res.JSON200, "expected 200 response, got status %d", res.StatusCode())
	require.Equal(t, api.Paused, res.JSON200.State)

	// While paused, missing envd access token must not auto-resume.
	req = utils.NewRequest(sbx, &envdHealthURL, envdPort, nil)
	resp, err = client.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusForbidden, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	res, err = c.GetSandboxesSandboxIDWithResponse(ctx, sbx.SandboxID, setup.WithAPIKey())
	require.NoError(t, err)
	require.NotNil(t, res.JSON200, "expected 200 response, got status %d", res.StatusCode())
	require.Equal(t, api.Paused, res.JSON200.State)

	// Valid envd access token should auto-resume.
	req = utils.NewRequest(sbx, &envdHealthURL, envdPort, headers)
	resp, err = client.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	res, err = c.GetSandboxesSandboxIDWithResponse(ctx, sbx.SandboxID, setup.WithAPIKey())
	require.NoError(t, err)
	require.NotNil(t, res.JSON200, "expected 200 response, got status %d", res.StatusCode())
	require.Equal(t, api.Running, res.JSON200.State)
}
