package api

import (
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/id"
	"github.com/e2b-dev/infra/tests/integration/internal/api"
	"github.com/e2b-dev/infra/tests/integration/internal/setup"
	"github.com/e2b-dev/infra/tests/integration/internal/utils"
)

func TestSandboxNotFound(t *testing.T) {
	t.Parallel()
	url, err := url.Parse(setup.EnvdProxy)
	require.NoError(t, err)

	// Test closed port
	port := 3210

	client := &http.Client{
		Timeout: 1000 * time.Second,
	}

	sbxID := "i" + id.Generate()
	sbx := &api.Sandbox{
		SandboxID: sbxID,
		ClientID:  "unknown",
	}

	resp := utils.WaitForStatus(t, client, sbx, url, port, nil, http.StatusBadGateway)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, http.StatusBadGateway, resp.StatusCode)

	assert.Equal(t, "application/json; charset=utf-8", resp.Header.Get("Content-Type"))
	// Parse error response
	var errorResp struct {
		Message   string `json:"message"`
		SandboxID string `json:"sandboxId"`
	}
	err = json.NewDecoder(resp.Body).Decode(&errorResp)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, "The sandbox was not found", errorResp.Message)
	assert.Equal(t, sbx.SandboxID, errorResp.SandboxID)

	// Pretend to be a browser
	headers := &http.Header{"User-Agent": []string{"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"}}
	resp = utils.WaitForStatus(t, client, sbx, url, port, headers, http.StatusBadGateway)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, http.StatusBadGateway, resp.StatusCode)
	assert.Equal(t, "text/html; charset=utf-8", resp.Header.Get("Content-Type"))
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	assert.True(t, strings.HasPrefix(string(body), "<!DOCTYPE html>"))
	assert.Contains(t, string(body), "Sandbox not found")
	assert.Contains(t, string(body), sbx.SandboxID)
	assert.True(t, strings.HasSuffix(strings.TrimSpace(string(body)), "</html>"))
}

// A browser only shows JS the proxy's error if the preflight got a 2xx and the
// response carries Access-Control-Allow-Origin; without them the SDK sees an
// opaque network error instead of the 502 and its JSON body.
func TestSandboxNotFoundCORS(t *testing.T) {
	t.Parallel()
	proxyURL, err := url.Parse(setup.EnvdProxy)
	require.NoError(t, err)

	port := 3210

	client := &http.Client{
		Timeout: 1000 * time.Second,
	}

	sbxID := "i" + id.Generate()
	sbx := &api.Sandbox{
		SandboxID: sbxID,
		ClientID:  "unknown",
	}

	preflight := utils.NewRequest(sbx, proxyURL, port, &http.Header{
		"Origin":                         []string{"https://app.example.com"},
		"Access-Control-Request-Method":  []string{http.MethodGet},
		"Access-Control-Request-Headers": []string{"e2b-sandbox-id,e2b-sandbox-port"},
	})
	preflight.Method = http.MethodOptions

	resp, err := client.Do(preflight)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	assert.GreaterOrEqual(t, resp.StatusCode, 200)
	assert.Less(t, resp.StatusCode, 300)
	assert.Equal(t, "*", resp.Header.Get("Access-Control-Allow-Origin"))
	assert.Equal(t, "e2b-sandbox-id,e2b-sandbox-port", resp.Header.Get("Access-Control-Allow-Headers"))

	headers := &http.Header{"Origin": []string{"https://app.example.com"}}
	resp = utils.WaitForStatus(t, client, sbx, proxyURL, port, headers, http.StatusBadGateway)
	require.NotNil(t, resp)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusBadGateway, resp.StatusCode)
	assert.Equal(t, "*", resp.Header.Get("Access-Control-Allow-Origin"))
}
