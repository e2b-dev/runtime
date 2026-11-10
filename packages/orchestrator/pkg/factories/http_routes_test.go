//go:build linux

package factories

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/healthcheck"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/localupload"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/service"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/service/machineinfo"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/servicestatus"
	orchestratorinfo "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator-info"
	e2bHealth "github.com/e2b-dev/infra/packages/shared/pkg/health"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

func TestHTTPHandlerExposesHealthAndLiveness(t *testing.T) {
	t.Parallel()
	info := service.NewInfoContainer(t.Context(), "node", "version", "route-test", "instance", machineinfo.MachineInfo{}, cfg.Config{BuilderConfig: cfg.BuilderConfig{OrchestratorBaseDir: t.TempDir()}})
	check, err := healthcheck.NewHealthcheck(info)
	require.NoError(t, err)
	server := httptest.NewServer(newHTTPHandler(check.CreateHandler(), nil))
	t.Cleanup(server.Close)
	client := server.Client()
	client.Timeout = 3 * time.Second
	for _, state := range []orchestratorinfo.ServiceInfoStatus{orchestratorinfo.ServiceInfoStatus_Unhealthy, orchestratorinfo.ServiceInfoStatus_Healthy} {
		require.NoError(t, info.OverrideStatus(t.Context(), servicestatus.Request{Status: state}))
		for _, route := range []string{"/live", "/health"} {
			request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL+route, nil)
			require.NoError(t, err)
			response, err := client.Do(request)
			require.NoError(t, err)
			t.Cleanup(func() { _ = response.Body.Close() })
			if route == "/live" {
				require.Equal(t, http.StatusOK, response.StatusCode, "the actual HTTP listener must expose process liveness independently of readiness")
			}
			var body e2bHealth.Response
			decodeErr := json.NewDecoder(response.Body).Decode(&body)
			require.NoError(t, response.Body.Close())
			require.NoError(t, decodeErr, "the outer HTTP router must expose the health handler")
			require.Equal(t, "route-test", body.Version)
			if route == "/health" && state == orchestratorinfo.ServiceInfoStatus_Unhealthy {
				require.Equal(t, http.StatusServiceUnavailable, response.StatusCode)
				require.Equal(t, e2bHealth.Unhealthy, body.Status)
			} else {
				require.Equal(t, http.StatusOK, response.StatusCode)
				require.Equal(t, e2bHealth.Healthy, body.Status)
			}
		}
	}
}

func TestHTTPHandlerPreservesUploadRoute(t *testing.T) {
	t.Parallel()
	info := service.NewInfoContainer(t.Context(), "node", "version", "route-test", "instance", machineinfo.MachineInfo{}, cfg.Config{BuilderConfig: cfg.BuilderConfig{OrchestratorBaseDir: t.TempDir()}})
	check, err := healthcheck.NewHealthcheck(info)
	require.NoError(t, err)
	root := t.TempDir()
	key := []byte("isolated-route-test-key")
	server := httptest.NewServer(newHTTPHandler(check.CreateHandler(), localupload.NewHandler(root, key)))
	t.Cleanup(server.Close)
	client := server.Client()
	client.Timeout = 3 * time.Second
	expiresSec := time.Now().Add(time.Minute).Unix()
	url := fmt.Sprintf("%s/upload?path=artifact&expires=%d&token=%s", server.URL, expiresSec, storage.ComputeUploadHMAC(key, "artifact", expiresSec))
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPut, url, bytes.NewBufferString("uploaded artifact"))
	require.NoError(t, err)
	response, err := client.Do(request)
	require.NoError(t, err)
	_, readErr := io.Copy(io.Discard, response.Body)
	require.NoError(t, response.Body.Close())
	require.NoError(t, readErr)
	require.Equal(t, http.StatusOK, response.StatusCode)
	contents, err := os.ReadFile(filepath.Join(root, "artifact"))
	require.NoError(t, err)
	require.Equal(t, "uploaded artifact", string(contents))
}
