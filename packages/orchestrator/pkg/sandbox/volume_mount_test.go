//go:build linux

package sandbox

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/envd"
)

// Verify the actual /init producer, not a separately constructed mount fixture:
// new-lifecycle targets change, same-lifecycle targets remain stable, and the
// workload destination never acquires the remote export's lifecycle prefix.
func TestEnvdInitSendsLifecycleQualifiedVolumeTargets(t *testing.T) { //nolint:paralleltest // package-global HTTP client
	sbx := newTestSandboxWithBundle("")
	sbx.config.NetworkConfig.OrchestratorInSandboxIPAddress = "169.254.0.21"
	sbx.Config.VolumeMounts = []VolumeMountConfig{{ID: uuid.New(), Name: "data", Path: "/mnt/data", Type: "test", Nconnect: 4}}
	oldLifecycle, newLifecycle := uuid.NewString(), uuid.NewString()
	type capture struct {
		body envd.PostInitJSONBody
		err  error
	}
	captured := make(chan capture, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body envd.PostInitJSONBody
		err := json.NewDecoder(r.Body).Decode(&body)
		captured <- capture{body: body, err: err}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(server.Close)
	originalClient := sandboxHttpClient
	sandboxHttpClient = http.Client{Timeout: time.Second}
	t.Cleanup(func() { sandboxHttpClient = originalClient })
	for _, lifecycle := range []string{oldLifecycle, oldLifecycle, newLifecycle} {
		sbx.LifecycleID = lifecycle
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		response, _, err := sbx.doRequestWithInfiniteRetries(ctx, sbx.newEnvdInitRequest(server.URL+"/init", sbx.ExpectedEnvdVersion()))
		cancel()
		require.NoError(t, err)
		require.NoError(t, response.Body.Close())
		got := <-captured
		require.NoError(t, got.err)
		require.Equal(t, lifecycle, got.body.LifecycleID)
		require.Len(t, got.body.VolumeMounts, 1)
		require.Equal(t, "169.254.0.21:/"+lifecycle+"/data", got.body.VolumeMounts[0].NfsTarget)
		require.Equal(t, "/mnt/data", got.body.VolumeMounts[0].Path)
		require.Equal(t, 4, got.body.VolumeMounts[0].Nconnect)
	}
}
