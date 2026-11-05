//go:build linux

package healthcheck

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/service"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/service/machineinfo"
	orchestratorinfo "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator-info"
	e2bHealth "github.com/e2b-dev/infra/packages/shared/pkg/health"
)

func TestHealthAndLivenessRoutesPreserveServiceState(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		state  orchestratorinfo.ServiceInfoStatus
		code   int
		status e2bHealth.Status
	}{
		{"booting", orchestratorinfo.ServiceInfoStatus_Unhealthy, http.StatusServiceUnavailable, e2bHealth.Unhealthy},
		{"not ready", orchestratorinfo.ServiceInfoStatus_NotReady, http.StatusServiceUnavailable, e2bHealth.Unhealthy},
		{"ready", orchestratorinfo.ServiceInfoStatus_Healthy, http.StatusOK, e2bHealth.Healthy},
		{"draining", orchestratorinfo.ServiceInfoStatus_Draining, http.StatusOK, e2bHealth.Draining},
		{"standby", orchestratorinfo.ServiceInfoStatus_Standby, http.StatusOK, e2bHealth.Draining},
		{"shutdown", orchestratorinfo.ServiceInfoStatus_ShuttingDown, http.StatusOK, e2bHealth.Draining},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			info := service.NewInfoContainer(t.Context(), "node", "version", "startup-test", "service-generation", machineinfo.MachineInfo{}, cfg.Config{BuilderConfig: cfg.BuilderConfig{OrchestratorBaseDir: t.TempDir()}})
			check, err := NewHealthcheck(info)
			require.NoError(t, err)
			info.SetStatus(t.Context(), test.state)
			for _, route := range []string{"/health", "/live"} {
				response := httptest.NewRecorder()
				check.CreateHandler().ServeHTTP(response, httptest.NewRequestWithContext(t.Context(), http.MethodGet, route, nil))
				var body e2bHealth.Response
				require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
				require.Equal(t, "startup-test", body.Version)
				if route == "/live" {
					require.Equal(t, http.StatusOK, response.Code, "live process must answer independently of startup readiness")
					require.Equal(t, e2bHealth.Healthy, body.Status)
				} else {
					require.Equal(t, test.code, response.Code)
					require.Equal(t, test.status, body.Status, "preserve readiness and draining discovery contract")
				}
			}
		})
	}
}
