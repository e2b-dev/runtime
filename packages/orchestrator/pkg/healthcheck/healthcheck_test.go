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

func TestShuttingDownHealthcheckRemainsDraining(t *testing.T) {
	t.Parallel()

	info := service.NewInfoContainer(t.Context(), "node", "version", "test-commit", "instance", machineinfo.MachineInfo{}, cfg.Config{BuilderConfig: cfg.BuilderConfig{OrchestratorBaseDir: t.TempDir()}})
	info.SetStatus(t.Context(), orchestratorinfo.ServiceInfoStatus_ShuttingDown)
	healthcheck, err := NewHealthcheck(info)
	require.NoError(t, err)

	response := httptest.NewRecorder()
	healthcheck.CreateHandler().ServeHTTP(response, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/health", nil))
	require.Equal(t, http.StatusOK, response.Code)

	var body e2bHealth.Response
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
	require.Equal(t, e2bHealth.Draining, body.Status)
	require.Equal(t, info.SourceCommit, body.Version)
}
