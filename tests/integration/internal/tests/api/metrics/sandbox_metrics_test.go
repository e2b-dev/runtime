package metrics

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/tests/integration/internal/api"
	"github.com/e2b-dev/infra/tests/integration/internal/setup"
	"github.com/e2b-dev/infra/tests/integration/internal/utils"
)

func TestSandboxMetrics(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()

	// Create a sandbox for testing
	sbx := utils.SetupSandboxWithCleanup(t, c)
	var metrics []api.SandboxMetric

	maxDuration := 15 * time.Second
	tick := 500 * time.Millisecond

	require.Eventually(t, func() bool {
		response, err := c.GetSandboxesSandboxIDMetricsWithResponse(t.Context(), sbx.SandboxID, &api.GetSandboxesSandboxIDMetricsParams{}, setup.WithAPIKey())
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, response.StatusCode())

		require.NotNil(t, response.JSON200)
		if len(*response.JSON200) == 0 {
			return false
		}

		metrics = *response.JSON200

		return true
	}, maxDuration, tick, "sandbox metrics not available in time")

	require.NotEmpty(t, metrics, "Expected at least one metric in the response")
	for _, metric := range metrics {
		require.NotEmpty(t, metric.CpuCount)
		require.GreaterOrEqual(t, metric.CpuUsedPct, float32(0))
		require.LessOrEqual(t, metric.CpuUsedPct, float32(100))
		require.NotEmpty(t, metric.MemUsed)
		require.NotEmpty(t, metric.MemTotal)
		require.NotEmpty(t, metric.DiskUsed)
		require.NotEmpty(t, metric.DiskTotal)
		require.NotEmpty(t, metric.Timestamp)
		require.NotEmpty(t, metric.TimestampUnix)
	}
}
