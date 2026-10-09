package metrics

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/tests/integration/internal/api"
	"github.com/e2b-dev/infra/tests/integration/internal/setup"
	"github.com/e2b-dev/infra/tests/integration/internal/utils"
)

func TestSandboxListMetrics(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()

	// Create a sandbox for testing
	sbx1 := utils.SetupSandboxWithCleanup(t, c)
	sbx2 := utils.SetupSandboxWithCleanup(t, c)

	maxDuration := 15 * time.Second
	tick := 500 * time.Millisecond
	var metrics map[string]api.SandboxMetric

	require.Eventually(t, func() bool {
		response, err := c.GetSandboxesMetricsWithResponse(t.Context(), &api.GetSandboxesMetricsParams{SandboxIds: []string{sbx1.SandboxID, sbx2.SandboxID}}, setup.WithAPIKey())
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, response.StatusCode())

		require.NotNil(t, response.JSON200)
		require.NotNil(t, response.JSON200.Sandboxes)
		if len(response.JSON200.Sandboxes) < 2 {
			return false
		}

		metrics = response.JSON200.Sandboxes

		return true
	}, maxDuration, tick, "sandbox metrics not available in time")

	require.Len(t, metrics, 2, "Expected two metrics in the response")
	assert.Contains(t, metrics, sbx1.SandboxID, "Expected sandbox metrics to include the created sandbox")
	assert.Contains(t, metrics, sbx2.SandboxID, "Expected sandbox metrics to include the second created sandbox")
	for _, sbx := range metrics {
		assert.NotEmpty(t, sbx.Timestamp, "Metric timestamp should not be empty")
		assert.NotEmpty(t, sbx.TimestampUnix, "Metric timestamp unix should not be empty")
		assert.GreaterOrEqual(t, sbx.CpuUsedPct, float32(0), "CPU usage cannot be negative")
		assert.LessOrEqual(t, sbx.CpuUsedPct, float32(100), "CPU usage cannot exceed 100 percent")
		assert.NotEmpty(t, sbx.CpuCount, "Cpu count should not be empty")
		assert.NotEmpty(t, sbx.MemUsed, "Memory used should not be empty")
		assert.NotEmpty(t, sbx.MemTotal, "Memory total should not be empty")
		assert.NotEmpty(t, sbx.DiskUsed, "Disk used should not be empty")
		assert.NotEmpty(t, sbx.DiskTotal, "Disk total should not be empty")
	}
}
