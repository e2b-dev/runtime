package metrics

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/integration/internal/api"
	"github.com/e2b-dev/infra/packages/api/integration/internal/setup"
	"github.com/e2b-dev/infra/packages/api/integration/internal/utils"
	clickhouse "github.com/e2b-dev/infra/packages/clickhouse/pkg"
)

func TestTeamMetrics(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()

	// Create multiple sandboxes to generate team metrics
	utils.SetupSandboxWithCleanup(t, c)
	utils.SetupSandboxWithCleanup(t, c)
	var metrics []api.TeamMetric

	maxDuration := 60 * time.Second
	tick := 500 * time.Millisecond

	require.Eventually(t, func() bool {
		response, err := c.GetTeamsTeamIDMetricsWithResponse(t.Context(), setup.TeamID, nil, setup.WithAPIKey())
		if err != nil || response == nil || response.StatusCode() != http.StatusOK || response.JSON200 == nil {
			return false
		}

		metrics = *response.JSON200
		startRateGreaterThanZero := false
		concurrentSandboxesGreaterThanZero := false
		for _, metric := range metrics {
			if metric.SandboxStartRate > 0 {
				startRateGreaterThanZero = true
			}
			if metric.ConcurrentSandboxes > 0 {
				concurrentSandboxesGreaterThanZero = true
			}
		}

		return startRateGreaterThanZero && concurrentSandboxesGreaterThanZero
	}, maxDuration, tick, "team metrics did not reach expected state in time")

	// Test getting team metrics
	require.NotEmpty(t, metrics, "Expected at least one team metric in the response")

	// Verify the structure of team metrics
	for _, metric := range metrics {
		require.NotEmpty(t, metric.Timestamp, "Timestamp should not be empty")
		require.NotEmpty(t, metric.TimestampUnix, "Timestamp should not be empty")
	}
}

func TestTeamMetricsWithTimeRange(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()

	// Create a sandbox to generate metrics
	utils.SetupSandboxWithCleanup(t, c)
	maxDuration := 25 * time.Second

	// Test with custom time range (last hour)
	now := time.Now()
	startSec := now.Add(-1 * time.Hour).Unix()
	endSec := now.Add(maxDuration).Unix()
	var metrics []api.TeamMetric

	tick := 500 * time.Millisecond

	require.Eventually(t, func() bool {
		resp, err := c.GetTeamsTeamIDMetricsWithResponse(
			t.Context(), setup.TeamID,
			&api.GetTeamsTeamIDMetricsParams{Start: &startSec, End: &endSec},
			setup.WithAPIKey(),
		)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode())
		require.NotNil(t, resp.JSON200)
		if len(*resp.JSON200) == 0 {
			return false
		}

		metrics = *resp.JSON200

		return true
	}, maxDuration, tick, "team metrics not available in time")

	require.NotEmpty(t, metrics, "Expected at least one team metric in the response")

	// Verify all timestamps are within the requested range
	for _, metric := range metrics {
		require.GreaterOrEqual(t, metric.TimestampUnix, startSec, "Metric timestamp should be >= start time")
		require.LessOrEqual(t, metric.TimestampUnix, endSec, "Metric timestamp should be <= end time")
	}
}

func TestTeamMetricsEmpty(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()

	db := setup.GetTestDBClient(t)
	teamID := utils.CreateTeamWithUser(t, db, "test-team-no-metrics", setup.UserID)
	apiKey := utils.CreateAPIKey(t, t.Context(), c, setup.UserID, teamID)

	response, err := c.GetTeamsTeamIDMetricsWithResponse(
		t.Context(),
		teamID.String(),
		nil,
		setup.WithAPIKey(apiKey),
	)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode())
	require.NotNil(t, response.JSON200)
	metrics := *response.JSON200
	require.Empty(t, metrics, "Expected no team metrics for historical time range")
}

func TestTeamMetricsInvalidDate(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()

	// Test getting metrics for a time range where no sandboxes existed
	nowSec := time.Now().Unix()
	endSec := clickhouse.MaxDate64.Unix() + 1
	response, err := c.GetTeamsTeamIDMetricsWithResponse(t.Context(), setup.TeamID, &api.GetTeamsTeamIDMetricsParams{
		Start: &nowSec,
		End:   &endSec,
	}, setup.WithAPIKey())
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, response.StatusCode())
	require.NotNil(t, response.JSON400)
	require.Contains(t, response.JSON400.Message, "end time cannot be after")
}
