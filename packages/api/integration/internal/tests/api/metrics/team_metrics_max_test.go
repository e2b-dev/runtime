package metrics

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/integration/internal/api"
	"github.com/e2b-dev/infra/packages/api/integration/internal/setup"
	"github.com/e2b-dev/infra/packages/api/integration/internal/utils"
)

func TestTeamMetricsMaxConcurrentSandboxes(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()

	// Create multiple sandboxes to generate team metrics
	utils.SetupSandboxWithCleanup(t, c)
	utils.SetupSandboxWithCleanup(t, c)

	maxDuration := 15 * time.Second
	tick := 500 * time.Millisecond

	var maxMetric api.MaxTeamMetric
	metric := api.ConcurrentSandboxes

	require.Eventually(t, func() bool {
		response, err := c.GetTeamsTeamIDMetricsMaxWithResponse(
			t.Context(),
			setup.TeamID,
			&api.GetTeamsTeamIDMetricsMaxParams{
				Metric: metric,
			},
			setup.WithAPIKey(),
		)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, response.StatusCode())

		if response.JSON200 == nil {
			return false
		}

		maxMetric = *response.JSON200

		return maxMetric.Value > 1
	}, maxDuration, tick, "max concurrent sandboxes metric not available in time")

	// Verify the metric structure
	require.NotZero(t, maxMetric.TimestampUnix, "TimestampUnix should not be zero")
	require.NotZero(t, maxMetric.Timestamp, "Timestamp should not be zero")
	require.GreaterOrEqual(t, maxMetric.Value, float32(1), "Should have at least 1 concurrent sandbox")
}

func TestTeamMetricsMaxSandboxStartRate(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()

	// Create sandboxes to generate start rate metrics
	utils.SetupSandboxWithCleanup(t, c)
	utils.SetupSandboxWithCleanup(t, c)

	maxDuration := 15 * time.Second
	tick := 500 * time.Millisecond

	var maxMetric api.MaxTeamMetric
	metric := api.SandboxStartRate

	require.Eventually(t, func() bool {
		response, err := c.GetTeamsTeamIDMetricsMaxWithResponse(
			t.Context(),
			setup.TeamID,
			&api.GetTeamsTeamIDMetricsMaxParams{
				Metric: metric,
			},
			setup.WithAPIKey(),
		)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, response.StatusCode())

		if response.JSON200 == nil {
			return false
		}

		maxMetric = *response.JSON200

		return maxMetric.Value > 0
	}, maxDuration, tick, "max sandbox start rate metric not available in time")

	// Verify the metric structure
	require.NotZero(t, maxMetric.TimestampUnix, "TimestampUnix should not be zero")
	require.NotZero(t, maxMetric.Timestamp, "Timestamp should not be zero")
	require.Greater(t, maxMetric.Value, float32(0), "Should have a positive sandbox start rate")
}

func TestTeamMetricsMaxEmpty(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()

	db := setup.GetTestDBClient(t)

	teamID := utils.CreateTeamWithUser(t, db, "metric-test", setup.UserID)
	apiKey := utils.CreateAPIKey(t, t.Context(), c, setup.UserID, teamID)

	tests := []struct {
		metric api.GetTeamsTeamIDMetricsMaxParamsMetric
	}{
		{
			metric: api.ConcurrentSandboxes,
		},
		{
			metric: api.SandboxStartRate,
		},
	}

	for _, test := range tests {
		t.Run(string(test.metric), func(t *testing.T) {
			t.Parallel()
			// Test getting metrics for a time range where no sandboxes existed
			response, err := c.GetTeamsTeamIDMetricsMaxWithResponse(
				t.Context(),
				teamID.String(),
				&api.GetTeamsTeamIDMetricsMaxParams{
					Metric: test.metric,
				},
				setup.WithAPIKey(apiKey),
			)
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, response.StatusCode())
			require.NotNil(t, response.JSON200)

			// For empty data, the max should be 0
			maxMetric := *response.JSON200
			require.Zero(t, maxMetric.Value, "Expected max value to be 0 for historical time range with no data")
		})
	}
}
