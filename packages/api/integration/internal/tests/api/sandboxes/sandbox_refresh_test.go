package sandboxes

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/integration/internal/api"
	"github.com/e2b-dev/infra/packages/api/integration/internal/setup"
	"github.com/e2b-dev/infra/packages/api/integration/internal/utils"
)

func TestSandboxRefresh(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()
	testCases := []struct {
		name   string
		extend bool
		same   bool

		initialDuration int
		newDuration     int
	}{
		{
			name:   "extend",
			extend: true,
			same:   false,

			initialDuration: 60,
			newDuration:     120,
		},
		{
			name:            "shorten",
			extend:          false,
			same:            true,
			initialDuration: 120,
			newDuration:     60,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithTimeout(int32(tc.initialDuration)))

			// Get initial sandbox details
			detailResp, err := c.GetSandboxesSandboxIDWithResponse(t.Context(), sbx.SandboxID, setup.WithAPIKey())
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, detailResp.StatusCode())
			require.NotNil(t, detailResp.JSON200)

			initialEndTime := detailResp.JSON200.EndAt

			timeoutResp, err := c.PostSandboxesSandboxIDRefreshesWithResponse(t.Context(), sbx.SandboxID, api.PostSandboxesSandboxIDRefreshesJSONRequestBody{
				Duration: &tc.newDuration,
			}, setup.WithAPIKey())
			require.NoError(t, err)
			assert.Equal(t, http.StatusNoContent, timeoutResp.StatusCode())

			detailResp2, err := c.GetSandboxesSandboxIDWithResponse(t.Context(), sbx.SandboxID, setup.WithAPIKey())
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, detailResp2.StatusCode())
			require.NotNil(t, detailResp2.JSON200)

			newEndTime := detailResp2.JSON200.EndAt

			assert.Equal(t, tc.extend, newEndTime.After(initialEndTime), "End time should be extended")
			assert.Equal(t, tc.same, newEndTime.Equal(initialEndTime), "End time should be updated")
		})
	}
}

func TestSandboxRefresh_NotFound(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()

	timeoutResp, err := c.PostSandboxesSandboxIDRefreshesWithResponse(t.Context(), "nonexistentsandboxid", api.PostSandboxesSandboxIDRefreshesJSONRequestBody{}, setup.WithAPIKey())
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, timeoutResp.StatusCode())
}

func TestSandboxRefresh_CrossTeamAccess(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()
	db := setup.GetTestDBClient(t)

	// Create a sandbox with the default team's API key
	sbx := utils.SetupSandboxWithCleanup(t, c)

	// Create a second team with a different API key
	foreignUserID := utils.CreateUser(t, db)
	foreignTeamID := utils.CreateTeamWithUser(t, db, "foreign-team-refresh", foreignUserID.String())
	foreignAPIKey := utils.CreateAPIKey(t, t.Context(), c, foreignUserID.String(), foreignTeamID)

	// Try to refresh the first team's sandbox using the second team's API key
	duration := 120
	refreshResp, err := c.PostSandboxesSandboxIDRefreshesWithResponse(t.Context(), sbx.SandboxID, api.PostSandboxesSandboxIDRefreshesJSONRequestBody{
		Duration: &duration,
	}, setup.WithAPIKey(foreignAPIKey))
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, refreshResp.StatusCode(), "Should return 404 Not Found when trying to refresh a sandbox owned by a different team")
}
