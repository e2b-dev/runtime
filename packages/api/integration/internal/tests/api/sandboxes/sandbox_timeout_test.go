package sandboxes

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/e2b-dev/infra/packages/api/integration/internal/api"
	"github.com/e2b-dev/infra/packages/api/integration/internal/setup"
	"github.com/e2b-dev/infra/packages/api/integration/internal/utils"
)

func TestSandboxTimeout(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()
	testCases := []struct {
		name   string
		extend bool

		initialDuration int32
		newDuration     int32
	}{
		{
			name:            "extend",
			extend:          true,
			initialDuration: 60,
			newDuration:     120,
		},
		{
			name:            "shorten",
			extend:          false,
			initialDuration: 120,
			newDuration:     60,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithTimeout(tc.initialDuration))

			// Get initial sandbox details
			detailResp, err := c.GetSandboxesSandboxIDWithResponse(t.Context(), sbx.SandboxID, setup.WithAPIKey())
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, detailResp.StatusCode())
			require.NotNil(t, detailResp.JSON200)

			initialEndTime := detailResp.JSON200.EndAt

			timeoutResp, err := c.PostSandboxesSandboxIDTimeoutWithResponse(t.Context(), sbx.SandboxID, api.PostSandboxesSandboxIDTimeoutJSONRequestBody{
				Timeout: tc.newDuration,
			}, setup.WithAPIKey())
			require.NoError(t, err)
			assert.Equal(t, http.StatusNoContent, timeoutResp.StatusCode())

			detailResp2, err := c.GetSandboxesSandboxIDWithResponse(t.Context(), sbx.SandboxID, setup.WithAPIKey())
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, detailResp2.StatusCode())
			require.NotNil(t, detailResp2.JSON200)

			newEndTime := detailResp2.JSON200.EndAt

			assert.Equal(t, tc.extend, newEndTime.After(initialEndTime), "End time should be extended")
			assert.NotEqual(t, initialEndTime, newEndTime, "End time should be updated")
		})
	}
}

func TestSandboxTimeout_NotFound(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()

	timeoutResp, err := c.PostSandboxesSandboxIDTimeoutWithResponse(t.Context(), "nonexistentsandboxid", api.PostSandboxesSandboxIDTimeoutJSONRequestBody{
		Timeout: 60,
	}, setup.WithAPIKey())
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, timeoutResp.StatusCode())
}

func TestSandboxSetTimeoutPausingSandbox(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()

	t.Run("test set timeout while pausing", func(t *testing.T) {
		t.Parallel()
		sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithAutoPause(true))
		sbxId := sbx.SandboxID

		// Pause the sandbox
		wg := errgroup.Group{}
		wg.Go(func() error {
			pauseResp, err := c.PostSandboxesSandboxIDPauseWithResponse(t.Context(), sbxId, api.PostSandboxesSandboxIDPauseJSONRequestBody{}, setup.WithAPIKey())
			if err != nil {
				return err
			}

			if pauseResp.StatusCode() != http.StatusNoContent {
				return fmt.Errorf("unexpected status code: %d", pauseResp.StatusCode())
			}

			return nil
		})

		for range 5 {
			time.Sleep(200 * time.Millisecond)
			wg.Go(func() error {
				setTimeoutResp, err := c.PostSandboxesSandboxIDTimeoutWithResponse(t.Context(), sbxId, api.PostSandboxesSandboxIDTimeoutJSONRequestBody{
					Timeout: 15,
				},
					setup.WithAPIKey())
				if err != nil {
					return err
				}

				// The sandbox is currently transitioning
				if setTimeoutResp.StatusCode() != http.StatusConflict && setTimeoutResp.StatusCode() != http.StatusNotFound {
					return fmt.Errorf("unexpected status code: %d", setTimeoutResp.StatusCode())
				}

				return nil
			})
		}

		err := wg.Wait()
		require.NoError(t, err)
	})
}

func TestSandboxTimeout_CrossTeamAccess(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()
	db := setup.GetTestDBClient(t)

	// Create a sandbox with the default team's API key
	sbx := utils.SetupSandboxWithCleanup(t, c)

	// Create a second team with a different API key
	foreignUserID := utils.CreateUser(t, db)
	foreignTeamID := utils.CreateTeamWithUser(t, db, "foreign-team-timeout", foreignUserID.String())
	foreignAPIKey := utils.CreateAPIKey(t, t.Context(), c, foreignUserID.String(), foreignTeamID)

	// Try to set timeout on the first team's sandbox using the second team's API key
	timeoutResp, err := c.PostSandboxesSandboxIDTimeoutWithResponse(t.Context(), sbx.SandboxID, api.PostSandboxesSandboxIDTimeoutJSONRequestBody{
		Timeout: 120,
	}, setup.WithAPIKey(foreignAPIKey))
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, timeoutResp.StatusCode(), "Should return 404 Not Found when trying to set timeout on a sandbox owned by a different team")
}
