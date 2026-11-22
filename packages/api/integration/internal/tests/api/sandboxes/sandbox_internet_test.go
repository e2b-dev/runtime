package sandboxes

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/integration/internal/api"
	"github.com/e2b-dev/infra/packages/api/integration/internal/setup"
	"github.com/e2b-dev/infra/packages/api/integration/internal/utils"
)

func TestInternetAccess(t *testing.T) {
	t.Parallel()

	client := setup.GetAPIClient()

	testCases := []struct {
		name           string
		internetAccess bool
	}{
		{
			name:           "allow_internet_access",
			internetAccess: true,
		},
		{
			name:           "deny_internet_access",
			internetAccess: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()

			sbx := utils.SetupSandboxWithCleanup(t, client, utils.WithTimeout(30), utils.WithAllowInternetAccess(tc.internetAccess))

			envdClient := setup.GetEnvdClient(t, ctx)

			err := utils.ExecCommand(t, ctx, sbx, envdClient, "curl", "--connect-timeout", "3", "--max-time", "5", "-Is", "https://www.gstatic.com/generate_204")
			if tc.internetAccess {
				require.NoError(t, err, "Expected curl command to succeed when internet access is allowed")
			} else {
				require.Error(t, err, "Expected curl command to fail when internet access is denied")
				require.Contains(t, err.Error(), "failed with exit code", "Expected connection failure message")
			}
		})
	}
}

func TestInternetAccessResumedSbx(t *testing.T) {
	t.Parallel()

	client := setup.GetAPIClient()

	testCases := []struct {
		name           string
		internetAccess bool
	}{
		{
			name:           "allow_internet_access",
			internetAccess: true,
		},
		{
			name:           "deny_internet_access",
			internetAccess: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			sbxTimeout := int32(30)

			sbx := utils.SetupSandboxWithCleanup(t, client, utils.WithTimeout(sbxTimeout), utils.WithAllowInternetAccess(tc.internetAccess))

			// Pause and resume the sandbox
			respPause, err := client.PostSandboxesSandboxIDPauseWithResponse(ctx, sbx.SandboxID, api.PostSandboxesSandboxIDPauseJSONRequestBody{}, setup.WithAPIKey())
			require.NoError(t, err, "Expected to pause sandbox without error")
			require.Equal(t, http.StatusNoContent, respPause.StatusCode(), "Expected status code 204 No Content, got %d", respPause.StatusCode())

			respResume, err := client.PostSandboxesSandboxIDResumeWithResponse(ctx, sbx.SandboxID, api.PostSandboxesSandboxIDResumeJSONRequestBody{
				Timeout: &sbxTimeout,
			}, setup.WithAPIKey())
			require.NoError(t, err, "Expected to resume sandbox without error")
			require.Equal(t, http.StatusCreated, respResume.StatusCode(), "Expected status code 200 OK, got %d", respResume.StatusCode())

			envdClient := setup.GetEnvdClient(t, ctx)
			err = utils.ExecCommand(t, ctx, sbx, envdClient, "curl", "--connect-timeout", "3", "--max-time", "5", "-Is", "https://www.gstatic.com/generate_204")
			if tc.internetAccess {
				require.NoError(t, err, "Expected curl command to succeed when internet access is allowed")
			} else {
				require.Error(t, err, "Expected curl command to fail when internet access is denied")
				require.Contains(t, err.Error(), "failed with exit code", "Expected connection failure message")
			}
		})
	}
}
