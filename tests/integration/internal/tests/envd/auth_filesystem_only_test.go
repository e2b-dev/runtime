package envd

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/tests/integration/internal/api"
	"github.com/e2b-dev/infra/tests/integration/internal/envd"
	"github.com/e2b-dev/infra/tests/integration/internal/setup"
	"github.com/e2b-dev/infra/tests/integration/internal/utils"
)

// TestSecureSandboxFilesystemOnlyResumeAuth verifies that a secure sandbox
// survives a filesystem-only (cold-boot) resume with working envd auth: after
// the reboot the original envd access token still authorizes requests, and a
// file written before the pause is still readable. This exercises the MMDS
// access-token path in RebootSandbox, which re-establishes envd /init auth on a
// cold boot just like a memory resume does for non-rebooted sandboxes.
func TestSecureSandboxFilesystemOnlyResumeAuth(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	sbx := createSandbox(t, setup.WithAPIKey())
	require.NotNil(t, sbx.JSON201)
	require.NotNil(t, sbx.JSON201.EnvdAccessToken)

	envdClient := setup.GetEnvdClient(t, ctx)
	sbxMeta := sbx.JSON201

	filePath := "demo.txt"
	fileContent := "Hello, world!"
	utils.UploadFile(t, ctx, sbxMeta, envdClient, filePath, fileContent)

	c := setup.GetAPIClient()

	// Pause as a filesystem-only snapshot (memory:false).
	memory := false
	pauseResp, err := c.PostSandboxesSandboxIDPauseWithResponse(ctx, sbxMeta.SandboxID,
		api.PostSandboxesSandboxIDPauseJSONRequestBody{Memory: &memory}, setup.WithAPIKey())
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, pauseResp.StatusCode())

	// Explicit resume — cold-boots the secure sandbox.
	sbxIDWithClient := sbxMeta.SandboxID + "-" + sbxMeta.ClientID
	sbxResume, err := c.PostSandboxesSandboxIDResumeWithResponse(ctx, sbxIDWithClient,
		api.PostSandboxesSandboxIDResumeJSONRequestBody{}, setup.WithAPIKey())
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, sbxResume.StatusCode())

	// The original access token must still authorize after the cold boot, and
	// the file written before the pause must still be on the rootfs.
	fileResponse, err := envdClient.HTTPClient.GetFilesWithResponse(
		ctx,
		&envd.GetFilesParams{Path: &filePath, Username: new("user")},
		setup.WithSandbox(t, sbxMeta),
	)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, fileResponse.StatusCode())
	assert.Equal(t, fileContent, string(fileResponse.Body))
}
