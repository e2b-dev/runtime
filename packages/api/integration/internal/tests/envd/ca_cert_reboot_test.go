package envd

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/integration/internal/api"
	"github.com/e2b-dev/infra/packages/api/integration/internal/setup"
	"github.com/e2b-dev/infra/packages/api/integration/internal/utils"
)

// TestCACertTrustedAfterFilesystemOnlyReboot is the boot-time half of the cert
// validation (the build-time half is TestCACertFromBuildSurvivesInBakedBundle).
// It builds a template that installs a CA, then pauses it memory:false and
// resumes — a cold boot, the only path that actually runs envd.service's
// ExecStartPre seeding of /etc/ssl/certs from the baked tar. After the reboot
// the *live* trust store must contain the CA, proving the seeding works
// end-to-end (not just that the tar artifact is correct).
func TestCACertTrustedAfterFilesystemOnlyReboot(t *testing.T) {
	t.Parallel()
	c := setup.GetAPIClient()
	ctx := t.Context()

	certPEM := generateSelfSignedCA(t, "e2b-test-ca-fs-only-reboot")

	tmpl := utils.BuildTemplate(t, utils.TemplateBuildOptions{
		Name:       "test-fs-only-ca-reboot",
		LogHandler: utils.DefaultBuildLogHandler(t),
		ReqEditors: []api.RequestEditorFn{setup.WithAPIKey()},
		BuildData: api.TemplateBuildStartV2{
			FromImage: new("ubuntu:22.04"),
			Steps: new([]api.TemplateStep{
				{
					Type:  "RUN",
					Force: new(true),
					// Run as root: the CA dir is root-owned and not world-writable
					// until finalize.
					Args: new([]string{injectCACommand(certPEM, "/usr/local/share/ca-certificates/e2b-reboot-ca.crt"), "root"}),
				},
			}),
		},
	})

	sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithTemplateID(tmpl.TemplateID), utils.WithAutoPause(false))
	envdClient := setup.GetEnvdClient(t, ctx)

	// Pause filesystem-only, then resume — cold boot, which re-seeds the
	// /etc/ssl/certs tmpfs from the baked tar via envd.service ExecStartPre.
	memory := false
	pauseResp, err := c.PostSandboxesSandboxIDPauseWithResponse(ctx, sbx.SandboxID,
		api.PostSandboxesSandboxIDPauseJSONRequestBody{Memory: &memory}, setup.WithAPIKey())
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, pauseResp.StatusCode())

	resumeResp, err := c.PostSandboxesSandboxIDResumeWithResponse(ctx, sbx.SandboxID,
		api.PostSandboxesSandboxIDResumeJSONRequestBody{}, setup.WithAPIKey())
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, resumeResp.StatusCode())

	// The live trust store must contain the build-time CA after the cold boot.
	bundle, err := utils.ExecCommandAsRootWithOutput(t, ctx, sbx, envdClient,
		"cat", "/etc/ssl/certs/ca-certificates.crt")
	require.NoError(t, err)
	assert.Contains(t, bundle, strings.TrimRight(certPEM, "\n"),
		"build-time CA must be in the live trust store after a filesystem-only cold boot")
}
