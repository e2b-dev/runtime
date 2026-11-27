package envd

import (
	"encoding/base64"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/integration/internal/api"
	"github.com/e2b-dev/infra/packages/api/integration/internal/setup"
	"github.com/e2b-dev/infra/packages/api/integration/internal/utils"
)

// bakedCertBundlePath is where the build packs the system trust store. On a
// cold boot (filesystem-only reboot resume) envd.service seeds the /etc/ssl/certs
// tmpfs from this tar and skips update-ca-certificates, so whatever is in here
// is the trust store the sandbox comes up with.
const bakedCertBundlePath = "/usr/local/share/e2b/ssl-certs.tar"

// injectCACommand writes a PEM into destPath as a shell command, WITHOUT running
// update-ca-certificates — the realistic case (the /usr/local/share/ca-certificates
// dir exists precisely so a dropped cert is picked up later). base64 avoids
// shell-escaping the multi-line PEM.
func injectCACommand(certPEM, destPath string) string {
	b64 := base64.StdEncoding.EncodeToString([]byte(certPEM))

	return fmt.Sprintf("echo '%s' | base64 -d > %s", b64, destPath)
}

// readBakedCertBundle extracts ca-certificates.crt from the build-time tar — i.e.
// exactly what a cold boot would seed /etc/ssl/certs from.
func readBakedCertBundle(t *testing.T, sbx *api.Sandbox, client *setup.EnvdClient) string {
	t.Helper()

	out, err := utils.ExecCommandAsRootWithOutput(t, t.Context(), sbx, client,
		"tar", "-xOf", bakedCertBundlePath, "./ca-certificates.crt")
	require.NoError(t, err, "extract ca-certificates.crt from baked tar")

	return out
}

// TestCACertFromBuildSurvivesInBakedBundle verifies that a CA a template adds
// during the build ends up in the cert tar packed at build time — the trust
// store a cold-boot (filesystem-only reboot) resume comes up with.
//
// Because the boot fast path seeds /etc/ssl/certs from the tar and skips
// update-ca-certificates, the tar must already contain every CA the template
// produced. The finalize phase runs update-ca-certificates and packs the tar as
// the build's last guest step (after all build steps and start/ready), so:
//   - a CA dropped under /usr/local/share/ca-certificates without the user
//     running update-ca-certificates must still be merged (RUN-step case);
//   - a CA installed by the start command — after configure.sh — must still be
//     captured (start-command case).
//
// This validates the tar *artifact* only. The boot-time seeding that consumes
// it runs solely on a cold boot, which no runtime path triggers today — every
// resume is a memory resume that restores the tmpfs from RAM and skips
// ExecStartPre. Once filesystem-only reboot resume lands, the seeding becomes a
// live path and should get its own end-to-end test (build a CA template, pause
// memory:false, resume, assert the trust store). Keeping this guard now means
// the artifact is already proven correct when that path turns on.
func TestCACertFromBuildSurvivesInBakedBundle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		template  string
		cn        string
		buildData func(certPEM string) api.TemplateBuildStartV2
	}{
		{
			name:     "added in RUN step without update-ca-certificates",
			template: "test-fs-only-ca-buildstep",
			cn:       "e2b-test-ca-buildstep",
			buildData: func(certPEM string) api.TemplateBuildStartV2 {
				return api.TemplateBuildStartV2{
					FromImage: new("ubuntu:22.04"),
					Steps: new([]api.TemplateStep{
						{
							Type:  "RUN",
							Force: new(true),
							// Run as root (RUN's optional 2nd arg): the CA dir is
							// root-owned and not world-writable until finalize, so
							// don't depend on the default build-step user.
							Args: new([]string{injectCACommand(certPEM, "/usr/local/share/ca-certificates/e2b-buildstep-ca.crt"), "root"}),
						},
					}),
				}
			},
		},
		{
			name:     "installed by start command after configure",
			template: "test-fs-only-ca-startcmd",
			cn:       "e2b-test-ca-startcmd",
			buildData: func(certPEM string) api.TemplateBuildStartV2 {
				return api.TemplateBuildStartV2{
					FromImage: new("ubuntu:22.04"),
					// Install the CA directly from the start command, which runs
					// after configure.sh (where /usr/local becomes world-writable),
					// then keep running. Exercises certs added after the
					// configuration script. Writing straight to the persisted CA dir
					// avoids staging through /tmp, which is tmpfs and would not
					// survive into the fresh finalize sandbox.
					StartCmd: new(injectCACommand(certPEM, "/usr/local/share/ca-certificates/e2b-startcmd-ca.crt") + " && sleep infinity"),
					ReadyCmd: new("for _ in $(seq 1 50); do test -f /usr/local/share/ca-certificates/e2b-startcmd-ca.crt && exit 0; sleep 0.2; done; exit 1"),
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := setup.GetAPIClient()
			client := setup.GetEnvdClient(t, t.Context())

			certPEM := generateSelfSignedCA(t, tc.cn)

			tmpl := utils.BuildTemplate(t, utils.TemplateBuildOptions{
				Name:       tc.template,
				BuildData:  tc.buildData(certPEM),
				LogHandler: utils.DefaultBuildLogHandler(t),
				ReqEditors: []api.RequestEditorFn{setup.WithAPIKey()},
			})

			sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithTemplateID(tmpl.TemplateID))

			bundle := readBakedCertBundle(t, sbx, client)
			assert.Contains(t, bundle, strings.TrimRight(certPEM, "\n"),
				"CA added during the build must be in the tar that seeds the cold-boot trust store")
		})
	}
}
