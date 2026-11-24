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

// TestCertDirWriteInBuildStepSurvivesColdBoot verifies that a file a build step
// writes straight into /etc/ssl/certs is still there after the next cold boot.
// The directory is a tmpfs seeded from the baked tar on every cold boot, which
// the finalize phase and the first uncached step after a cache hit both do, so
// the step's writes reach them only because the step packs the tar itself.
// Before that, the finalize boot came up without them and the sandbox lost
// both a dropped-in file and the keystore ca-certificates-java had written.
func TestCertDirWriteInBuildStepSurvivesColdBoot(t *testing.T) {
	t.Parallel()

	c := setup.GetAPIClient()
	client := setup.GetEnvdClient(t, t.Context())

	const marker = "e2b-test-buildstep-marker"

	tmpl := utils.BuildTemplate(t, utils.TemplateBuildOptions{
		Name: "test-cert-dir-buildstep-write",
		BuildData: api.TemplateBuildStartV2{
			FromImage: new("ubuntu:22.04"),
			Steps: new([]api.TemplateStep{
				{
					Type:  "RUN",
					Force: new(true),
					Args:  new([]string{"echo buildstep > /etc/ssl/certs/" + marker, "root"}),
				},
			}),
		},
		LogHandler: utils.DefaultBuildLogHandler(t),
		ReqEditors: []api.RequestEditorFn{setup.WithAPIKey()},
	})

	sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithTemplateID(tmpl.TemplateID))

	baked, err := utils.ExecCommandAsRootWithOutput(t, t.Context(), sbx, client,
		"tar", "-xOf", bakedCertBundlePath, "./"+marker)
	require.NoError(t, err, "a file written to /etc/ssl/certs in a build step must be in the baked tar")
	assert.Equal(t, "buildstep", strings.TrimSpace(baked))

	live, err := utils.ExecCommandAsRootWithOutput(t, t.Context(), sbx, client,
		"cat", "/etc/ssl/certs/"+marker)
	require.NoError(t, err, "a file written to /etc/ssl/certs in a build step must survive the finalize cold boot")
	assert.Equal(t, "buildstep", strings.TrimSpace(live))
}

// Two builds exercise both cache-boundary seeding and replacement of an older
// archive: the second build reuses the certificate-writing layer, reads it in
// the changed next step, then deletes it before finalize cold-boots again.
func TestCACertCachedStepAndDeletionSurviveColdBoot(t *testing.T) {
	t.Parallel()

	c := setup.GetAPIClient()
	client := setup.GetEnvdClient(t, t.Context())
	cert := generateSelfSignedCA(t, "e2b-cache-boundary-ca")
	const source = "/usr/local/share/ca-certificates/e2b-cache-boundary.crt"
	const direct = "/etc/ssl/certs/e2b-cache-boundary-direct.pem"
	// Each generated certificate makes this layer unique, so its cache entry can
	// only come from this test's first build, not an older CI run.
	install := injectCACommand(cert, source) + " && update-ca-certificates && cp " + source + " " + direct
	verify := "cmp " + source + " " + direct + " && openssl verify -CAfile /etc/ssl/certs/ca-certificates.crt " + source
	buildData := api.TemplateBuildStartV2{
		FromImage: new("ubuntu:22.04"),
		Steps: new([]api.TemplateStep{
			{Type: "RUN", Args: new([]string{install, "root"})},
			{Type: "RUN", Args: new([]string{verify, "root"})},
		}),
	}
	first := utils.BuildTemplate(t, utils.TemplateBuildOptions{
		Name: "test-cert-cache-and-delete", BuildData: buildData,
		LogHandler: utils.DefaultBuildLogHandler(t), ReqEditors: []api.RequestEditorFn{setup.WithAPIKey()},
	})
	sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithTemplateID(first.TemplateID))
	_, err := utils.ExecCommandAsRootWithOutput(t, t.Context(), sbx, client, "sh", "-ec", verify)
	require.NoError(t, err, "first build must contain the custom CA and direct cert file")

	// No Force: the first layer must be reused, while changing step 2 makes it
	// cold-boot the cached rootfs rather than resume the previous step's memory.
	(*buildData.Steps)[1].Args = new([]string{verify + " && rm " + source + " " + direct + " && update-ca-certificates --fresh", "root"})
	var messages []string
	second := utils.BuildTemplate(t, utils.TemplateBuildOptions{
		Name: "test-cert-cache-and-delete", BuildData: buildData,
		LogHandler: func(alias string, entry api.BuildLogEntry) {
			messages = append(messages, entry.Message)
			utils.DefaultBuildLogHandler(t)(alias, entry)
		},
		ReqEditors: []api.RequestEditorFn{setup.WithAPIKey()},
	})
	logs := strings.Join(messages, "\n")
	require.Contains(t, logs, "CACHED [builder 1/2] RUN "+install)
	require.NotContains(t, logs, "CACHED [builder 2/2]", "the verifying/removing step must execute")

	sbx = utils.SetupSandboxWithCleanup(t, c, utils.WithTemplateID(second.TemplateID))
	_, err = utils.ExecCommandAsRootWithOutput(t, t.Context(), sbx, client, "sh", "-ec", "test ! -e "+direct+" && test ! -e "+source)
	require.NoError(t, err, "deleted certificates must not reappear after finalize's cold boot")
	listing, err := utils.ExecCommandAsRootWithOutput(t, t.Context(), sbx, client, "tar", "-tf", bakedCertBundlePath)
	require.NoError(t, err)
	require.NotContains(t, listing, "e2b-cache-boundary-direct.pem")
	require.NotContains(t, readBakedCertBundle(t, sbx, client), strings.TrimSpace(cert), "the removed CA must leave the baked trust store")
}
