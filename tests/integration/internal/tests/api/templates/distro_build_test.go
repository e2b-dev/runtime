package api_templates

import (
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/tests/integration/internal/api"
)

// One base image per supported distro family. Provisioning installs the family's
// own package names under set -e, links its own init binary and regenerates its
// own CA bundle, so a build that reaches ready with the start and ready commands
// executed proves the whole profile resolves and that envd boots under it.
//
// NixOS installs nothing — see its case below — so there the same assertions
// prove the boot path rather than the package set.
func TestTemplateBuildDistroFamilies(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name         string
		templateName string
		fromImage    string
	}{
		{
			name:         "Debian family",
			templateName: "test-distro-ubuntu",
			fromImage:    "ubuntu:26.04",
		},
		{
			name:         "RPM family",
			templateName: "test-distro-fedora",
			fromImage:    "fedora:44",
		},
		{
			name:         "Arch",
			templateName: "test-distro-arch",
			fromImage:    "archlinux:base",
		},
		{
			name:         "Alpine on OpenRC",
			templateName: "test-distro-alpine",
			fromImage:    "alpine:3.24",
		},
		// Premade, so the profile declares no packages and a PkgInstall that
		// exits 1: everything provisioning installs elsewhere is baked into the
		// image's own NixOS configuration. Reaching ready therefore proves the
		// parts that are still ours — the busybox Bootstrap standing in for the
		// FHS userland the image has no /bin/sh for before its first activation,
		// the /sbin/e2b-nixos-init activation shim, and the drop-in removal that
		// lets setup-etc take over /etc/systemd/system.
		//
		// Pinned to the immutable tag, never :latest: the base-layer cache key is
		// the image reference as written (phases/base/hash.go), so a republished
		// :latest would keep building from the stale cached layer.
		{
			name:         "NixOS premade",
			templateName: "test-distro-nixos",
			fromImage:    "e2bdev/nixos:26.05-20260731",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var logMessages []string
			logHandler := func(alias string, entry api.BuildLogEntry) {
				logMessages = append(logMessages, entry.Message)
				defaultBuildLogHandler(t)(alias, entry)
			}

			buildConfig := api.TemplateBuildStartV2{
				Force:     new(ForceBaseBuild),
				FromImage: new(tc.fromImage),
				Steps:     new([]api.TemplateStep{}),
				StartCmd:  new("echo 'Sandbox started'"),
				// Proves the parity binaries exist in the booted sandbox, not
				// just that their packages resolved (Alpine ships ss in the
				// iproute2-ss subpackage — a name-level check can't see it).
				//
				// The trailing sleep holds the start command's log stream open:
				// the build cancels it the moment the ready command returns, so
				// a ready command that answers in milliseconds races the start
				// command's stdout to the build log and the assertion below on
				// "[start] [stdout]" then fails.
				ReadyCmd: new("command -v ss && command -v curl && sleep 1"),
			}

			outcome := runTemplateBuild(t, tc.templateName, buildConfig, logHandler)
			require.True(t, outcome.ready, "Build failed: %s", outcome.reason)

			for _, expectedLog := range []string{"[start] [stdout]: Sandbox started", "Template is ready"} {
				assert.True(t, slices.ContainsFunc(logMessages, func(msg string) bool {
					return strings.Contains(msg, expectedLog)
				}), "Expected log message not found: %s", expectedLog)
			}
		})
	}
}

// An image from a distro E2B doesn't provision must be rejected while
// provisioning, and the reason must reach the customer instead of a bare exit
// status. Oracle Linux declares ID_LIKE=fedora, so this also covers the
// rejection guard running before the ID_LIKE fallback.
func TestTemplateBuildUnsupportedDistro(t *testing.T) {
	t.Parallel()

	buildConfig := api.TemplateBuildStartV2{
		Force:     new(ForceBaseBuild),
		FromImage: new("oraclelinux:9"),
		Steps:     new([]api.TemplateStep{}),
	}

	outcome := runTemplateBuild(t, "test-distro-unsupported", buildConfig, defaultBuildLogHandler(t))
	require.False(t, outcome.ready, "Build of an unsupported distro must fail")
	assert.Contains(t, outcome.reason, "ID='ol' is not supported")
	assert.Contains(t, outcome.reason, "Sandboxes boot E2B's kernel")
}

// The guest kernel builds SELinux in as the default LSM and the rootfs is
// unlabeled OCI layers, so an image that carries a policy with
// SELINUX=enforcing boots systemd enforcing over an unlabeled tree, PID 1
// freezes, and envd never starts. The policy is installed in a step, not the
// base image: that covers the base image case (the finalize boot carries it)
// and the customer-Dockerfile case that a provisioning-time fix would miss.
func TestTemplateBuildSELinuxPolicyImage(t *testing.T) {
	t.Parallel()

	buildConfig := api.TemplateBuildStartV2{
		Force:     new(ForceBaseBuild),
		FromImage: new("fedora:44"),
		Steps: new([]api.TemplateStep{
			// Steps run as the template's default user; the second arg runs this
			// one as root (the sudoers entry is only written at finalize).
			{
				Type:  "RUN",
				Force: new(true),
				Args:  new([]string{"dnf install -y selinux-policy-targeted && grep -q '^SELINUX=enforcing' /etc/selinux/config", "root"}),
			},
		}),
		StartCmd: new("echo 'Sandbox started'"),
		// The guest must have booted with SELinux off at the kernel, not merely
		// have survived the policy load.
		ReadyCmd: new("grep -qw selinux=0 /proc/cmdline && ! test -e /sys/fs/selinux/enforce && sleep 1"),
	}

	outcome := runTemplateBuild(t, "test-selinux-policy", buildConfig, defaultBuildLogHandler(t))
	require.True(t, outcome.ready, "Build failed: %s", outcome.reason)
}
