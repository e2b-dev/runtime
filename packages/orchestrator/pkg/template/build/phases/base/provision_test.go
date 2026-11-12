//go:build linux

package base

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Provisioning must not decide the chrony time source: it runs on a build node,
// and the sandbox can cold-boot on a node with a different PHC situation. The
// baked config only includes what e2b-chrony-source writes at boot.
func TestProvisionScriptDefersChronySourceToBoot(t *testing.T) {
	t.Parallel()
	// E2B_CHRONY_PHC was the provision-time verdict the Alpine seccomp workaround
	// used to read. It no longer exists, and under `set -u` a leftover reference
	// is a hard provisioning failure on every distro, not just Alpine.
	for _, bad := range []string{"[ -e /dev/ptp0 ]", "refclock PHC", "E2B_CHRONY_PHC"} {
		assert.NotContains(t, provisionScriptFile, bad, "provision.sh must not decide the time source — that happens at boot")
	}
	for _, want := range []string{
		`echo "include /run/chrony-e2b/source.conf"`,
		`echo "makestep 1.0 3"`,
	} {
		assert.Contains(t, provisionScriptFile, want, "provision.sh missing chrony config line")
	}
}

// Appending to /etc/ssh/sshd_config marks the package's conffile modified, and any later
// openssh upgrade in a build step then blocks on dpkg's prompt until the build times out.
// The extracted SSH block runs against two fixtures: a config that includes sshd_config.d
// gets a drop-in and stays byte-identical; one without the Include gets the append.
func TestProvisionScriptWritesSSHSettingsAsDropIn(t *testing.T) {
	t.Parallel()
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available")
	}
	begin := strings.Index(provisionScriptFile, "echo \"Setting up SSH\"")
	require.GreaterOrEqual(t, begin, 0, "provision.sh has no SSH block")
	end := strings.Index(provisionScriptFile[begin:], "\nEOF\n")
	require.GreaterOrEqual(t, end, 0, "SSH block has no heredoc terminator")
	block := provisionScriptFile[begin : begin+end+len("\nEOF\n")]

	// mainConfig may reference /etc/ssh; the fixture path is substituted into both the
	// config and the block so the Include grep sees what sshd would.
	run := func(t *testing.T, mainConfig string) (main string, dropIn string, dropIns []string) {
		t.Helper()
		sshDir := filepath.Join(t.TempDir(), "etc", "ssh")
		require.NoError(t, os.MkdirAll(sshDir, 0o755))
		mainConfig = strings.ReplaceAll(mainConfig, "/etc/ssh", sshDir)
		require.NoError(t, os.WriteFile(filepath.Join(sshDir, "sshd_config"), []byte(mainConfig), 0o644))
		script := strings.ReplaceAll(block, "/etc/ssh", sshDir)
		out, err := exec.CommandContext(t.Context(), "bash", "-eu", "-c", script).CombinedOutput()
		require.NoError(t, err, "SSH block failed:\n%s", out)
		got, err := os.ReadFile(filepath.Join(sshDir, "sshd_config"))
		require.NoError(t, err)
		conf, _ := os.ReadFile(filepath.Join(sshDir, "sshd_config.d", "00-e2b.conf"))
		entries, _ := os.ReadDir(filepath.Join(sshDir, "sshd_config.d"))
		for _, e := range entries {
			dropIns = append(dropIns, e.Name())
		}

		return string(got), string(conf), dropIns
	}

	t.Run("include present writes a drop-in and leaves the main file alone", func(t *testing.T) {
		t.Parallel()
		const mainConfig = "Include /etc/ssh/sshd_config.d/*.conf\nPort 22\n"
		main, dropIn, dropIns := run(t, mainConfig)
		assert.True(t, strings.HasSuffix(main, "Port 22\n"), "main sshd_config changed:\n%s", main)
		assert.NotContains(t, main, "Permit", "main sshd_config changed")
		require.Equal(t, []string{"00-e2b.conf"}, dropIns)
		for _, want := range []string{"PermitRootLogin yes", "PermitEmptyPasswords yes", "PasswordAuthentication yes"} {
			assert.Contains(t, dropIn, want, "drop-in missing a directive")
		}
	})

	t.Run("no include appends to the main file", func(t *testing.T) {
		t.Parallel()
		main, _, dropIns := run(t, "Port 22\n")
		assert.True(t, strings.HasPrefix(main, "Port 22\n"), "expected the directives appended after the existing config, got:\n%s", main)
		assert.Contains(t, main, "PermitRootLogin yes", "expected the directives appended after the existing config")
		assert.Empty(t, dropIns, "no drop-in expected without an Include line")
	})
}
