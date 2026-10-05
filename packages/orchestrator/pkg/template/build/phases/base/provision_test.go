//go:build linux

package base

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
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
		if strings.Contains(provisionScriptFile, bad) {
			t.Errorf("provision.sh must not decide the time source (%q) — that happens at boot", bad)
		}
	}
	for _, want := range []string{
		`echo "include /run/chrony-e2b/source.conf"`,
		`echo "makestep 1.0 3"`,
	} {
		if !strings.Contains(provisionScriptFile, want) {
			t.Errorf("provision.sh missing chrony config line %q", want)
		}
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
	if begin < 0 {
		t.Fatal("provision.sh has no SSH block")
	}
	end := strings.Index(provisionScriptFile[begin:], "\nEOF\n")
	if end < 0 {
		t.Fatal("SSH block has no heredoc terminator")
	}
	block := provisionScriptFile[begin : begin+end+len("\nEOF\n")]

	// mainConfig may reference /etc/ssh; the fixture path is substituted into both the
	// config and the block so the Include grep sees what sshd would.
	run := func(t *testing.T, mainConfig string) (main string, dropIn string, dropIns []string) {
		t.Helper()
		sshDir := filepath.Join(t.TempDir(), "etc", "ssh")
		if err := os.MkdirAll(sshDir, 0o755); err != nil {
			t.Fatal(err)
		}
		mainConfig = strings.ReplaceAll(mainConfig, "/etc/ssh", sshDir)
		if err := os.WriteFile(filepath.Join(sshDir, "sshd_config"), []byte(mainConfig), 0o644); err != nil {
			t.Fatal(err)
		}
		script := strings.ReplaceAll(block, "/etc/ssh", sshDir)
		if out, err := exec.CommandContext(t.Context(), "bash", "-eu", "-c", script).CombinedOutput(); err != nil {
			t.Fatalf("SSH block failed: %v\n%s", err, out)
		}
		got, err := os.ReadFile(filepath.Join(sshDir, "sshd_config"))
		if err != nil {
			t.Fatal(err)
		}
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
		if !strings.HasSuffix(main, "Port 22\n") || strings.Contains(main, "Permit") {
			t.Errorf("main sshd_config changed:\n%s", main)
		}
		if len(dropIns) != 1 || dropIns[0] != "00-e2b.conf" {
			t.Fatalf("expected exactly 00-e2b.conf, got %v", dropIns)
		}
		for _, want := range []string{"PermitRootLogin yes", "PermitEmptyPasswords yes", "PasswordAuthentication yes"} {
			if !strings.Contains(dropIn, want) {
				t.Errorf("drop-in missing %q", want)
			}
		}
	})

	t.Run("no include appends to the main file", func(t *testing.T) {
		t.Parallel()
		main, _, dropIns := run(t, "Port 22\n")
		if !strings.HasPrefix(main, "Port 22\n") || !strings.Contains(main, "PermitRootLogin yes") {
			t.Errorf("expected the directives appended after the existing config, got:\n%s", main)
		}
		if len(dropIns) != 0 {
			t.Errorf("no drop-in expected without an Include line, got %v", dropIns)
		}
	})
}
