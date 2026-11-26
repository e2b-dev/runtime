//go:build linux

package handler

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func runOomWrapper(t *testing.T, oomScore int, credential *syscall.Credential) string {
	t.Helper()

	cmd := exec.CommandContext(t.Context(), "/bin/sh", "-c", fmt.Sprintf(oomWrapperFormat, oomScore, ""), "--", "cat", "/proc/self/oom_score_adj")
	cmd.Dir = "/"
	cmd.SysProcAttr = &syscall.SysProcAttr{Credential: credential}
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, string(out))

	return strings.TrimSpace(string(out))
}

func TestOomWrapper(t *testing.T) {
	t.Parallel()

	raw, err := os.ReadFile("/proc/self/oom_score_adj")
	require.NoError(t, err)
	inherited := strings.TrimSpace(string(raw))

	t.Run("applies the score", func(t *testing.T) {
		t.Parallel()
		// Raising the score needs no privilege.
		assert.Equal(t, "1000", runOomWrapper(t, 1000, nil))
	})

	t.Run("runs the command when the write is refused", func(t *testing.T) {
		t.Parallel()
		if inherited == "-1000" {
			t.Skip("oom_score_adj is already at its minimum")
		}
		// Lowering the score needs CAP_SYS_RESOURCE; as root, drop it by running as nobody.
		var credential *syscall.Credential
		if os.Geteuid() == 0 {
			credential = &syscall.Credential{Uid: 65534, Gid: 65534, Groups: []uint32{65534}}
		}
		assert.Equal(t, inherited, runOomWrapper(t, -1000, credential))
	})
}
