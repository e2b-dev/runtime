//go:build unix

package api

import (
	"context"
	"os/exec"
	"strings"
	"syscall"
	"time"
)

// maxMountOutputBytes caps the mount output kept in an error.
const maxMountOutputBytes = 512

// nfsCmdWaitDelay bounds how long a canceled command may keep its output pipes open
// before Wait stops reading them.
const nfsCmdWaitDelay = 2 * time.Second

// runProcessGroup runs a command in its own process group and kills the whole group
// when ctx ends. mount forks mount.nfs, which would outlive a kill of mount alone and
// could finish the mount after the timeout. It returns the combined output, trimmed and
// capped at maxMountOutputBytes.
func runProcessGroup(ctx context.Context, name string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL); err != nil {
			return cmd.Process.Kill()
		}

		return nil
	}
	cmd.WaitDelay = nfsCmdWaitDelay

	data, err := cmd.CombinedOutput()

	return strings.TrimSpace(string(data[:min(len(data), maxMountOutputBytes)])), err
}
