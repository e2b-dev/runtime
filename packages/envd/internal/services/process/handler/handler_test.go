package handler

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The process wrapper must degrade cleanly when the priority helpers are absent
// — the user command still runs; a present helper is applied with its resolved
// path.
func TestWrapperPrefix(t *testing.T) {
	t.Parallel()

	notFound := func(string) (string, error) { return "", errors.New("not found") }
	all := func(name string) (string, error) { return "/usr/bin/" + name, nil }
	only := func(want string) func(string) (string, error) {
		return func(name string) (string, error) {
			if name == want {
				return "/bin/" + name, nil
			}

			return "", errors.New("not found")
		}
	}

	t.Run("both present", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, "/usr/bin/ionice -c 2 -n 4 /usr/bin/nice -n 5 ", ioniceNicePrefix(2, 4, 5, all))
	})

	t.Run("both absent degrades to bare exec", func(t *testing.T) {
		t.Parallel()
		assert.Empty(t, ioniceNicePrefix(2, 4, 5, notFound))
	})

	t.Run("only nice", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, "/bin/nice -n -3 ", ioniceNicePrefix(2, 4, -3, only("nice")))
	})

	t.Run("only ionice", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, "/bin/ionice -c 2 -n 4 ", ioniceNicePrefix(2, 4, 0, only("ionice")))
	})

	t.Run("class and priority are caller-controlled", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, "/bin/ionice -c 1 -n 6 ", ioniceNicePrefix(1, 6, 0, only("ionice")))
	})
}

func TestSendSignalTargetsOnlyLeaderByDefault(t *testing.T) {
	t.Parallel()

	cmd := startCommandGroup(t)

	h := &Handler{cmd: cmd, outCancel: func() {}}
	t.Cleanup(func() {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		_, _ = cmd.Process.Wait()
	})

	require.NoError(t, h.SendSignal(syscall.SIGTERM, false))
	state, err := cmd.Process.Wait()
	require.NoError(t, err)
	assert.False(t, state.Success())
	require.NoError(t, syscall.Kill(-cmd.Process.Pid, syscall.Signal(0)), "child should remain in the group")
}

func TestSendSignalTargetsProcessGroupWhenDescendantsRequested(t *testing.T) {
	t.Parallel()

	cmd := startCommandGroup(t)

	h := &Handler{cmd: cmd, outCancel: func() {}}
	t.Cleanup(func() {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		_, _ = cmd.Process.Wait()
	})

	require.NoError(t, h.SendSignal(syscall.SIGTERM, true))
	state, err := cmd.Process.Wait()
	require.NoError(t, err)
	assert.False(t, state.Success())
	require.Eventually(t, func() bool {
		return syscall.Kill(-cmd.Process.Pid, syscall.Signal(0)) != nil
	}, 2*time.Second, 10*time.Millisecond)
}

func TestSendSignalRejectsDescendantsWithoutOwnedProcessGroup(t *testing.T) {
	t.Parallel()

	cmd := exec.CommandContext(t.Context(), "sleep", "60")
	require.NoError(t, cmd.Start())
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	})

	outputCancelled := false
	h := &Handler{cmd: cmd, outCancel: func() { outputCancelled = true }}
	err := h.SendSignal(syscall.SIGKILL, true)
	require.ErrorContains(t, err, "does not own a process group")
	require.NoError(t, cmd.Process.Signal(syscall.Signal(0)), "leader should remain alive")
	assert.False(t, outputCancelled, "a rejected signal must keep the live process output connected")
}

func TestConfigureProcessGroup(t *testing.T) {
	t.Parallel()

	nonPTY := &syscall.SysProcAttr{}
	configureProcessGroup(nonPTY, false)
	assert.True(t, nonPTY.Setpgid)

	pty := &syscall.SysProcAttr{}
	configureProcessGroup(pty, true)
	assert.False(t, pty.Setpgid, "PTY startup creates its own session")
}

func startCommandGroup(t *testing.T) *exec.Cmd {
	t.Helper()

	childReady := filepath.Join(t.TempDir(), "child.pid")
	cmd := exec.CommandContext(t.Context(), "sh", "-c", fmt.Sprintf("sleep 60 & echo $! > %q; exec sleep 60", childReady))
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	require.NoError(t, cmd.Start())
	require.Eventually(t, func() bool {
		_, err := os.Stat(childReady)

		return err == nil
	}, 2*time.Second, 10*time.Millisecond)

	return cmd
}
