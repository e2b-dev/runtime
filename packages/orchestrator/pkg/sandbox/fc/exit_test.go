//go:build linux

package fc

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

func TestClassifyExit(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name     string
		info     *ExitInfo
		expected CrashCause
	}{
		{
			name:     "never reaped",
			info:     nil,
			expected: CrashCauseUnknown,
		},
		{
			name:     "guest powered itself off",
			info:     &ExitInfo{Exited: true, ExitCode: 0},
			expected: CrashCauseCleanExit,
		},
		{
			name:     "firecracker failed",
			info:     &ExitInfo{Exited: true, ExitCode: 1},
			expected: CrashCauseExitError,
		},
		{
			name:     "killed by someone else",
			info:     &ExitInfo{Signaled: true, Signal: syscall.SIGKILL},
			expected: CrashCauseExternalSignal,
		},
		{
			name:     "terminated by someone else",
			info:     &ExitInfo{Signaled: true, Signal: syscall.SIGTERM},
			expected: CrashCauseExternalSignal,
		},
		{
			name:     "killed by our own stop",
			info:     &ExitInfo{Signaled: true, Signal: syscall.SIGKILL, SentByStop: true},
			expected: CrashCauseRequestedSignal,
		},
		{
			// A clean exit is the guest's doing either way.
			name:     "clean exit during our stop",
			info:     &ExitInfo{Exited: true, ExitCode: 0, SentByStop: true},
			expected: CrashCauseCleanExit,
		},
		{
			// Before Firecracker installs its handlers, a fault still kills
			// it by the signal.
			name:     "faulted on a memory access",
			info:     &ExitInfo{Signaled: true, Signal: syscall.SIGBUS},
			expected: CrashCauseFault,
		},
		{
			// uffd failing to serve a page raises SIGBUS, which Firecracker
			// catches and exits 149 for; hugepage exhaustion has its own
			// alert and must not arrive as a Firecracker failure.
			name:     "caught a memory fault",
			info:     &ExitInfo{Exited: true, ExitCode: 149},
			expected: CrashCauseFault,
		},
		{
			name:     "caught a bad syscall",
			info:     &ExitInfo{Exited: true, ExitCode: 148},
			expected: CrashCauseFault,
		},
		{
			name:     "caught a segfault",
			info:     &ExitInfo{Exited: true, ExitCode: 150},
			expected: CrashCauseFault,
		},
		{
			name:     "caught an illegal instruction",
			info:     &ExitInfo{Exited: true, ExitCode: 157},
			expected: CrashCauseFault,
		},
		{
			// Nothing inside Firecracker raises SIGHUP.
			name:     "caught a hangup",
			info:     &ExitInfo{Exited: true, ExitCode: 156},
			expected: CrashCauseExternalSignal,
		},
		{
			name:     "exceeded a file size limit",
			info:     &ExitInfo{Exited: true, ExitCode: 151},
			expected: CrashCauseExitError,
		},
		{
			name:     "aborted itself",
			info:     &ExitInfo{Signaled: true, Signal: syscall.SIGABRT, CoreDumped: true},
			expected: CrashCauseFault,
		},
		{
			// A fault during our teardown is still a fault: the SIGTERM we
			// sent is not what killed it.
			name:     "faulted while we were stopping it",
			info:     &ExitInfo{Signaled: true, Signal: syscall.SIGSEGV, SentByStop: true},
			expected: CrashCauseFault,
		},
		{
			name:     "neither exited nor signaled",
			info:     &ExitInfo{},
			expected: CrashCauseUnknown,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.expected, ClassifyExit(tt.info))
		})
	}
}

func TestExitInfoLogFields(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name     string
		info     *ExitInfo
		expected []zap.Field
	}{
		{
			name:     "never reaped",
			info:     nil,
			expected: nil,
		},
		{
			name: "signalled",
			info: &ExitInfo{Signaled: true, Signal: syscall.SIGKILL, CoreDumped: true},
			expected: []zap.Field{
				zap.String("fc_signal", "killed"),
				zap.Bool("fc_core_dumped", true),
			},
		},
		{
			name:     "exited",
			info:     &ExitInfo{Exited: true, ExitCode: 7},
			expected: []zap.Field{zap.Int("fc_exit_code", 7)},
		},
		{
			name:     "neither exited nor signaled",
			info:     &ExitInfo{},
			expected: nil,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.expected, tt.info.LogFields())
		})
	}
}

// The recorded status must survive the branches that resolve Exit clean.
func TestHandleExitPublishesExitInfo(t *testing.T) {
	t.Parallel()

	t.Run("signalled exit resolves Exit without an error", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()

		cmd := startSetsidCommand(t, ctx, "sleep", "60")
		require.NoError(t, cmd.Process.Kill())

		p := &Process{cmd: cmd, Exit: utils.NewErrorOnce()}
		require.NoError(t, p.handleExit(ctx, cmd.Wait()))
		require.NoError(t, p.Exit.Error())

		info := p.ExitInfo()
		require.NotNil(t, info)
		assert.True(t, info.Signaled)
		assert.Equal(t, syscall.SIGKILL, info.Signal)
		assert.Equal(t, CrashCauseExternalSignal, ClassifyExit(info))
	})

	t.Run("non-zero exit resolves Exit with an error", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()

		cmd := startSetsidCommand(t, ctx, "/bin/sh", "-c", "exit 7")

		p := &Process{cmd: cmd, Exit: utils.NewErrorOnce()}
		require.Error(t, p.handleExit(ctx, cmd.Wait()))
		require.Error(t, p.Exit.Error())

		info := p.ExitInfo()
		require.NotNil(t, info)
		assert.Equal(t, &ExitInfo{Exited: true, ExitCode: 7}, info)
		assert.Equal(t, CrashCauseExitError, ClassifyExit(info))
	})

	t.Run("clean exit", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()

		cmd := startSetsidCommand(t, ctx, "/bin/sh", "-c", "exit 0")

		p := &Process{cmd: cmd, Exit: utils.NewErrorOnce()}
		require.NoError(t, p.handleExit(ctx, cmd.Wait()))
		require.NoError(t, p.Exit.Error())

		assert.Equal(t, &ExitInfo{Exited: true, ExitCode: 0}, p.ExitInfo())
		assert.Equal(t, CrashCauseCleanExit, ClassifyExit(p.ExitInfo()))
	})

	t.Run("fault caught by firecracker's handler", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()

		cmd := startSetsidCommand(t, ctx, "/bin/sh", "-c", "exit 149")

		p := &Process{cmd: cmd, Exit: utils.NewErrorOnce()}
		require.Error(t, p.handleExit(ctx, cmd.Wait()))

		assert.Equal(t, &ExitInfo{Exited: true, ExitCode: 149}, p.ExitInfo())
		assert.Equal(t, CrashCauseFault, ClassifyExit(p.ExitInfo()))
	})
}

// Stop records the signals it sends, which is what separates our signal from a
// foreign one.
func TestProcessStopClaimsTheKill(t *testing.T) {
	t.Parallel()

	t.Run("claims its own kill however the reap is ordered", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()

		cmd := startSetsidCommand(t, ctx, "sleep", "60")
		pid := cmd.Process.Pid

		p := testStopProcess(t, cmd)
		waitDone := make(chan struct{})
		go func() {
			defer close(waitDone)
			_ = p.handleExit(ctx, cmd.Wait())
		}()
		t.Cleanup(func() {
			_ = syscall.Kill(-pid, syscall.SIGKILL)
			<-waitDone
		})

		require.NoError(t, p.Stop(ctx))

		info := p.ExitInfo()
		require.NotNil(t, info)
		assert.Equal(t, syscall.SIGTERM, info.Signal)
		assert.True(t, info.SentByStop)
		assert.Equal(t, CrashCauseRequestedSignal, ClassifyExit(info))
	})

	// Teardown calls Stop after the process is gone. Without the reap-time
	// sample that late claim relabels every external kill as ours.
	t.Run("does not claim an exit already classified", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()

		cmd := startSetsidCommand(t, ctx, "sleep", "60")
		require.NoError(t, cmd.Process.Kill())

		p := testStopProcess(t, cmd)
		require.NoError(t, p.handleExit(ctx, cmd.Wait()))

		// What the exit-wait goroutine does before anything classifies.
		require.NoError(t, p.Stop(ctx))
		assert.Zero(t, p.sentSignals.Load())

		info := p.ExitInfo()
		require.NotNil(t, info)
		assert.False(t, info.SentByStop)
		assert.Equal(t, CrashCauseExternalSignal, ClassifyExit(info))
	})

	// The resume path stops the sandbox as soon as uffd OR Firecracker exits,
	// so Stop can signal a Firecracker that something else already killed but
	// nothing has reaped yet. A zombie accepts the signal, so the record is
	// set; the terminating signal is still not the one we sent.
	t.Run("does not claim a kill that landed before its signal", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()

		cmd := startSetsidCommand(t, ctx, "sleep", "60")
		require.NoError(t, syscall.Kill(cmd.Process.Pid, syscall.SIGKILL))
		waitForZombie(t, cmd.Process.Pid)

		p := testStopProcess(t, cmd)
		require.NoError(t, p.signal(syscall.SIGTERM), "a zombie accepts signals")

		require.NoError(t, p.handleExit(ctx, cmd.Wait()))

		info := p.ExitInfo()
		require.NotNil(t, info)
		assert.Equal(t, syscall.SIGKILL, info.Signal)
		assert.False(t, info.SentByStop)
		assert.Equal(t, CrashCauseExternalSignal, ClassifyExit(info))
	})
}

func testStopProcess(t *testing.T, cmd *exec.Cmd) *Process {
	t.Helper()

	return &Process{
		cmd:         cmd,
		Exit:        utils.NewErrorOnce(),
		files:       &storage.SandboxFiles{SandboxID: "test"},
		metricsPath: filepath.Join(t.TempDir(), "metrics.fifo"),
	}
}

// waitForZombie waits until pid has died and is waiting to be reaped.
func waitForZombie(t *testing.T, pid int) {
	t.Helper()

	statPath := fmt.Sprintf("/proc/%d/stat", pid)
	require.Eventually(t, func() bool {
		stat, err := os.ReadFile(statPath)
		if err != nil {
			return false
		}

		// The state follows the parenthesised command name.
		rest := stat[bytes.LastIndexByte(stat, ')')+1:]

		return len(rest) > 1 && rest[1] == 'Z'
	}, 10*time.Second, time.Millisecond)
}
