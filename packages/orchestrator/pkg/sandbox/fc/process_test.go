//go:build linux

package fc

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

func TestValidateFirecrackerBinaryReportsBothMissingPaths(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	config := cfg.BuilderConfig{FirecrackerVersionsDir: dir}
	versions := Config{FirecrackerVersion: "v1.14.1_431f1fc"}
	archPath, legacyPath := versions.firecrackerPaths(config)

	err := validateFirecrackerBinary(versions, config)

	require.ErrorIs(t, err, os.ErrNotExist)
	require.ErrorContains(t, err, "firecracker binary not found; checked architecture-specific path")
	require.ErrorContains(t, err, archPath)
	require.ErrorContains(t, err, legacyPath)
}

func TestProcessStopIsIdempotent(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()

	cmd := startSetsidCommand(t, ctx, "sleep", "60")
	pid := cmd.Process.Pid

	exit := utils.NewErrorOnce()
	waitDone := make(chan struct{})
	go func() {
		defer close(waitDone)
		_ = cmd.Wait()
		exit.SetError(nil)
	}()
	t.Cleanup(func() {
		_ = syscall.Kill(-pid, syscall.SIGKILL)
		<-waitDone
	})

	p := &Process{
		cmd:         cmd,
		Exit:        exit,
		files:       &storage.SandboxFiles{SandboxID: "test"},
		metricsPath: t.TempDir() + "/metrics.fifo",
	}
	require.NoError(t, p.Stop(ctx))
	require.NoError(t, p.Stop(ctx))
}

func TestProcessStopDoesNotSignalProcessGroupAfterLeaderExit(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()

	childPidPath := filepath.Join(t.TempDir(), "child.pid")
	cmd := startSetsidCommand(t, ctx, "bash", "-c", "sleep 60 & echo $! > \"$1\"", "sh", childPidPath)
	pid := cmd.Process.Pid

	exit := utils.NewErrorOnce()
	waitDone := make(chan struct{})
	go func() {
		defer close(waitDone)
		_ = cmd.Wait()
		exit.SetError(nil)
	}()

	var childPid int
	require.Eventually(t, func() bool {
		data, err := os.ReadFile(childPidPath)
		if err != nil {
			return false
		}

		childPid, err = strconv.Atoi(strings.TrimSpace(string(data)))

		return err == nil && childPid > 0
	}, time.Second, 10*time.Millisecond)
	t.Cleanup(func() {
		_ = syscall.Kill(childPid, syscall.SIGKILL)
		_ = syscall.Kill(-pid, syscall.SIGKILL)
		<-waitDone
	})

	require.Eventually(t, func() bool {
		select {
		case <-waitDone:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)

	p := &Process{
		cmd:         cmd,
		Exit:        exit,
		files:       &storage.SandboxFiles{SandboxID: "test"},
		metricsPath: filepath.Join(t.TempDir(), "metrics.fifo"),
	}
	require.NoError(t, p.Stop(ctx))
	require.NoError(t, syscall.Kill(childPid, 0))
}

func startSetsidCommand(t *testing.T, ctx context.Context, name string, args ...string) *exec.Cmd {
	t.Helper()

	cmd := exec.CommandContext(ctx, name, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	require.NoError(t, cmd.Start())

	return cmd
}
