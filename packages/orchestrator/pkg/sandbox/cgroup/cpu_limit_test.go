//go:build linux

package cgroup

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestCpuLimitsWithoutCgroup(t *testing.T) {
	t.Parallel()

	for name, handle := range map[string]*CgroupHandle{
		"nil":     nil,
		"noop":    newNoopHandle("sbx-noop"),
		"removed": {path: t.TempDir(), removed: true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			require.ErrorIs(t, handle.SetSandboxCpuLimit(ctx, 2), ErrNoCgroup, "a limit nothing enforces must not look set")
			require.ErrorIs(t, handle.ConfineVcpuThreads(ctx, 2, 4), ErrNoCgroup)
			require.NoError(t, handle.SetSandboxCpuLimit(ctx, 0), "lifting is nothing")
			require.NoError(t, handle.ConfineVcpuThreads(ctx, 0, 4))
			require.NoError(t, handle.ConfineVcpuThreads(ctx, 2, 2), "a VM the limit cannot bind needs no cgroup")
		})
	}
}

func TestSetSandboxCpuLimit(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	handle := newTestCgroup(t, "test-cpu-limit")

	require.NoError(t, handle.SetSandboxCpuLimit(ctx, 2), "the limit goes on an empty cgroup, before Firecracker joins")
	assert.Equal(t, "200000 100000", cgroupFile(t, handle.path, "cpu.max"))

	require.NoError(t, handle.SetSandboxCpuLimit(ctx, 0))
	assert.Equal(t, "max 100000", cgroupFile(t, handle.path, "cpu.max"))
}

func TestConfineVcpuThreadsCompletedMarker(t *testing.T) {
	t.Parallel()

	parentPath := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(parentPath, vcpuCgroupName), 0o755))
	handle := &CgroupHandle{path: parentPath, vcpuThreadsConfined: true}
	require.NoError(t, os.WriteFile(filepath.Join(handle.vcpuCgroupPath(), "cpu.max"), []byte("max 100000"), 0o600))

	require.NoError(t, handle.ConfineVcpuThreads(t.Context(), 1, 2))
	assert.Equal(t, "100000 100000", cgroupFile(t, handle.vcpuCgroupPath(), "cpu.max"))
	assert.True(t, handle.vcpuThreadsConfined)
}

func TestParseUniqueThreadIDs(t *testing.T) {
	t.Parallel()

	assert.Equal(t, []int{12, 7}, parseUniqueThreadIDs([]byte("12\n7\n12\ninvalid\n7\n")))
}

func TestConfineVcpuThreads(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	handle := newTestCgroup(t, "test-vcpu-quota")
	vcpuPath := handle.vcpuCgroupPath()
	require.NoFileExists(t, vcpuPath, "Create leaves the cgroup as it was without the quota")

	cmd := startVcpuHelper(t, handle)
	require.NoError(t, handle.ConfineVcpuThreads(ctx, 2, 2), "two vCPU threads cannot exceed two CPUs")
	require.NoFileExists(t, vcpuPath, "so no quota is set")
	assert.False(t, handle.vcpuThreadsConfined)

	require.NoError(t, handle.SetSandboxCpuLimit(ctx, 1), "the boot-time limit on the whole cgroup")
	require.ErrorContains(t, handle.ConfineVcpuThreads(ctx, 1, 4), "found 2 of 4", "a VM said to have four vCPUs shows two: a subset is not confined")
	require.NoFileExists(t, vcpuPath)
	assert.False(t, handle.vcpuThreadsConfined)
	require.NoError(t, handle.ConfineVcpuThreads(ctx, 1, 2), "the vcpu child is set up after Firecracker joined")
	assert.True(t, handle.vcpuThreadsConfined)
	assert.Equal(t, "threaded", cgroupFile(t, vcpuPath, "cgroup.type"))
	assert.Equal(t, "100000 100000", cgroupFile(t, vcpuPath, "cpu.max"))
	assert.Len(t, strings.Fields(cgroupFile(t, vcpuPath, "cgroup.threads")), 2, "only the two vCPU threads move")
	remaining, err := vcpuThreads(handle.path)
	require.NoError(t, err)
	assert.Empty(t, remaining)
	assert.Equal(t, "100000 100000", cgroupFile(t, handle.path, "cpu.max"), "confining alone leaves the cgroup-wide limit")

	require.NoError(t, handle.SetSandboxCpuLimit(ctx, 0))
	assert.Equal(t, "max 100000", cgroupFile(t, handle.path, "cpu.max"))

	require.NoError(t, handle.ConfineVcpuThreads(ctx, 1, 2), "a completed move can safely be repeated")

	require.NoError(t, cmd.Process.Kill())
	_ = cmd.Wait()
	require.NoError(t, handle.Remove(ctx), "Remove deletes the vcpu child before the parent")
	assert.NoDirExists(t, handle.path)
}

func TestConfineVcpuThreadsUnknownChild(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	handle := newTestCgroup(t, "test-vcpu-unknown-child")
	startVcpuHelper(t, handle)
	require.NoError(t, handle.SetSandboxCpuLimit(ctx, 1))
	require.NoError(t, handle.createVcpuCgroup(), "simulate a child left by an incomplete move")

	require.ErrorContains(t, handle.ConfineVcpuThreads(ctx, 1, 2), "failed to create vcpu cgroup")
	assert.False(t, handle.vcpuThreadsConfined)
	assert.Equal(t, "100000 100000", cgroupFile(t, handle.path, "cpu.max"), "the whole-sandbox fallback stays in force")
}

func TestConfineVcpuThreadsWithoutThreads(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	handle := newTestCgroup(t, "test-vcpu-quota-empty")

	require.NoError(t, handle.SetSandboxCpuLimit(ctx, 1))
	require.ErrorContains(t, handle.ConfineVcpuThreads(ctx, 1, 4), "found 0 of 4")
	assert.Equal(t, "100000 100000", cgroupFile(t, handle.path, "cpu.max"), "the cgroup-wide limit stays")
}

func TestRemoveKillsConfinedThreads(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	handle := newTestCgroup(t, "test-vcpu-remove-live")
	cmd := startVcpuHelper(t, handle)
	require.NoError(t, handle.ConfineVcpuThreads(ctx, 1, 2))

	require.NoError(t, handle.Remove(ctx), "a populated vcpu child is killed and removed with its parent")
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	select {
	case <-exited:
	case <-time.After(5 * time.Second):
		t.Fatal("the helper outlived Remove")
	}
	assert.NoDirExists(t, handle.path)
}

func TestRemoveRetriesAfterFailure(t *testing.T) {
	t.Parallel()

	parentPath := t.TempDir()
	childPath := filepath.Join(parentPath, vcpuCgroupName)
	require.NoError(t, os.Mkdir(childPath, 0o755))
	blockingFile := filepath.Join(childPath, "busy")
	require.NoError(t, os.WriteFile(blockingFile, nil, 0o600))
	handle := &CgroupHandle{path: parentPath}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, handle.Remove(ctx), context.Canceled)
	assert.False(t, handle.removed, "failed cleanup must remain retryable")
	assert.DirExists(t, childPath)

	require.NoError(t, os.Remove(blockingFile))
	require.NoError(t, handle.Remove(t.Context()))
	assert.True(t, handle.removed)
	assert.NoDirExists(t, parentPath)
	require.NoError(t, handle.Remove(t.Context()), "successful cleanup remains idempotent")
}

// newTestCgroup returns a fresh sandbox cgroup, removed at the end of the test; skips without a writable cgroup v2.
func newTestCgroup(t *testing.T, name string) *CgroupHandle {
	t.Helper()
	requireWritableCgroup(t)

	mgr, err := NewManager()
	require.NoError(t, err)
	require.NoError(t, mgr.Initialize(t.Context()))
	handle, err := mgr.Create(t.Context(), name)
	require.NoError(t, err)
	// t.Context() is already canceled when Cleanup runs, and Remove's kill loop would return it.
	t.Cleanup(func() { _ = handle.Remove(context.WithoutCancel(t.Context())) })

	return handle
}

// cgroupFile reads a cgroup control file, trimmed.
func cgroupFile(t *testing.T, dir, name string) string {
	t.Helper()

	data, err := os.ReadFile(filepath.Join(dir, name))
	require.NoError(t, err)

	return strings.TrimSpace(string(data))
}

// startVcpuHelper runs TestHelperVcpuThreads in the cgroup and returns once its threads are named.
func startVcpuHelper(t *testing.T, handle *CgroupHandle) *exec.Cmd {
	t.Helper()

	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestHelperVcpuThreads$", "-test.v")
	cmd.Env = append(os.Environ(), "CGROUP_VCPU_HELPER=1")
	cmd.Stderr = os.Stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{UseCgroupFD: true, CgroupFD: handle.GetFD()}
	out, err := cmd.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	handle.ReleaseCgroupFD()

	for lines := bufio.NewScanner(out); lines.Scan(); {
		if lines.Text() == "ready" {
			return cmd
		}
	}
	t.Fatal("the helper never named its threads")

	return nil
}

// TestHelperVcpuThreads stands in for Firecracker: two threads named like vCPUs, one like the API, then it waits.
func TestHelperVcpuThreads(t *testing.T) {
	t.Parallel()

	if os.Getenv("CGROUP_VCPU_HELPER") != "1" {
		t.Skip("helper process only")
	}

	names := []string{"fc_vcpu 0", "fc_vcpu 1", "fc_api"}
	named := make(chan error)
	for _, name := range names {
		go func() {
			runtime.LockOSThread()
			named <- os.WriteFile(fmt.Sprintf("/proc/self/task/%d/comm", unix.Gettid()), []byte(name), 0)
			// Not an empty select: with every goroutine blocked the runtime reports a deadlock and exits.
			time.Sleep(time.Hour)
		}()
	}
	for range names {
		require.NoError(t, <-named)
	}
	fmt.Println("ready")
	time.Sleep(time.Hour)
}
