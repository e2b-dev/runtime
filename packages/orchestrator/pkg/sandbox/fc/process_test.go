//go:build linux

package fc

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/cgroup"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/rootfs"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/testutils"
	sbxlogger "github.com/e2b-dev/infra/packages/shared/pkg/logger/sandbox"
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

// TestConfigureRejectsProcessExitBeforeSocketReadiness: a process that exits
// before its API socket accepts fails the start with that cause, whether the
// start context or the guard sees the exit first; the guard alone is
// TestRefuseReapedProcess's.
func TestConfigureRejectsProcessExitBeforeSocketReadiness(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	socketPath := filepath.Join(dir, "firecracker.sock")
	cmd := exec.CommandContext(t.Context(), "/bin/sh", "-c", "exit 0")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true} //nolint:exhaustruct_v5 // focused process-lifetime fixture
	p := &Process{                                       //nolint:exhaustruct_v5 // focused process-lifetime fixture
		cmd:                   cmd,
		Exit:                  utils.NewErrorOnce(),
		reaped:                make(chan struct{}),
		firecrackerSocketPath: socketPath,
		metricsPath:           filepath.Join(dir, "metrics.fifo"),
		files:                 &storage.SandboxFiles{SandboxID: "test"}, //nolint:exhaustruct_v5 // only identity is read
	}
	returned := make(chan error, 1)
	go func() {
		returned <- p.configure(
			t.Context(),
			sbxlogger.SandboxMetadata{SandboxID: "test"}, //nolint:exhaustruct_v5 // only sandbox identity is logged
			nil,
			nil,
			cgroup.NoCgroupFD,
		)
	}()

	<-p.Exit.Done()
	l, err := (&net.ListenConfig{}).Listen(t.Context(), "unix", socketPath)
	require.NoError(t, err)
	defer l.Close()

	err = <-returned
	require.ErrorIs(t, err, errFirecrackerExitedBeforeSocketReadiness)
	require.NoError(t, p.Exit.Error(), "startup refusal must not relabel a clean process exit")
}

// TestRefuseReapedProcess: a socket that accepts is not readiness for a
// process already reaped, whichever way it exited.
func TestRefuseReapedProcess(t *testing.T) {
	t.Parallel()

	running := utils.NewErrorOnce()
	require.NoError(t, refuseReapedProcess(running), "a running process is ready")

	clean := utils.NewErrorOnce()
	require.NoError(t, clean.SetSuccess())
	require.ErrorIs(t, refuseReapedProcess(clean), errFirecrackerExitedBeforeSocketReadiness)

	crashed := utils.NewErrorOnce()
	boom := errors.New("boom")
	require.NoError(t, crashed.SetError(boom))
	require.ErrorIs(t, refuseReapedProcess(crashed), boom)
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

// fakeFirecracker answers the Firecracker API calls of a snapshot restore and
// records them in order. It shows which calls reach the VMM and when, not what
// a real guest does.
type fakeFirecracker struct {
	mu    sync.Mutex
	calls []string
	// failResumes is how many vCPU resumes fail before one succeeds.
	failResumes int
}

func (f *fakeFirecracker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var body struct {
		State    string `json:"state"`
		ResumeVM bool   `json:"resume_vm"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)

	f.mu.Lock()
	defer f.mu.Unlock()

	call := r.Method + " " + r.URL.Path
	switch {
	case call == "PUT /snapshot/load" && body.ResumeVM:
		call += " resume_vm"
	case call == "PATCH /vm":
		call += " " + body.State
	}
	f.calls = append(f.calls, call)

	if call == "PATCH /vm Resumed" && f.failResumes > 0 {
		f.failResumes--
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"fault_message":"resume failed"}`))

		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (f *fakeFirecracker) recorded() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return slices.Clone(f.calls)
}

type restoreRootfs struct {
	rootfs.Provider

	path string
}

func (r restoreRootfs) Path() (string, error) { return r.path, nil }

// newRestoreProcess is a Process whose VMM is a sleeping placeholder behind
// fake's API socket.
func newRestoreProcess(t *testing.T, fake *fakeFirecracker) *Process {
	t.Helper()

	dir := testutils.SocketDir(t)

	socket := dir + "/fc.sock"
	listener, err := new(net.ListenConfig).Listen(t.Context(), "unix", socket)
	require.NoError(t, err)
	server := &http.Server{Handler: fake} //nolint:gosec // no timeouts needed for a stub on a private socket
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close() })

	cmd := exec.CommandContext(t.Context(), "sleep", "60")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	p := &Process{
		cmd:                   cmd,
		Exit:                  utils.NewErrorOnce(),
		reaped:                make(chan struct{}),
		metricsDone:           make(chan struct{}),
		config:                cfg.BuilderConfig{StorageConfig: storage.Config{SandboxCacheDir: dir}},
		firecrackerSocketPath: socket,
		metricsPath:           dir + "/metrics.fifo",
		rootfsProvider:        restoreRootfs{path: dir + "/rootfs.ext4"},
		files:                 &storage.SandboxFiles{SandboxID: "test"},
		client:                newApiClient(socket),
	}
	t.Cleanup(func() { _ = p.Stop(context.WithoutCancel(t.Context())) })

	return p
}

// restore loads a snapshot into p paused, or runs it too when runVcpus is set.
func restore(t *testing.T, p *Process, runVcpus bool) error {
	t.Helper()

	dir := filepath.Dir(p.firecrackerSocketPath)
	uffdSocket := dir + "/uffd.sock"
	require.NoError(t, os.WriteFile(uffdSocket, nil, 0o600))
	uffdReady := make(chan struct{})
	close(uffdReady)

	metadata := sbxlogger.SandboxMetadata{SandboxID: "test"}
	snapfile := stubSnapfile(dir + "/snapfile")
	vcpus := ResumeVcpus{Target: 1, Snapshot: 1, Recorded: true}
	if runVcpus {
		return p.Resume(t.Context(), metadata, uffdSocket, snapfile, uffdReady, nil,
			nil, false, false, GuestOptions{}, "", RateLimiterConfig{}, RateLimiterConfig{}, vcpus)
	}

	return p.RestorePaused(t.Context(), metadata, uffdSocket, snapfile, uffdReady,
		nil, false, false, GuestOptions{}, "", RateLimiterConfig{}, RateLimiterConfig{}, vcpus)
}

// A paused restore configures the VM without running a vCPU or setting MMDS.
// Its resume acts on the same VMM: a failed reply leaves it for a retry, and
// no resume, repeated or not, loads the snapshot again. MMDS follows the
// vCPU resume, as in an ordinary resume.
func TestRestorePausedRunsNoVcpuUntilResumedInPlace(t *testing.T) {
	t.Parallel()

	fake := &fakeFirecracker{failResumes: 1}
	p := newRestoreProcess(t, fake)
	require.NoError(t, restore(t, p, false))

	restored := fake.recorded()
	require.Contains(t, restored, "PUT /snapshot/load")
	require.NotContains(t, restored, "PUT /mmds")
	require.NotContains(t, restored, "PATCH /vm Resumed")

	require.Error(t, p.ResumeInPlace(t.Context()))
	require.False(t, p.Exited(), "a failed resume keeps the VMM for a retry")
	require.NoError(t, p.ResumeInPlace(t.Context()))
	accessToken := "token"
	require.NoError(t, p.SetMmds(t.Context(), sbxlogger.SandboxMetadata{SandboxID: "test"}, &accessToken))
	require.NoError(t, p.ResumeInPlace(t.Context()), "a repeated resume acts on the running VM")

	resumed := []string{"PATCH /vm Resumed", "PATCH /vm Resumed", "PUT /mmds", "PATCH /vm Resumed"}
	require.Equal(t, append(restored, resumed...), fake.recorded())
}

// An ordinary resume is the paused restore followed by one vCPU resume, and
// MMDS follows the vCPU resume. A failed resume stops the VMM.
func TestResumeRestoresPausedThenRunsVcpus(t *testing.T) {
	t.Parallel()

	t.Run("resumes", func(t *testing.T) {
		t.Parallel()

		paused := &fakeFirecracker{}
		require.NoError(t, restore(t, newRestoreProcess(t, paused), false))

		fake := &fakeFirecracker{}
		p := newRestoreProcess(t, fake)
		require.NoError(t, restore(t, p, true))

		require.Equal(t, append(paused.recorded(), "PATCH /vm Resumed", "PUT /mmds"), fake.recorded())
		require.False(t, p.Exited())
	})

	t.Run("resume fails", func(t *testing.T) {
		t.Parallel()

		fake := &fakeFirecracker{failResumes: 1}
		p := newRestoreProcess(t, fake)
		require.Error(t, restore(t, p, true))
		require.True(t, p.Exited())
	})
}
