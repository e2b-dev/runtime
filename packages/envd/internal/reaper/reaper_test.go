package reaper

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func eventually(t *testing.T, cond func() bool) {
	t.Helper()
	require.Eventually(t, cond, 5*time.Second, 5*time.Millisecond)
}

func exported(t *testing.T, r *Registry) []int32 {
	t.Helper()
	pids, release := r.ExportHold()
	release()

	return pids
}

func TestRunListsTheChildWhileItLives(t *testing.T) {
	t.Parallel()

	var r Registry
	cat := exec.CommandContext(t.Context(), "cat")
	stdin, err := cat.StdinPipe()
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- r.Run(cat) }()

	eventually(t, func() bool { return len(exported(t, &r)) == 1 })
	listed := exported(t, &r)

	require.NoError(t, stdin.Close())
	require.NoError(t, <-done)
	assert.Equal(t, []int32{int32(cat.Process.Pid)}, listed)
	assert.Empty(t, exported(t, &r))
}

// No child may start between the export and the execve it feeds, or the new envd would
// never hear of it.
func TestExportHoldKeepsChildrenFromStarting(t *testing.T) {
	t.Parallel()

	var r Registry
	mark := filepath.Join(t.TempDir(), "started")
	pids, release := r.ExportHold()
	assert.Empty(t, pids)

	done := make(chan error, 1)
	go func() { done <- r.Run(exec.CommandContext(t.Context(), "touch", mark)) }()
	time.Sleep(50 * time.Millisecond)
	_, err := os.Stat(mark)
	require.ErrorIs(t, err, fs.ErrNotExist, "a child started while the export was held")

	release()
	release() // a failed upgrade's deferred release must be safe to repeat

	require.NoError(t, <-done)
	_, err = os.Stat(mark)
	require.NoError(t, err)
	assert.Empty(t, exported(t, &r))
}

func TestRunReportsTheChildsFailure(t *testing.T) {
	t.Parallel()

	var r Registry
	var exitErr *exec.ExitError
	require.ErrorAs(t, r.Run(exec.CommandContext(t.Context(), "false")), &exitErr)
	require.ErrorIs(t, r.Run(exec.CommandContext(t.Context(), "/nonexistent/helper")), fs.ErrNotExist)
	assert.Empty(t, exported(t, &r), "a child that failed or never started is not listed")
}

func TestAdoptListsTheChildUntilReaped(t *testing.T) {
	t.Parallel()

	cat := exec.CommandContext(t.Context(), "cat")
	stdin, err := cat.StdinPipe()
	require.NoError(t, err)
	require.NoError(t, cat.Start())
	pid := cat.Process.Pid
	require.NoError(t, cat.Process.Release(), "the previous envd's handle is gone with it")

	var r Registry
	r.Adopt(pid)

	assert.Equal(t, []int32{int32(pid)}, exported(t, &r), "a further upgrade hands the child on")

	require.NoError(t, stdin.Close())
	eventually(t, func() bool { return errors.Is(syscall.Kill(pid, 0), syscall.ESRCH) })
	eventually(t, func() bool { return len(exported(t, &r)) == 0 })
}

func waited(r *Registry) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		r.WaitAdopted()
		close(done)
	}()

	return done
}

func TestWaitAdoptedReturnsOnceEveryAdoptedChildIsReaped(t *testing.T) {
	t.Parallel()

	var none Registry
	select {
	case <-waited(&none):
	case <-time.After(5 * time.Second):
		t.Fatal("nothing adopted, so nothing to wait for")
	}

	var r Registry

	var stdins []io.Closer
	for range 2 {
		cat := exec.CommandContext(t.Context(), "cat")
		stdin, err := cat.StdinPipe()
		require.NoError(t, err)
		require.NoError(t, cat.Start())
		pid := cat.Process.Pid
		require.NoError(t, cat.Process.Release())
		r.Adopt(pid)
		stdins = append(stdins, stdin)
	}
	adopted := waited(&r)

	require.NoError(t, stdins[0].Close())
	time.Sleep(50 * time.Millisecond)
	select {
	case <-adopted:
		t.Fatal("closed while an adopted child still runs")
	default:
	}

	require.NoError(t, stdins[1].Close())
	eventually(t, func() bool {
		select {
		case <-adopted:
			return true
		default:
			return false
		}
	})
}

func TestAdoptDropsAPidThatIsNotAChild(t *testing.T) {
	t.Parallel()

	var r Registry
	r.Adopt(os.Getppid())

	eventually(t, func() bool { return len(exported(t, &r)) == 0 })
}

func TestReapCollectsTheHandedOverChild(t *testing.T) {
	t.Parallel()

	child := exec.CommandContext(t.Context(), "true")
	require.NoError(t, child.Start())
	pid := child.Process.Pid
	require.NoError(t, child.Process.Release())

	go Reap(pid)

	eventually(t, func() bool { return errors.Is(syscall.Kill(pid, 0), syscall.ESRCH) })
}

// wait4 reads a pid at or below zero as a process group, and would take the exit status
// of whichever child in it exits first, including one os/exec is waiting for.
func TestReapRefusesAProcessGroup(t *testing.T) {
	t.Parallel()

	cat := exec.CommandContext(t.Context(), "cat")
	cat.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	stdin, err := cat.StdinPipe()
	require.NoError(t, err)
	require.NoError(t, cat.Start())

	done := make(chan struct{})
	go func() {
		Reap(-cat.Process.Pid)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Reap waited on a process group")
	}

	require.NoError(t, stdin.Close())
	require.NoError(t, cat.Wait())
}
