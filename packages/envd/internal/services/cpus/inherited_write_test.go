//go:build unix

package cpus

import (
	"os/exec"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests model a write inherited across a live upgrade, which envd
// performs only on unix.

// An upgrade can land while the previous envd's write is still in the kernel, after the
// target that write served was lowered. The new worker must let that write land before
// reconciling, or it stops on a count the write is about to change.
func TestInheritedWriteLandsBeforeReconciling(t *testing.T) {
	t.Parallel()

	sys := newFakeCPUSysfs(4, 2)
	var writes atomic.Int32
	sys.setHook(func(int, bool) error {
		writes.Add(1)

		return nil
	})
	m := newTestManager(t, sys)
	writer := exec.CommandContext(t.Context(), "cat")
	stdin, err := writer.StdinPipe()
	require.NoError(t, err)
	require.NoError(t, writer.Start())
	pid := writer.Process.Pid
	require.NoError(t, writer.Process.Release())
	m.children.Adopt(pid)
	require.NoError(t, m.SetTarget(2))

	m.Start(t.Context())

	eventually(t, func() bool { return m.Status().WritePending > 0 })
	time.Sleep(50 * time.Millisecond)
	assert.Zero(t, writes.Load(), "nothing is written while the inherited write runs")

	sys.mu.Lock()
	sys.online[2] = true // the inherited write lands, then its process exits
	sys.mu.Unlock()
	require.NoError(t, stdin.Close())

	eventually(t, func() bool { return len(sys.onlineCPUs()) == 2 && converged(m.Status()) })
	assert.Equal(t, []int{0, 1}, sys.onlineCPUs())
	assert.Equal(t, time.Duration(0), m.Status().WritePending)
}

// When the inherited write leaves nothing to do, nothing else clears the pending time.
func TestInheritedWriteStopsBeingPendingOnceReaped(t *testing.T) {
	t.Parallel()

	sys := newFakeCPUSysfs(4, 2)
	m := newTestManager(t, sys)
	writer := exec.CommandContext(t.Context(), "cat")
	stdin, err := writer.StdinPipe()
	require.NoError(t, err)
	require.NoError(t, writer.Start())
	pid := writer.Process.Pid
	require.NoError(t, writer.Process.Release())
	m.children.Adopt(pid)
	m.Start(t.Context())

	eventually(t, func() bool { return m.Status().WritePending > 0 })
	require.NoError(t, stdin.Close())
	eventually(t, func() bool { return m.Status().WritePending == 0 })
}
