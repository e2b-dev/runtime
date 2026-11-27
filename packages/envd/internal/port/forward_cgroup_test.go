//go:build unix

package port

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/envd/internal/services/cgroups"
)

// newSocatCgroupManager is a socatCgroupManager whose socat cgroup exists and holds nothing.
func newSocatCgroupManager(t *testing.T) *socatCgroupManager {
	t.Helper()

	m := &socatCgroupManager{root: t.TempDir()}
	path, _ := m.PathOf(cgroups.ProcessTypeSocat)
	require.NoError(t, os.MkdirAll(path, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(path, "cgroup.procs"), nil, 0o600))

	return m
}

// withSocatCgroup gives f a socat cgroup listing members and moved, and a process
// filesystem where each member's own cgroup file names that cgroup and each moved pid's
// names another, as a pid reused by a process elsewhere after the listing.
func withSocatCgroup(t *testing.T, f *Forwarder, members, moved []int) {
	t.Helper()

	mgr := newSocatCgroupManager(t)
	f.freezer = cgroups.NewWorkloadFreezer(mgr)
	f.procRoot = t.TempDir()
	path, ok := mgr.PathOf(cgroups.ProcessTypeSocat)
	require.True(t, ok)

	var listed []string
	place := func(pid int, cgroup string) {
		dir := filepath.Join(f.procRoot, strconv.Itoa(pid))
		require.NoError(t, os.MkdirAll(dir, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(dir, "cgroup"), []byte("0::"+cgroup+"\n"), 0o600))
		listed = append(listed, strconv.Itoa(pid))
	}
	for _, pid := range members {
		place(pid, "/socats")
	}
	for _, pid := range moved {
		place(pid, "/user.slice")
	}
	require.NoError(t, os.WriteFile(filepath.Join(path, "cgroup.procs"), []byte(strings.Join(listed, "\n")), 0o600))
}

func TestForwarder_OffKillsTheSocatCgroup(t *testing.T) {
	t.Parallel()

	socat, groupMate := startGroup(t)
	handler := startFakeSocat(t)
	reused, _ := startGroup(t)
	f := newHandoverTestForwarder()
	withSocatCgroup(t, f, []int{socat, handler.Process.Pid}, []int{reused})

	require.NoError(t, f.SetLocalhostForwarding(false))

	assert.False(t, f.sweepPending.Load())
	assert.True(t, waitExited([]int{socat, handler.Process.Pid}, time.Now().Add(5*time.Second)),
		"every process in the socat cgroup is killed, tracked or not")
	assert.False(t, waitExited([]int{groupMate}, time.Now().Add(300*time.Millisecond)),
		"a process in a killed socat's group but outside the cgroup is left alone")
	assert.False(t, waitExited([]int{reused}, time.Now().Add(300*time.Millisecond)),
		"a listed pid now held by a process outside the cgroup is left alone")
}

func TestForwarder_OffRetriesWhileASocatLingers(t *testing.T) {
	t.Parallel()

	lingering := exec.CommandContext(t.Context(), "sleep", "30")
	require.NoError(t, lingering.Start())
	t.Cleanup(func() {
		_ = lingering.Process.Kill()
		_ = lingering.Wait()
	})
	f := newHandoverTestForwarder()
	withSocatCgroup(t, f, []int{lingering.Process.Pid}, nil)

	require.ErrorIs(t, f.SetLocalhostForwarding(false), errSocatSurvived)
	assert.True(t, f.LocalhostForwardingOff())
	assert.True(t, f.sweepPending.Load(), "a killed socat not reaped yet is still in the cgroup, so the scan loop kills again")
}

func TestForwarder_MaintenanceKillsWhatAnOffLeft(t *testing.T) {
	t.Parallel()

	left := startFakeSocat(t)
	f := newHandoverTestForwarder()
	withSocatCgroup(t, f, []int{left.Process.Pid}, nil)
	f.scannerSubscriber = NewScannerSubscriber("test", &ScannerFilter{})
	f.off.Store(true)
	f.sweepPending.Store(true)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		f.StartForwarding(ctx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	assert.Eventually(t, func() bool { return !f.sweepPending.Load() }, 5*time.Second, 50*time.Millisecond)
	assert.True(t, waitExited([]int{left.Process.Pid}, time.Now().Add(5*time.Second)),
		"the retry kills what the off left in the socat cgroup")
}

func TestForwarder_OffWithAnUnreadableSocatCgroup(t *testing.T) {
	t.Parallel()

	f := newHandoverTestForwarder()
	f.freezer = cgroups.NewWorkloadFreezer(&socatCgroupManager{root: t.TempDir()})

	require.Error(t, f.SetLocalhostForwarding(false))
	assert.True(t, f.LocalhostForwardingOff())
	assert.True(t, f.sweepPending.Load(), "a socat may still run in a cgroup that could not be read")
}
