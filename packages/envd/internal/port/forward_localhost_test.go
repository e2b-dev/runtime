//go:build unix

package port

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

	"github.com/shirou/gopsutil/v4/net"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/envd/internal/services/cgroups"
)

// startFakeSocat starts a child in its own process group and reaps it, the way the
// forwarder starts a socat.
func startFakeSocat(t *testing.T) *exec.Cmd {
	t.Helper()

	cmd := exec.CommandContext(t.Context(), "sleep", "30")
	cmd.SysProcAttr = socatProcAttr()
	require.NoError(t, cmd.Start())
	t.Cleanup(func() { _ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) })
	go func() { _ = cmd.Wait() }()

	return cmd
}

func TestForwarder_SetLocalhostForwardingOff_StopsEveryForward(t *testing.T) {
	t.Parallel()

	f := newHandoverTestForwarder()
	first, second := startFakeSocat(t), startFakeSocat(t)
	f.ports["100-8080"] = &PortToForward{pid: 100, port: 8080, family: 4, socat: first, state: PortStateForward}
	f.ports["101-9090"] = &PortToForward{pid: 101, port: 9090, family: 4, socat: second, state: PortStateForward}

	require.NoError(t, f.SetLocalhostForwarding(false))

	assert.Empty(t, f.ports)
	assert.True(t, f.LocalhostForwardingOff())
	assert.False(t, processAlive(first.Process.Pid), "a socat outlived turning forwarding off")
	assert.False(t, processAlive(second.Process.Pid), "a socat outlived turning forwarding off")
}

// A re-adopted socat is known by its pid alone. One that exited may have handed the pid to
// another process, which turning forwarding off must leave alone.
func TestForwarder_SetLocalhostForwardingOff_LeavesAReusedPid(t *testing.T) {
	t.Parallel()

	f := newHandoverTestForwarder()
	other := startFakeSocat(t)
	f.ports["100-8080"] = &PortToForward{pid: 100, port: 8080, family: 4, socatPid: other.Process.Pid, state: PortStateForward}

	require.NoError(t, f.SetLocalhostForwarding(false))

	assert.Empty(t, f.ports)
	assert.False(t, waitExited([]int{other.Process.Pid}, time.Now().Add(300*time.Millisecond)),
		"the process now holding the pid is not a forwarder")
}

func TestForwarder_SetLocalhostForwardingOn_ClearsTheSwitch(t *testing.T) {
	t.Parallel()

	f := newHandoverTestForwarder()
	require.NoError(t, f.SetLocalhostForwarding(false))
	require.NoError(t, f.SetLocalhostForwarding(true))

	assert.False(t, f.LocalhostForwardingOff())
}

// TestForwarder_ScanWhileOff_ForwardsNothing feeds the scan loop a localhost
// listener while forwarding is off. The subscriber channel is unbuffered, so the
// second send returns only after the loop has finished with the first.
func TestForwarder_ScanWhileOff_ForwardsNothing(t *testing.T) {
	t.Parallel()

	f := newHandoverTestForwarder()
	f.scannerSubscriber = NewScannerSubscriber("test", &ScannerFilter{})
	f.freezer = cgroups.NewWorkloadFreezer(newSocatCgroupManager(t))
	require.NoError(t, f.SetLocalhostForwarding(false))

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

	listener := []net.ConnectionStat{{
		Pid:    100,
		Family: syscall.AF_INET,
		Status: "LISTEN",
		Laddr:  net.Addr{IP: "127.0.0.1", Port: 8080},
	}}
	f.scannerSubscriber.Messages <- listener
	f.scannerSubscriber.Messages <- listener

	f.mu.Lock()
	defer f.mu.Unlock()
	assert.Empty(t, f.ports, "no port may be forwarded while forwarding is off")
}

// The setting survives an envd that restarts in the same boot, and a freshly booted
// guest forwards nothing until the host's first /init.
func TestForwarder_StateSurvivesARestart(t *testing.T) {
	t.Parallel()

	state := filepath.Join(t.TempDir(), "e2b", "localhost-forwarding")
	boot := func() *Forwarder {
		f := newHandoverTestForwarder()
		f.stateFile = state
		f.RestoreLocalhostForwarding(nil)

		return f
	}

	fresh := boot()
	assert.True(t, fresh.held.Load(), "a fresh boot holds forwarding until /init")
	fresh.Release()
	assert.False(t, fresh.held.Load())
	assert.FileExists(t, state)
	require.NoError(t, fresh.SetLocalhostForwarding(false))

	restarted := boot()
	assert.False(t, restarted.held.Load(), "a restart in the same boot is not held")
	assert.True(t, restarted.LocalhostForwardingOff(), "a restarted envd keeps the host's off")
	require.NoError(t, restarted.SetLocalhostForwarding(true))

	again := boot()
	assert.False(t, again.held.Load())
	assert.False(t, again.LocalhostForwardingOff())

	require.NoError(t, os.WriteFile(state, nil, 0o600))
	assert.True(t, boot().held.Load(), "an unreadable record holds until /init rather than forward")
}

// A live upgrade carries the running envd's setting, which is newer than the record.
func TestForwarder_CarriedSettingWinsOverTheRecord(t *testing.T) {
	t.Parallel()

	upgrade := func(state string, off bool) *Forwarder {
		f := newHandoverTestForwarder()
		f.stateFile = state
		f.RestoreLocalhostForwarding(&off)

		return f
	}
	stale := func(t *testing.T, value string) string {
		t.Helper()

		state := filepath.Join(t.TempDir(), "localhost-forwarding")
		require.NoError(t, os.WriteFile(state, []byte(value), 0o600))

		return state
	}

	t.Run("carried on replaces a stale off", func(t *testing.T) {
		t.Parallel()

		state := stale(t, "off")
		f := upgrade(state, false)
		assert.False(t, f.LocalhostForwardingOff())
		recorded, err := os.ReadFile(state)
		require.NoError(t, err)
		assert.Equal(t, "on", string(recorded), "a later restart keeps on too")
	})

	t.Run("carried off replaces a stale on", func(t *testing.T) {
		t.Parallel()

		f := upgrade(stale(t, "on"), true)
		assert.True(t, f.LocalhostForwardingOff())
	})

	t.Run("an upgrade from an envd that wrote no record keeps forwarding", func(t *testing.T) {
		t.Parallel()

		state := filepath.Join(t.TempDir(), "localhost-forwarding")
		f := upgrade(state, false)
		assert.False(t, f.held.Load(), "no /init may follow the upgrade to release a hold")
		assert.False(t, f.LocalhostForwardingOff())
		assert.FileExists(t, state)
	})
}

func TestForwarder_RepeatedOff(t *testing.T) {
	t.Parallel()

	blocker := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(blocker, nil, 0o600))

	t.Run("a finished off is not redone", func(t *testing.T) {
		t.Parallel()

		f := newHandoverTestForwarder()
		f.off.Store(true)
		f.stateFile = filepath.Join(blocker, "localhost-forwarding")

		require.NoError(t, f.SetLocalhostForwarding(false), "no record write and no /proc scan on a repeated off")
	})

	t.Run("an unfinished off is retried", func(t *testing.T) {
		t.Parallel()

		f := newHandoverTestForwarder()
		f.off.Store(true)
		f.sweepPending.Store(true)

		require.NoError(t, f.SetLocalhostForwarding(false))
		assert.False(t, f.sweepPending.Load())
	})
	t.Run("an unrecorded off is recorded", func(t *testing.T) {
		t.Parallel()

		state := filepath.Join(t.TempDir(), "localhost-forwarding")
		f := newHandoverTestForwarder()
		f.off.Store(true)
		f.unrecorded.Store(true)
		f.stateFile = state

		require.NoError(t, f.SetLocalhostForwarding(false))
		recorded, err := os.ReadFile(state)
		require.NoError(t, err)
		assert.Equal(t, "off", string(recorded))
	})
}

func TestForwarder_OffThatCannotBeRecordedFails(t *testing.T) {
	t.Parallel()

	blocker := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(blocker, nil, 0o600))
	f := newHandoverTestForwarder()
	f.stateFile = filepath.Join(blocker, "localhost-forwarding")

	require.Error(t, f.SetLocalhostForwarding(false))
	assert.True(t, f.LocalhostForwardingOff(), "off is in force even when it cannot be recorded")
	assert.True(t, f.unrecorded.Load(), "the scan loop retries the record")

	f.stateFile = filepath.Join(t.TempDir(), "localhost-forwarding")
	require.NoError(t, f.SetLocalhostForwarding(false), "a repeated off repairs the record")
	assert.False(t, f.unrecorded.Load())
}

func TestForwarder_ScanWhileHeld_ForwardsNothing(t *testing.T) {
	t.Parallel()

	f := newHandoverTestForwarder()
	f.scannerSubscriber = NewScannerSubscriber("test", &ScannerFilter{})
	f.freezer = cgroups.NewWorkloadFreezer(newSocatCgroupManager(t))
	f.held.Store(true)

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

	listener := []net.ConnectionStat{{
		Pid:    100,
		Family: syscall.AF_INET,
		Status: "LISTEN",
		Laddr:  net.Addr{IP: "127.0.0.1", Port: 8080},
	}}
	f.scannerSubscriber.Messages <- listener
	f.scannerSubscriber.Messages <- listener

	f.mu.Lock()
	defer f.mu.Unlock()
	assert.Empty(t, f.ports, "a freshly booted guest forwards nothing before /init")
}

func TestIsLoopbackForwarder(t *testing.T) {
	t.Parallel()

	const bind = "169.254.0.21"
	tests := map[string]struct {
		cmdline string
		want    bool
	}{
		"envd's socat":                 {"socat\x00-d\x00TCP4-LISTEN:8080,bind=169.254.0.21,reuseaddr,fork\x00TCP4:localhost:8080\x00", true},
		"by full path, to IPv6":        {"/usr/bin/socat\x00TCP4-LISTEN:9090,bind=169.254.0.21\x00TCP6:localhost:9090\x00", true},
		"another bind address":         {"socat\x00TCP4-LISTEN:7070,bind=10.0.0.1,fork\x00TCP4:localhost:7070\x00", false},
		"not socat":                    {"python3\x00TCP4-LISTEN:1,bind=169.254.0.21,\x00TCP4:localhost:1\x00", false},
		"a longer bind address":        {"socat\x00TCP4-LISTEN:6060,bind=169.254.0.211,fork\x00TCP4:localhost:6060\x00", false},
		"relaying elsewhere":           {"socat\x00TCP4-LISTEN:8080,bind=169.254.0.21,reuseaddr,fork\x00TCP4:remote-host:443\x00", false},
		"to another port on localhost": {"socat\x00TCP4-LISTEN:5050,bind=169.254.0.21,fork\x00TCP4:localhost:5051\x00", false},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			args := strings.Split(strings.TrimRight(test.cmdline, "\x00"), "\x00")
			assert.Equal(t, test.want, isLoopbackForwarder(args, bind))
		})
	}
}

// startGroup starts a process leading its own process group and a second one in that
// group. Both are the test's children and reaped by it, so a killed one never lingers
// as a zombie whatever the container's init does.
func startGroup(t *testing.T) (leader, member int) {
	t.Helper()

	start := func(attr *syscall.SysProcAttr) int {
		cmd := exec.CommandContext(t.Context(), "sleep", "30")
		cmd.SysProcAttr = attr
		require.NoError(t, cmd.Start())
		t.Cleanup(func() { _ = cmd.Process.Kill() })
		go func() { _ = cmd.Wait() }()

		return cmd.Process.Pid
	}
	leader = start(&syscall.SysProcAttr{Setpgid: true})

	return leader, start(&syscall.SysProcAttr{Setpgid: true, Pgid: leader})
}

func TestKillForwarder(t *testing.T) {
	t.Parallel()

	const bind = "192.0.2.1"
	// killForwarder reads a pid's command line again once the pid is pinned, so procs fakes
	// the /proc entries of the pids that still forward.
	procs := func(t *testing.T, forwarders ...int) string {
		t.Helper()

		root := t.TempDir()
		for _, pid := range forwarders {
			dir := filepath.Join(root, strconv.Itoa(pid))
			require.NoError(t, os.MkdirAll(dir, 0o755))
			cmdline := "socat\x00TCP4-LISTEN:7,bind=" + bind + ",fork\x00TCP4:localhost:7\x00"
			require.NoError(t, os.WriteFile(filepath.Join(dir, "cmdline"), []byte(cmdline), 0o600))
		}

		return root
	}

	t.Run("a forwarder goes alone, not with its group", func(t *testing.T) {
		t.Parallel()

		leader, member := startGroup(t)
		killed, err := killForwarder(procs(t, leader), bind, leader)
		require.NoError(t, err)
		assert.True(t, killed)
		assert.True(t, waitExited([]int{leader}, time.Now().Add(5*time.Second)))
		assert.False(t, waitExited([]int{member}, time.Now().Add(300*time.Millisecond)),
			"a group a user's command leads keeps its other processes")
	})

	t.Run("a process that no longer forwards is left alone", func(t *testing.T) {
		t.Parallel()

		leader, _ := startGroup(t)
		killed, err := killForwarder(procs(t), bind, leader)
		require.NoError(t, err)
		assert.False(t, killed, "its pid was reused since the scan")
		assert.False(t, waitExited([]int{leader}, time.Now().Add(300*time.Millisecond)))
	})
}

func TestForwarder_MaintenanceRunsWithoutScans(t *testing.T) {
	t.Parallel()

	f := newHandoverTestForwarder()
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

	assert.Eventually(t, func() bool { return !f.sweepPending.Load() }, 5*time.Second, 50*time.Millisecond,
		"no scan arrives while the port table cannot be read, and the sweep is still retried")
}

func TestForwarder_ReleaseRepairsAnUnrecordedOff(t *testing.T) {
	t.Parallel()

	blocker := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(blocker, nil, 0o600))
	f := newHandoverTestForwarder()
	f.held.Store(true)
	f.stateFile = filepath.Join(blocker, "localhost-forwarding")

	require.Error(t, f.SetLocalhostForwarding(false))
	require.True(t, f.unrecorded.Load())

	f.stateFile = filepath.Join(t.TempDir(), "localhost-forwarding")
	f.Release()
	assert.False(t, f.unrecorded.Load(), "the record is kept now")
}

func TestForwarder_ScanRetriesAFailedRecord(t *testing.T) {
	t.Parallel()

	state := filepath.Join(t.TempDir(), "localhost-forwarding")
	f := newHandoverTestForwarder()
	f.scannerSubscriber = NewScannerSubscriber("test", &ScannerFilter{})
	f.freezer = cgroups.NewWorkloadFreezer(newSocatCgroupManager(t))
	f.stateFile = state
	require.NoError(t, f.SetLocalhostForwarding(false))

	blocker := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(blocker, nil, 0o600))
	f.stateFile = filepath.Join(blocker, "localhost-forwarding")
	require.NoError(t, f.SetLocalhostForwarding(true))
	f.stateFile = state
	recorded, err := os.ReadFile(state)
	require.NoError(t, err)
	require.Equal(t, "off", string(recorded), "the on was not recorded")

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
	f.scannerSubscriber.Messages <- nil
	f.scannerSubscriber.Messages <- nil

	f.mu.Lock()
	defer f.mu.Unlock()
	recorded, err = os.ReadFile(state)
	require.NoError(t, err)
	assert.Equal(t, "on", string(recorded), "a restart must not bring back the stale off")
}

func TestForwarder_ScanRetriesAnIncompleteSweep(t *testing.T) {
	t.Parallel()

	f := newHandoverTestForwarder()
	f.scannerSubscriber = NewScannerSubscriber("test", &ScannerFilter{})
	f.freezer = cgroups.NewWorkloadFreezer(newSocatCgroupManager(t))
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
	f.scannerSubscriber.Messages <- nil
	f.scannerSubscriber.Messages <- nil

	assert.False(t, f.sweepPending.Load(), "a sweep that finds nothing left ends the retries")
}
