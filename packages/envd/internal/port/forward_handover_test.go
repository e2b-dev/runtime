package port

import (
	"os/exec"
	"syscall"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/envd/internal/services/spec/upgrade"
)

func newHandoverTestForwarder() *Forwarder {
	l := zerolog.Nop()

	return &Forwarder{
		logger:   &l,
		ports:    make(map[string]*PortToForward),
		sourceIP: defaultGatewayIP,
	}
}

// TestForwarder_ExportForwards exports only ports that actually have a socat, so
// a half-set-up entry is not carried as if it were forwarded.
func TestForwarder_ExportForwards(t *testing.T) {
	t.Parallel()

	f := newHandoverTestForwarder()
	f.ports["100-8080"] = &PortToForward{pid: 100, port: 8080, family: 4, socatPid: 555, state: PortStateForward}
	f.ports["101-9090"] = &PortToForward{pid: 101, port: 9090, family: 4, state: PortStateForward} // no socat

	out := f.ExportForwards()
	require.Len(t, out, 1)
	assert.Equal(t, "100-8080", out[0].GetKey())
	assert.Equal(t, uint32(8080), out[0].GetPort())
	assert.Equal(t, int32(100), out[0].GetListenerPid())
	assert.Equal(t, uint32(4), out[0].GetFamily())
	assert.Equal(t, int32(555), out[0].GetSocatPid())
}

// TestForwarder_ImportForwards_SkipsDeadSocat leaves out a socat that did not
// survive the handover, so the next scan respawns a fresh one for the still-open
// port instead of recording a dead pid as "forwarded".
func TestForwarder_ImportForwards_SkipsDeadSocat(t *testing.T) {
	t.Parallel()

	f := newHandoverTestForwarder()
	// A very high pid that is overwhelmingly unlikely to exist: kill -0 fails.
	n := f.ImportForwards([]*upgrade.ForwardedPort{
		{Key: "1-1", Port: 1, ListenerPid: 1, Family: 4, SocatPid: 2147483646},
	})
	assert.Zero(t, n)
	assert.Empty(t, f.ports)
}

// TestForwarder_ImportForwards_ReadoptsLiveSocat re-adopts a still-running socat
// into the ports map (so the next scan won't spawn a duplicate) and records it as
// pid-only (no *exec.Cmd survives the execve). A real child process stands in for
// the socat.
func TestForwarder_ImportForwards_ReadoptsLiveSocat(t *testing.T) {
	t.Parallel()

	cmd := exec.CommandContext(t.Context(), "sleep", "30")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	require.NoError(t, cmd.Start())
	pid := cmd.Process.Pid
	// The re-adopt path starts a wait4 reaper for the pid; killing the group lets
	// it reap so the child does not linger.
	t.Cleanup(func() { _ = syscall.Kill(-pid, syscall.SIGKILL) })

	f := newHandoverTestForwarder()
	n := f.ImportForwards([]*upgrade.ForwardedPort{
		{Key: "100-8080", Port: 8080, ListenerPid: 100, Family: 4, SocatPid: int32(pid)},
	})
	require.Equal(t, 1, n)

	// ImportForwards normalizes an old single-dash key ("pid-port") to the new
	// "pid-port-ip" format (IP inferred from Family) so a live upgrade does not
	// spawn a duplicate socat alongside the scan loop's own entry.
	p, ok := f.ports["100-8080-127.0.0.1"]
	require.True(t, ok, "a live socat must be re-adopted into the ports map under the normalized key")
	assert.Equal(t, pid, p.socatPID())
	assert.Nil(t, p.socat, "a re-adopted socat has no *exec.Cmd")
}
