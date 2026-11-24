package port

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFakeSocat is the process startLookalike runs: the test binary under argv[0] socat,
// with the command line of a forwarding socat.
func TestFakeSocat(t *testing.T) {
	t.Parallel()

	if os.Getenv("ENVD_FAKE_SOCAT") != "1" {
		t.Skip("run by startLookalike")
	}
	time.Sleep(30 * time.Second)
}

// startLookalike starts a process with exactly the command line of envd's socat forwarding
// port onto bind, outside any cgroup envd manages: a user's own relay.
func startLookalike(t *testing.T, bind net.IP, port int) int {
	t.Helper()

	exe, err := os.Executable()
	require.NoError(t, err)
	cmd := exec.CommandContext(t.Context(), exe, "-test.run=^TestFakeSocat$")
	cmd.Args = []string{
		"socat", "-test.run=^TestFakeSocat$",
		fmt.Sprintf("TCP4-LISTEN:%d,bind=%s,reuseaddr,fork", port, bind),
		fmt.Sprintf("TCP4:localhost:%d", port),
	}
	cmd.Env = append(os.Environ(), "ENVD_FAKE_SOCAT=1")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	require.NoError(t, cmd.Start())
	t.Cleanup(func() { _ = cmd.Process.Kill() })
	go func() { _ = cmd.Wait() }()

	return cmd.Process.Pid
}

func TestForwarder_OffLeavesAUsersIdenticalSocat(t *testing.T) {
	t.Parallel()

	bind := net.IPv4(127, 3, 8, 84)
	users := startLookalike(t, bind, 4001)
	require.Eventually(t, func() bool { return isForwarderProcess(procRoot, bind.String(), users) },
		5*time.Second, 10*time.Millisecond, "its command line is envd's own")

	withCgroups := newHandoverTestForwarder()
	withCgroups.sourceIP = bind
	withSocatCgroup(t, withCgroups, nil, nil)
	require.NoError(t, withCgroups.SetLocalhostForwarding(false))

	withoutCgroups := newHandoverTestForwarder()
	withoutCgroups.sourceIP = bind
	require.NoError(t, withoutCgroups.SetLocalhostForwarding(false))

	assert.False(t, waitExited([]int{users}, time.Now().Add(300*time.Millisecond)),
		"off kills only what envd started, never a process outside the socat cgroup")
}
