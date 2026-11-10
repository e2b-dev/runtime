//go:build linux

package sandboxtools

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type settleRun struct {
	state   string
	calls   int
	elapsed time.Duration
}

// runSettle runs the script against a fake systemctl that prints the given
// states one per call, repeating the last. A nil states slice installs no
// systemctl at all.
func runSettle(t *testing.T, systemdBooted bool, states []string, limit time.Duration) settleRun {
	t.Helper()

	dir := t.TempDir()
	runtimeDir := filepath.Join(dir, "run-systemd-system")
	if systemdBooted {
		require.NoError(t, os.Mkdir(runtimeDir, 0o755))
	}
	// PATH holds only these, so a host systemctl can never stand in for the fake.
	bin := filepath.Join(dir, "bin")
	require.NoError(t, os.Mkdir(bin, 0o755))
	for _, tool := range []string{"sleep", "sed", "wc", "tail"} {
		path, err := exec.LookPath(tool)
		require.NoError(t, err)
		require.NoError(t, os.Symlink(path, filepath.Join(bin, tool)))
	}
	calls := filepath.Join(dir, "calls")
	if states != nil {
		seq := filepath.Join(dir, "states")
		require.NoError(t, os.WriteFile(seq, []byte(strings.Join(states, "\n")+"\n"), 0o644))
		fake := "#!/bin/sh\n" +
			"[ \"$*\" = is-system-running ] || exit 2\n" +
			"echo x >> " + calls + "\n" +
			"n=$(wc -l < " + calls + ")\n" +
			"s=$(sed -n \"${n}p\" " + seq + ")\n" +
			"[ -n \"$s\" ] || s=$(tail -n 1 " + seq + ")\n" +
			"echo \"$s\"\n" +
			"[ \"$s\" = running ]\n"
		require.NoError(t, os.WriteFile(filepath.Join(bin, "systemctl"), []byte(fake), 0o755))
	}

	cmd := exec.CommandContext(t.Context(), "/bin/sh", "-c", settleScript(runtimeDir, limit))
	cmd.Env = append(os.Environ(), "PATH="+bin)
	start := time.Now()
	out, err := cmd.Output()
	require.NoError(t, err, "the script must never fail the build")
	run := settleRun{state: strings.TrimSpace(string(out)), elapsed: time.Since(start)}
	if data, err := os.ReadFile(calls); err == nil {
		run.calls = strings.Count(string(data), "\n")
	}

	return run
}

func TestSettleScriptWaitsUntilSystemdHasBooted(t *testing.T) {
	t.Parallel()

	run := runSettle(t, true, []string{"starting", "starting", "running"}, 10*time.Second)
	assert.Equal(t, "running", run.state)
	assert.Equal(t, 3, run.calls)
	assert.GreaterOrEqual(t, run.elapsed, 2*time.Second)
	assert.False(t, StillBooting(run.state))
}

func TestSettleScriptAcceptsDegraded(t *testing.T) {
	t.Parallel()

	run := runSettle(t, true, []string{"degraded"}, 10*time.Second)
	assert.Equal(t, "degraded", run.state)
	assert.Equal(t, 1, run.calls)
	assert.False(t, StillBooting(run.state))
}

func TestSettleScriptReportsAGuestThatNeverSettles(t *testing.T) {
	t.Parallel()

	run := runSettle(t, true, []string{"initializing", "starting"}, 2*time.Second)
	assert.Equal(t, "starting", run.state)
	assert.Equal(t, 3, run.calls, "one initial check and one per second of the limit")
	assert.Less(t, run.elapsed, 5*time.Second)
	assert.True(t, StillBooting(run.state))
}

func TestSettleScriptSkipsGuestsWithoutSystemd(t *testing.T) {
	t.Parallel()

	run := runSettle(t, false, []string{"running"}, 10*time.Second)
	assert.Empty(t, run.state)
	assert.Zero(t, run.calls)
}

func TestSettleScriptSkipsGuestsWithoutSystemctl(t *testing.T) {
	t.Parallel()

	run := runSettle(t, true, nil, 10*time.Second)
	assert.Empty(t, run.state)
}
