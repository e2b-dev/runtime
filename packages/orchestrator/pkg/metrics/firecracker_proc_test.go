//go:build linux

package metrics

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func fakeProcessStat(pid int, state string, start uint64) []byte {
	fields := strings.Fields(strings.Repeat("0 ", 20))
	fields[0] = state
	fields[19] = strconv.FormatUint(start, 10)

	return fmt.Appendf(nil, "%d (name with ) parentheses) %s", pid, strings.Join(fields, " "))
}

func writeFakeProcess(t *testing.T, root string, pid int, exe, state, cmdline string) {
	t.Helper()
	dir := filepath.Join(root, strconv.Itoa(pid))
	require.NoError(t, os.Mkdir(dir, 0o700))
	require.NoError(t, os.Symlink(exe, filepath.Join(dir, "exe")))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "stat"), fakeProcessStat(pid, state, 900000), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "cmdline"), []byte(cmdline), 0o600))
}

func fakeProcReader(t *testing.T) firecrackerProcReader {
	t.Helper()
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "uptime"), []byte("10000.00 20000.00\n"), 0o600))

	return firecrackerProcReader{root: root, clockHz: 100, readFile: os.ReadFile}
}

func TestFirecrackerProcTrackerCountsProcessesNotWrappers(t *testing.T) {
	t.Parallel()
	r := fakeProcReader(t)
	writeFakeProcess(t, r.root, 1, "/opt/v1/firecracker", "S", "renamed\x00--api-sock\x00/tmp/fc-one.sock\x00")
	writeFakeProcess(t, r.root, 2, "/opt/v2/arm64/firecracker (deleted)", "D", "firecracker\x00--api-sock=/tmp/fc-two.sock\x00")
	writeFakeProcess(t, r.root, 3, "/bin/bash", "S", "bash\x00-c\x00firecracker --api-sock /tmp/wrapper.sock\x00")
	writeFakeProcess(t, r.root, 4, "/bin/firecracker-monitor", "S", "firecracker\x00")
	writeFakeProcess(t, r.root, 5, "/opt/firecracker", "Z", "firecracker\x00")
	writeFakeProcess(t, r.root, 6, "/opt/firecracker", "T", "firecracker\x00")
	processes, err := r.scan(t.Context())
	require.NoError(t, err)
	require.Len(t, processes, 3)
	require.Equal(t, "/tmp/fc-one.sock", processes[0].socket)
	require.Equal(t, "/tmp/fc-two.sock", processes[1].socket)
	require.Empty(t, processes[2].socket, "missing metadata cannot hide a confirmed Firecracker")
	require.Equal(t, 1000*time.Second, processes[0].age)
}

func TestFirecrackerProcTrackerSkipsPIDReuse(t *testing.T) {
	t.Parallel()
	r := fakeProcReader(t)
	writeFakeProcess(t, r.root, 10, "/opt/firecracker", "R", "firecracker\x00--api-sock\x00/tmp/fc.sock\x00")
	reads := 0
	r.readFile = func(path string) ([]byte, error) {
		if strings.HasSuffix(path, "/10/stat") {
			reads++

			return fakeProcessStat(10, "R", uint64(900000+reads)), nil
		}

		return os.ReadFile(path)
	}
	processes, err := r.scan(t.Context())
	require.NoError(t, err)
	require.Empty(t, processes)
}

func TestFirecrackerProcTrackerRechecksExecutable(t *testing.T) {
	t.Parallel()
	r := fakeProcReader(t)
	writeFakeProcess(t, r.root, 10, "/opt/firecracker", "R", "sleep\x00")
	r.readFile = func(path string) ([]byte, error) {
		if strings.HasSuffix(path, "cmdline") {
			exe := filepath.Join(r.root, "10", "exe")
			require.NoError(t, os.Remove(exe))
			require.NoError(t, os.Symlink("/bin/sleep", exe))
		}

		return os.ReadFile(path)
	}
	processes, err := r.scan(t.Context())
	require.NoError(t, err)
	require.Empty(t, processes)
}

func TestFirecrackerProcTrackerDistinguishesDisappearanceAndReadFailure(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		err       error
		wantError bool
	}{
		{name: "exited", err: os.ErrNotExist},
		{name: "exited-esrch", err: syscall.ESRCH},
		{name: "inaccessible", err: os.ErrPermission, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := fakeProcReader(t)
			writeFakeProcess(t, r.root, 10, "/opt/firecracker", "S", "firecracker\x00")
			r.readFile = func(path string) ([]byte, error) {
				if strings.HasSuffix(path, "cmdline") {
					return nil, tc.err
				}

				return os.ReadFile(path)
			}
			processes, err := r.scan(t.Context())
			if tc.wantError {
				require.ErrorIs(t, err, tc.err)
			} else {
				require.NoError(t, err)
			}
			require.Empty(t, processes)
		})
	}
}

func TestFirecrackerProcTrackerRejectsMalformedStat(t *testing.T) {
	t.Parallel()
	r := fakeProcReader(t)
	writeFakeProcess(t, r.root, 1, "/opt/firecracker", "S", "firecracker\x00")
	require.NoError(t, os.WriteFile(filepath.Join(r.root, "1", "stat"), []byte("broken"), 0o600))
	_, err := r.scan(t.Context())
	require.Error(t, err)
}
