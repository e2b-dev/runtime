package cpus

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/envd/internal/reaper"
	"github.com/e2b-dev/infra/packages/envd/internal/reexec"
)

// The test binary doubles as the hotplug writer child, as envd does.
func init() { reexec.Main() }

func TestKernelSysfs(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "possible"), []byte("0-3\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(root, "online"), []byte("0-1\n"), 0o644))
	require.NoError(t, os.MkdirAll(filepath.Join(root, "cpu2"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(root, "cpu2", "online"), []byte("0\n"), 0o644))
	require.NoError(t, os.MkdirAll(filepath.Join(root, "cpu3"), 0o755))
	k := kernelCPUSysfs{dir: root, children: &reaper.Registry{}}

	possible, err := k.Possible()
	require.NoError(t, err)
	assert.Equal(t, []int{0, 1, 2, 3}, possible)
	online, err := k.Online()
	require.NoError(t, err)
	assert.Equal(t, []int{0, 1}, online)

	for _, online := range []bool{true, false} {
		require.NoError(t, k.SetOnline(2, online))
		b, err := os.ReadFile(filepath.Join(root, "cpu2", "online"))
		require.NoError(t, err)
		assert.Equal(t, map[bool]string{true: "1", false: "0"}[online], string(b))
	}

	require.ErrorContains(t, k.SetOnline(3, true), "cpu3/online: no such file", "an attribute the kernel did not create is never made up")
	_, err = os.Stat(filepath.Join(root, "cpu3", "online"))
	require.ErrorIs(t, err, os.ErrNotExist)
	pids, release := k.children.ExportHold()
	release()
	assert.Empty(t, pids, "a finished write leaves nothing to hand over")
}

// Every rejection names its check: a bad path must fail the path check, not merely the
// open that would follow it.
func TestWriteOnlineFileRejectsBadArgs(t *testing.T) {
	t.Parallel()

	const usage, notCPU = "usage:", "is not a cpuN/online file"
	for name, tc := range map[string]struct {
		args []string
		want string
	}{
		"missing value": {[]string{"/sys/devices/system/cpu/cpu2/online"}, usage},
		"not a bit":     {[]string{"/sys/devices/system/cpu/cpu2/online", "2"}, usage},
		"not a cpu dir": {[]string{"/sys/devices/system/cpu/cpuX/online", "1"}, notCPU},
		"negative cpu":  {[]string{"/sys/devices/system/cpu/cpu-2/online", "1"}, notCPU},
		"huge cpu":      {[]string{"/sys/devices/system/cpu/cpu8192/online", "1"}, notCPU},
		"not online":    {[]string{"/sys/devices/system/cpu/cpu2/uevent", "1"}, notCPU},
		"other file":    {[]string{"/etc/passwd", "1"}, notCPU},
		"traversal":     {[]string{"/sys/devices/system/cpu/cpu2/online/../../../../../etc/passwd", "1"}, notCPU},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			require.ErrorContains(t, writeOnlineFile(tc.args), tc.want)
		})
	}
}

func TestKernelSysfsRejectsOversizedMask(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "online"), bytes.Repeat([]byte("0"), maxMaskBytes+1), 0o644))

	_, err := kernelCPUSysfs{dir: root}.Online()
	require.ErrorContains(t, err, "longer than")
}

func TestParseMask(t *testing.T) {
	t.Parallel()

	for in, want := range map[string][]int{
		"":          nil,
		"0":         {0},
		"0-3":       {0, 1, 2, 3},
		"0-1,4,6-7": {0, 1, 4, 6, 7},
	} {
		got, err := parseMask(in)
		require.NoError(t, err, in)
		assert.Equal(t, want, got, in)
	}

	for _, in := range []string{
		"0-x", "0,,2", "0,", ",0", "-", "-1", "+1", "0x1", "1e3", "0 ,1", "1-2-3", "\x00", "\uff11",
		"3-1", "1,0", "0-3,2", "0-3,3",
		"8192", "0-8192", "99999999999999999999", strings.Repeat("9", 1<<16),
	} {
		_, err := parseMask(in)
		require.Error(t, err, "%.32q", in)
	}

	cpus, err := parseMask("0-8191")
	require.NoError(t, err)
	assert.Len(t, cpus, maxCPUs)
}

func FuzzParseMask(f *testing.F) {
	for _, s := range []string{"", "0", "0-3", "0-1,4,6-7", "3-1", "0,,2", "+1", "0-8191", "1,0"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		cpus, err := parseMask(s)
		if err != nil {
			return
		}
		require.LessOrEqual(t, len(cpus), maxCPUs)
		for i, cpu := range cpus {
			require.True(t, cpu >= 0 && cpu < maxCPUs, cpu)
			if i > 0 {
				require.Greater(t, cpu, cpus[i-1])
			}
		}
	})
}
