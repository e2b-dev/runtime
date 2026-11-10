//go:build linux

package rootfs

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/core/fstab"
)

func TestClassifyFstabStat(t *testing.T) {
	t.Parallel()

	const banner = "debugfs 1.47.0 (5-Feb-2023)\n"
	regular := "Inode: 12   Type: regular    Mode:  0644   Flags: 0x80000\n" +
		"Generation: 1    Version: 0x00000000:00000001\n" +
		"User:     0   Group:     0   Project:     0   Size: 38\n" +
		"Size of extra inode fields: 32\n"

	tests := []struct {
		name        string
		stdout      string
		stderr      string
		runErr      error
		wantSize    int64
		wantReport  fstab.Report
		wantRegular bool
	}{
		{name: "regular with size", stdout: regular, stderr: banner, wantSize: 38, wantRegular: true},
		{
			name:       "regular over the cap",
			stdout:     "Inode: 12   Type: regular    Mode:  0644\nUser:     0   Group:     0   Size: 65537\n",
			wantReport: unknownFstab(fstab.ReasonTooLarge),
		},
		{
			name:       "regular without size",
			stdout:     "Inode: 12   Type: regular    Mode:  0644\n",
			wantReport: unknownFstab(fstab.ReasonMalformedStat),
		},
		{
			name:       "symlink",
			stdout:     "Inode: 13   Type: symlink    Mode:  0777\nUser:     0   Group:     0   Size: 14\n",
			wantReport: unknownFstab(fstab.ReasonNotRegular),
		},
		{
			name:       "directory",
			stdout:     "Inode: 14   Type: directory    Mode:  0755\nUser:     0   Group:     0   Size: 4096\n",
			wantReport: unknownFstab(fstab.ReasonNotRegular),
		},
		{
			name:       "not found",
			stderr:     banner + "/etc/fstab: File not found by ext2_lookup \n",
			wantReport: fstab.Report{Outcome: fstab.OutcomeAbsent},
		},
		{
			name:       "garbage",
			stdout:     "something else entirely\n",
			stderr:     banner,
			wantReport: unknownFstab(fstab.ReasonMalformedStat),
		},
		{name: "empty", wantReport: unknownFstab(fstab.ReasonMalformedStat)},
		{
			name:       "debugfs failed",
			stderr:     banner + "debugfs: Bad magic number in super-block while trying to open img\n",
			runErr:     errors.New("exit status 1"),
			wantReport: unknownFstab(fstab.ReasonExecError),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			size, report, regular := classifyFstabStat([]byte(tt.stdout), []byte(tt.stderr), tt.runErr)
			assert.Equal(t, tt.wantSize, size)
			assert.Equal(t, tt.wantReport, report)
			assert.Equal(t, tt.wantRegular, regular)
		})
	}
}

func TestInspectFstabOnImage(t *testing.T) {
	t.Parallel()

	for _, tool := range []string{"mkfs.ext4", "debugfs"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("requires %s: %v", tool, err)
		}
	}

	const rootUUID = "0f0f0f0f-0f0f-0f0f-0f0f-0f0f0f0f0f0f"

	tests := []struct {
		name  string
		mkfs  []string
		fstab func(t *testing.T, etc string)
		want  fstab.Report
	}{
		{
			name:  "second mount of the root filesystem",
			mkfs:  []string{"-U", rootUUID, "-L", "rootfs"},
			fstab: writeFstab([]byte("UUID=" + rootUUID + " /mnt/root ext4 defaults 0 0\nLABEL=rootfs /mnt/label ext4 defaults 0 0\n")),
			want:  fstab.Report{Outcome: fstab.OutcomeOK, Entries: 2},
		},
		{
			name:  "uuid of another filesystem",
			mkfs:  []string{"-U", rootUUID},
			fstab: writeFstab([]byte("UUID=1f1f1f1f-1f1f-1f1f-1f1f-1f1f1f1f1f1f /mnt/data ext4 defaults 0 0\n")),
			want: fstab.Report{
				Outcome: fstab.OutcomeOK, Entries: 1, Blockers: 1,
				BySource: map[fstab.SourceClass]int{fstab.SourceUUID: 1},
			},
		},
		{
			name:  "required missing device",
			fstab: writeFstab([]byte("# static\n/dev/sdz1 /mnt/data ext4 defaults 0 0\n")),
			want: fstab.Report{
				Outcome: fstab.OutcomeOK, Entries: 1, Blockers: 1,
				BySource: map[fstab.SourceClass]int{fstab.SourceDev: 1},
			},
		},
		{
			name:  "no fstab",
			fstab: func(*testing.T, string) {},
			want:  fstab.Report{Outcome: fstab.OutcomeAbsent},
		},
		{
			name: "symlink",
			fstab: func(t *testing.T, etc string) {
				t.Helper()
				require.NoError(t, os.WriteFile(filepath.Join(etc, "fstab.real"), []byte("/dev/sdz1 /mnt/data ext4 defaults 0 0\n"), 0o644))
				require.NoError(t, os.Symlink("fstab.real", filepath.Join(etc, "fstab")))
			},
			want: unknownFstab(fstab.ReasonNotRegular),
		},
		{
			name:  "oversize",
			fstab: writeFstab(bytes.Repeat([]byte("# padding\n"), 10<<10)),
			want:  unknownFstab(fstab.ReasonTooLarge),
		},
		{
			name:  "nul byte",
			fstab: writeFstab([]byte("/dev/sdz1 /mnt/data ext4\x00 defaults 0 0\n")),
			want:  unknownFstab(fstab.ReasonNotText),
		},
		{
			name:  "empty",
			fstab: writeFstab(nil),
			want:  fstab.Report{Outcome: fstab.OutcomeOK},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			root := t.TempDir()
			etc := filepath.Join(root, "etc")
			require.NoError(t, os.Mkdir(etc, 0o755))
			tt.fstab(t, etc)

			img := filepath.Join(t.TempDir(), "rootfs.ext4")
			args := append(append([]string{"-q"}, tt.mkfs...), "-d", root, img, "16M")
			out, err := exec.CommandContext(t.Context(), "mkfs.ext4", args...).CombinedOutput()
			require.NoError(t, err, "mkfs.ext4: %s", out)

			start := time.Now()
			got, err := InspectFstab(t.Context(), img)
			t.Logf("inspection took %s", time.Since(start))

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// inspect runs InspectFstab and requires that it did not report a live inspector.
func inspect(t *testing.T, rootfsPath string) fstab.Report {
	t.Helper()

	report, err := InspectFstab(t.Context(), rootfsPath)
	require.NoError(t, err)

	return report
}

func TestInspectFstabWithoutTools(t *testing.T) {
	t.Setenv("PATH", t.TempDir())

	assert.Equal(t, unknownFstab(fstab.ReasonToolMissing), inspect(t, regularFile(t)), "file without debugfs")
	assert.Equal(t, unknownFstab(fstab.ReasonToolMissing), inspect(t, "/dev/nbd4095"), "device without systemd-run")
}

// debugfsOnPath puts a debugfs script on PATH whose body sees the -R request as $3.
func debugfsOnPath(t *testing.T, script string) {
	t.Helper()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "debugfs"), []byte("#!/bin/sh\n"+script+"\n"), 0o755))
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
}

func regularFile(t *testing.T) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "rootfs.ext4")
	require.NoError(t, os.WriteFile(path, nil, 0o644))

	return path
}

func TestInspectFstabCallTimeout(t *testing.T) { //nolint:paralleltest // t.Setenv cannot be used with t.Parallel.
	debugfsOnPath(t, "exec sleep 30")

	assert.Equal(t, unknownFstab(fstab.ReasonTimeout), inspect(t, regularFile(t)))
}

func TestInspectFstabCatShorterThanStat(t *testing.T) { //nolint:paralleltest // t.Setenv cannot be used with t.Parallel.
	debugfsOnPath(t, `case "$3" in
"stat /etc/fstab") printf 'Inode: 12   Type: regular    Mode:  0644\nUser:     0   Group:     0   Size: 38\n' ;;
"cat /etc/fstab") printf '/dev/sdz1\n' ;;
esac`)

	assert.Equal(t, unknownFstab(fstab.ReasonExecError), inspect(t, regularFile(t)))
}

func TestInspectFstabRefusesOtherDevices(t *testing.T) { //nolint:paralleltest // t.Setenv cannot be used with t.Parallel.
	debugfsOnPath(t, `[ "$3" = "stat /etc/fstab" ] && printf 'Inode: 12   Type: regular    Mode:  0644\nSize: 0\n'; exit 0`)

	assert.Equal(t, unknownFstab(fstab.ReasonExecError), inspect(t, "/dev/null"))
	assert.Equal(t, fstab.Report{Outcome: fstab.OutcomeOK}, inspect(t, regularFile(t)))
}

func writeFstab(data []byte) func(t *testing.T, etc string) {
	return func(t *testing.T, etc string) {
		t.Helper()
		require.NoError(t, os.WriteFile(filepath.Join(etc, "fstab"), data, 0o644))
	}
}

func TestInspectFstabJailedCallReturnsWhileUnitHoldsPipes(t *testing.T) {
	if _, err := os.Stat(debugfsPath); err != nil {
		t.Skipf("requires %s: %v", debugfsPath, err)
	}

	// The backgrounded sleep stands in for a jailed debugfs that outlives the
	// killed systemd-run while holding its output pipes.
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "systemd-run"), []byte("#!/bin/sh\nsleep 30 &\nexec sleep 30\n"), 0o755))
	stopArgs := filepath.Join(dir, "systemctl.args")
	require.NoError(t, os.WriteFile(filepath.Join(dir, "systemctl"), []byte("#!/bin/sh\necho \"$@\" >> "+stopArgs+"\n"), 0o755))
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))

	var checked []string
	stubUnitExited(t, func(unit string) bool {
		checked = append(checked, unit)

		return true
	})

	start := time.Now()
	got, err := InspectFstab(t.Context(), "/dev/nbd4095")

	require.NoError(t, err)
	assert.Equal(t, unknownFstab(fstab.ReasonTimeout), got)
	assert.Less(t, time.Since(start), 10*time.Second)

	// The unit must be stopped synchronously, then confirmed empty.
	args, err := os.ReadFile(stopArgs)
	require.NoError(t, err)
	assert.Contains(t, string(args), "stop")
	assert.NotContains(t, string(args), "--no-block")
	require.Len(t, checked, 1)
	assert.Contains(t, string(args), checked[0]+".service")
}

// stubUnitExited replaces the systemd check for the length of the test.
func stubUnitExited(t *testing.T, exited func(unit string) bool) {
	t.Helper()

	prevExited, prevTimeout, prevPoll := jailUnitExited, jailExitTimeout, jailExitPoll
	jailUnitExited, jailExitTimeout, jailExitPoll = exited, 300*time.Millisecond, 10*time.Millisecond
	t.Cleanup(func() { jailUnitExited, jailExitTimeout, jailExitPoll = prevExited, prevTimeout, prevPoll })
}

// hungJailOnPath puts a systemd-run on PATH that never answers, standing in
// for a jailed debugfs blocked on a stalled NBD read.
func hungJailOnPath(t *testing.T) {
	t.Helper()

	if _, err := os.Stat(debugfsPath); err != nil {
		t.Skipf("requires %s: %v", debugfsPath, err)
	}

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "systemd-run"), []byte("#!/bin/sh\nexec sleep 30\n"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "systemctl"), []byte("#!/bin/sh\nexit 0\n"), 0o755))
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
}

func TestInspectFstabWaitsForJailedProcessesToExit(t *testing.T) { //nolint:paralleltest // t.Setenv cannot be used with t.Parallel.
	hungJailOnPath(t)

	// The processes exit on the third check, after the stop.
	checks := 0
	stubUnitExited(t, func(string) bool {
		checks++

		return checks >= 3
	})

	got, err := InspectFstab(t.Context(), "/dev/nbd4095")

	require.NoError(t, err)
	assert.Equal(t, unknownFstab(fstab.ReasonTimeout), got)
	assert.Equal(t, 3, checks)
}

func TestInspectFstabReportsInspectorThatOutlivesItsStop(t *testing.T) { //nolint:paralleltest // t.Setenv cannot be used with t.Parallel.
	hungJailOnPath(t)
	stubUnitExited(t, func(string) bool { return false })

	// A canceled build still waits, since the device stays attached until return.
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	got, err := InspectFstab(ctx, "/dev/nbd4095")

	require.ErrorIs(t, err, ErrInspectorRunning)
	assert.Equal(t, unknownFstab(fstab.ReasonTimeout), got)
}

func TestUnitExitedWithoutCgroup(t *testing.T) {
	dir := t.TempDir()
	unit := "fstab-inspect-test-" + filepath.Base(dir)
	t.Setenv("PATH", dir)

	require.NoError(t, os.WriteFile(filepath.Join(dir, "systemctl"), []byte("#!/bin/sh\nexit 1\n"), 0o755))
	assert.False(t, unitExited(unit), "systemd unreachable")

	require.NoError(t, os.WriteFile(filepath.Join(dir, "systemctl"), []byte("#!/bin/sh\necho\n"), 0o755))
	assert.True(t, unitExited(unit), "unloaded unit with no cgroup left")
}

func TestParseExt4Identity(t *testing.T) {
	t.Parallel()

	superblock := func(id uuid.UUID, label string) []byte {
		sb := make([]byte, ext4SuperblockSize)
		binary.LittleEndian.PutUint16(sb[ext4MagicOffset:], ext4Magic)
		copy(sb[ext4UUIDOffset:], id[:])
		copy(sb[ext4LabelOffset:], label)

		return sb
	}
	id := uuid.MustParse("0f0f0f0f-0f0f-0f0f-0f0f-0f0f0f0f0f0f")

	gotUUID, gotLabel, ok := parseExt4Identity(superblock(id, "rootfs"))
	assert.True(t, ok)
	assert.Equal(t, id.String(), gotUUID)
	assert.Equal(t, "rootfs", gotLabel)

	gotUUID, gotLabel, ok = parseExt4Identity(superblock(uuid.Nil, ""))
	assert.True(t, ok, "no uuid or label is still a known identity")
	assert.Empty(t, gotUUID)
	assert.Empty(t, gotLabel)

	_, _, ok = parseExt4Identity(make([]byte, ext4SuperblockSize))
	assert.False(t, ok, "bad magic")

	_, _, ok = parseExt4Identity(nil)
	assert.False(t, ok, "short read")
}
