//go:build linux

package rootfs

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNBDDevicePath pins the device allowlist the jailed debugfs is constrained
// to: only a bare NBD node, so a bad caller can't point the rewrite at an
// arbitrary block device (a partition, a real disk, or a traversal).
func TestNBDDevicePath(t *testing.T) {
	t.Parallel()

	for _, ok := range []string{"/dev/nbd0", "/dev/nbd1", "/dev/nbd42"} {
		assert.Truef(t, nbdDevicePath.MatchString(ok), "%q should be accepted", ok)
	}
	for _, bad := range []string{
		"/dev/sda", "/dev/nbd", "/dev/nbd0p1", "/dev/nbd0 ", "/dev/nbdx",
		"../../dev/nbd0", "/dev/nbd0;rm", "/tmp/nbd0", "",
	} {
		assert.Falsef(t, nbdDevicePath.MatchString(bad), "%q should be rejected", bad)
	}
}

// TestRunDebugfsRejectsNonNBDDevice verifies the guard is actually wired into the
// jail launcher (not just the regex): a non-NBD device is refused before any
// debugfs/systemd-run process is spawned, so this needs no root or debugfs.
func TestRunDebugfsRejectsNonNBDDevice(t *testing.T) {
	t.Parallel()

	out, err := runDebugfs(t.Context(), "/dev/sda", t.TempDir(), "swap", "rm /usr/bin/envd\n", true)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "refusing to run debugfs on unexpected device path")
	assert.Empty(t, out)
}

func TestSwapEnvdBinaryRejectsRelativeStageRoot(t *testing.T) {
	t.Parallel()

	_, err := SwapEnvdBinary(t.Context(), "/dev/nbd0", "/fc-envd/envd", "relative")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "envd swap stage root must be absolute")
}

// TestEnvdExecutable pins the mode check the swap/rollback verify relies on: a
// content match is not enough — a silent `sif` failure could leave envd
// non-executable, which this must catch from debugfs `stat` output.
func TestEnvdExecutable(t *testing.T) {
	t.Parallel()

	exec := "Inode: 12   Type: regular    Mode:  0755   Flags: 0x0\nUser: 0   Group: 0   Size: 100\n"
	nonExec := "Inode: 12   Type: regular    Mode:  0644   Flags: 0x0\nUser: 0   Group: 0   Size: 100\n"
	dir := "Inode: 2   Type: directory    Mode:  0755   Flags: 0x0\n"

	assert.True(t, envdExecutable(exec), "regular file 0755 is executable")
	assert.True(t, envdExecutable("Type: regular    Mode:  0555"), "0555 has the owner-exec bit")
	assert.False(t, envdExecutable(nonExec), "regular file 0644 is NOT executable (silent sif failure)")
	assert.False(t, envdExecutable(dir), "a directory is not an executable regular file")
	assert.False(t, envdExecutable(""), "empty stat output is not executable")
	assert.False(t, envdExecutable("Type: regular"), "missing Mode is not executable")
}

// TestParseEnvdSize pins the size read that gates the swap. The size is
// guest-controlled and every dump lands on the host, so a misparse that silently
// yielded a small number would defeat the maxEnvdSize gate — hence a real debugfs
// header, including the "Size of extra inode fields" line that must NOT be picked up
// instead of the actual size.
func TestParseEnvdSize(t *testing.T) {
	t.Parallel()

	// Shape of real `debugfs stat` output, decoy line included.
	statOut := "Inode: 12   Type: regular    Mode:  0755   Flags: 0x80000\n" +
		"Generation: 0    Version: 0x00000000:00000001\n" +
		"User:     0   Group:     0   Project:     0   Size: 18967672\n" +
		"File ACL: 0\n" +
		"Links: 1   Blockcount: 37048\n" +
		"Fragment:  Address: 0    Number: 0    Size: of extra inode fields: 32\n"

	size, err := parseEnvdSize(statOut)
	require.NoError(t, err)
	assert.Equal(t, int64(18967672), size, "must read the inode Size, not a later decoy")

	huge, err := parseEnvdSize("Type: regular   Mode: 0755   Size: 8589934592")
	require.NoError(t, err)
	assert.Greater(t, huge, int64(maxEnvdSize), "an 8 GiB envd must land over the gate")

	small, err := parseEnvdSize("Type: regular   Mode: 0755   Size: 18967672")
	require.NoError(t, err)
	assert.LessOrEqual(t, small, int64(maxEnvdSize), "a real envd must pass the gate")

	_, err = parseEnvdSize("Inode: 12   Type: regular    Mode:  0755\n")
	require.ErrorIs(t, err, ErrStatUnparseable, "a stat with no Size must fail rather than read as 0")

	_, err = parseEnvdSize("")
	require.ErrorIs(t, err, ErrStatUnparseable, "empty stat output must fail rather than read as 0")

	// A crafted i_size past MaxInt64 (ext4's field is 64-bit unsigned) matches the
	// \d+ capture but overflows ParseInt — it must decline like any other
	// unparseable stat, and the tenant-chosen digits must not ride the error out
	// (strconv's own text quotes them too, so %w alone would leak them).
	const overflow = "18446744073709551615"
	_, err = parseEnvdSize("Type: regular   Mode: 0755   Size: " + overflow)
	require.ErrorIs(t, err, ErrStatUnparseable, "an i_size past MaxInt64 must decline like any other unparseable stat")
	assert.NotContains(t, err.Error(), overflow, "the tenant-chosen digits must not ride the error out")
}

// TestClassifyEnvdState walks EVERY cell of the decision table. The table is total
// over (presence x content) — 16 cells — and this enumerates all of them explicitly,
// because the three previous versions of this logic were each correct on the cases
// their author had in mind and wrong on one they had not.
//
// Two rows carry the history:
//   - `absent` must be DAMAGED, not unknown. A missing inode and a failed read both
//     yield zero bytes, but the first is knowledge (roll the original back) and the
//     second is ignorance (refuse to boot). Conflating them turned the primitive's
//     central case — `rm` landed, `write` did not — into a failed resume with no
//     rollback attempted, which a dev-cluster run caught after review and CI did not.
//   - `presentNotExecutable` must be DAMAGED whatever the content says. On the
//     idempotent re-fire the rootfs already carries the target (wantSHA == origSHA),
//     so content alone cannot reveal a `sif` failure that cleared the exec bit.
func TestClassifyEnvdState(t *testing.T) {
	t.Parallel()

	presences := map[envdPresence]string{
		presenceUnknown:      "presenceUnknown",
		presenceAbsent:       "presenceAbsent",
		presentNotExecutable: "presentNotExecutable",
		presentExecutable:    "presentExecutable",
	}
	contents := map[envdContent]string{
		contentUnreadable: "contentUnreadable",
		contentTarget:     "contentTarget",
		contentOriginal:   "contentOriginal",
		contentOther:      "contentOther",
	}

	// The full table. Every (presence, content) pair appears exactly once.
	want := map[envdPresence]map[envdContent]envdSwapState{
		// Cannot stat: nothing is established, so never rm and never boot.
		presenceUnknown: {
			contentUnreadable: envdUnknown,
			contentTarget:     envdUnknown,
			contentOriginal:   envdUnknown,
			contentOther:      envdUnknown,
		},
		// Absent is KNOWN, and it is the rollback case. Content is not consulted:
		// readEnvdState short-circuits the dump, so it can only be unreadable here,
		// but the verdict must not depend on that.
		presenceAbsent: {
			contentUnreadable: envdDamaged,
			contentTarget:     envdDamaged,
			contentOriginal:   envdDamaged,
			contentOther:      envdDamaged,
		},
		// Present but not executable: rewrite it, whatever the bytes are. Unreadable
		// still wins, because an unreadable device is not a thing to run `rm` on.
		presentNotExecutable: {
			contentUnreadable: envdUnknown,
			contentTarget:     envdDamaged,
			contentOriginal:   envdDamaged,
			contentOther:      envdDamaged,
		},
		// Present and executable: the content decides which binary booted.
		presentExecutable: {
			contentUnreadable: envdUnknown,
			contentTarget:     envdSwapApplied,
			contentOriginal:   envdOriginalIntact,
			contentOther:      envdDamaged,
		},
	}

	seen := 0
	for p, pn := range presences {
		for c, cn := range contents {
			t.Run(pn+"/"+cn, func(t *testing.T) {
				t.Parallel()

				assert.Equal(t, want[p][c], classifyEnvdState(p, c))
			})
			seen++
		}
	}
	assert.Equal(t, len(presences)*len(contents), seen, "every table cell must be exercised")
}

// TestClassifyEnvdStateNeverBricks states the safety property the table exists to
// guarantee, independently of the cell-by-cell expectations above: the destructive
// rollback (which starts with `rm`) may only be chosen when the read-back actually
// established something. If we could not read the rootfs, we must not rewrite it.
func TestClassifyEnvdStateNeverBricks(t *testing.T) {
	t.Parallel()

	for _, p := range []envdPresence{presenceUnknown, presenceAbsent, presentNotExecutable, presentExecutable} {
		got := classifyEnvdState(p, contentUnreadable)
		if p == presenceAbsent {
			assert.Equal(t, envdDamaged, got, "absent is established knowledge: roll back")

			continue
		}
		assert.Equal(t, envdUnknown, got,
			"an unreadable inode must never authorise the rollback's rm")
	}

	// And the converse: a bootable target must never be disturbed.
	assert.Equal(t, envdSwapApplied, classifyEnvdState(presentExecutable, contentTarget))
}

// TestEnvdAbsent pins the "no such file" detection the absent/unreadable split rests
// on. debugfs exits 0 on a failed scripted command, so this is only ever visible in
// its stdout — misparse it and a deleted envd reads as an unreadable device, which is
// exactly the regression this replaced.
func TestEnvdAbsent(t *testing.T) {
	t.Parallel()

	for _, out := range []string{
		"/usr/bin/envd: File not found by ext2_lookup",
		"stat: File not found by ext2_lookup while looking up /usr/bin/envd",
		"debugfs: No such file or directory while opening /usr/bin/envd",
	} {
		assert.Truef(t, envdAbsent(out), "%q must read as absent", out)
	}
	for _, out := range []string{
		"Inode: 12   Type: regular    Mode:  0755   Flags: 0x80000",
		"",
		"debugfs: Filesystem opened read/write",
	} {
		assert.Falsef(t, envdAbsent(out), "%q must NOT read as absent", out)
	}
}

// TestCappedBuffer verifies debugfs output is bounded: writes past the limit are
// discarded, but every Write still reports a full write so the child is never
// blocked or handed a short-write error.
func TestCappedBuffer(t *testing.T) {
	t.Parallel()

	b := &cappedBuffer{limit: 4}

	n, err := b.Write([]byte("abcdef"))
	require.NoError(t, err)
	assert.Equal(t, 6, n, "Write must report the full length even when capped")
	assert.Equal(t, "abcd", string(b.Bytes()))

	n, err = b.Write([]byte("ghij"))
	require.NoError(t, err)
	assert.Equal(t, 4, n)
	assert.Equal(t, "abcd", string(b.Bytes()), "buffer stays capped across writes")
}
