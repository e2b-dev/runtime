//go:build linux

package rootfs

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// rcStream builds the sentinel stdout stream the wrapper's printf produces.
func rcStream(rc int) []byte {
	return fmt.Appendf(nil, "__E2FSCK_RC__%d__", rc)
}

func TestClassifyE2fsckResult(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		rcOut       []byte
		runErr      error
		timedOut    bool
		wantOutcome RecoverOutcome
		wantReason  RecoverReason
		wantErr     error // nil (replayed / failed_open) or ErrRecoveryFailed
	}{
		// Below bit 4: journal replayed / regenerated / nothing to do — mountable.
		// rc 0 needed no replay; 1..3 applied one (the efficacy split).
		{"replay clean (0)", rcStream(0), nil, false, RecoverOutcomeReplayed, RecoverReasonNothingToDo, nil},
		{"replay corrected (1)", rcStream(1), nil, false, RecoverOutcomeReplayed, RecoverReasonJournalReplayed, nil},
		{"replay reboot-recommended (2)", rcStream(2), nil, false, RecoverOutcomeReplayed, RecoverReasonJournalReplayed, nil},
		{"replay corrected bits (3)", rcStream(3), nil, false, RecoverOutcomeReplayed, RecoverReasonJournalReplayed, nil},
		// Bit 4 and above (excluding 126/127): e2fsck opened the device and did not
		// complete a clean replay. Journal replay cannot tell an unmountable
		// filesystem apart from a transient fault (8 covers a bad superblock AND a
		// device it could not open), so all are fail-closed operational/retryable —
		// never a permanent verdict, but never a boot either (a torn replay must not
		// mount).
		{"failed uncorrected-bit (4)", rcStream(4), nil, false, RecoverOutcomeFailed, RecoverReasonE2fsck4, ErrRecoveryFailed},
		{"failed (5)", rcStream(5), nil, false, RecoverOutcomeFailed, RecoverReasonE2fsckOther, ErrRecoveryFailed},
		{"failed (6)", rcStream(6), nil, false, RecoverOutcomeFailed, RecoverReasonE2fsckOther, ErrRecoveryFailed},
		{"failed bad-superblock/device (8)", rcStream(8), nil, false, RecoverOutcomeFailed, RecoverReasonE2fsck8, ErrRecoveryFailed},
		{"failed (12)", rcStream(12), nil, false, RecoverOutcomeFailed, RecoverReasonE2fsckOther, ErrRecoveryFailed},
		{"failed (125)", rcStream(125), nil, false, RecoverOutcomeFailed, RecoverReasonE2fsckOther, ErrRecoveryFailed},
		{"signal killed (137)", rcStream(137), nil, false, RecoverOutcomeFailed, RecoverReasonE2fsckOther, ErrRecoveryFailed},
		// 126/127 prove the shell could not exec e2fsck, so it never opened the
		// device — fail OPEN (boot on the guest kernel's mount replay, flag-off parity).
		{"exec fail not-executable (126)", rcStream(126), nil, false, RecoverOutcomeFailedOpen, RecoverReasonExecFailure, nil},
		{"exec fail e2fsck missing (127)", rcStream(127), nil, false, RecoverOutcomeFailedOpen, RecoverReasonExecFailure, nil},
		// No sentinel + a non-zero, non-signal exit without the deadline firing: the
		// jail could not START the unit (a completed script exits 0 via the trailing
		// printf, a signalled one exits 128+N), so e2fsck never ran — fail OPEN.
		{"launcher failed, wrapper exit 1", nil, &fakeExitError{1}, false, RecoverOutcomeFailedOpen, RecoverReasonLauncherFailure, nil},
		// No sentinel + a signal exit without the deadline firing: the unit was killed
		// mid-run (OOM, RuntimeMaxSec, external stop). Go reports a signalled process as
		// exit -1; a service signal may surface as 128+N — both must land on killed
		// (fail closed), NOT launcher_failure.
		{"signalled unit, exit -1", nil, &fakeExitError{-1}, false, RecoverOutcomeFailed, RecoverReasonKilled, ErrRecoveryFailed},
		{"signalled unit, exit 137", nil, &fakeExitError{137}, false, RecoverOutcomeFailed, RecoverReasonKilled, ErrRecoveryFailed},
		// No sentinel + deadline fired: e2fsck may have been killed mid-replay — fail closed.
		{"timeout, killed mid-run", nil, errors.New("signal: killed"), true, RecoverOutcomeFailed, RecoverReasonTimeout, ErrRecoveryFailed},
		{"timeout, nil runErr", nil, nil, true, RecoverOutcomeFailed, RecoverReasonTimeout, ErrRecoveryFailed},
		// No sentinel + systemd-run exited 0: e2fsck ran but the sentinel was lost
		// (truncated/corrupted), so it opened the device — fail closed, no %!w.
		{"sentinel lost, nil runErr", nil, nil, false, RecoverOutcomeFailed, RecoverReasonNoSentinel, ErrRecoveryFailed},
		// Pre-launch device guard is a bug, not a jail malfunction — fail closed.
		{"device refused", nil, fmt.Errorf("%w %q", errDeviceRefused, "/dev/sda"), false, RecoverOutcomeFailed, RecoverReasonNoSentinel, ErrRecoveryFailed},
		// No sentinel + an unrecognized (non-exit) error: the catch-all must fail
		// CLOSED, never fall through to a boot.
		{"unrecognized error fails closed", nil, errors.New("dbus boom"), false, RecoverOutcomeFailed, RecoverReasonNoSentinel, ErrRecoveryFailed},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			outcome, reason, err := classifyE2fsckResult(tt.runErr, tt.rcOut, tt.timedOut, journalClean)
			assert.Equal(t, tt.wantOutcome, outcome)
			assert.Equal(t, tt.wantReason, reason)
			if tt.wantErr == nil {
				require.NoError(t, err)

				return
			}
			require.ErrorIs(t, err, tt.wantErr)
			assert.NotContains(t, err.Error(), "%!w", "no malformed nil wrap")
		})
	}
}

// TestClassifyE2fsckResult_NeverCondemns pins the redesign's central invariant:
// journal replay never condemns. No exit code — present or a future one this
// table does not enumerate — nor any (runErr, timedOut) combination may make
// classifyE2fsckResult return anything but replayed, failed_operational, or
// failed_open, and the error presence must track the outcome (a booting outcome
// carries no error; a fail-closed outcome is always retryable). A later edit
// reintroducing a permanent band keyed on an untested code fails here instead of
// shipping.
func TestClassifyE2fsckResult_NeverCondemns(t *testing.T) {
	t.Parallel()

	assertInvariant := func(t *testing.T, outcome RecoverOutcome, err error, label string) {
		t.Helper()
		switch outcome {
		case RecoverOutcomeReplayed, RecoverOutcomeFailedOpen:
			require.NoError(t, err, "%s: a booting outcome must carry no error", label)
		case RecoverOutcomeFailed:
			require.ErrorIs(t, err, ErrRecoveryFailed, "%s: a failed outcome must be retryable", label)
		default:
			t.Fatalf("%s: outcome %q — recovery must only ever be replayed, failed_operational, or failed_open", label, outcome)
		}
	}

	for _, timedOut := range []bool{false, true} {
		for rc := range 256 {
			for _, journal := range []journalState{journalUnknown, journalClean, journalDirty} {
				outcome, _, err := classifyE2fsckResult(nil, rcStream(rc), timedOut, journal)
				assertInvariant(t, outcome, err, fmt.Sprintf("rc=%d timedOut=%v journal=%d", rc, timedOut, journal))
			}
		}
		// The no-sentinel space (launcher failure, sentinel loss, timeout) is part of
		// the invariant under both a nil and a non-nil run error.
		for _, runErr := range []error{nil, errors.New("boom")} {
			outcome, _, err := classifyE2fsckResult(runErr, nil, timedOut, journalClean)
			assertInvariant(t, outcome, err, fmt.Sprintf("no sentinel timedOut=%v runErr=%v", timedOut, runErr))
		}
	}
}

type fakeExitError struct{ code int }

func (e *fakeExitError) Error() string { return fmt.Sprintf("exit status %d", e.code) }
func (e *fakeExitError) ExitCode() int { return e.code }

// e2fsckRC reads only its dedicated stdout stream, so tenant-influenced content
// on the log stream cannot spoof the code, and the genuine (last) sentinel wins.
func TestE2fsckRC(t *testing.T) {
	t.Parallel()

	rc, ok := e2fsckRC(rcStream(4))
	assert.True(t, ok)
	assert.Equal(t, 4, rc)

	_, ok = e2fsckRC([]byte("no sentinel here"))
	assert.False(t, ok, "absence must be reported so it maps to operational, not corrected")

	// Defensive last-match: were a spoofed sentinel ever to precede the genuine
	// one on the same stream, the real (final) code wins.
	rc, ok = e2fsckRC([]byte("__E2FSCK_RC__0__ noise __E2FSCK_RC__4__"))
	assert.True(t, ok)
	assert.Equal(t, 4, rc, "the genuine trailing sentinel must win over an earlier spoof")
}

// journalOnlyRejected keys on e2fsck's own arg-parse message, whose wording varies
// across e2fsprogs ("Unknown"/"Unrecognized"), so it matches the stable substring.
func TestJournalOnlyRejected(t *testing.T) {
	t.Parallel()

	assert.True(t, journalOnlyRejected([]byte("Unknown extended option: journal_only\n")))
	assert.True(t, journalOnlyRejected([]byte(`Unrecognized extended option "journal_only"`)))
	assert.False(t, journalOnlyRejected([]byte("e2fsck 1.47.4 (6-Mar-2025)\n")), "a supporting build's banner is not a rejection")
	assert.False(t, journalOnlyRejected(nil))
}

// On any host with a modern e2fsprogs (or none — a probe that can't run reports
// supported) the option is accepted, so the probe does not flag the tooling.
func TestJournalOnlySupported(t *testing.T) {
	t.Parallel()

	assert.True(t, JournalOnlySupported(t.Context()))
}

// The guard is wired into the jail launcher, not just the regex: a non-NBD
// device is refused before any e2fsck/systemd-run process is spawned.
func TestRunE2fsckRejectsNonNBDDevice(t *testing.T) {
	t.Parallel()

	rcOut, err := runE2fsck(t.Context(), "/dev/sda")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "refusing to run e2fsck on unexpected device path")
	assert.Empty(t, rcOut)
}

// A non-NBD device path never reaches the sentinel classifier: RecoverFilesystem
// returns the retryable operational failure.
func TestRecoverFilesystemRejectsNonNBDDevice(t *testing.T) {
	t.Parallel()

	outcome, _, err := RecoverFilesystem(t.Context(), "/dev/sda")
	assert.Equal(t, RecoverOutcomeFailed, outcome)
	require.ErrorIs(t, err, ErrRecoveryFailed)
}

// TestClassifyE2fsckResult_ReplayReasonFromSuperblock pins that rc 0 is labelled
// from the pre-run superblock, not guessed: journal-only e2fsck exits 0 after a
// real replay, so without the bit every replay would read as nothing_to_do.
func TestClassifyE2fsckResult_ReplayReasonFromSuperblock(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		rc         int
		journal    journalState
		wantReason RecoverReason
	}{
		{"rc 0, journal dirty -> replayed", 0, journalDirty, RecoverReasonJournalReplayed},
		{"rc 0, journal clean -> nothing to do", 0, journalClean, RecoverReasonNothingToDo},
		{"rc 0, superblock unreadable -> unknown", 0, journalUnknown, RecoverReasonReplayUnknown},
		{"rc 1, journal clean -> replayed (repair applied)", 1, journalClean, RecoverReasonJournalReplayed},
		{"rc 2, superblock unreadable -> replayed", 2, journalUnknown, RecoverReasonJournalReplayed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			outcome, reason, err := classifyE2fsckResult(nil, rcStream(tt.rc), false, tt.journal)
			require.NoError(t, err)
			assert.Equal(t, RecoverOutcomeReplayed, outcome)
			assert.Equal(t, tt.wantReason, reason)
		})
	}
}

// superblockImage builds a 2 KiB image whose primary ext4 superblock carries the
// given magic and incompat feature word.
func superblockImage(magic uint16, incompat uint32) []byte {
	img := make([]byte, ext4SuperblockOffset+ext4SuperblockSize)
	binary.LittleEndian.PutUint16(img[ext4SuperblockOffset+ext4MagicOffset:], magic)
	binary.LittleEndian.PutUint32(img[ext4SuperblockOffset+ext4IncompatOffset:], incompat)

	return img
}

func TestExt4JournalState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		img  []byte // nil: no file at the path at all
		want journalState
	}{
		{"needs_recovery set", superblockImage(ext4Magic, 0x2C2|ext4IncompatRecover), journalDirty},
		{"needs_recovery clear", superblockImage(ext4Magic, 0x2C2), journalClean},
		{"not ext4", superblockImage(0x1234, ext4IncompatRecover), journalUnknown},
		{"short image", make([]byte, 512), journalUnknown},
		{"missing device", nil, journalUnknown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			path := filepath.Join(t.TempDir(), "rootfs.img")
			if tt.img != nil {
				require.NoError(t, os.WriteFile(path, tt.img, 0o600))
			}
			assert.Equal(t, tt.want, ext4JournalState(path))
		})
	}
}

// A device that never answers must not hold the create: the probe gives up at
// ext4ProbeTimeout and the label degrades to unknown. A FIFO with no writer
// blocks open(2) exactly like a stalled block device blocks read(2).
func TestExt4JournalState_StalledDeviceIsBounded(t *testing.T) {
	t.Parallel()

	fifo := filepath.Join(t.TempDir(), "stalled")
	require.NoError(t, syscall.Mkfifo(fifo, 0o600))

	start := time.Now()
	state := ext4JournalState(fifo)
	elapsed := time.Since(start)

	assert.Equal(t, journalUnknown, state)
	assert.GreaterOrEqual(t, elapsed, ext4ProbeTimeout-100*time.Millisecond)
	assert.Less(t, elapsed, ext4ProbeTimeout+2*time.Second, "the probe must return at its own bound")

	// Release the blocked opener so the test leaves no goroutine behind.
	w, err := os.OpenFile(fifo, os.O_WRONLY, 0)
	if err == nil {
		w.Close()
	}
}
