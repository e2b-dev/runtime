//go:build linux

package rootfs

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"time"

	"github.com/google/uuid"
)

// e2fsckPath is the absolute path used both for the jailed replay and the support
// probe, so a PATH miss can't surface as a 127 the classifier might misread.
const e2fsckPath = "/usr/sbin/e2fsck"

// FsRecoverTimeout bounds the jailed e2fsck run. Recovery is journal replay only
// (`-p -E journal_only`), so the cost is bounded by journal content, not filesystem
// size — it exits after replay without the O(inode) full scan a non-quiesced
// snapshot would otherwise force. Local replay measures ~0.01s; 15s covers a large
// (≤128 MiB) busy journal read cold over the COW/object-store chain with wide margin.
const FsRecoverTimeout = 15 * time.Second

// ErrRecoveryFailed marks a recovery run that did not reach a mountable, replayed
// state. Journal replay does NOT condemn a snapshot: it cannot tell an unmountable
// filesystem apart from a transient device fault (e2fsck's exit 8 covers a bad
// superblock AND a device it could not open, EIO/ENOENT; exit 4 covers a truncated
// read), so every non-replayed outcome — an operational failure (timeout, spawn,
// kill), or any e2fsck exit that is not a clean replay — is treated as node-local
// or transient. The caller fails this start (the filesystem was not brought up)
// but the resume may succeed on a retry or another node, so it is NEVER reported
// to the customer as permanently unrecoverable. Condemning a rootfs is the job of a
// separate opt-in full-filesystem repair, not of pre-boot journal replay.
var ErrRecoveryFailed = errors.New("rootfs filesystem recovery did not complete")

// errDeviceRefused is the pre-launch guard tripping on a non-NBD device path. It
// is a programming error (the path is always the sandbox's own NBD node), never a
// jail malfunction, so it fails closed rather than booting like a launch failure.
var errDeviceRefused = errors.New("refusing to run e2fsck on unexpected device path")

// RecoverOutcome labels what the recovery run observed, for logs and meters.
type RecoverOutcome string

const (
	// RecoverOutcomeReplayed — the journal was replayed (or there was nothing to
	// replay, or a corrupt journal was cleared and regenerated): the fs is
	// mountable and boots. The expected outcome for the admitted population.
	RecoverOutcomeReplayed RecoverOutcome = "replayed"
	// RecoverOutcomeFailed — the replay did not complete: an operational failure
	// (timeout, or an e2fsck exit that is not a clean replay, including a
	// device/superblock it could not open). Fails the start, but always retryable —
	// never a permanent verdict on the snapshot.
	RecoverOutcomeFailed RecoverOutcome = "failed_operational"
	// RecoverOutcomeFailedOpen — recovery could not run AND e2fsck provably never
	// opened the device (the jail could not launch it, or the host image cannot exec
	// e2fsck), so the on-disk state is exactly what a flag-off cold boot would mount
	// and the guest kernel replays the journal itself. The boot proceeds — matching
	// the sibling envd swap's best-effort stance through the same jail — rather than
	// letting a host-image regression take out every flag-on cold boot.
	RecoverOutcomeFailedOpen RecoverOutcome = "failed_open"
	// RecoverOutcomeSkippedQuiesced — no replay ran: the rootfs was frozen at pause
	// and is consistent by construction.
	RecoverOutcomeSkippedQuiesced RecoverOutcome = "skipped_quiesced"
	// RecoverOutcomeNone — no pre-boot recovery applied to this create at all: a
	// memory resume, the recovery flag off, or a path that never reached the reboot
	// arm. Distinguishes "recovery did not run" from a run that was skipped.
	RecoverOutcomeNone RecoverOutcome = "none"
)

// RecoverReason sub-labels a RecoverOutcome so a ramp can act on it without
// grepping logs: within failed_operational it separates tooling breakage (roll
// back) from an unreplayable snapshot (expected), and within replayed it
// separates a no-op from a journal that actually needed replaying (the efficacy
// denominator). Bounded and low-cardinality — the metric never carries a raw
// exit code or any tenant-influenced bytes.
type RecoverReason string

const (
	// replayed sub-reasons. e2fsck's exit code alone cannot tell them apart: under
	// -p -E journal_only a replay that needs no further repair exits 0 exactly like
	// a clean journal (the kernel keeps the superblock's valid-fs bit set while a
	// journaled filesystem is mounted, and a replay is not a "fixed problem"). The
	// superblock's needs_recovery bit, read before the run, is what says whether
	// there was a journal to replay.
	RecoverReasonNothingToDo     RecoverReason = "nothing_to_do"    // journal was clean; rc 0
	RecoverReasonJournalReplayed RecoverReason = "journal_replayed" // needs_recovery was set and rc 0, or rc 1..3
	RecoverReasonReplayUnknown   RecoverReason = "replay_unknown"   // rc 0 but the superblock could not be read before the run
	// failed_operational sub-reasons (fail closed — e2fsck may have opened the device).
	RecoverReasonTimeout     RecoverReason = "timeout"      // Go deadline fired: e2fsck may have been killed mid-replay
	RecoverReasonKilled      RecoverReason = "killed"       // unit signalled (OOM, RuntimeMaxSec, external stop): mid-replay kill, NOT a launch failure
	RecoverReasonNoSentinel  RecoverReason = "no_sentinel"  // ran but no result (sentinel lost), or the pre-launch device guard
	RecoverReasonE2fsck4     RecoverReason = "e2fsck_4"     // e2fsck exit 4
	RecoverReasonE2fsck8     RecoverReason = "e2fsck_8"     // e2fsck exit 8 (bad superblock AND device-open, indistinguishable)
	RecoverReasonE2fsckOther RecoverReason = "e2fsck_other" // any other exit >= 4 (incl. signal codes)
	// failed_open sub-reasons (boot anyway — e2fsck never opened the device).
	RecoverReasonLauncherFailure RecoverReason = "launcher_failure" // systemd-run/jail could not start the unit
	RecoverReasonExecFailure     RecoverReason = "exec_failure"     // 126/127: the shell could not exec e2fsck
	// skipped_quiesced sub-reason.
	RecoverReasonQuiesced RecoverReason = "quiesced" // no run: rootfs frozen at pause
)

// Under `-p -E journal_only` e2fsck replays the journal and exits WITHOUT a full
// scan (man e2fsck: bit 1 corrected, bit 2 reboot-recommended). Anything below
// bit 4 is a successful replay/regenerate — mountable, boot. Any other exit — bit
// 4 and up, the shell's exec-failure codes (126/127), a signal code (128+N), or no
// sentinel at all — means the replay did not complete; because those codes cannot
// be told apart from a transient device fault, they are all operational/retryable,
// never a filesystem verdict.
const e2fsckReplayedMax = 4

// e2fsckRCPattern matches the sentinel the jailed command echoes after e2fsck.
var e2fsckRCPattern = regexp.MustCompile(`__E2FSCK_RC__(\d+)__`)

// e2fsckRC extracts e2fsck's exit code from the sentinel on the recovery
// command's stdout. e2fsck's own output is redirected to stderr (see runE2fsck),
// so this stream carries ONLY the wrapper's `printf` sentinel — tenant-influenced
// bytes (a crafted volume label e2fsck would echo) never reach it and cannot
// spoof a code. The last match is taken defensively; the genuine sentinel is
// always printed after e2fsck exits. The bool is false when no sentinel was
// emitted (the run never reached the printf: launcher failure, kill, or timeout).
func e2fsckRC(rcOut []byte) (int, bool) {
	ms := e2fsckRCPattern.FindAllSubmatch(rcOut, -1)
	if len(ms) == 0 {
		return 0, false
	}
	rc, err := strconv.Atoi(string(ms[len(ms)-1][1]))
	if err != nil {
		return 0, false
	}

	return rc, true
}

// RecoverFilesystem brings the crash-consistent filesystem on the sandbox's own
// NBD device to a mountable state before the VM boots, with `e2fsck -p -E
// journal_only`: it replays the journal (the same recovery the guest kernel would
// do at mount) and exits, without the O(inode) full scan a non-quiesced snapshot
// would otherwise force. A clean replay boots; anything else surfaces as
// ErrRecoveryFailed (retryable) — journal replay does not condemn a snapshot (see
// ErrRecoveryFailed). Body corruption is deliberately NOT scanned for, and an
// unmountable filesystem is not distinguished from a transient fault here; both
// whole-fs repair and condemnation are a separate opt-in full-filesystem repair.
//
// Cancel-free by construction: request cancellation must not kill e2fsck mid-write
// (a torn replay would be served to the boot that follows), so the run detaches
// from the caller's cancellation and is bounded by FsRecoverTimeout alone, plus
// the separately bounded superblock probe that precedes it (ext4ProbeTimeout).
func RecoverFilesystem(ctx context.Context, devicePath string) (RecoverOutcome, RecoverReason, error) {
	// Read before e2fsck runs (a successful replay clears the bit) and before the
	// replay budget starts, so a stalled probe costs at most its own bound.
	journal := journalUnknown
	if nbdDevicePath.MatchString(devicePath) {
		journal = ext4JournalState(devicePath)
	}

	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), FsRecoverTimeout)
	defer cancel()

	rcOut, runErr := runE2fsck(rctx, devicePath)
	// rctx is cancelled only by its own deadline here (the defer above has not run
	// yet), so a non-nil Err means the run hit FsRecoverTimeout — e2fsck may have
	// been killed mid-replay, which the classifier must fail closed.
	timedOut := rctx.Err() != nil

	return classifyE2fsckResult(runErr, rcOut, timedOut, journal)
}

// journalState is what the ext4 superblock said about the journal before the run.
type journalState int

const (
	journalUnknown journalState = iota // superblock unreadable or not ext4
	journalClean                       // needs_recovery clear: nothing to replay
	journalDirty                       // needs_recovery set: a replay is pending
)

// ext4 superblock layout (fs/ext4/ext4.h): 1 KiB in, magic at 0x38,
// s_feature_incompat at 0x60; INCOMPAT_RECOVER marks a journal that still needs
// replaying.
const (
	ext4SuperblockOffset = 1024
	ext4SuperblockSize   = 1024
	ext4MagicOffset      = 0x38
	ext4Magic            = 0xEF53
	ext4IncompatOffset   = 0x60
	ext4IncompatRecover  = 0x0004
)

// ext4ProbeTimeout bounds the superblock read. A cold first chunk over the
// COW/object-store chain can take seconds; past this the label degrades to
// unknown and the replay proceeds on its own budget.
const ext4ProbeTimeout = 3 * time.Second

// ext4JournalState reads the needs_recovery bit off the device's primary
// superblock. Read-only, one block, no jail: it reads fixed offsets and parses two
// integers, never tenant-influenced content. Any failure or a read past
// ext4ProbeTimeout is journalUnknown — the label degrades, the boot decision does
// not depend on it. The blocking syscalls run in their own goroutine because a
// stalled block device cannot be interrupted; on timeout that goroutine finishes
// (and closes the file) whenever the device answers.
func ext4JournalState(devicePath string) journalState {
	result := make(chan journalState, 1)
	go func() { result <- readExt4JournalState(devicePath) }()

	timer := time.NewTimer(ext4ProbeTimeout)
	defer timer.Stop()

	select {
	case state := <-result:
		return state
	case <-timer.C:
		return journalUnknown
	}
}

func readExt4JournalState(devicePath string) journalState {
	f, err := os.Open(devicePath)
	if err != nil {
		return journalUnknown
	}
	defer f.Close()

	sb := make([]byte, ext4SuperblockSize)
	if _, err := f.ReadAt(sb, ext4SuperblockOffset); err != nil {
		return journalUnknown
	}

	return parseExt4JournalState(sb)
}

func parseExt4JournalState(sb []byte) journalState {
	if len(sb) < ext4SuperblockSize || binary.LittleEndian.Uint16(sb[ext4MagicOffset:]) != ext4Magic {
		return journalUnknown
	}
	if binary.LittleEndian.Uint32(sb[ext4IncompatOffset:])&ext4IncompatRecover != 0 {
		return journalDirty
	}

	return journalClean
}

// classifyE2fsckResult maps a jailed journal-replay run to an outcome, keyed on
// e2fsck's own exit code parsed from rcOut (the wrapper's sentinel stream), never
// the systemd-run wrapper's. The error carries only the exit code — never e2fsck's
// output, which is tenant-influenced (crafted volume labels, filenames) and would
// otherwise reach shared observability through the create-failure telemetry. Only a
// clean replay (rc < bit 4) boots; every other exit is retryable/operational,
// because journal replay cannot tell an unmountable filesystem apart from a
// transient fault (see ErrRecoveryFailed).
func classifyE2fsckResult(runErr error, rcOut []byte, timedOut bool, journal journalState) (RecoverOutcome, RecoverReason, error) {
	rc, ok := e2fsckRC(rcOut)
	if !ok {
		// No result — fail CLOSED by default; only a positively-identified launch
		// failure (e2fsck provably never ran) fails open. An unanticipated shape must
		// retry, not boot.
		switch {
		case errors.Is(runErr, errDeviceRefused):
			// Pre-launch guard tripped (a bug — the path is always the sandbox's own
			// NBD node). Not a jail malfunction; fail closed rather than boot an
			// unvalidated device.
			return RecoverOutcomeFailed, RecoverReasonNoSentinel, fmt.Errorf("%w: %w", ErrRecoveryFailed, runErr)
		case timedOut:
			// The deadline fired: e2fsck may have been killed mid-replay. Fail closed.
			if runErr == nil {
				runErr = errors.New("recovery timed out with no result")
			}

			return RecoverOutcomeFailed, RecoverReasonTimeout, fmt.Errorf("%w: e2fsck did not report a result: %w", ErrRecoveryFailed, runErr)
		case isSignalExit(runErr):
			// The unit was killed by a signal — the systemd-run client SIGKILLed (Go
			// reports ExitCode()==-1), or a service signal surfaced as 128+N (OOM,
			// RuntimeMaxSec, external stop). e2fsck may have been mid-replay: same as
			// timeout, fail closed — and never launcher_failure (which means broken
			// tooling).
			return RecoverOutcomeFailed, RecoverReasonKilled, fmt.Errorf("%w: recovery unit signalled: %w", ErrRecoveryFailed, runErr)
		case isLaunchFailure(runErr):
			// systemd-run exited with a real, non-signal, non-zero code: it could not
			// START the unit (a completed script exits 0 via the trailing printf; a
			// signalled one is caught above), so e2fsck never ran and nothing was
			// written. Boot on the guest kernel's mount-time replay (flag-off parity).
			return RecoverOutcomeFailedOpen, RecoverReasonLauncherFailure, nil
		default:
			// Unrecognized: systemd-run exited 0 with the sentinel lost, or a non-exit
			// error. e2fsck may have run — fail closed (a cheap retry on a fresh overlay).
			if runErr == nil {
				return RecoverOutcomeFailed, RecoverReasonNoSentinel, fmt.Errorf("%w: e2fsck did not report a result", ErrRecoveryFailed)
			}

			return RecoverOutcomeFailed, RecoverReasonNoSentinel, fmt.Errorf("%w: e2fsck did not report a result: %w", ErrRecoveryFailed, runErr)
		}
	}

	if rc < e2fsckReplayedMax {
		// Below bit 4: journal replayed, regenerated, or nothing to do — mountable.
		// 1..3 always applied a repair; 0 is a replay or a no-op depending on what
		// the superblock said before the run — the feature's efficacy split.
		reason := RecoverReasonJournalReplayed
		if rc == 0 {
			switch journal {
			case journalClean:
				reason = RecoverReasonNothingToDo
			case journalUnknown:
				reason = RecoverReasonReplayUnknown
			}
		}

		return RecoverOutcomeReplayed, reason, nil
	}

	if rc == 126 || rc == 127 {
		// The shell could not exec e2fsck (a missing/broken host image), so e2fsck
		// never opened the device — boot on the guest kernel's mount-time replay.
		return RecoverOutcomeFailedOpen, RecoverReasonExecFailure, nil
	}

	// Any other exit >= bit 4 — e2fsck opened the device and could not complete a
	// clean replay (a device/superblock it could not open, a truncated read, a
	// signal). Not a verdict on the snapshot: fail closed, retryable.
	return RecoverOutcomeFailed, failedReason(rc), fmt.Errorf("%w: e2fsck exit %d", ErrRecoveryFailed, rc)
}

// exitCode extracts a process exit code from err. ok is false when err carries no
// exit code (a nil error, or a non-exit failure like a context error). Matched via
// an interface so a test double stands in for *exec.ExitError.
func exitCode(err error) (int, bool) {
	var ec interface{ ExitCode() int }
	if errors.As(err, &ec) {
		return ec.ExitCode(), true
	}

	return 0, false
}

// isSignalExit reports whether the run was killed by a signal. Go reports a
// signalled process as ExitCode()==-1; a service signal propagated by systemd-run
// may instead surface as 128+N. Either shape means a possibly-mid-replay kill.
func isSignalExit(err error) bool {
	code, ok := exitCode(err)

	return ok && (code < 0 || code >= 128)
}

// isLaunchFailure reports a real, non-signal, non-zero exit (1..127): systemd-run
// could not start the unit, so e2fsck never ran. This is the ONLY no-sentinel shape
// that fails open — everything else fails closed.
func isLaunchFailure(err error) bool {
	code, ok := exitCode(err)

	return ok && code > 0 && code < 128
}

// journalOnlyRejected reports whether e2fsck's stderr shows it refused the
// -E journal_only option. e2fsck prints this at argument-parse time, before it
// opens any device, so the message is its own (tenant-independent). The wording
// varies by e2fsprogs ("Unknown" vs "Unrecognized extended option"); the stable
// substring is "extended option".
func journalOnlyRejected(stderr []byte) bool {
	return bytes.Contains(stderr, []byte("extended option"))
}

// JournalOnlySupported probes whether the host e2fsck accepts -E journal_only. It
// runs read-only against a nonexistent device: the option is parsed before the
// device is opened, so this reads no filesystem and needs no jail. A build lacking
// the option prints "extended option" and exits; one that has it fails later on the
// missing device instead. A probe that cannot run at all (e2fsck absent) reports
// supported — that case surfaces at recovery time as an exec failure, metered there.
func JournalOnlySupported(ctx context.Context) bool {
	var stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, e2fsckPath, "-n", "-E", "journal_only", "/nonexistent-fs-recover-probe")
	cmd.Stderr = &stderr
	_ = cmd.Run()

	return !journalOnlyRejected(stderr.Bytes())
}

// failedReason buckets an e2fsck exit >= bit 4 (excluding the 126/127 exec-failure
// codes handled above) into a low-cardinality reason so a ramp can act on it.
func failedReason(rc int) RecoverReason {
	switch rc {
	case 4:
		return RecoverReasonE2fsck4
	case 8:
		return RecoverReasonE2fsck8
	default:
		return RecoverReasonE2fsckOther
	}
}

// runE2fsck runs `e2fsck -p -E journal_only <device>` under the shared systemd-run
// jail (jail_linux.go) — same confinement as the envd swap's debugfs: empty root,
// DynamicUser in group disk, no network, minimal syscall surface, device access
// pinned to the sandbox's own NBD node. e2fsck opens the device read-write on
// purpose — the journal replay writes the recovered metadata to exactly that node
// (the boot's own overlay), and to nothing else. `journal_only` MUST pair with `-p`:
// alone it demands a terminal for repairs and exits 8.
//
// The command is wrapped so e2fsck's own exit code is echoed as a sentinel on
// STDOUT while e2fsck's diagnostics go to STDERR: classification reads the
// sentinel stream, which carries nothing e2fsck (hence nothing tenant-influenced,
// like a crafted volume label) wrote, so it can neither be spoofed nor truncated
// away by a large repair log. e2fsck's stderr is discarded rather than captured:
// it is tenant-influenced and would otherwise reach shared observability through
// the create-failure telemetry, and the exit code is the whole classification
// signal. e2fsck is invoked by absolute path so a PATH miss can't surface as a
// 127 the classifier might mistake for a filesystem verdict. The unit name
// carries a per-invocation nonce so a leftover unit from a prior run on a
// recycled NBD slot can never collide.
//
// Returns (sentinel stdout, run error).
func runE2fsck(ctx context.Context, devicePath string) ([]byte, error) {
	if !nbdDevicePath.MatchString(devicePath) {
		return nil, fmt.Errorf("%w %q", errDeviceRefused, devicePath)
	}

	unit := "fs-recover-" + uuid.NewString()

	// $0 is the device; e2fsck's stdout goes to stderr so only the sentinel reaches
	// stdout. The `1>&2` dup cannot fail, so printf always echoes the wrapped
	// command's real exit; a fallible redirect (e.g. `>/dev/null`) could run printf
	// on the shell's own exit and boot an fs e2fsck never opened. /usr binds are
	// jail read-only.
	script := e2fsckPath + ` -p -E journal_only "$0" 1>&2; printf '__E2FSCK_RC__%d__' "$?"`
	args := append(jailProperties(unit, FsRecoverTimeout, devicePath),
		"--",
		"/bin/sh", "-c", script, devicePath,
	)

	rcOut := &cappedBuffer{limit: 4 << 10}
	cmd := exec.CommandContext(ctx, "systemd-run", args...)
	cmd.Stdout = rcOut
	// e2fsck's (tenant-influenced) diagnostics are discarded, not persisted.
	cmd.Stderr = io.Discard
	// Bound the call even if e2fsck wedges holding the pipe write ends: on ctx
	// expiry Go SIGKILLs the client, then WaitDelay forces Wait to return rather
	// than block on the copy goroutines forever.
	cmd.WaitDelay = 10 * time.Second
	err := cmd.Run()
	// Only err != nil can leave a unit alive server-side (a completed script exits 0
	// via the trailing printf, so err == nil means --wait/--collect already reaped
	// it). RuntimeMaxSec bounds such a unit anyway; the stop is synchronous so e2fsck
	// is confirmed gone before a failed create's NBD slot recycles. Error ignored.
	if err != nil {
		stopCtx, stopCancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		_ = exec.CommandContext(stopCtx, "systemctl", "stop", "--quiet", unit+".service").Run()
		stopCancel()
	}

	return rcOut.Bytes(), err
}
