//go:build linux

package rootfs

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/core/fstab"
)

const (
	debugfsPath        = "/usr/sbin/debugfs"
	fstabRequestPath   = "/etc/fstab"
	debugfsCallTimeout = 2 * time.Second
	// debugfsWaitDelay bounds Wait after a kill: a jailed debugfs holds the
	// output pipes until its unit stops.
	debugfsWaitDelay = 500 * time.Millisecond
	// jailStopTimeout bounds the synchronous stop of a jailed unit after a
	// failed call. RuntimeMaxSec already caps the unit, so it rarely waits long.
	jailStopTimeout = 3 * time.Second

	ext4UUIDOffset  = 0x68
	ext4LabelOffset = 0x78
	ext4LabelSize   = 16

	cgroupRoot = "/sys/fs/cgroup"
)

var (
	// jailExitTimeout bounds the wait for a stopped unit's processes to exit.
	// A debugfs blocked on an NBD read cannot take its SIGKILL until the read
	// ends, and the kernel fails such a read after the NBD I/O timeout (90s)
	// plus the dead-connection timeout (30s), as long as the device stays
	// connected. A var so tests can shorten it.
	jailExitTimeout = 150 * time.Second
	jailExitPoll    = 100 * time.Millisecond
	// jailUnitExited is a var so tests can stand in for systemd.
	jailUnitExited = unitExited

	statTypePattern = regexp.MustCompile(`\bType:\s+(\S+)`)

	errDebugfsTimeout = errors.New("debugfs timed out")

	// ErrInspectorRunning means a jailed debugfs may still hold the rootfs
	// device. The caller must not boot from or release the device.
	ErrInspectorRunning = errors.New("fstab inspector still holds the rootfs device")
)

// InspectFstab reads /etc/fstab from the ext4 filesystem at rootfsPath without
// mounting or modifying it. A file it cannot read is reported with
// OutcomeUnknown and the reason. It returns an error only when a jailed debugfs
// outlived its stop: then it wraps ErrInspectorRunning, since the device is
// not yet safe to boot from or recycle.
//
// An NBD device holds blocks a build guest wrote, so debugfs parses it inside
// the jail. A regular file is the base layer's rootfs, built on the host and
// already checked by an unconfined e2fsck before its boot, so it is read directly.
func InspectFstab(ctx context.Context, rootfsPath string) (fstab.Report, error) {
	ctx, span := tracer.Start(ctx, "inspect-fstab")
	defer span.End()

	report, running := inspectFstab(ctx, rootfsPath)
	if running {
		span.RecordError(ErrInspectorRunning)

		return report, ErrInspectorRunning
	}
	span.SetAttributes(attribute.String("fstab.inspect", string(report.Outcome)))
	if report.Outcome == fstab.OutcomeUnknown {
		span.SetAttributes(attribute.String("fstab.inspect_reason", string(report.Reason)))
	}

	return report, nil
}

// inspectFstab also reports whether a jailed debugfs outlived its stop.
func inspectFstab(ctx context.Context, rootfsPath string) (report fstab.Report, running bool) {
	jailed := nbdDevicePath.MatchString(rootfsPath)
	if jailed {
		if _, err := exec.LookPath("systemd-run"); err != nil {
			return unknownFstab(fstab.ReasonToolMissing), false
		}
		if _, err := os.Stat(debugfsPath); err != nil {
			return unknownFstab(fstab.ReasonToolMissing), false
		}
	} else {
		if info, err := os.Stat(rootfsPath); err != nil || !info.Mode().IsRegular() {
			return unknownFstab(fstab.ReasonExecError), false
		}
		if _, err := exec.LookPath("debugfs"); err != nil {
			return unknownFstab(fstab.ReasonToolMissing), false
		}
	}

	// cat reads a symlink's inode as file data and has no size cap, so stat first.
	stdout, stderr, err := runFstabDebugfs(ctx, rootfsPath, jailed, "stat "+fstabRequestPath)
	switch {
	case errors.Is(err, ErrInspectorRunning):
		return unknownFstab(fstab.ReasonTimeout), true
	case errors.Is(err, errDebugfsTimeout):
		return unknownFstab(fstab.ReasonTimeout), false
	}
	size, statReport, regular := classifyFstabStat(stdout, stderr, err)
	if !regular {
		return statReport, false
	}

	data, _, err := runFstabDebugfs(ctx, rootfsPath, jailed, "cat "+fstabRequestPath)
	switch {
	case errors.Is(err, ErrInspectorRunning):
		return unknownFstab(fstab.ReasonTimeout), true
	case errors.Is(err, errDebugfsTimeout):
		return unknownFstab(fstab.ReasonTimeout), false
	case err != nil, int64(len(data)) != size:
		return unknownFstab(fstab.ReasonExecError), false
	}

	// This read is not jailed or bounded; the hook holds the device until it
	// returns, and debugfs read the same superblock moments ago.
	target := fstab.DefaultTarget()
	if uuid, label, ok := readExt4Identity(rootfsPath); ok {
		target.IdentityKnown, target.UUID, target.Label = true, uuid, label
	}

	return fstab.Parse(data, target), false
}

// readExt4Identity reads the filesystem UUID and volume label off the primary
// superblock, like ext4JournalState: fixed offsets, no jail. The UUID is empty
// when the filesystem has none.
func readExt4Identity(devicePath string) (uuidStr, label string, ok bool) {
	f, err := os.Open(devicePath)
	if err != nil {
		return "", "", false
	}
	defer f.Close()

	sb := make([]byte, ext4SuperblockSize)
	if _, err := f.ReadAt(sb, ext4SuperblockOffset); err != nil {
		return "", "", false
	}

	return parseExt4Identity(sb)
}

func parseExt4Identity(sb []byte) (uuidStr, label string, ok bool) {
	if len(sb) < ext4SuperblockSize || binary.LittleEndian.Uint16(sb[ext4MagicOffset:]) != ext4Magic {
		return "", "", false
	}

	id, err := uuid.FromBytes(sb[ext4UUIDOffset : ext4UUIDOffset+16])
	if err != nil {
		return "", "", false
	}
	if id != uuid.Nil {
		uuidStr = id.String()
	}
	label = string(bytes.TrimRight(sb[ext4LabelOffset:ext4LabelOffset+ext4LabelSize], "\x00"))

	return uuidStr, label, true
}

// classifyFstabStat reads debugfs's stat output, which exits 0 even when the
// lookup fails. It returns the file size and true only for a regular file
// small enough to parse.
func classifyFstabStat(stdout, stderr []byte, runErr error) (int64, fstab.Report, bool) {
	if m := statTypePattern.FindSubmatch(stdout); m != nil {
		if string(m[1]) != "regular" {
			return 0, unknownFstab(fstab.ReasonNotRegular), false
		}

		m := debugfsSizeRe.FindSubmatch(stdout)
		if m == nil {
			return 0, unknownFstab(fstab.ReasonMalformedStat), false
		}
		size, err := strconv.ParseInt(string(m[1]), 10, 64)
		if err != nil {
			return 0, unknownFstab(fstab.ReasonMalformedStat), false
		}
		if size > fstab.MaxBytes {
			return 0, unknownFstab(fstab.ReasonTooLarge), false
		}

		return size, fstab.Report{}, true
	}

	if bytes.Contains(stderr, []byte("File not found by ext2_lookup")) {
		return 0, fstab.Report{Outcome: fstab.OutcomeAbsent}, false
	}
	if runErr != nil {
		return 0, unknownFstab(fstab.ReasonExecError), false
	}

	return 0, unknownFstab(fstab.ReasonMalformedStat), false
}

func runFstabDebugfs(ctx context.Context, rootfsPath string, jailed bool, request string) (stdout, stderr []byte, err error) {
	ctx, cancel := context.WithTimeout(ctx, debugfsCallTimeout)
	defer cancel()

	// -c skips the allocation bitmaps, which stat and cat never read.
	var cmd *exec.Cmd
	unit := ""
	if jailed {
		unit = "fstab-inspect-" + uuid.NewString()
		args := append(jailProperties(unit, debugfsCallTimeout, rootfsPath), "--", debugfsPath, "-c", "-R", request, rootfsPath)
		cmd = exec.CommandContext(ctx, "systemd-run", args...)
	} else {
		cmd = exec.CommandContext(ctx, "debugfs", "-c", "-R", request, rootfsPath)
		// The stat classifier relies on debugfs's English field names.
		cmd.Env = append(os.Environ(), "LC_ALL=C")
	}

	outBuf := &cappedBuffer{limit: fstab.MaxBytes + 1}
	errBuf := &cappedBuffer{limit: fstab.MaxBytes + 1}
	cmd.Stdout = outBuf
	cmd.Stderr = errBuf
	cmd.WaitDelay = debugfsWaitDelay

	err = cmd.Run()
	// A failed call can leave debugfs running in its unit. Returning lets the
	// boot start or a failed build recycle the NBD slot, so first stop the unit
	// and wait until none of its processes are left.
	if unit != "" && err != nil {
		stopCtx, stopCancel := context.WithTimeout(context.WithoutCancel(ctx), jailStopTimeout)
		_ = exec.CommandContext(stopCtx, "systemctl", "stop", "--quiet", unit+".service").Run()
		stopCancel()

		if !waitUnitExited(unit) {
			return nil, nil, ErrInspectorRunning
		}
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return nil, nil, errDebugfsTimeout
	}

	return outBuf.Bytes(), errBuf.Bytes(), err
}

// waitUnitExited polls until the unit has no processes left, for at most
// jailExitTimeout. It ignores the build's context: the device has to stay
// attached until debugfs is gone, whether or not the build was canceled.
func waitUnitExited(unit string) bool {
	deadline := time.Now().Add(jailExitTimeout)
	for !jailUnitExited(unit) {
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(jailExitPoll)
	}

	return true
}

// unitExited reports whether the unit's cgroup has no processes left. The
// unit's state alone is not enough: systemd gives up on a process that
// survives the final SIGKILL and marks the unit failed while it still runs.
// A cgroup cannot be removed while it has processes.
func unitExited(unit string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), jailStopTimeout)
	defer cancel()

	out, err := exec.CommandContext(ctx, "systemctl", "show", "--property=ControlGroup", "--value", unit+".service").Output()
	if err != nil {
		return false
	}
	cgroup := string(bytes.TrimSpace(out))
	if cgroup == "" {
		// Already unloaded: look where systemd-run places a system unit.
		cgroup = "/system.slice/" + unit + ".service"
	}

	procs, err := os.ReadFile(filepath.Join(cgroupRoot, cgroup, "cgroup.procs"))
	if errors.Is(err, fs.ErrNotExist) {
		return true
	}

	return err == nil && len(bytes.TrimSpace(procs)) == 0
}

func unknownFstab(reason fstab.Reason) fstab.Report {
	return fstab.Report{Outcome: fstab.OutcomeUnknown, Reason: reason}
}
