// Package fstab counts the /etc/fstab entries that would hold a sandbox boot
// waiting for a block device the sandbox does not attach. Its Report carries
// counts and enums only, so it can be logged without exposing the file.
package fstab

import (
	"bytes"
	"path"
	"strings"
	"unicode/utf8"
)

// MaxBytes is the largest fstab that is parsed.
const MaxBytes = 64 << 10

// Outcome is what an inspection concluded about the file.
type Outcome string

const (
	OutcomeOK      Outcome = "ok"
	OutcomeAbsent  Outcome = "absent"
	OutcomeUnknown Outcome = "unknown"
)

// Reason says why an Outcome is unknown.
type Reason string

const (
	ReasonTooLarge      Reason = "too_large"
	ReasonNotText       Reason = "not_text"
	ReasonNotRegular    Reason = "not_regular"
	ReasonToolMissing   Reason = "tool_missing"
	ReasonTimeout       Reason = "timeout"
	ReasonExecError     Reason = "exec_error"
	ReasonMalformedStat Reason = "malformed_stat"
)

// SourceClass is the kind of device reference in an entry's first field.
type SourceClass string

const (
	SourceDev       SourceClass = "dev"
	SourceUUID      SourceClass = "uuid"
	SourceLabel     SourceClass = "label"
	SourcePartUUID  SourceClass = "partuuid"
	SourcePartLabel SourceClass = "partlabel"
)

// SourceClasses lists every SourceClass.
var SourceClasses = []SourceClass{SourceDev, SourceUUID, SourceLabel, SourcePartUUID, SourcePartLabel}

// Target describes the block devices the booted guest has.
type Target struct {
	Devices map[string]struct{}
	// IdentityKnown says UUID and Label were read from the root filesystem.
	// Without it, UUID= and LABEL= entries cannot be checked.
	IdentityKnown bool
	UUID          string
	Label         string
}

// DefaultTarget is the guest of a template build: one unpartitioned virtio
// root drive. Its filesystem UUID and label are not known until read.
func DefaultTarget() Target {
	return Target{Devices: map[string]struct{}{"/dev/vda": {}, "/dev/root": {}}}
}

// Report summarises an fstab. BySource is nil when Blockers is zero.
type Report struct {
	Outcome Outcome
	Reason  Reason
	// Entries counts lines with at least three fields; Malformed counts the rest.
	Entries   int
	Malformed int
	// Blockers counts required mounts of a device the guest lacks.
	Blockers int
	BySource map[SourceClass]int
	// SwapMissing counts swap entries that would otherwise be blockers.
	SwapMissing int
	// Unresolved counts required UUID= and LABEL= mounts that could not be
	// checked because the root filesystem's identity was not known.
	Unresolved int
}

// entryState is what classify concluded about one entry.
type entryState int

const (
	entryFine entryState = iota
	entryMissing
	entryUnresolved
)

var skippedTypes = map[string]struct{}{
	"tmpfs": {}, "devtmpfs": {}, "proc": {}, "sysfs": {}, "devpts": {}, "cgroup": {}, "cgroup2": {},
	"securityfs": {}, "debugfs": {}, "tracefs": {}, "configfs": {}, "mqueue": {}, "hugetlbfs": {},
	"pstore": {}, "bpf": {}, "efivarfs": {}, "fusectl": {}, "binfmt_misc": {}, "autofs": {},
	"ramfs": {}, "overlay": {},
	"nfs": {}, "nfs4": {}, "cifs": {}, "smb3": {}, "smbfs": {}, "ceph": {}, "glusterfs": {},
	"9p": {}, "virtiofs": {}, "afs": {}, "coda": {}, "davfs": {},
}

// skippedOptions mark entries local-fs.target does not wait for. A device
// timeout is absent on purpose: it ends the wait by failing local-fs.target.
// noauto and nofail are handled in classify, since later options and explicit
// ordering or install targets can undo them.
var skippedOptions = map[string]struct{}{
	"bind": {}, "rbind": {}, "_netdev": {},
	"x-systemd.automount": {},
}

const orderBeforeLocalFS = "x-systemd.before=local-fs.target"

// Parse classifies data, the contents of an fstab, against target.
func Parse(data []byte, target Target) Report {
	if len(data) > MaxBytes {
		return Report{Outcome: OutcomeUnknown, Reason: ReasonTooLarge}
	}
	if !utf8.Valid(data) || bytes.IndexByte(data, 0) >= 0 {
		return Report{Outcome: OutcomeUnknown, Reason: ReasonNotText}
	}

	r := Report{Outcome: OutcomeOK}
	for line := range strings.SplitSeq(string(data), "\n") {
		line = strings.TrimSpace(strings.TrimSuffix(line, "\r"))
		if line == "" || line[0] == '#' {
			continue
		}

		fields := strings.Fields(line)
		for i, f := range fields {
			if f[0] == '#' {
				fields = fields[:i]

				break
			}
		}
		if len(fields) < 3 {
			r.Malformed++

			continue
		}
		r.Entries++

		class, state := classify(fields, target)
		if state == entryFine {
			continue
		}
		// envd is not ordered after swap.target, so a missing swap device does not delay it.
		if fields[2] == "swap" {
			if state == entryMissing {
				r.SwapMissing++
			}

			continue
		}
		if state == entryUnresolved {
			r.Unresolved++

			continue
		}
		if r.BySource == nil {
			r.BySource = make(map[SourceClass]int)
		}
		r.Blockers++
		r.BySource[class]++
	}

	return r
}

// classify reports whether the entry is a required mount of a device the
// guest lacks, and the class of its source.
func classify(fields []string, target Target) (SourceClass, entryState) {
	source, mountpoint, fstype := unescape(fields[0]), unescape(fields[1]), fields[2]
	options := "defaults"
	if len(fields) > 3 {
		options = fields[3]
	}

	// The kernel mounts the root; systemd only remounts it.
	if mountpoint == "/" {
		return "", entryFine
	}
	if _, ok := skippedTypes[fstype]; ok || strings.HasPrefix(fstype, "fuse") {
		return "", entryFine
	}
	// For auto/noauto and fail/nofail the last option wins, as in
	// systemd-fstab-generator.
	noauto, nofail, orderedBeforeLocalFS := false, false, false
	hasInstallTarget, installedInLocalFS := false, false
	for opt := range strings.SplitSeq(options, ",") {
		switch opt {
		case "noauto", "auto":
			noauto = opt == "noauto"
		case "nofail", "fail":
			nofail = opt == "nofail"
		case orderBeforeLocalFS:
			orderedBeforeLocalFS = true
		}
		for _, prefix := range []string{"x-systemd.wanted-by=", "x-systemd.required-by="} {
			if unit, ok := strings.CutPrefix(opt, prefix); ok {
				hasInstallTarget = true
				installedInLocalFS = installedInLocalFS || unit == "local-fs.target"
			}
		}
		if _, ok := skippedOptions[opt]; ok {
			return "", entryFine
		}
	}
	// nofail drops the mount's ordering before local-fs.target, which an
	// explicit x-systemd.before= puts back (systemd.mount(5)).
	if nofail && !orderedBeforeLocalFS {
		return "", entryFine
	}
	// wanted-by= and required-by= replace the mount's link from local-fs.target
	// (systemd.mount(5)). Whether another target holds up the boot is not known
	// here, so only local-fs.target counts.
	if hasInstallTarget && !installedInLocalFS {
		return "", entryFine
	}
	// noauto only drops the default link, so an explicit install into
	// local-fs.target still starts the mount at boot.
	if noauto && !installedInLocalFS {
		return "", entryFine
	}

	if v, ok := strings.CutPrefix(source, "UUID="); ok {
		return SourceUUID, identityState(target, v != "" && strings.EqualFold(v, target.UUID))
	}
	if v, ok := strings.CutPrefix(source, "LABEL="); ok {
		return SourceLabel, identityState(target, v != "" && v == target.Label)
	}
	if strings.HasPrefix(source, "PARTUUID=") {
		return SourcePartUUID, entryMissing
	}
	if strings.HasPrefix(source, "PARTLABEL=") {
		return SourcePartLabel, entryMissing
	}
	if strings.HasPrefix(source, "ID=") {
		return SourceDev, entryMissing
	}
	if strings.HasPrefix(source, "/dev/") {
		if _, present := target.Devices[path.Clean(source)]; present {
			return SourceDev, entryFine
		}

		return SourceDev, entryMissing
	}

	return "", entryFine
}

// identityState checks a UUID= or LABEL= entry against the root filesystem.
func identityState(target Target, matches bool) entryState {
	switch {
	case !target.IdentityKnown:
		return entryUnresolved
	case matches:
		return entryFine
	default:
		return entryMissing
	}
}

// unescape decodes the three-digit octal escapes fstab(5) uses for blanks.
func unescape(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}

	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+3 < len(s) && isOctal(s[i+1]) && isOctal(s[i+2]) && isOctal(s[i+3]) {
			b.WriteByte((s[i+1]-'0')<<6 | (s[i+2]-'0')<<3 | (s[i+3] - '0'))
			i += 3

			continue
		}
		b.WriteByte(s[i])
	}

	return b.String()
}

func isOctal(c byte) bool {
	return c >= '0' && c <= '7'
}
