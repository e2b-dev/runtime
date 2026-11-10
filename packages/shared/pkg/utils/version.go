package utils

import (
	"fmt"
	"regexp"

	"golang.org/x/mod/semver"
)

const MinEnvdVersionForSnapshot = "0.5.0"

// MinEnvdVersionForCgroupFreeze is the first envd we trust to freeze user
// cgroups across pause/resume. /freeze plus /init thaw landed in 0.6.0, but
// 0.6.0-0.6.2 also froze the socat cgroup, breaking port forwarding (fixed
// in 0.6.3 by #2923), so we gate on 0.6.3.
const MinEnvdVersionForCgroupFreeze = "0.6.3"

// MinEnvdVersionForHeapCollapse is the first envd that exposes a native
// POST /collapse endpoint, which compacts envd's own anonymous heap into 2 MiB
// hugepages before pause to reduce the frames it faults on resume. 0.6.4
// already exists in the fleet without /collapse, so the gate must be 0.6.5 (the
// version that introduces the endpoint) to avoid POSTing /collapse at a 0.6.4
// envd that 404s.
const MinEnvdVersionForHeapCollapse = "0.6.5"

// MinEnvdVersionForFsFreeze is the first envd that exposes the native
// POST /fsfreeze and /fsthaw endpoints, which quiesce the guest rootfs before a
// filesystem-only pause. Older envds fall back to a plain guest sync.
const MinEnvdVersionForFsFreeze = "0.6.6"

// MinEnvdVersionForUpgrade is the first envd that both exposes the live-upgrade
// POST /upgrade endpoint and writes the protobuf handover blob the incoming envd
// decodes. The resume-time auto-upgrade trigger delivers to the *running* (old)
// envd, which serializes the handover, so anything older either lacks /upgrade
// or writes the pre-proto (JSON) format the new envd can't read — both must be
// skipped.
const MinEnvdVersionForUpgrade = "0.6.12"

// MinEnvdVersionForHeldInit is the first envd that corrects the /init timestamp by its own
// time from reading the body to the clock step, honours /init's thaw: "skip", and withholds
// the inline thaw of an /init a freeze overtook. The held envd init request runs only at or
// above it.
const MinEnvdVersionForHeldInit = "0.10.0"

func sanitizeVersion(version string) string {
	if len(version) > 0 && version[0] != 'v' {
		version = "v" + version
	}

	return version
}

func CheckEnvdVersionForSnapshot(envdVersion string) error {
	ok, err := IsGTEVersion(envdVersion, MinEnvdVersionForSnapshot)
	if err != nil {
		return fmt.Errorf("invalid envd version %q: %w", envdVersion, err)
	}

	if !ok {
		return fmt.Errorf("sandbox envd version must be at least %s to create snapshots, current version: %s", MinEnvdVersionForSnapshot, envdVersion)
	}

	return nil
}

// IsValidVersion reports whether version parses as semver, with or without its leading v.
func IsValidVersion(version string) bool {
	return semver.IsValid(sanitizeVersion(version))
}

// timestampedBuild matches a build version, MAJOR.MINOR.YYYYMMDDHHMM with an
// optional suffix. The timestamp takes the patch position, so as plain semver a
// build sorts above every release of its line, including releases cut after it.
var timestampedBuild = regexp.MustCompile(`^v?[0-9]+\.[0-9]+\.[0-9]{12}([-+]|$)`)

func parseVersions(curVersion, minVersion string) (string, string, error) {
	curVersion = sanitizeVersion(curVersion)
	minVersion = sanitizeVersion(minVersion)

	if !semver.IsValid(curVersion) {
		return "", "", fmt.Errorf("invalid current version format: %s", curVersion)
	}

	if !semver.IsValid(minVersion) {
		return "", "", fmt.Errorf("invalid minimum version format: %s", minVersion)
	}

	return curVersion, minVersion, nil
}

// IsGTEVersion reports whether curVersion is at or above minVersion as semver.
func IsGTEVersion(curVersion, minVersion string) (bool, error) {
	curVersion, minVersion, err := parseVersions(curVersion, minVersion)
	if err != nil {
		return false, err
	}

	return semver.Compare(curVersion, minVersion) >= 0, nil
}

// IsGTEVersionWithTimestamp is IsGTEVersion for versions that may be a release or a
// timestamped build. Two versions of the same form compare as semver. Across forms
// only what is certain counts: a later line is above an earlier line, and a build
// is above its line's first release, nothing else. A gate in the middle of a line
// therefore names two floors, a release and a build, and passes when either is met.
func IsGTEVersionWithTimestamp(curVersion, minVersion string) (bool, error) {
	curVersion, minVersion, err := parseVersions(curVersion, minVersion)
	if err != nil {
		return false, err
	}

	curBuild := timestampedBuild.MatchString(curVersion)
	if curBuild == timestampedBuild.MatchString(minVersion) {
		return semver.Compare(curVersion, minVersion) >= 0, nil
	}

	minLine := semver.MajorMinor(minVersion)
	if lines := semver.Compare(semver.MajorMinor(curVersion), minLine); lines != 0 {
		return lines > 0, nil
	}

	return curBuild && semver.Compare(minVersion, minLine+".0") <= 0, nil
}
