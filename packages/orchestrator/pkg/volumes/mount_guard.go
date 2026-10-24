//go:build linux

package volumes

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// procMountsPath is the kernel's mount table. Overridable in tests.
const procMountsPath = "/proc/mounts"

// mountFSTypeFrom returns the filesystem type of the mount that contains target,
// read from a mount table in /proc/mounts format. It selects the mount whose
// mount point is the longest prefix of target, so a not-yet-created leaf resolves
// to its enclosing mount. Returns ("", nil) if no mount point is a prefix.
func mountFSTypeFrom(mountsPath, target string) (string, error) {
	f, err := os.Open(mountsPath)
	if err != nil {
		return "", fmt.Errorf("open %q: %w", mountsPath, err)
	}
	defer f.Close()

	bestLen := -1
	bestType := ""

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) < 3 {
			continue
		}
		mp, fsType := fields[1], fields[2]
		if !pathHasPrefix(target, mp) {
			continue
		}
		if len(mp) > bestLen {
			bestLen = len(mp)
			bestType = fsType
		}
	}
	if err := sc.Err(); err != nil {
		return "", fmt.Errorf("scan %q: %w", mountsPath, err)
	}

	return bestType, nil
}

// pathHasPrefix reports whether mp is a path-component prefix of target (so
// "/mnt/jfs" matches "/mnt/jfs/x" and "/mnt/jfs" itself, but not "/mnt/jfs-2").
func pathHasPrefix(target, mp string) bool {
	if mp == "/" {
		return true
	}
	if target == mp {
		return true
	}

	return strings.HasPrefix(target, mp+"/")
}

// verifyVolumeMount fails fast when expectedFSType is set and the mount
// containing path is not of that type - the usual cause being that the
// configured distributed filesystem (JuiceFS, CephFS, ...) is not actually
// mounted, so volume data would silently land on the local root disk. Empty
// expectedFSType disables the check, preserving the default local behavior.
func verifyVolumeMount(path, expectedFSType string) error {
	return verifyVolumeMountFrom(procMountsPath, path, expectedFSType)
}

func verifyVolumeMountFrom(mountsPath, path, expectedFSType string) error {
	if expectedFSType == "" {
		return nil
	}

	got, err := mountFSTypeFrom(mountsPath, path)
	if err != nil {
		return fmt.Errorf("verify volume mount for %q: %w", path, err)
	}
	if got != expectedFSType {
		return fmt.Errorf("volume path %q is on a %q mount, expected %q - is the configured volume filesystem mounted?", path, got, expectedFSType)
	}

	return nil
}
