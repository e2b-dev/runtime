//go:build linux

package startupreclaim

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const mountInfoPath = "/proc/self/mountinfo"

// mountEntry is one mount: the filesystem it shows (device), the directory of
// that filesystem it shows (root) and where it shows it (target).
type mountEntry struct {
	device string
	root   string
	target string
}

type mountTable []mountEntry

// readMounts parses a mountinfo file.
func readMounts(path string) (mountTable, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read %s: %w", path, err)
	}

	var mounts mountTable
	for line := range strings.Lines(string(data)) {
		fields := strings.Fields(line)
		if len(fields) < 5 {
			continue
		}

		mounts = append(mounts, mountEntry{
			device: fields[2],
			root:   unescapeMountPath(fields[3]),
			target: unescapeMountPath(fields[4]),
		})
	}

	return mounts, nil
}

// locate returns the filesystem path lies on and the directory within it, from
// the deepest mount holding path. Of mounts stacked on one target, the last
// mounted is the one visible.
func (m mountTable) locate(path string) (string, string) {
	best := -1
	for i, mount := range m {
		if !within(mount.target, path) {
			continue
		}

		if best < 0 || len(mount.target) >= len(m[best].target) {
			best = i
		}
	}

	if best < 0 {
		return "", ""
	}

	rel, err := filepath.Rel(m[best].target, path)
	if err != nil {
		return "", ""
	}

	return m[best].device, filepath.Join(m[best].root, rel)
}

// unescapeMountPath decodes the octal escapes mountinfo writes for spaces,
// tabs, newlines and backslashes in paths, such as \040 for a space.
func unescapeMountPath(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}

	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+4 <= len(s) {
			if v, err := strconv.ParseUint(s[i+1:i+4], 8, 8); err == nil {
				b.WriteByte(byte(v))
				i += 3

				continue
			}
		}

		b.WriteByte(s[i])
	}

	return b.String()
}
