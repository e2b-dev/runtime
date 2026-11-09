//go:build unix

package permissions

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
)

func getSubpaths(path string) (subpaths []string) {
	for {
		subpaths = append(subpaths, path)

		path = filepath.Dir(path)
		if path == "/" {
			break
		}
	}

	slices.Reverse(subpaths)

	return subpaths
}

func EnsureDirs(path string, uid, gid int) error {
	subpaths := getSubpaths(path)
	for _, subpath := range subpaths {
		info, err := os.Stat(subpath)
		if err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("failed to stat directory: %w", err)
		}

		if err != nil && os.IsNotExist(err) {
			err = os.Mkdir(subpath, 0o755)
			if err != nil && !os.IsExist(err) {
				return fmt.Errorf("failed to create directory: %w", err)
			}

			// Chown even if another process created the directory (os.IsExist),
			// since it may not have called Chown yet. Chown is idempotent.
			err = Chown(subpath, uid, gid)
			if err != nil {
				return fmt.Errorf("failed to chown directory: %w", err)
			}

			continue
		}

		if !info.IsDir() {
			return fmt.Errorf("path is a file: %s", subpath)
		}
	}

	return nil
}

// rootedButNotAbs is false: a rooted path here is absolute.
func rootedButNotAbs(string) bool { return false }

// refuseAmbiguousRelative accepts every relative path: each is under the home.
func refuseAmbiguousRelative(string) error { return nil }
