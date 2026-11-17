//go:build unix

package permissions

import (
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

// rootedButNotAbs is false: a rooted path here is absolute.
func rootedButNotAbs(string) bool { return false }

// refuseAmbiguousRelative accepts every relative path: each is under the home.
func refuseAmbiguousRelative(string) error { return nil }
