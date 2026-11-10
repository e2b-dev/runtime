//go:build linux

package startupreclaim

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

// staleSuffix names a cache dir moved aside at startup and left for removeStale.
const staleSuffix = ".stale-"

// resetCacheDirs empties each of dirs, keeping the directories themselves, and
// returns how many it reset and the trees removeStale still has to delete.
// Their entries are named by IDs the previous process minted at runtime, so
// nothing reads one back after a restart.
//
// A non-empty dir is renamed to a sibling and recreated, so startup does not
// wait on deleting millions of entries. One that cannot be renamed, a symlink
// or a mount point, is emptied in place. A dir that is, or holds, a path in
// keep or another of dirs is skipped and reported: a cache path pointed at the
// base directory must not take the shared chunk cache or a mounted volume with it.
// The checks compare each path as written, with its symlinks resolved, and by
// where it lives on its filesystem: emptying a symlinked dir deletes what its
// target holds, and a bind mount shows a directory under a second name.
func resetCacheDirs(dirs, keep []string) (int, []string, []error) {
	mounts, err := readMounts(mountInfoPath)
	if err != nil {
		return 0, nil, []error{fmt.Errorf("not resetting cache dirs: %w", err)}
	}

	return resetCacheDirsOn(dirs, keep, mounts)
}

func resetCacheDirsOn(dirs, keep []string, mounts mountTable) (int, []string, []error) {
	dirPaths, err := absPaths(dirs)
	if err != nil {
		return 0, nil, []error{err}
	}

	keepPaths, err := absPaths(keep)
	if err != nil {
		return 0, nil, []error{err}
	}

	cacheDirs := withResolved(dirPaths, mounts)
	kept := withResolved(keepPaths, mounts)

	reset := 0
	var stale []string
	var failures []error
	for _, d := range cacheDirs {
		dir := d.path
		if d.isRoot() {
			failures = append(failures, fmt.Errorf("not emptying %s: it is the filesystem root", dir))

			continue
		}

		if held, ok := heldPath(d, cacheDirs, kept); ok {
			failures = append(failures, fmt.Errorf("not emptying %s: it holds %s", dir, held))

			continue
		}

		// Trees an earlier process moved aside but did not finish deleting:
		// only names moveAside creates, and never one that is or holds a
		// protected path or another cache dir.
		leftovers, err := filepath.Glob(dir + staleSuffix + "*")
		if err != nil {
			failures = append(failures, fmt.Errorf("failed to list stale copies of %s: %w", dir, err))
		}
		for _, leftover := range leftovers {
			if !isStaleName(dir, leftover) {
				continue
			}

			l := withResolved([]string{leftover}, mounts)[0]
			if held, ok := heldPath(l, cacheDirs, kept); ok {
				failures = append(failures, fmt.Errorf("not removing %s: it is or holds %s", leftover, held))

				continue
			}

			stale = append(stale, leftover)
		}

		empty, err := isEmptyDir(dir)
		if err != nil {
			if !os.IsNotExist(err) {
				failures = append(failures, fmt.Errorf("failed to read %s: %w", dir, err))
			}

			continue
		}

		if empty {
			continue
		}

		moved, err := moveAside(dir)
		if moved != "" {
			stale = append(stale, moved)
			reset++
			if err != nil {
				failures = append(failures, err)
			}

			continue
		}

		if errs := emptyDir(dir); len(errs) > 0 {
			failures = append(failures, errs...)

			continue
		}

		reset++
	}

	return reset, stale, failures
}

// moveAside renames dir to a fresh sibling and recreates it with the same
// permissions, returning the sibling once dir has been recreated. A symlink is
// refused: it would come back as a real directory, possibly on another
// filesystem than the one it pointed to.
func moveAside(dir string) (string, error) {
	info, err := os.Lstat(dir)
	if err != nil {
		return "", err
	}

	if !info.IsDir() {
		return "", fmt.Errorf("%s is not a directory", dir)
	}

	stale := dir + staleSuffix + strconv.FormatInt(time.Now().UnixNano(), 10)
	if err := os.Rename(dir, stale); err != nil {
		return "", err
	}

	// Chmod as well, since Mkdir applies the umask.
	if err := os.Mkdir(dir, info.Mode().Perm()); err != nil {
		return "", errors.Join(fmt.Errorf("failed to recreate %s: %w", dir, err), os.Rename(stale, dir))
	}

	if err := os.Chmod(dir, info.Mode().Perm()); err != nil {
		return stale, fmt.Errorf("failed to restore the mode of %s: %w", dir, err)
	}

	return stale, nil
}

// emptyDir removes every entry of dir.
func emptyDir(dir string) []error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return []error{fmt.Errorf("failed to read %s: %w", dir, err)}
	}

	var failures []error
	for _, entry := range entries {
		path := filepath.Join(dir, entry.Name())
		if err := os.RemoveAll(path); err != nil {
			failures = append(failures, fmt.Errorf("failed to remove %s: %w", path, err))
		}
	}

	return failures
}

func isEmptyDir(dir string) (bool, error) {
	f, err := os.Open(dir)
	if err != nil {
		return false, err
	}
	defer f.Close()

	if _, err := f.Readdirnames(1); err != nil {
		if errors.Is(err, io.EOF) {
			return true, nil
		}

		return false, err
	}

	return false, nil
}

// removeStale deletes the trees resetCacheDirs moved aside. A tree it does not
// finish before the process exits is picked up again by the next startup.
func removeStale(ctx context.Context, paths []string) {
	for _, path := range paths {
		start := time.Now()
		if err := os.RemoveAll(path); err != nil {
			logger.L().Warn(ctx, "startup reclaim: failed to remove stale cache directory", zap.String("path", path), zap.Error(err))

			continue
		}

		logger.L().Info(ctx, "startup reclaim: removed stale cache directory", zap.String("path", path), zap.Duration("duration", time.Since(start)))
	}
}

// absPaths returns paths absolute, without empty entries.
func absPaths(paths []string) ([]string, error) {
	out := make([]string, 0, len(paths))
	for _, path := range paths {
		if path == "" {
			continue
		}

		abs, err := filepath.Abs(path)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve %q to an absolute path: %w", path, err)
		}

		out = append(out, abs)
	}

	return out, nil
}

// isStaleName reports whether path is a name moveAside gives dir.
func isStaleName(dir, path string) bool {
	suffix, ok := strings.CutPrefix(path, dir+staleSuffix)
	if !ok || suffix == "" {
		return false
	}

	for _, r := range suffix {
		if r < '0' || r > '9' {
			return false
		}
	}

	return true
}

// guardPath is a path as configured, with its symlinks resolved when it exists,
// the filesystem and directory within it that the resolved path shows, and every
// filesystem directory reachable through its tree: its own, and that of each
// mount below it.
type guardPath struct {
	path     string
	resolved string
	device   string
	fsPath   string
	reach    []fsLocation
}

// fsLocation is a directory of one filesystem.
type fsLocation struct {
	device string
	path   string
}

func withResolved(paths []string, mounts mountTable) []guardPath {
	out := make([]guardPath, 0, len(paths))
	for _, path := range paths {
		resolved := path
		if r, err := filepath.EvalSymlinks(path); err == nil {
			resolved = r
		}

		device, fsPath := mounts.locate(resolved)
		var reach []fsLocation
		if device != "" {
			reach = append(reach, fsLocation{device: device, path: fsPath})
		}
		for _, mount := range mounts {
			if mount.target != resolved && within(resolved, mount.target) {
				reach = append(reach, fsLocation{device: mount.device, path: mount.root})
			}
		}

		out = append(out, guardPath{path: path, resolved: resolved, device: device, fsPath: fsPath, reach: reach})
	}

	return out
}

// sameDir reports whether p and other name one directory, in any form.
func (p guardPath) sameDir(other guardPath) bool {
	return p.path == other.path || p.resolved == other.resolved ||
		(p.device != "" && p.device == other.device && p.fsPath == other.fsPath)
}

func (p guardPath) isRoot() bool {
	return p.path == filepath.Dir(p.path) || p.resolved == filepath.Dir(p.resolved)
}

// holds reports whether other is p or lies below it, comparing every
// combination of configured and resolved forms, and whether other's place on
// disk is reachable through p's tree, including through mounts below p that
// other is also mounted from under another name.
func (p guardPath) holds(other guardPath) bool {
	for _, dir := range []string{p.path, p.resolved} {
		for _, path := range []string{other.path, other.resolved} {
			if within(dir, path) {
				return true
			}
		}
	}

	if other.device == "" {
		return false
	}

	for _, location := range p.reach {
		if location.device == other.device && within(location.path, other.fsPath) {
			return true
		}
	}

	return false
}

// heldPath returns the first path in keep, or in dirs other than dir itself,
// that is dir or lies below it. A cache dir that resolves to the same directory
// as dir is dir itself, not a dir it holds: two names for one cache are both
// emptied.
func heldPath(dir guardPath, dirs, keep []guardPath) (string, bool) {
	for _, path := range keep {
		if dir.holds(path) {
			return path.path, true
		}
	}

	for _, path := range dirs {
		if path.sameDir(dir) {
			continue
		}

		if dir.holds(path) {
			return path.path, true
		}
	}

	return "", false
}

func within(dir, path string) bool {
	rel, err := filepath.Rel(dir, path)

	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}
