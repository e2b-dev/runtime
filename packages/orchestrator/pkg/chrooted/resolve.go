//go:build linux

package chrooted

import (
	"errors"
	"os"
	"path"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

// maxSymlinkHops matches the kernel's limit on symlink traversals in one
// path resolution.
const maxSymlinkHops = 40

// maxResolveSteps bounds the components one resolution visits, symlink
// targets included, so a tenant's chain of long links costs linear time and
// no more than this.
const maxResolveSteps = 4096

// kernelPath is the form handed to openat2: absolute, otherwise untouched.
// Collapsing ".." here would decide lexically what the kernel decides
// physically ("/link/../x" is a sibling of the link's target, not of the
// link), so the components stay as given and RESOLVE_IN_ROOT clamps them.
// It also names files in errors and in File.Name.
func kernelPath(name string) string {
	if !strings.HasPrefix(name, "/") {
		return "/" + name
	}

	return name
}

// displayPath is the name reported for an open file: absolute inside the
// tree and lexically cleaned, as os.File.Name would show it. It is never used
// to address anything.
func displayPath(name string) string {
	return path.Clean(kernelPath(name))
}

// splitParent splits a path inside the tree into its parent directory and
// final component without collapsing anything, and reports whether the path
// ended in a slash, which the kernel takes as a promise that the entry is a
// directory. The root's parent is the root itself and its final component is
// ".", so the callers' *at syscalls address the root directory.
func splitParent(name string) (dir, base string, trailingSlash bool) {
	name = kernelPath(name)
	trailingSlash = len(name) > 1 && strings.HasSuffix(name, "/")

	name = strings.TrimRight(name, "/")
	if name == "" {
		return "/", ".", false
	}

	i := strings.LastIndex(name, "/")
	if i == 0 {
		return "/", name[1:], trailingSlash
	}

	return name[:i], name[i+1:], trailingSlash
}

func splitPath(p string) []string {
	return strings.Split(strings.Trim(p, "/"), "/")
}

// resolvePath resolves the symlinks in name the way the kernel does for a
// chrooted process and reports the resulting path inside the tree, which is
// what EvalSymlinks and GetEntry need; the operations themselves let openat2
// resolve.
//
// The walk holds the descriptor of the directory it has reached and moves
// one component at a time with fd-relative syscalls, so it is linear in the
// components visited and never leaves the tree: ".." is clamped at the root,
// an absolute symlink target restarts at the root, and a relative one
// continues from the link's directory. A component that must be a directory
// and is not fails with ENOTDIR, as it does for the kernel. The caller holds
// the root.
func (fs *Chrooted) resolvePath(name string, followFinal bool) (string, error) {
	remaining := splitPath(kernelPath(name))
	resolved := "/"

	// cur is the directory the walk has reached; the root itself is never
	// closed, so it is tracked separately from descriptors this walk opened.
	cur := fs.rootFD
	closeCur := func() {
		if cur != fs.rootFD {
			_ = unix.Close(cur)
		}
	}
	defer closeCur()

	hops := 0
	for steps := 0; len(remaining) > 0; steps++ {
		if steps >= maxResolveSteps {
			return "", &os.PathError{Op: "resolve", Path: kernelPath(name), Err: syscall.ENAMETOOLONG}
		}

		component := remaining[0]
		remaining = remaining[1:]

		switch component {
		case "", ".":
			continue
		case "..":
			if resolved == "/" {
				continue
			}

			parent, err := retryEINTR(func() (int, error) {
				return unix.Openat(cur, "..", unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
			})
			if err != nil {
				return "", &os.PathError{Op: "resolve", Path: resolved, Err: err}
			}

			closeCur()
			cur = parent
			resolved = path.Dir(resolved)

			continue
		}

		candidate := path.Join(resolved, component)

		var sys unix.Stat_t
		if err := ignoringEINTR(func() error {
			return unix.Fstatat(cur, component, &sys, unix.AT_SYMLINK_NOFOLLOW)
		}); err != nil {
			return "", &os.PathError{Op: "resolve", Path: candidate, Err: err}
		}

		switch sys.Mode & unix.S_IFMT {
		case unix.S_IFLNK:
			if len(remaining) == 0 && !followFinal {
				return candidate, nil
			}

			hops++
			if hops > maxSymlinkHops {
				resolveLoops.Add(1)

				return "", &os.PathError{Op: "resolve", Path: candidate, Err: syscall.ELOOP}
			}

			target, err := readlinkAt(cur, component)
			if err != nil {
				return "", &os.PathError{Op: "resolve", Path: candidate, Err: err}
			}

			if path.IsAbs(target) {
				closeCur()
				cur = fs.rootFD
				resolved = "/"
			}

			remaining = append(splitPath(target), remaining...)

		case unix.S_IFDIR:
			if len(remaining) == 0 {
				return candidate, nil
			}

			next, err := retryEINTR(func() (int, error) {
				return unix.Openat(cur, component, unix.O_PATH|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
			})
			if err != nil {
				return "", &os.PathError{Op: "resolve", Path: candidate, Err: err}
			}

			closeCur()
			cur = next
			resolved = candidate

		default:
			if len(remaining) == 0 {
				return candidate, nil
			}

			return "", &os.PathError{Op: "resolve", Path: candidate, Err: syscall.ENOTDIR}
		}
	}

	return resolved, nil
}

// readlinkAt reads the target of the symlink name in the open directory dirFD.
func readlinkAt(dirFD int, name string) (string, error) {
	for size := 128; ; size *= 2 {
		buf := make([]byte, size)

		n, err := unix.Readlinkat(dirFD, name, buf)
		if err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}

			return "", err
		}

		if n < size {
			return string(buf[:n]), nil
		}
	}
}
