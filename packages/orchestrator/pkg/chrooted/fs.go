//go:build linux

package chrooted

import (
	"errors"
	"io"
	"math/rand/v2"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"

	"github.com/e2b-dev/infra/packages/shared/pkg/filesystem"
)

func pathError(op, name string, err error) error {
	return &os.PathError{Op: op, Path: kernelPath(name), Err: err}
}

// open resolves name inside the tree and opens it with flags. The caller
// owns the returned descriptor.
func (fs *Chrooted) open(op, name string, flags int, mode os.FileMode) (int, error) {
	fd, err := openat2(fs.rootFD, kernelPath(name), flags|unix.O_CLOEXEC, syscallMode(mode))
	if err != nil {
		return -1, pathError(op, name, err)
	}

	return fd, nil
}

// parent is an open directory together with the name of one of its entries,
// for the *at syscalls that must act on the entry itself rather than on what
// a symlink there points to.
type parent struct {
	fd   int
	base string
	// trailingSlash records that the caller wrote the entry as "name/", which
	// the kernel takes as a promise that the entry is a directory.
	trailingSlash bool
}

func (p parent) close() {
	_ = unix.Close(p.fd)
}

// requireDirectory enforces the trailing-slash promise the way the kernel
// does for a path it resolves itself: the entry must exist and be a directory.
func (p parent) requireDirectory() error {
	if !p.trailingSlash {
		return nil
	}

	var sys unix.Stat_t
	if err := ignoringEINTR(func() error {
		return unix.Fstatat(p.fd, p.base, &sys, unix.AT_SYMLINK_NOFOLLOW)
	}); err != nil {
		return err
	}
	if sys.Mode&unix.S_IFMT != unix.S_IFDIR {
		return unix.ENOTDIR
	}

	return nil
}

// openParent opens the directory holding name.
//
// Only the directory goes through openat2; the final component is handed to
// the syscall as a plain name relative to that descriptor. A final "." or
// ".." would therefore name the directory itself or, at the root, the host
// directory above the tree, so both are refused. The kernel refuses to
// create, unlink or rename "." and ".." as well.
func (fs *Chrooted) openParent(op, name string) (parent, error) {
	dir, base, trailingSlash := splitParent(name)
	if err := checkComponent(base); err != nil {
		return parent{}, pathError(op, name, err)
	}

	fd, err := openat2(fs.rootFD, dir, unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return parent{}, pathError(op, name, err)
	}

	return parent{fd: fd, base: base, trailingSlash: trailingSlash}, nil
}

func (fs *Chrooted) Create(filename string) (*os.File, error) {
	return fs.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o666)
}

func (fs *Chrooted) Open(filename string) (*os.File, error) {
	return fs.OpenFile(filename, os.O_RDONLY, 0)
}

// OpenFile opens filename inside the tree. The file's Name is the path
// inside the tree, not a host path; go-nfs relies on that for post-op
// attributes.
func (fs *Chrooted) OpenFile(filename string, flag int, perm os.FileMode) (*os.File, error) {
	release, err := fs.acquire()
	if err != nil {
		return nil, err
	}
	defer release()

	fd, err := fs.open("open", filename, flag, perm)
	if err != nil {
		return nil, err
	}

	return os.NewFile(uintptr(fd), displayPath(filename)), nil
}

// EvalSymlinks returns the path inside the tree that filename resolves to
// once every symlink in it is followed, as a chrooted process resolves it.
func (fs *Chrooted) EvalSymlinks(filename string) (string, error) {
	release, err := fs.acquire()
	if err != nil {
		return "", err
	}
	defer release()

	return fs.resolvePath(filename, true)
}

func (fs *Chrooted) stat(op, name string, flags int) (os.FileInfo, error) {
	release, err := fs.acquire()
	if err != nil {
		return nil, err
	}
	defer release()

	return fs.statLocked(op, name, flags)
}

// statLocked is stat for callers that already hold the root.
func (fs *Chrooted) statLocked(op, name string, flags int) (os.FileInfo, error) {
	fd, err := fs.open(op, name, unix.O_PATH|flags, 0)
	if err != nil {
		return nil, err
	}
	defer unix.Close(fd)

	info, err := statFD(fd, name)
	if err != nil {
		return nil, pathError(op, name, err)
	}

	return info, nil
}

func (fs *Chrooted) Stat(filename string) (os.FileInfo, error) {
	return fs.stat("stat", filename, 0)
}

func (fs *Chrooted) Lstat(filename string) (os.FileInfo, error) {
	return fs.stat("lstat", filename, unix.O_NOFOLLOW)
}

// GetEntry describes the entry at filename. A symlink is reported with the
// path it resolves to inside the tree and the type of that target.
func (fs *Chrooted) GetEntry(filename string) (filesystem.EntryInfo, error) {
	info, err := fs.Lstat(filename)
	if err != nil {
		return filesystem.EntryInfo{}, err
	}

	return fs.EntryInfo(filename, info), nil
}

// EntryInfo describes the entry at filename from an Lstat of it, resolving
// a symlink inside the tree rather than on the host. Callers listing a
// directory use it to describe each entry without another stat.
func (fs *Chrooted) EntryInfo(filename string, info os.FileInfo) filesystem.EntryInfo {
	return filesystem.GetEntryInfoWithResolver(filename, info, func(p string) (string, os.FileInfo) {
		target, err := fs.EvalSymlinks(p)
		if err != nil {
			return p, nil
		}

		targetInfo, err := fs.Stat(target)
		if err != nil {
			return target, nil
		}

		return target, targetInfo
	})
}

// Rename renames oldpath to newpath with the filesystem's own semantics: an
// existing empty directory at newpath is replaced, a non-empty one fails
// with ENOTEMPTY, and a file over a directory fails with EISDIR. (os.Rename
// reports EEXIST for all three.)
func (fs *Chrooted) Rename(oldpath, newpath string) error {
	release, err := fs.acquire()
	if err != nil {
		return err
	}
	defer release()

	from, err := fs.openParent("rename", oldpath)
	if err != nil {
		return err
	}
	defer from.close()

	to, err := fs.openParent("rename", newpath)
	if err != nil {
		return err
	}
	defer to.close()

	if to.trailingSlash {
		// "dir/" as the destination promises a directory is being renamed.
		from.trailingSlash = true
	}
	if err := from.requireDirectory(); err != nil {
		return &os.LinkError{Op: "rename", Old: kernelPath(oldpath), New: kernelPath(newpath), Err: err}
	}

	if err := ignoringEINTR(func() error {
		return unix.Renameat(from.fd, from.base, to.fd, to.base)
	}); err != nil {
		return &os.LinkError{Op: "rename", Old: kernelPath(oldpath), New: kernelPath(newpath), Err: err}
	}

	return nil
}

// Remove removes the file or empty directory at filename, like os.Remove.
func (fs *Chrooted) Remove(filename string) error {
	release, err := fs.acquire()
	if err != nil {
		return err
	}
	defer release()

	p, err := fs.openParent("remove", filename)
	if err != nil {
		return err
	}
	defer p.close()

	if err := p.requireDirectory(); err != nil {
		return pathError("remove", filename, err)
	}

	unlinkErr := ignoringEINTR(func() error { return unix.Unlinkat(p.fd, p.base, 0) })
	if unlinkErr == nil {
		return nil
	}

	rmdirErr := ignoringEINTR(func() error { return unix.Unlinkat(p.fd, p.base, unix.AT_REMOVEDIR) })
	if rmdirErr == nil {
		return nil
	}

	// Report the error of the call that matched the entry's type, as
	// os.Remove does.
	if errors.Is(rmdirErr, unix.ENOTDIR) {
		return pathError("remove", filename, unlinkErr)
	}

	return pathError("remove", filename, rmdirErr)
}

// RemoveAll removes filename and everything below it, like os.RemoveAll. It
// descends through directory descriptors, never through paths, so nothing
// renamed during the walk can lead it out of the tree, and it re-reads each
// directory until it is empty, so entries created while it runs are removed
// too.
func (fs *Chrooted) RemoveAll(filename string) error {
	release, err := fs.acquire()
	if err != nil {
		return err
	}
	defer release()

	p, err := fs.openParent("removeall", filename)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}

		return err
	}
	defer p.close()

	if err := p.requireDirectory(); err != nil {
		if errors.Is(err, unix.ENOENT) {
			return nil
		}

		return pathError("removeall", filename, err)
	}

	if err := removeAllAt(p.fd, p.base); err != nil {
		return pathError("removeall", filename, err)
	}

	return nil
}

// removeAllBatch is how many names one pass over a directory takes before
// removing them, so a very large directory is never held in memory whole.
const removeAllBatch = 1024

func removeAllAt(dirFD int, name string) error {
	if err := checkComponent(name); err != nil {
		return err
	}

	unlinkErr := ignoringEINTR(func() error { return unix.Unlinkat(dirFD, name, 0) })
	if unlinkErr == nil || errors.Is(unlinkErr, unix.ENOENT) {
		return nil
	}
	// EISDIR is a directory; EPERM and EACCES may be one on filesystems that
	// report them for unlink of a directory, as os.RemoveAll assumes too.
	if !errors.Is(unlinkErr, unix.EISDIR) && !errors.Is(unlinkErr, unix.EPERM) && !errors.Is(unlinkErr, unix.EACCES) {
		return unlinkErr
	}

	for {
		fd, err := retryEINTR(func() (int, error) {
			return unix.Openat(dirFD, name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		})
		if err != nil {
			if errors.Is(err, unix.ENOENT) {
				return nil
			}
			if errors.Is(err, unix.ENOTDIR) {
				// Not a directory after all: the unlink error is the real one.
				return unlinkErr
			}

			return err
		}

		dir := os.NewFile(uintptr(fd), name)
		names, readErr := dir.Readdirnames(removeAllBatch)

		var firstErr error
		for _, child := range names {
			if err := removeAllAt(fd, child); err != nil && firstErr == nil {
				firstErr = err
			}
		}

		if err := dir.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		if firstErr != nil {
			return firstErr
		}
		// io.EOF from Readdirnames means the directory is empty.
		if readErr != nil && !errors.Is(readErr, io.EOF) && !errors.Is(readErr, os.ErrNotExist) {
			return readErr
		}
		if len(names) == 0 {
			break
		}
	}

	err := ignoringEINTR(func() error { return unix.Unlinkat(dirFD, name, unix.AT_REMOVEDIR) })
	if err == nil || errors.Is(err, unix.ENOENT) {
		return nil
	}

	return err
}

func (fs *Chrooted) Join(elem ...string) string {
	joined := filepath.Join(elem...)
	if !strings.HasPrefix(joined, "/") {
		joined = "/" + joined
	}

	return joined
}

// maxTempFileAttempts bounds the search for an unused temporary file name,
// like os.CreateTemp does.
const maxTempFileAttempts = 10000

// TempFile creates a new file in dir with a name starting with prefix, open
// for reading and writing. An empty dir means the root of the tree.
func (fs *Chrooted) TempFile(dir, prefix string) (*os.File, error) {
	if dir == "" {
		dir = "/"
	}

	for range maxTempFileAttempts {
		name := path.Join(dir, prefix+strconv.FormatUint(uint64(rand.Uint32()), 10))

		f, err := fs.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
		if errors.Is(err, os.ErrExist) {
			continue
		}

		return f, err
	}

	return nil, &os.PathError{Op: "createtemp", Path: path.Join(dir, prefix+"*"), Err: os.ErrExist}
}

// ReadDir lists the directory at dirPath sorted by name, as os.ReadDir does.
// Each entry carries its own attributes, not those of a symlink's target. An
// entry removed while the directory is being read is left out.
func (fs *Chrooted) ReadDir(dirPath string) ([]os.FileInfo, error) {
	release, err := fs.acquire()
	if err != nil {
		return nil, err
	}
	defer release()

	fd, err := fs.open("readdir", dirPath, unix.O_RDONLY|unix.O_DIRECTORY, 0)
	if err != nil {
		return nil, err
	}

	dir := os.NewFile(uintptr(fd), displayPath(dirPath))
	defer dir.Close()

	names, err := dir.Readdirnames(-1)
	if err != nil {
		return nil, pathError("readdir", dirPath, err)
	}
	slices.Sort(names)

	infos := make([]os.FileInfo, 0, len(names))
	for _, name := range names {
		info, err := lstatAt(fd, name)
		if err != nil {
			if errors.Is(err, unix.ENOENT) {
				continue
			}

			return nil, pathError("readdir", path.Join(dirPath, name), err)
		}

		infos = append(infos, info)
	}

	return infos, nil
}

func (fs *Chrooted) Mkdir(filename string, perm os.FileMode) error {
	release, err := fs.acquire()
	if err != nil {
		return err
	}
	defer release()

	return fs.mkdir(filename, perm)
}

func (fs *Chrooted) mkdir(filename string, perm os.FileMode) error {
	p, err := fs.openParent("mkdir", filename)
	if err != nil {
		return err
	}
	defer p.close()

	if err := ignoringEINTR(func() error { return unix.Mkdirat(p.fd, p.base, syscallMode(perm)) }); err != nil {
		return pathError("mkdir", filename, err)
	}

	return nil
}

// MkdirAll creates the directory at filename and any missing parents, like
// os.MkdirAll.
func (fs *Chrooted) MkdirAll(filename string, perm os.FileMode) error {
	release, err := fs.acquire()
	if err != nil {
		return err
	}
	defer release()

	return fs.mkdirAll(kernelPath(filename), perm)
}

func (fs *Chrooted) mkdirAll(name string, perm os.FileMode) error {
	if info, err := fs.statLocked("mkdir", name, 0); err == nil {
		if info.IsDir() {
			return nil
		}

		return pathError("mkdir", name, syscall.ENOTDIR)
	}

	if dir, _, _ := splitParent(name); dir != name {
		if err := fs.mkdirAll(dir, perm); err != nil {
			return err
		}
	}

	if err := fs.mkdir(name, perm); err != nil {
		// Created concurrently, reached through a symlink, or named with a
		// final "." or ".." that the recursion above has already created.
		if info, statErr := fs.statLocked("mkdir", name, 0); statErr == nil && info.IsDir() {
			return nil
		}

		return err
	}

	return nil
}

func (fs *Chrooted) Symlink(target, link string) error {
	release, err := fs.acquire()
	if err != nil {
		return err
	}
	defer release()

	p, err := fs.openParent("symlink", link)
	if err != nil {
		return err
	}
	defer p.close()

	if p.trailingSlash {
		// symlink(2) with a trailing slash: EEXIST when the name exists,
		// ENOENT when it does not, never a new link.
		err := p.requireDirectory()
		if err == nil || errors.Is(err, unix.ENOTDIR) {
			err = unix.EEXIST
		}

		return &os.LinkError{Op: "symlink", Old: target, New: kernelPath(link), Err: err}
	}

	if err := ignoringEINTR(func() error { return unix.Symlinkat(target, p.fd, p.base) }); err != nil {
		return &os.LinkError{Op: "symlink", Old: target, New: kernelPath(link), Err: err}
	}

	return nil
}

func (fs *Chrooted) Readlink(link string) (string, error) {
	release, err := fs.acquire()
	if err != nil {
		return "", err
	}
	defer release()

	return fs.readlinkLocked(link)
}

// readlinkLocked is Readlink for callers that already hold the root.
func (fs *Chrooted) readlinkLocked(link string) (string, error) {
	fd, err := fs.open("readlink", link, unix.O_PATH|unix.O_NOFOLLOW, 0)
	if err != nil {
		return "", err
	}
	defer unix.Close(fd)

	target, err := readlinkFD(fd)
	if err != nil {
		return "", pathError("readlink", link, err)
	}

	return target, nil
}

func (fs *Chrooted) Chroot(_ string) (*Chrooted, error) {
	return nil, errors.New("chroot not supported")
}

func (fs *Chrooted) Root() string {
	return fs.ActualRoot
}
