//go:build linux

package chrooted

import (
	"errors"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

// resolveFlags make openat2 resolve every path the way the kernel resolves
// one for a chrooted process, and refuse procfs magic links.
const resolveFlags = unix.RESOLVE_IN_ROOT | unix.RESOLVE_NO_MAGICLINKS

// maxResolveRetries bounds the retries of an openat2 that the kernel aborted
// with EAGAIN because a rename or mount raced the resolution.
const maxResolveRetries = 32

// openat2 resolves name inside the tree rooted at rootFD and opens it with
// flags. name is a path inside the tree; with RESOLVE_IN_ROOT an absolute
// path and an absolute symlink target both start at rootFD.
func openat2(rootFD int, name string, flags int, mode uint32) (int, error) {
	how := unix.OpenHow{
		Flags:   uint64(flags),
		Resolve: resolveFlags,
	}
	// openat2 rejects a mode on a call that cannot create anything. O_TMPFILE
	// shares a bit with O_DIRECTORY, so it is compared whole.
	if flags&unix.O_CREAT != 0 || flags&unix.O_TMPFILE == unix.O_TMPFILE {
		how.Mode = uint64(mode)
	}

	for attempt := range maxResolveRetries {
		if attempt > 0 {
			// Let whatever rename or mount raced the walk finish.
			runtime.Gosched()
		}

		fd, err := retryEINTR(func() (int, error) {
			return unix.Openat2(rootFD, name, &how)
		})
		if !errors.Is(err, unix.EAGAIN) {
			return fd, err
		}
	}

	resolveRetriesExhausted.Add(1)

	return -1, unix.EAGAIN
}

func retryEINTR(fn func() (int, error)) (int, error) {
	for {
		fd, err := fn()
		if !errors.Is(err, unix.EINTR) {
			return fd, err
		}
	}
}

func ignoringEINTR(fn func() error) error {
	for {
		err := fn()
		if !errors.Is(err, unix.EINTR) {
			return err
		}
	}
}

// procFDPath names an open descriptor's object through procfs, the fallback
// for a kernel without the descriptor-only forms of chmod and utimensat. The
// link leads to the object the descriptor already refers to, so following it
// cannot leave the tree.
func procFDPath(fd int) string {
	return "/proc/self/fd/" + strconv.Itoa(fd)
}

// syscallMode converts a FileMode into the mode bits mkdirat and openat take.
func syscallMode(mode os.FileMode) uint32 {
	bits := uint32(mode.Perm())
	if mode&os.ModeSetuid != 0 {
		bits |= syscall.S_ISUID
	}
	if mode&os.ModeSetgid != 0 {
		bits |= syscall.S_ISGID
	}
	if mode&os.ModeSticky != 0 {
		bits |= syscall.S_ISVTX
	}

	return bits
}

// checkComponent accepts only a name that addresses one entry of a
// directory. Everything handed to a syscall relative to a directory
// descriptor, rather than resolved by openat2, must pass it: a "/" would
// start a resolution the kernel does not confine, and "." or ".." would
// address the directory itself or its parent, which at the root is outside
// the tree.
func checkComponent(name string) error {
	if name == "" || name == "." || name == ".." || strings.Contains(name, "/") {
		pathsRefused.Add(1)

		return unix.EINVAL
	}

	return nil
}

// fileInfo is the os.FileInfo for a stat taken through a descriptor. Sys
// returns a *syscall.Stat_t like the os package's own FileInfo does, which
// the NFS proxy relies on for inode numbers, link counts and ownership.
type fileInfo struct {
	name string
	mode os.FileMode
	sys  syscall.Stat_t
}

var _ os.FileInfo = (*fileInfo)(nil)

func (fi *fileInfo) Name() string       { return fi.name }
func (fi *fileInfo) Size() int64        { return fi.sys.Size }
func (fi *fileInfo) Mode() os.FileMode  { return fi.mode }
func (fi *fileInfo) ModTime() time.Time { return time.Unix(fi.sys.Mtim.Unix()) }
func (fi *fileInfo) IsDir() bool        { return fi.mode.IsDir() }
func (fi *fileInfo) Sys() any           { return &fi.sys }

// newFileInfo describes a stat result. name is the path the caller asked
// about; its base becomes the entry's Name, as with os.Stat.
func newFileInfo(name string, sys *syscall.Stat_t) *fileInfo {
	fi := &fileInfo{name: path.Base(name), sys: *sys}

	fi.mode = os.FileMode(fi.sys.Mode & 0o777)
	switch fi.sys.Mode & syscall.S_IFMT {
	case syscall.S_IFBLK:
		fi.mode |= os.ModeDevice
	case syscall.S_IFCHR:
		fi.mode |= os.ModeDevice | os.ModeCharDevice
	case syscall.S_IFDIR:
		fi.mode |= os.ModeDir
	case syscall.S_IFIFO:
		fi.mode |= os.ModeNamedPipe
	case syscall.S_IFLNK:
		fi.mode |= os.ModeSymlink
	case syscall.S_IFSOCK:
		fi.mode |= os.ModeSocket
	}
	if fi.sys.Mode&syscall.S_ISGID != 0 {
		fi.mode |= os.ModeSetgid
	}
	if fi.sys.Mode&syscall.S_ISUID != 0 {
		fi.mode |= os.ModeSetuid
	}
	if fi.sys.Mode&syscall.S_ISVTX != 0 {
		fi.mode |= os.ModeSticky
	}

	return fi
}

// statFD stats the object behind fd, which may be an O_PATH descriptor.
func statFD(fd int, name string) (*fileInfo, error) {
	var sys syscall.Stat_t
	if err := ignoringEINTR(func() error { return syscall.Fstat(fd, &sys) }); err != nil {
		return nil, err
	}

	return newFileInfo(name, &sys), nil
}

// unix.Stat_t and syscall.Stat_t are both generated from the kernel's struct
// stat for the target architecture, so one can be viewed as the other. The
// array index below fails to compile should their sizes ever differ.
var _ = [1]struct{}{}[unsafe.Sizeof(unix.Stat_t{})-unsafe.Sizeof(syscall.Stat_t{})]

// lstatAt stats the single directory entry name of the open directory
// dirFD, without following it if it is a symlink, in one syscall. Going
// through the directory's descriptor rather than a path means a directory
// swapped for a symlink after it was opened cannot redirect the stat.
func lstatAt(dirFD int, name string) (*fileInfo, error) {
	if err := checkComponent(name); err != nil {
		return nil, err
	}

	var sys unix.Stat_t
	if err := ignoringEINTR(func() error {
		return unix.Fstatat(dirFD, name, &sys, unix.AT_SYMLINK_NOFOLLOW)
	}); err != nil {
		return nil, err
	}

	return newFileInfo(name, (*syscall.Stat_t)(unsafe.Pointer(&sys))), nil
}

// readlinkFD reads the target of the symlink behind fd, an O_PATH|O_NOFOLLOW
// descriptor. Anything but a symlink is EINVAL, as readlink(2) has it;
// readlinkat with an empty path would report ENOENT instead.
func readlinkFD(fd int) (string, error) {
	var sys syscall.Stat_t
	if err := ignoringEINTR(func() error { return syscall.Fstat(fd, &sys) }); err != nil {
		return "", err
	}
	if sys.Mode&syscall.S_IFMT != syscall.S_IFLNK {
		return "", unix.EINVAL
	}

	for size := 128; ; size *= 2 {
		buf := make([]byte, size)

		n, err := unix.Readlinkat(fd, "", buf)
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
