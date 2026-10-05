//go:build linux

package chrooted

import (
	"errors"
	"os"
	"time"

	"golang.org/x/sys/unix"
)

// Chmod changes the mode of the file at name, following a symlink there.
func (fs *Chrooted) Chmod(name string, mode os.FileMode) error {
	release, err := fs.acquire()
	if err != nil {
		return err
	}
	defer release()

	fd, err := fs.open("chmod", name, unix.O_PATH, 0)
	if err != nil {
		return err
	}
	defer unix.Close(fd)

	bits := syscallMode(mode)

	// fchmodat2 (Linux 6.6) takes the descriptor itself. Before that the
	// descriptor is addressed through its procfs link.
	err = ignoringEINTR(func() error { return unix.Fchmodat(fd, "", bits, unix.AT_EMPTY_PATH) })
	if errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.ENOSYS) {
		err = ignoringEINTR(func() error { return unix.Fchmodat(unix.AT_FDCWD, procFDPath(fd), bits, 0) })
	}
	if err != nil {
		return pathError("chmod", name, err)
	}

	return nil
}

func (fs *Chrooted) Lchown(name string, uid, gid int) error {
	return fs.chown("lchown", name, unix.O_NOFOLLOW, uid, gid)
}

func (fs *Chrooted) Chown(name string, uid, gid int) error {
	return fs.chown("chown", name, 0, uid, gid)
}

func (fs *Chrooted) chown(op, name string, flags, uid, gid int) error {
	release, err := fs.acquire()
	if err != nil {
		return err
	}
	defer release()

	fd, err := fs.open(op, name, unix.O_PATH|flags, 0)
	if err != nil {
		return err
	}
	defer unix.Close(fd)

	if err := ignoringEINTR(func() error { return unix.Fchownat(fd, "", uid, gid, unix.AT_EMPTY_PATH) }); err != nil {
		return pathError(op, name, err)
	}

	return nil
}

// Chtimes sets the access and modification times of the file at name,
// following a symlink there. A zero time leaves that timestamp unchanged.
func (fs *Chrooted) Chtimes(name string, atime, mtime time.Time) error {
	release, err := fs.acquire()
	if err != nil {
		return err
	}
	defer release()

	fd, err := fs.open("chtimes", name, unix.O_PATH, 0)
	if err != nil {
		return err
	}
	defer unix.Close(fd)

	times := []unix.Timespec{timespec(atime), timespec(mtime)}

	// utimensat accepts AT_EMPTY_PATH since Linux 5.8. Before that the
	// descriptor is addressed through its procfs link.
	err = ignoringEINTR(func() error { return unix.UtimesNanoAt(fd, "", times, unix.AT_EMPTY_PATH) })
	if errors.Is(err, unix.EINVAL) || errors.Is(err, unix.ENOENT) || errors.Is(err, unix.EOPNOTSUPP) {
		err = ignoringEINTR(func() error { return unix.UtimesNanoAt(unix.AT_FDCWD, procFDPath(fd), times, 0) })
	}
	if err != nil {
		return pathError("chtimes", name, err)
	}

	return nil
}

func timespec(t time.Time) unix.Timespec {
	if t.IsZero() {
		return unix.Timespec{Nsec: unix.UTIME_OMIT}
	}

	return unix.NsecToTimespec(t.UnixNano())
}
