package api

import (
	"errors"

	"golang.org/x/sys/unix"
)

func (execNFSMounter) Unmount(path string) error {
	// MNT_FORCE aborts requests still waiting on the old server connection, which the
	// next mount of the same server would otherwise queue behind.
	err := unix.Unmount(path, unix.MNT_FORCE|unix.MNT_DETACH)
	if errors.Is(err, unix.EINVAL) || errors.Is(err, unix.ENOENT) {
		return nil // not a mount point
	}

	return err
}

func (execNFSMounter) MountID(path string) (uint64, error) {
	var st unix.Statx_t
	// Asks for no attributes the NFS client would fetch from the server.
	if err := unix.Statx(unix.AT_FDCWD, path, unix.AT_STATX_DONT_SYNC, unix.STATX_MNT_ID, &st); err != nil {
		return 0, err
	}
	if st.Mask&unix.STATX_MNT_ID == 0 {
		return 0, errors.New("the kernel does not report mount IDs")
	}

	return st.Mnt_id, nil
}
