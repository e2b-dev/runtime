//go:build unix

package filesystem

import (
	"fmt"
	"syscall"
)

// Filesystem magic numbers from Linux kernel (include/uapi/linux/magic.h)
const (
	nfsSuperMagic   = 0x6969
	cifsMagic       = 0xFF534D42
	smbSuperMagic   = 0x517B
	smb2MagicNumber = 0xFE534D42
	fuseSuperMagic  = 0x65735546
)

// IsPathOnNetworkMount checks if the given path is on a network filesystem mount.
// Returns true if the path is on NFS, CIFS, SMB, or FUSE filesystem.
func IsPathOnNetworkMount(path string) (bool, error) {
	var statfs syscall.Statfs_t
	if err := syscall.Statfs(path, &statfs); err != nil {
		return false, fmt.Errorf("failed to statfs %s: %w", path, err)
	}

	switch statfs.Type {
	case nfsSuperMagic, cifsMagic, smbSuperMagic, smb2MagicNumber, fuseSuperMagic:
		return true, nil
	default:
		return false, nil
	}
}
