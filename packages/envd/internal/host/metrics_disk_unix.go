//go:build unix

package host

import "golang.org/x/sys/unix"

// diskRoot is the filesystem the sandbox's disk usage is reported for.
func diskRoot() string { return "/" }

func diskStats(path string) (diskSpace, error) {
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		return diskSpace{}, err
	}

	block := uint64(st.Bsize)

	// all data blocks
	total := st.Blocks * block
	// blocks available
	available := st.Bavail * block

	return diskSpace{Total: total, Available: available}, nil
}
