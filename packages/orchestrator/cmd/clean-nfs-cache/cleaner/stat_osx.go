//go:build darwin

package cleaner

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

func (c *Cleaner) stat(path string) (*Stat, error) {
	c.StatxC.Add(1)
	stat, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("could not stat info: %w", err)
	}

	actualStruct, ok := stat.Sys().(*syscall.Stat_t)
	if !ok {
		return nil, fmt.Errorf("stat did not return a syscall.Stat_t for %q: %T",
			stat.Name(), stat.Sys())
	}

	return &Stat{
		ATimeUnix: actualStruct.Atimespec.Sec,
		BTimeUnix: actualStruct.Birthtimespec.Sec,
	}, nil
}

func (c *Cleaner) statInDir(df *os.File, filename string) (*Stat, error) {
	// performance on OS X does not matter, so just use the full stat
	return c.stat(filepath.Join(df.Name(), filename))
}

// GetDiskInfo reads the size of the filesystem holding path.
func GetDiskInfo(path string) (DiskInfo, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return DiskInfo{}, fmt.Errorf("statfs %s: %w", path, err)
	}

	return newDiskInfo(uint64(st.Bsize), st.Blocks, st.Bfree)
}
