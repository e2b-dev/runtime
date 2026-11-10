//go:build linux

package block

import (
	"errors"
	"fmt"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// CacheBackingError is a fetch read that faulted writing into the cache file's
// mapping. The cache is sparse, so the usual cause is a full filesystem, which
// surfaces as EFAULT from read(2) or SIGBUS from a Go copy, never ENOSPC.
type CacheBackingError struct {
	Path       string
	FreeBytes  int64
	TotalBytes int64
	Err        error
}

func (e *CacheBackingError) Error() string {
	if e.TotalBytes > 0 {
		return fmt.Sprintf("cache file %s could not be populated: the read faulted on its mapping (%v); the filesystem has %s free of %s, a full disk cannot back a sparse cache page",
			e.Path, e.Err, humanBytes(e.FreeBytes), humanBytes(e.TotalBytes))
	}

	return fmt.Sprintf("cache file %s could not be populated: the read faulted on its mapping (%v)", e.Path, e.Err)
}

func (e *CacheBackingError) Unwrap() error { return e.Err }

func classifyBackingError(path string, err error) error {
	if _, faulted := errors.AsType[*MemoryFaultError](err); !faulted && !errors.Is(err, unix.EFAULT) {
		return err
	}

	be := &CacheBackingError{Path: path, Err: err}

	var st unix.Statfs_t
	if statErr := unix.Statfs(filepath.Dir(path), &st); statErr == nil {
		be.FreeBytes = int64(st.Bavail) * st.Bsize
		be.TotalBytes = int64(st.Blocks) * st.Bsize
	}

	return be
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}

	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}
