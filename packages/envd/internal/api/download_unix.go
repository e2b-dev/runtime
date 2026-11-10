//go:build unix

package api

import (
	"errors"
	"io"
	"os"
	"syscall"
)

// Filesystem magic numbers from Linux kernel (include/uapi/linux/magic.h)
const (
	procSuperMagic = 0x9fa0
	sysfsMagic     = 0x62656572
)

func isVirtualFS(path string) bool {
	var statfs syscall.Statfs_t
	if err := syscall.Statfs(path, &statfs); err != nil {
		return false
	}

	return statfs.Type == procSuperMagic || statfs.Type == sysfsMagic
}

// newAvailableReader returns a reader that yields what the file has available
// and then reports EOF.
func newAvailableReader(file *os.File) (io.Reader, error) {
	conn, err := file.SyscallConn()
	if err != nil {
		return nil, err
	}

	return nonBlockingReader{conn: conn}, nil
}

// Go keeps pollable files such as /proc/kmsg or trace_pipe in non-blocking
// mode and would park the read until new data arrives; reading through the
// raw fd returns EAGAIN instead, which ends the download.
type nonBlockingReader struct {
	conn syscall.RawConn
}

func (r nonBlockingReader) Read(p []byte) (int, error) {
	var n int
	var readErr error
	err := r.conn.Read(func(fd uintptr) bool {
		for {
			n, readErr = syscall.Read(int(fd), p)
			if !errors.Is(readErr, syscall.EINTR) {
				return true
			}
		}
	})

	switch {
	case err != nil:
		return 0, err
	case errors.Is(readErr, syscall.EAGAIN), readErr == nil && n == 0:
		return 0, io.EOF
	case readErr != nil:
		return 0, readErr
	}

	return n, nil
}
