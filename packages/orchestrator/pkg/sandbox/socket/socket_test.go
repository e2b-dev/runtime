//go:build unix

package socket

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// TestWaitAcceptingWaitsForListen: a socket bound but not yet listening
// exists as a file and refuses connections; WaitAccepting returns only once
// it listens, where Wait would have returned at once.
func TestWaitAcceptingWaitsForListen(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "api.sock")
	fd, err := unix.Socket(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	require.NoError(t, err)
	defer unix.Close(fd)
	require.NoError(t, unix.Bind(fd, &unix.SockaddrUnix{Name: path}))
	_, err = os.Stat(path)
	require.NoError(t, err, "the file exists before listen")

	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	require.Error(t, WaitAccepting(ctx, path), "a bound socket that does not listen is not ready")

	go func() {
		time.Sleep(50 * time.Millisecond)
		_ = unix.Listen(fd, 1)
	}()
	ctx, cancel = context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	require.NoError(t, WaitAccepting(ctx, path))
}
