//go:build linux

package uffd

import (
	"context"
	"net"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestServeLoop_HandleFailureSetsExitError asserts the invariant the serve
// loop ERROR log depends on: a genuine handle failure (here, a malformed
// handshake) completes u.Exit() with a non-nil error. That same non-nil
// exitErr is what gates the "uffd serve loop failed" log and what the
// resume-path lifecycle watcher turns into a sandbox stop.
func TestServeLoop_HandleFailureSetsExitError(t *testing.T) {
	t.Parallel()
	u := startHandshakeTest(t)

	conn, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: u.socketPath, Net: "unix"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	file, err := os.CreateTemp(t.TempDir(), "backing")
	require.NoError(t, err)
	t.Cleanup(func() { _ = file.Close() })

	// A malformed mapping body makes handle() fail (not an expected stop-time
	// close), so exitErr stays non-nil.
	_, _, err = conn.WriteMsgUnix([]byte("malformed mapping"), syscall.UnixRights(int(file.Fd())), nil)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	select {
	case <-u.Exit().Done():
	case <-ctx.Done():
		t.Fatal("handle failure did not complete uffd exit")
	}
	require.Error(t, u.Exit().Error(), "a genuine handle failure must surface a non-nil exit error")
}
