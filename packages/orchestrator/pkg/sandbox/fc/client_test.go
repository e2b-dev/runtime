//go:build linux

package fc

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// A uffd handler that exits after Firecracker loaded the snapshot ends the
// wait for it with its exit error as the cause, and the cause must reach the
// caller: Create tells a node whose pool could not back the guest memory
// (ENOMEM from the memfd mmap) from a failed create by it. The stub keeps the
// ready channel open so the wait takes the context branch; in production the
// handler closes the channel right after recording the exit, so this branch is
// the rarer ordering and the cause otherwise arrives through the next
// Firecracker call.
func TestLoadSnapshotKeepsUffdExitCause(t *testing.T) {
	t.Parallel()

	dir, err := os.MkdirTemp("", "fc") //nolint:usetesting // t.TempDir embeds the test name and overflows the unix sun_path limit
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	socket := dir + "/fc.sock"
	listener, err := new(net.ListenConfig).Listen(t.Context(), "unix", socket)
	require.NoError(t, err)

	loaded := make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /snapshot/load", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
		close(loaded)
	})
	server := &http.Server{Handler: mux} //nolint:gosec // no timeouts needed for a stub on a private socket
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close() })

	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)
	go func() {
		<-loaded
		// Give the client time to read the answer, so the exit lands in the wait for uffd ready.
		time.Sleep(100 * time.Millisecond)
		cancel(fmt.Errorf("uffd process exited: failed to wrap memfd: mmap memfd: %w", unix.ENOMEM))
	}()

	err = newApiClient(socket).loadSnapshot(ctx, dir+"/uffd.sock", make(chan struct{}), stubSnapfile(dir+"/snapfile"), false, false, false, nil, "", "team-1")
	require.ErrorIs(t, err, unix.ENOMEM)
}
