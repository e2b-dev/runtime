//go:build linux

package userfaultfd

import (
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSyncWPHandlerStopsWithoutFaults(t *testing.T) {
	t.Parallel()

	// An empty pipe exercises the idle reader without requiring userfaultfd.
	var events [2]int
	require.NoError(t, syscall.Pipe2(events[:], syscall.O_CLOEXEC|syscall.O_NONBLOCK))
	t.Cleanup(func() {
		_ = syscall.Close(events[0])
		_ = syscall.Close(events[1])
	})
	var stop [2]int
	require.NoError(t, syscall.Pipe2(stop[:], syscall.O_CLOEXEC|syscall.O_NONBLOCK))
	t.Cleanup(func() { _ = syscall.Close(stop[0]) })

	var resolved atomic.Int64
	done := make(chan serveResult, 1)
	go func() {
		done <- serveSyncWP(Fd(events[0]), stop[0], 4096, 1, &resolved)
	}()

	select {
	case res := <-done:
		_ = syscall.Close(stop[1])
		t.Fatalf("handler exited before cancellation: %+v", res)
	case <-time.After(20 * time.Millisecond):
	}

	require.NoError(t, syscall.Close(stop[1]))
	select {
	case res := <-done:
		require.NoError(t, res.err)
		require.Zero(t, res.resolved)
		require.Zero(t, res.nonWP)
		require.Zero(t, resolved.Load())
	case <-time.After(5 * time.Second):
		t.Fatal("idle handler did not stop after cancellation")
	}
}
