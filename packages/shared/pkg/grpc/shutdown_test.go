package grpc

import (
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// TestGracefulStopWithTimeout_Clean ensures the helper returns true when the
// server has no in-flight RPCs and GracefulStop completes immediately.
func TestGracefulStopWithTimeout_Clean(t *testing.T) {
	t.Parallel()

	srv := grpc.NewServer()
	listenCfg := &net.ListenConfig{}
	ln, err := listenCfg.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = srv.Serve(ln) }()

	start := time.Now()
	ok := GracefulStopWithTimeout(srv, time.Second)
	require.True(t, ok, "expected clean stop")
	require.LessOrEqual(t, time.Since(start), 500*time.Millisecond, "stop took too long")
}

// TestGracefulStopWithTimeout_AlreadyStopped is a sanity check that calling
// the helper on an already-stopped server doesn't hang.
func TestGracefulStopWithTimeout_AlreadyStopped(t *testing.T) {
	t.Parallel()

	srv := grpc.NewServer()
	listenCfg := &net.ListenConfig{}
	ln, err := listenCfg.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = srv.Serve(ln) }()

	require.True(t, GracefulStopWithTimeout(srv, time.Second), "first stop should be clean")
	// Second invocation: GracefulStop on a stopped server is a no-op.
	require.True(t, GracefulStopWithTimeout(srv, time.Second), "second stop should also report clean")
}
