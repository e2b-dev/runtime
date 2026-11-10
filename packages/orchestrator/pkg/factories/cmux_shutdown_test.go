//go:build linux

package factories

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/soheilhy/cmux"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
)

// noWrap serves the port with no listener in front of cmux.
func noWrap(l net.Listener) net.Listener { return l }

func waitCMUXShutdown(t *testing.T, done <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal(message)
	}
}

func TestCMUXCloseReleasesRootListener(t *testing.T) {
	t.Parallel()
	mux, err := NewCMUXServer(t.Context(), 0, noop.NewMeterProvider(), noWrap)
	require.NoError(t, err)
	listener := mux.Match(cmux.Any())
	done := make(chan struct{})
	go func() { _ = mux.Serve(); close(done) }()
	// Rescue only after assertions: matched listener Close reaches the raw
	// socket even on the old implementation. Never let rescue prove Close.
	t.Cleanup(func() {
		_ = listener.Close()
		mux.Close()
		waitCMUXShutdown(t, done, "fixture did not join cmux Serve")
	})
	_, port, err := net.SplitHostPort(listener.Addr().String())
	require.NoError(t, err)
	addr := net.JoinHostPort("127.0.0.1", port)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	client, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	accepted, err := listener.Accept()
	require.NoError(t, err, "real cmux Serve must have accepted the connection")
	require.NoError(t, accepted.Close())
	mux.Close()
	waitCMUXShutdown(t, done, "cmux Close left the root listener accepting connections")
	var listenConfig net.ListenConfig
	rebound, err := listenConfig.Listen(ctx, "tcp", addr)
	require.NoError(t, err, "Close must release the actual port for a replacement process")
	require.NoError(t, rebound.Close())
}

func TestCMUXCloseJoinsBlockedMatcher(t *testing.T) {
	t.Parallel()
	mux, err := NewCMUXServer(t.Context(), 0, noop.NewMeterProvider(), noWrap)
	require.NoError(t, err)
	started := make(chan struct{})
	var once sync.Once
	httpMatcher := cmux.HTTP1Fast()
	listener := mux.Match(func(reader io.Reader) bool {
		once.Do(func() { close(started) })

		return httpMatcher(reader)
	})
	done := make(chan struct{})
	go func() { _ = mux.Serve(); close(done) }()
	t.Cleanup(func() {
		_ = listener.Close()
		mux.Close()
		waitCMUXShutdown(t, done, "fixture did not join cmux Serve")
	})
	_, port, err := net.SplitHostPort(listener.Addr().String())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	client, err := (&net.Dialer{}).DialContext(ctx, "tcp", net.JoinHostPort("127.0.0.1", port))
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	_, err = io.WriteString(client, "G") // An incomplete method keeps HTTP1Fast reading.
	require.NoError(t, err)
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("real matcher did not observe the client")
	}
	// This is the normal Run ordering: HTTP/gRPC close the matched listener
	// (and thus root socket) before the final cmux closer joins matcher work.
	require.NoError(t, listener.Close())
	mux.Close()
	waitCMUXShutdown(t, done, "cmux Close left a silent client's matcher unjoined")
	require.NoError(t, client.SetReadDeadline(time.Now().Add(time.Second)))
	_, err = client.Read(make([]byte, 1))
	require.Error(t, err)
	if netErr, ok := errors.AsType[net.Error](err); ok {
		require.False(t, netErr.Timeout(), "the server must close the connection, not leave the client waiting")
	}
}
