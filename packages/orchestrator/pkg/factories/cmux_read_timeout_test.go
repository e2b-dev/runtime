//go:build linux

package factories

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/soheilhy/cmux"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
)

// A peer that stops after a partial prefix no longer holds the sniff
// goroutine: cmux passes it to the next matcher at the read timeout, and a
// matched connection keeps no deadline (in permissive mode the mtls listener
// reads a first byte, so a stalled peer must not hold a matcher goroutine).
func TestCMUXReadTimeoutPassesASilentPeerOn(t *testing.T) {
	t.Parallel()
	const readTimeout = 500 * time.Millisecond
	mux, err := newCMUXServer(t.Context(), 0, noop.NewMeterProvider(), noWrap, readTimeout)
	require.NoError(t, err)
	// The matchers Run registers: HTTP/1 first, then everything else for gRPC.
	httpListener := mux.Match(cmux.HTTP1Fast())
	grpcListener := mux.Match(cmux.Any())
	done := make(chan struct{})
	go func() { _ = mux.Serve(); close(done) }()
	t.Cleanup(func() {
		_ = httpListener.Close()
		mux.Close()
		waitCMUXShutdown(t, done, "fixture did not join cmux Serve")
	})
	_, port, err := net.SplitHostPort(httpListener.Addr().String())
	require.NoError(t, err)
	addr := net.JoinHostPort("127.0.0.1", port)
	dialer := &net.Dialer{}

	start := time.Now()
	silent, err := dialer.DialContext(t.Context(), "tcp", addr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = silent.Close() })
	_, err = io.WriteString(silent, "G") // An incomplete method keeps HTTP1Fast reading.
	require.NoError(t, err)
	passedOn := acceptBefore(t, grpcListener, start.Add(2*readTimeout), "the read timeout did not pass a silent peer to the next matcher")
	require.GreaterOrEqual(t, time.Since(start), readTimeout, "cmux passed the peer on before the read timeout")
	require.Equal(t, silent.LocalAddr().String(), passedOn.RemoteAddr().String())
	require.NoError(t, silent.SetReadDeadline(start.Add(3*readTimeout)))
	_, err = silent.Read(make([]byte, 1))
	netErr, ok := errors.AsType[net.Error](err)
	require.True(t, ok && netErr.Timeout(), "cmux must pass a silent peer on, not close it: %v", err)

	const request = "GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n"
	prompt, err := dialer.DialContext(t.Context(), "tcp", addr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = prompt.Close() })
	_, err = io.WriteString(prompt, request)
	require.NoError(t, err)
	matched := acceptBefore(t, httpListener, time.Now().Add(2*time.Second), "a complete request must still be matched after a silent peer was passed on")
	time.Sleep(2 * readTimeout)
	_, err = io.WriteString(prompt, "x")
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	stop := context.AfterFunc(ctx, func() { _ = matched.Close() })
	received := make([]byte, len(request)+1)
	_, err = io.ReadFull(matched, received)
	stop()
	require.NoError(t, err, "a matched connection must keep no read deadline")
	require.Equal(t, request+"x", string(received))
}

// acceptBefore returns the listener's next connection, or fails the test if
// none arrives before deadline.
func acceptBefore(t *testing.T, listener net.Listener, deadline time.Time, message string) net.Conn {
	t.Helper()
	type accepted struct {
		conn net.Conn
		err  error
	}
	result := make(chan accepted, 1)
	go func() {
		conn, err := listener.Accept()
		result <- accepted{conn: conn, err: err}
	}()
	select {
	case r := <-result:
		require.NoError(t, r.err, message)
		t.Cleanup(func() { _ = r.conn.Close() })

		return r.conn
	case <-time.After(time.Until(deadline)):
		t.Fatal(message)

		return nil
	}
}
