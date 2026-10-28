package pool

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/proxy/tracking"
)

func testDestination(connectionKey string, insecureSkipTLSVerify bool) *Destination {
	return &Destination{
		Url:                   &url.URL{Scheme: "https", Host: "127.0.0.1:8443"},
		SandboxId:             "test-sandbox",
		ConnectionKey:         connectionKey,
		RequestLogger:         logger.NewNopLogger(),
		InsecureSkipTLSVerify: insecureSkipTLSVerify,
	}
}

// Reuse is the expected outcome, not a bug: a key already in the pool keeps the
// TLS policy of the destination that built it.
func TestPoolReusesClientAcrossInsecureSkipTLSVerify(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name  string
		first bool
	}{
		{name: "first destination skips verification", first: true},
		{name: "first destination verifies", first: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := New(4, 1, time.Second, false, nil)

			client := p.Get(t.Context(), testDestination("lifecycle", tt.first))
			require.NotNil(t, client)

			assert.Same(t, client, p.Get(t.Context(), testDestination("lifecycle", !tt.first)))
			assert.Equal(t, tt.first, client.transport.TLSClientConfig != nil)
		})
	}
}

func TestPoolBuildsOneClientPerConnectionKey(t *testing.T) {
	t.Parallel()

	p := New(4, 1, time.Second, false, nil)

	first := p.Get(t.Context(), testDestination("lifecycle-a", true))
	second := p.Get(t.Context(), testDestination("lifecycle-b", false))

	assert.NotSame(t, first, second)
	assert.Equal(t, 2, p.Size())
	assert.NotNil(t, first.transport.TLSClientConfig)
	assert.Nil(t, second.transport.TLSClientConfig)
}

// ConnectDone pauses a real successful TCP dial before the transport can
// register it. Cleanup must wait for that socket, not just its registry.
func TestOwnerJoinsLateDial(t *testing.T) {
	t.Parallel()
	p := New(4, 1, time.Second, false, nil)
	d := testDestination("lifetime", false)
	d.Owner = p.NewOwner()
	client := p.Get(t.Context(), d)
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	connected, resume := make(chan struct{}), make(chan struct{})
	ctx := httptrace.WithClientTrace(t.Context(), &httptrace.ClientTrace{ConnectDone: func(_, _ string, err error) {
		if err == nil {
			close(connected)
			<-resume
		}
	}})
	type dialResult struct {
		conn net.Conn
		err  error
	}
	dialDone := make(chan dialResult, 1)
	go func() {
		conn, err := client.transport.DialContext(ctx, "tcp", listener.Addr().String())
		dialDone <- dialResult{conn, err}
	}()
	peer, err := listener.Accept()
	require.NoError(t, err)
	defer peer.Close()
	<-connected
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, d.Owner.Close(canceled), context.Canceled)
	close(resume)
	result := <-dialDone
	if result.conn != nil {
		defer result.conn.Close()
	}
	require.NoError(t, d.Owner.Close(t.Context()))
	require.NoError(t, peer.SetReadDeadline(time.Now().Add(5*time.Second)))
	_, err = peer.Read(make([]byte, 1))
	require.ErrorIs(t, err, io.EOF, "cleanup must close the late socket before returning")
	require.ErrorIs(t, result.err, errOwnerClosed)
	assert.Zero(t, p.CurrentConnections())
	// A resolved stale request cannot recreate a client, even after a successor
	// with the same textual key has been admitted.
	successor := *d
	successor.Owner = p.NewOwner()
	next := p.Get(t.Context(), &successor)
	require.NotSame(t, client, next)
	_, _, err = p.Get(t.Context(), d).Acquire(t.Context())
	require.ErrorIs(t, err, errOwnerClosed)
	require.NoError(t, d.Owner.Close(t.Context()))
	conn, err := next.transport.DialContext(t.Context(), "tcp", listener.Addr().String())
	require.NoError(t, err)
	require.NoError(t, conn.Close())
	require.NoError(t, successor.Owner.Close(t.Context()))
	assert.Zero(t, p.Size())
}

func TestOwnerCancelsReadinessRetry(t *testing.T) {
	t.Parallel()
	p := New(4, 5, time.Second, false, nil)
	d := testDestination("retry", false)
	d.Owner = p.NewOwner()
	client := p.Get(t.Context(), d)
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := listener.Addr().String()
	require.NoError(t, listener.Close())
	failed, resume := make(chan struct{}), make(chan struct{})
	var attempts atomic.Int32
	ctx := httptrace.WithClientTrace(t.Context(), &httptrace.ClientTrace{ConnectDone: func(_, _ string, _ error) {
		if attempts.Add(1) == 1 {
			close(failed)
			<-resume
		}
	}})
	done := make(chan error, 1)
	go func() { _, err := client.transport.DialContext(ctx, "tcp", address); done <- err }()
	<-failed
	d.Owner.Stop()
	close(resume)
	require.ErrorIs(t, <-done, context.Canceled)
	require.NoError(t, d.Owner.Close(t.Context()))
	assert.EqualValues(t, 1, attempts.Load())
}

func TestOwnerStopsTLSHandshake(t *testing.T) {
	t.Parallel()
	p := New(4, 1, time.Second, false, nil)
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	d := testDestination("tls", true)
	d.Url.Host = listener.Addr().String()
	d.Owner = p.NewOwner()
	client := p.Get(t.Context(), d)
	done := make(chan error, 1)
	go func() {
		ctx, release, err := client.Acquire(t.Context())
		if err != nil {
			done <- err

			return
		}
		defer release()
		r, _ := http.NewRequestWithContext(ctx, http.MethodGet, d.Url.String(), nil)
		response, err := client.transport.RoundTrip(r)
		if response != nil {
			_ = response.Body.Close()
		}
		done <- err
	}()
	peer, err := listener.Accept()
	require.NoError(t, err)
	defer peer.Close()
	require.NoError(t, peer.SetDeadline(time.Now().Add(5*time.Second)))
	hello := make([]byte, 5)
	_, err = io.ReadFull(peer, hello)
	require.NoError(t, err)
	require.Equal(t, byte(22), hello[0], "backend observed TLS handshake")
	require.NoError(t, d.Owner.Close(t.Context()))
	require.Error(t, <-done)
	_, err = io.Copy(io.Discard, peer)
	// TCP reset or EOF both prove the backend socket ended.
	if err != nil {
		var timeout net.Error
		require.False(t, errors.As(err, &timeout) && timeout.Timeout())
	}
	assert.Zero(t, p.CurrentConnections())
}

type resetErrorConn struct{ net.Conn }

func (c resetErrorConn) Close() error {
	_ = c.Conn.Close()

	return io.ErrUnexpectedEOF
}

func TestResetErrorDoesNotRetainSettledOwner(t *testing.T) {
	t.Parallel()
	p := New(4, 1, time.Second, false, nil)
	d := testDestination("reset-error", false)
	d.Owner = p.NewOwner()
	client := p.Get(t.Context(), d)
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	conn, err := client.transport.DialContext(t.Context(), "tcp", listener.Addr().String())
	require.NoError(t, err)
	defer conn.Close()
	peer, err := listener.Accept()
	require.NoError(t, err)
	defer peer.Close()
	tracked := conn.(*tracking.Connection)
	tracked.Conn = resetErrorConn{Conn: tracked.Conn}
	closeErr := d.Owner.Close(t.Context())
	require.NoError(t, peer.SetReadDeadline(time.Now().Add(5*time.Second)))
	_, err = peer.Read(make([]byte, 1))
	require.ErrorIs(t, err, io.EOF)
	assert.Zero(t, p.CurrentConnections())
	require.NoError(t, closeErr)
}

// A request that resolved its owner just before the stop still gets a client
// to be refused through; that client is not counted as a pool member.
func TestClientBuiltAfterStopIsNotCounted(t *testing.T) {
	t.Parallel()
	p := New(4, 1, time.Second, false, nil)
	d := testDestination("late", false)
	d.Owner = p.NewOwner()
	require.NoError(t, d.Owner.Close(t.Context()))
	_, _, err := p.Get(t.Context(), d).Acquire(t.Context())
	require.ErrorIs(t, err, errOwnerClosed)
	assert.Zero(t, p.Size())
}

// A release whose budget runs out is retried; each retry must cost nothing
// that the eventual settlement does not give back.
func TestRetriedCloseLeavesNothingBehind(t *testing.T) {
	t.Parallel()
	p := New(4, 1, time.Second, false, nil)
	d := testDestination("retried", false)
	d.Owner = p.NewOwner()
	client := p.Get(t.Context(), d)
	_, release, err := client.Acquire(t.Context())
	require.NoError(t, err)
	before := runtime.NumGoroutine()
	for range 50 {
		ctx, cancel := context.WithTimeout(t.Context(), time.Millisecond)
		require.ErrorIs(t, d.Owner.Close(ctx), context.DeadlineExceeded)
		cancel()
	}
	assert.LessOrEqual(t, runtime.NumGoroutine(), before+2, "retried closes must not park goroutines")
	release()
	require.NoError(t, d.Owner.Close(t.Context()))
}
