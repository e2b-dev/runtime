package tracking

import (
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/smap"
)

type halfCloser interface {
	CloseWrite() error
}

func tcpPair(t *testing.T) (net.Listener, net.Conn) {
	t.Helper()

	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })

	client, err := (&net.Dialer{}).DialContext(t.Context(), "tcp", listener.Addr().String())
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	return listener, client
}

// net/http half-closes a server connection before closing it over an unread
// request body, so the client gets the end of the response instead of a reset.
func TestServerConnectionHalfCloses(t *testing.T) {
	t.Parallel()

	listener, client := tcpPair(t)
	var counter atomic.Int64
	server, err := NewListener(listener, &counter).Accept()
	require.NoError(t, err)
	assert.Equal(t, int64(1), counter.Load())

	cw, ok := server.(halfCloser)
	require.True(t, ok, "the server connection should offer net/http a half-close")
	require.NoError(t, cw.CloseWrite())

	_, err = client.Read(make([]byte, 1))
	require.ErrorIs(t, err, io.EOF, "the client should see the end of the server's output")

	_, err = client.Write([]byte("x"))
	require.NoError(t, err)
	buf := make([]byte, 1)
	n, err := server.Read(buf)
	require.NoError(t, err, "the server should still read after the half-close")
	assert.Equal(t, "x", string(buf[:n]))

	require.NoError(t, server.Close())
	assert.Equal(t, int64(0), counter.Load())
}

// A pool connection keeps a full close: half-closing an upgraded connection
// toward the sandbox would leave the proxy waiting for the sandbox to hang up.
func TestPoolConnectionDoesNotHalfClose(t *testing.T) {
	t.Parallel()

	_, client := tcpPair(t)
	var counter atomic.Int64
	conn := NewConnection(client, &counter, nil)

	_, ok := any(conn).(halfCloser)
	assert.False(t, ok)
}

// A wrapper may report an error after closing its socket. Racing transport
// close and retirement must still release accounting exactly once.
type closeErrorConn struct {
	net.Conn

	calls atomic.Int32
}

func (c *closeErrorConn) Close() error {
	c.calls.Add(1)
	_ = c.Conn.Close()

	return io.ErrUnexpectedEOF
}

func TestConcurrentResetAndFailedCloseReleaseAccounting(t *testing.T) {
	t.Parallel()
	listener, client := tcpPair(t)
	peer, err := listener.Accept()
	require.NoError(t, err)
	defer peer.Close()
	wrapped := &closeErrorConn{Conn: client}
	var count atomic.Int64
	connections := smap.New[*Connection]()
	conn := NewConnection(wrapped, &count, connections)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() { assert.ErrorIs(t, conn.Close(), io.ErrUnexpectedEOF) })
		wg.Go(func() { assert.ErrorIs(t, conn.Reset(), io.ErrUnexpectedEOF) })
	}
	wg.Wait()
	require.NoError(t, peer.SetReadDeadline(time.Now().Add(5*time.Second)))
	_, err = peer.Read(make([]byte, 1))
	require.ErrorIs(t, err, io.EOF)
	assert.EqualValues(t, 1, wrapped.calls.Load())
	assert.Zero(t, count.Load())
	assert.Zero(t, connections.Count())
}
