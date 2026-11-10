package mtls

import (
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recordingListener hands out TCP connections that record a ReadFrom
// reaching them.
type recordingListener struct {
	net.Listener

	last atomic.Pointer[recordingConn]
}

func (l *recordingListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	rc := &recordingConn{TCPConn: conn.(*net.TCPConn)}
	l.last.Store(rc)

	return rc, nil
}

type recordingConn struct {
	*net.TCPConn

	readFrom bool
}

func (c *recordingConn) ReadFrom(r io.Reader) (int64, error) {
	c.readFrom = true

	return c.TCPConn.ReadFrom(r)
}

// A connection admitted as plaintext keeps the half-close and the sendfile
// path of the TCP connection under it: net/http half-closes a connection it
// is closing with an unread request body, and writes a file response
// through io.ReaderFrom. Off hands the connection over unread; permissive
// hands it over with its first byte replayed.
func TestPlaintextConnKeepsTheHalfCloseAndSendfileOfTheTCPConnection(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		mode Mode
	}{{name: "off", mode: ModeOff}, {name: "permissive", mode: ModePermissive}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			fx := newFixture(t, serverDNS)
			var lc net.ListenConfig
			inner, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
			require.NoError(t, err)
			rec := &recordingListener{Listener: inner}
			l := NewListener(rec, newServerConfig(t, fx, StaticMode(tc.mode), clientID))
			t.Cleanup(func() { _ = l.Close() })

			var dialer net.Dialer
			client, err := dialer.DialContext(t.Context(), "tcp", inner.Addr().String())
			require.NoError(t, err)
			t.Cleanup(func() { _ = client.Close() })
			require.NoError(t, client.SetDeadline(time.Now().Add(5*time.Second)))
			_, err = client.Write([]byte("G"))
			require.NoError(t, err)

			conn, err := l.Accept()
			require.NoError(t, err)
			t.Cleanup(func() { _ = conn.Close() })
			require.NoError(t, conn.SetDeadline(time.Now().Add(5*time.Second)))
			plain, ok := conn.(*PlaintextConn)
			require.True(t, ok, "admitted as plaintext")
			sender, ok := conn.(io.ReaderFrom)
			require.True(t, ok, "the sendfile path net/http takes for a file response")
			closer, ok := conn.(interface{ CloseWrite() error })
			require.True(t, ok, "the half-close net/http sends before closing a connection with an unread body")

			n, err := sender.ReadFrom(strings.NewReader("hello"))
			require.NoError(t, err)
			assert.Equal(t, int64(5), n)
			assert.True(t, rec.last.Load().readFrom, "ReadFrom reaches the TCP connection")
			got := make([]byte, 5)
			_, err = io.ReadFull(client, got)
			require.NoError(t, err)
			assert.Equal(t, "hello", string(got))

			require.NoError(t, closer.CloseWrite())
			_, err = client.Read(make([]byte, 1))
			require.ErrorIs(t, err, io.EOF, "the peer reads the FIN")
			_, err = client.Write([]byte("x"))
			require.NoError(t, err)
			got = make([]byte, 2)
			_, err = io.ReadFull(plain, got)
			require.NoError(t, err, "the read side stays open after the half-close")
			assert.Equal(t, "Gx", string(got))
		})
	}
}
