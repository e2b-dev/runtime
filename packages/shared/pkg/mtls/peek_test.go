package mtls

import (
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPeekTLSClassifiesByFirstByte(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		payload []byte
		isTLS   bool
	}{
		{name: "TLS handshake record", payload: []byte{0x16, 0x03, 0x01, 0x00, 0x80}, isTLS: true},
		{name: "HTTP/1.1 request", payload: []byte("GET /health HTTP/1.1\r\n"), isTLS: false},
		{name: "HTTP/2 preface", payload: []byte("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"), isTLS: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			serverSide, clientSide := net.Pipe()
			t.Cleanup(func() {
				_ = serverSide.Close()
				_ = clientSide.Close()
			})
			go func() {
				_, _ = clientSide.Write(tt.payload)
			}()

			isTLS, conn, err := peekTLS(serverSide)
			require.NoError(t, err)
			assert.Equal(t, tt.isTLS, isTLS)

			// The byte the classifier consumed is replayed first.
			got := make([]byte, len(tt.payload))
			_, err = io.ReadFull(conn, got)
			require.NoError(t, err)
			assert.Equal(t, tt.payload, got)
		})
	}
}

func TestPeekTLSHonoursTheDeadline(t *testing.T) {
	t.Parallel()

	serverSide, clientSide := net.Pipe()
	t.Cleanup(func() {
		_ = serverSide.Close()
		_ = clientSide.Close()
	})
	require.NoError(t, serverSide.SetReadDeadline(time.Now().Add(50*time.Millisecond)))

	start := time.Now()
	_, _, err := peekTLS(serverSide)
	require.Error(t, err)
	assert.True(t, isTimeout(err), "a silent connection is a timeout: %v", err)
	assert.Less(t, time.Since(start), time.Second)
}

func TestPeekTLSReportsAClosedConnection(t *testing.T) {
	t.Parallel()

	serverSide, clientSide := net.Pipe()
	t.Cleanup(func() { _ = serverSide.Close() })
	require.NoError(t, clientSide.Close())

	_, _, err := peekTLS(serverSide)
	require.ErrorIs(t, err, io.EOF)
	assert.False(t, isTimeout(err))
}
