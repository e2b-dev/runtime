package mtls

import (
	"errors"
	"io"
	"net"
	"os"
)

// tlsHandshakeRecord is the first byte of every TLS ClientHello: the
// handshake record type. Every plaintext protocol the services speak starts
// with an ASCII letter.
const tlsHandshakeRecord = 0x16

// peekedConn replays the byte the classifier consumed.
type peekedConn struct {
	net.Conn

	head []byte
}

func (c *peekedConn) Read(p []byte) (int, error) {
	if len(c.head) > 0 {
		n := copy(p, c.head)
		c.head = c.head[n:]

		return n, nil
	}

	return c.Conn.Read(p)
}

// peekTLS reads the first byte of conn under whatever deadline is set on it
// and reports whether the connection is TLS. The returned connection replays
// the byte. On error the caller closes conn.
func peekTLS(conn net.Conn) (bool, net.Conn, error) {
	head := make([]byte, 1)
	if _, err := io.ReadFull(conn, head); err != nil {
		return false, nil, err
	}

	return head[0] == tlsHandshakeRecord, &peekedConn{Conn: conn, head: head}, nil
}

// isTimeout reports whether err is a deadline expiry on a connection.
func isTimeout(err error) bool {
	if errors.Is(err, os.ErrDeadlineExceeded) {
		return true
	}

	netErr, ok := errors.AsType[net.Error](err)

	return ok && netErr.Timeout()
}
