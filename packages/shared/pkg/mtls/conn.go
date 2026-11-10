package mtls

import (
	"errors"
	"io"
	"net"
)

var errNoHalfClose = errors.New("mtls: the connection has no half-close")

// halfClose forwards the TCP half-close net/http asks of a connection
// before it closes one whose request body it did not read, so the peer
// reads a FIN instead of a reset.
func halfClose(conn net.Conn) error {
	if hc, ok := conn.(interface{ CloseWrite() error }); ok {
		return hc.CloseWrite()
	}

	return errNoHalfClose
}

// readFrom keeps the sendfile path of conn, which net/http takes for a file
// response, and copies otherwise.
func readFrom(conn net.Conn, r io.Reader) (int64, error) {
	if rf, ok := conn.(io.ReaderFrom); ok {
		return rf.ReadFrom(r)
	}

	return io.Copy(conn, r)
}

// CloseWrite half-closes the connection under the hook; a *PlaintextConn
// therefore keeps the half-close of the TCP connection it wraps.
func (c *hookedConn) CloseWrite() error {
	return halfClose(c.Conn)
}

// ReadFrom keeps the sendfile path of the connection under the hook.
func (c *hookedConn) ReadFrom(r io.Reader) (int64, error) {
	return readFrom(c.Conn, r)
}

// CloseWrite half-closes the connection whose first byte was peeked.
func (c *peekedConn) CloseWrite() error {
	return halfClose(c.Conn)
}

// ReadFrom keeps the sendfile path of the connection whose first byte was
// peeked.
func (c *peekedConn) ReadFrom(r io.Reader) (int64, error) {
	return readFrom(c.Conn, r)
}
