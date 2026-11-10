package tracking

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"

	"github.com/e2b-dev/infra/packages/shared/pkg/smap"
)

type Connection struct {
	net.Conn

	counter *atomic.Int64
	key     string

	m *smap.Map[*Connection]

	// closeOnce keeps the counter and the registry right when a cleanup reset
	// races the transport's own close.
	closeOnce sync.Once
	closeErr  error
}

func NewConnection(conn net.Conn, counter *atomic.Int64, m *smap.Map[*Connection]) *Connection {
	counter.Add(1)

	c := &Connection{
		Conn:    conn,
		counter: counter,
		m:       m,
	}

	if m != nil {
		c.key = uuid.New().String()

		m.Insert(c.key, c)
	}

	return c
}

// Reset closes the connection with an RST where the socket supports it, and
// with a plain close otherwise.
func (c *Connection) Reset() error {
	var errs []error

	if tcp, ok := c.Conn.(*net.TCPConn); ok {
		if err := tcp.SetLinger(0); err != nil {
			errs = append(errs, err)
		}
	}

	if err := c.Close(); err != nil {
		errs = append(errs, err)
	}

	return errors.Join(errs...)
}

// Close releases the socket and its accounting once, retaining any close
// error for every caller.
func (c *Connection) Close() error {
	c.closeOnce.Do(func() {
		c.closeErr = c.Conn.Close()
		c.counter.Add(-1)

		if c.m != nil {
			c.m.Remove(c.key)
		}
	})

	return c.closeErr
}
