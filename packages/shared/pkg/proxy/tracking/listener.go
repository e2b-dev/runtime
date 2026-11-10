package tracking

import (
	"errors"
	"net"
	"sync/atomic"

	"github.com/e2b-dev/infra/packages/shared/pkg/smap"
)

type Listener struct {
	net.Listener

	counter     *atomic.Int64
	connections *smap.Map[*Connection]
}

func NewListener(l net.Listener, counter *atomic.Int64, connections ...*smap.Map[*Connection]) *Listener {
	listener := &Listener{
		Listener: l,
		counter:  counter,
	}
	if len(connections) > 0 {
		listener.connections = connections[0]
	}

	return listener
}

func (l *Listener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}

	return &serverConnection{Connection: NewConnection(conn, l.counter, l.connections)}, nil
}

// serverConnection gives net/http the half-close it does before closing a
// connection over an unread request body. Pool connections do without it: a
// protocol upgrade half-closes through it and then waits for the other side.
type serverConnection struct {
	*Connection
}

func (c *serverConnection) CloseWrite() error {
	cw, ok := c.Conn.(interface{ CloseWrite() error })
	if !ok {
		return errors.ErrUnsupported
	}

	return cw.CloseWrite()
}
