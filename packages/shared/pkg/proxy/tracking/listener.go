package tracking

import (
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

	return NewConnection(conn, l.counter, l.connections), nil
}
