//go:build linux

package factories

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/soheilhy/cmux"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// cmuxReadTimeout bounds the protocol sniff: a peer that sends nothing, or
// stops after a partial prefix, is passed to the next matcher instead of
// holding the sniff goroutine. The same bound as the mtls handshake deadline.
const cmuxReadTimeout = 5 * time.Second

// NewCMUXServer opens the port, wraps its listener with wrap, and
// multiplexes HTTP and gRPC over it.
func NewCMUXServer(ctx context.Context, port uint16, meterProvider metric.MeterProvider, wrap func(net.Listener) net.Listener) (cmux.CMux, error) {
	return newCMUXServer(ctx, port, meterProvider, wrap, cmuxReadTimeout)
}

func newCMUXServer(ctx context.Context, port uint16, meterProvider metric.MeterProvider, wrap func(net.Listener) net.Listener, readTimeout time.Duration) (cmux.CMux, error) {
	var lisCfg net.ListenConfig
	lis, err := lisCfg.Listen(ctx, "tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, fmt.Errorf("failed to listen on port %d: %w", port, err)
	}

	root := &cmuxListener{Listener: wrap(lis), connections: make(map[*cmuxConn]struct{})}
	m := cmux.New(root)
	m.SetReadTimeout(readTimeout)

	meter := meterProvider.Meter("github.com/e2b-dev/infra/packages/orchestrator/pkg/factories")
	errCounter := utils.Must(telemetry.GetCounter(meter, telemetry.CmuxErrorsTotal))

	m.HandleError(func(err error) bool {
		logger.L().Warn(ctx, "cmux connection error", zap.Error(err))
		errCounter.Add(ctx, 1)

		return true // keep serving
	})

	return &ownedCMUX{CMux: m, root: root}, nil
}

// cmux.Close closes its channels, but not the root listener or clients still
// reading a protocol prefix. Own those resources so Serve can join its matchers.
// Run closes HTTP/gRPC servers before calling this final closer.
type ownedCMUX struct {
	cmux.CMux

	root *cmuxListener
}

func (m *ownedCMUX) Close() {
	_ = m.root.Close()
	m.root.closeConnections()
	m.CMux.Close()
}

type cmuxListener struct {
	net.Listener

	mu          sync.Mutex
	connections map[*cmuxConn]struct{}
	closed      bool
}

func (l *cmuxListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		_ = conn.Close()

		return nil, net.ErrClosed
	}
	owned := &cmuxConn{Conn: conn, owner: l}
	l.connections[owned] = struct{}{}
	l.mu.Unlock()

	return owned, nil
}

func (l *cmuxListener) Close() error {
	l.mu.Lock()
	l.closed = true
	l.mu.Unlock()
	// A matched HTTP/gRPC listener forwards Close here. Closing admission
	// must not interrupt accepted users before their servers have drained.
	return l.Listener.Close()
}

func (l *cmuxListener) closeConnections() {
	l.mu.Lock()
	connections := make([]*cmuxConn, 0, len(l.connections))
	for conn := range l.connections {
		connections = append(connections, conn)
	}
	l.mu.Unlock()
	for _, conn := range connections {
		_ = conn.Close()
	}
}

type cmuxConn struct {
	net.Conn

	owner *cmuxListener
	once  sync.Once
	err   error
}

func (c *cmuxConn) Close() error {
	c.once.Do(func() {
		c.err = c.Conn.Close()
		c.owner.mu.Lock()
		delete(c.owner.connections, c)
		c.owner.mu.Unlock()
	})

	return c.err
}
