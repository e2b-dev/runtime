package edge

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"

	e2bgrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

// ConnectionTarget names the shared connections in the connection metrics.
const ConnectionTarget = "remote-cluster-edge"

// Limits returns the pool's current sizing: the fewest connections to keep and
// how many users one connection takes. Values below one count as one.
type Limits func(ctx context.Context) (minConnections, perConnection int)

// Connections are the connections shared by the users of one remote cluster's
// edge, which routes each call by the service instance ID it carries. They are
// created as users join and closed with the pool. One left without users goes
// idle after gRPC's idle timeout, dropping its transport, and reconnects on
// its next call.
type Connections struct {
	clusterID   uuid.UUID
	endpoint    string
	dialOptions func() []grpc.DialOption
	limits      Limits

	mu     sync.Mutex
	conns  []*connection
	closed bool
}

// connection is one pooled connection and how many users hold it.
type connection struct {
	conn    *grpc.ClientConn
	clients int
}

// NewConnections pools connections to endpoint. dialOptions is called for
// each new connection.
func NewConnections(clusterID uuid.UUID, endpoint string, dialOptions func() []grpc.DialOption, limits Limits) *Connections {
	return &Connections{
		clusterID:   clusterID,
		endpoint:    endpoint,
		dialOptions: dialOptions,
		limits:      limits,
	}
}

// Acquire returns the least-used connection, opening another one while the
// pool is below its minimum or every connection is full. The caller holds the
// connection until it calls release, and must not close it. Limits apply to
// acquisitions after they change; held connections are kept.
func (c *Connections) Acquire(ctx context.Context) (conn *grpc.ClientConn, release func(), err error) {
	minConnections, perConnection := c.limits(ctx)
	minConnections, perConnection = max(minConnections, 1), max(perConnection, 1)

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, nil, errors.New("remote cluster connections are closed")
	}

	var least *connection
	for _, pooled := range c.conns {
		if least == nil || pooled.clients < least.clients {
			least = pooled
		}
	}

	if len(c.conns) < minConnections || least.clients >= perConnection {
		conn, err := grpc.NewClient(c.endpoint, c.dialOptions()...)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create remote cluster connection: %w", err)
		}

		e2bgrpc.ObserveConnection(ctx, conn, ConnectionTarget)
		go c.logStateChanges(context.WithoutCancel(ctx), conn, len(c.conns))
		least = &connection{conn: conn}
		c.conns = append(c.conns, least)
	}

	least.clients++

	return least.conn, sync.OnceFunc(func() { c.release(least) }), nil
}

func (c *Connections) release(pooled *connection) {
	c.mu.Lock()
	defer c.mu.Unlock()

	pooled.clients--
}

// logStateChanges logs each state change of one pooled connection until it
// shuts down. Moving into or out of TRANSIENT_FAILURE is a warning: every
// user of the connection is unreachable while it lasts.
func (c *Connections) logStateChanges(ctx context.Context, conn *grpc.ClientConn, index int) {
	state := conn.GetState()
	for state != connectivity.Shutdown && conn.WaitForStateChange(ctx, state) {
		next := conn.GetState()

		log := logger.L().Info
		if state == connectivity.TransientFailure || next == connectivity.TransientFailure {
			log = logger.L().Warn
		}
		log(ctx, "Remote cluster connection changed state",
			logger.WithClusterID(c.clusterID),
			zap.Int("connection_index", index),
			zap.Stringer("from", state),
			zap.Stringer("to", next),
		)

		state = next
	}
}

// Close closes every connection; Acquire fails from then on.
func (c *Connections) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.closed = true

	var errs []error
	for _, pooled := range c.conns {
		if err := pooled.conn.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	c.conns = nil

	return errors.Join(errs...)
}
