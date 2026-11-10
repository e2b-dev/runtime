package clusters

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"

	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	e2bgrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// A remote cluster's instances are all reached through the same edge
// endpoint, routed by the instance ID each call carries, so they share a pool
// of connections rather than holding one each. The pool grows with the cell:
// featureflags.RemoteClusterInstancesPerConnectionFlag caps the instances on
// one connection, and featureflags.RemoteClusterMinConnectionsFlag sets the
// smallest pool.

// remoteConnectionTarget names the shared connections in the connection
// metrics, apart from the local cluster's per-instance connections.
const remoteConnectionTarget = "remote-cluster-edge"

// remoteConnections are the connections shared by a remote cluster's
// instances. They are created as instances join and closed with the cluster.
// One left without instances goes idle after gRPC's idle timeout, dropping its
// transport, and reconnects on its next call.
type remoteConnections struct {
	tel         *telemetry.Client
	endpoint    string
	endpointTLS bool
	plaintext   credentials.TransportCredentials

	clusterID    uuid.UUID
	featureFlags *featureflags.Client

	mu     sync.Mutex
	conns  []*remoteConnection
	closed bool
}

// remoteConnection is one pooled connection and how many open instance
// clients are using it.
type remoteConnection struct {
	conn    *grpc.ClientConn
	clients int
}

func newRemoteConnections(
	tel *telemetry.Client,
	featureFlags *featureflags.Client,
	clusterID uuid.UUID,
	endpoint string,
	endpointTLS bool,
	plaintext credentials.TransportCredentials,
) *remoteConnections {
	return &remoteConnections{
		tel:          tel,
		clusterID:    clusterID,
		featureFlags: featureFlags,
		endpoint:     endpoint,
		endpointTLS:  endpointTLS,
		plaintext:    plaintext,
	}
}

// client returns a client for one instance, on the least-used connection.
// The instance keeps that connection until its client is closed.
func (r *remoteConnections) client(ctx context.Context, auth *instanceAuthorization) (*GRPCClient, error) {
	pooled, err := r.acquire(ctx)
	if err != nil {
		return nil, err
	}

	return newSharedGRPCClient(pooled.conn, remoteConnectionTarget, func() { r.release(pooled) }, grpc.PerRPCCredentials(auth)), nil
}

// limits reads the pool's current sizing. A change applies to instances that
// join after it; instances already placed keep their connection.
func (r *remoteConnections) limits(ctx context.Context) (minConnections, instancesPerConnection int) {
	minConnections = featureflags.RemoteClusterMinConnectionsFlag.Fallback()
	instancesPerConnection = featureflags.RemoteClusterInstancesPerConnectionFlag.Fallback()
	if r.featureFlags != nil {
		minConnections = r.featureFlags.IntFlag(ctx, featureflags.RemoteClusterMinConnectionsFlag, featureflags.ClusterContext(r.clusterID))
		instancesPerConnection = r.featureFlags.IntFlag(ctx, featureflags.RemoteClusterInstancesPerConnectionFlag, featureflags.ClusterContext(r.clusterID))
	}

	return max(minConnections, 1), max(instancesPerConnection, 1)
}

func (r *remoteConnections) acquire(ctx context.Context) (*remoteConnection, error) {
	minConnections, instancesPerConnection := r.limits(ctx)

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil, errors.New("remote cluster connections are closed")
	}

	var least *remoteConnection
	for _, c := range r.conns {
		if least == nil || c.clients < least.clients {
			least = c
		}
	}

	if len(r.conns) < minConnections || least.clients >= instancesPerConnection {
		conn, err := grpc.NewClient(r.endpoint, dialOptions(r.tel, r.endpoint, r.endpointTLS, r.plaintext)...)
		if err != nil {
			return nil, fmt.Errorf("failed to create remote cluster connection: %w", err)
		}

		e2bgrpc.ObserveConnection(ctx, conn, remoteConnectionTarget)
		go r.logStateChanges(context.WithoutCancel(ctx), conn, len(r.conns))
		least = &remoteConnection{conn: conn}
		r.conns = append(r.conns, least)
	}

	least.clients++

	return least, nil
}

func (r *remoteConnections) release(c *remoteConnection) {
	r.mu.Lock()
	defer r.mu.Unlock()

	c.clients--
}

// logStateChanges logs each state change of one pooled connection until it
// shuts down. Moving into or out of TRANSIENT_FAILURE is a warning: every
// instance on the connection is unreachable while it lasts.
func (r *remoteConnections) logStateChanges(ctx context.Context, conn *grpc.ClientConn, index int) {
	state := conn.GetState()
	for state != connectivity.Shutdown && conn.WaitForStateChange(ctx, state) {
		next := conn.GetState()

		log := logger.L().Info
		if state == connectivity.TransientFailure || next == connectivity.TransientFailure {
			log = logger.L().Warn
		}
		log(ctx, "Remote cluster connection changed state",
			logger.WithClusterID(r.clusterID),
			zap.Int("connection_index", index),
			zap.Stringer("from", state),
			zap.Stringer("to", next),
		)

		state = next
	}
}

func (r *remoteConnections) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.closed = true

	var errs []error
	for _, c := range r.conns {
		if err := c.conn.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	r.conns = nil

	return errors.Join(errs...)
}
