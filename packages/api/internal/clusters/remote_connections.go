package clusters

import (
	"context"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/e2b-dev/infra/packages/shared/pkg/clusters/edge"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// A remote cluster's instances are all reached through the same edge
// endpoint, routed by the instance ID each call carries, so they share a pool
// of connections rather than holding one each. The pool grows with the cell:
// featureflags.RemoteClusterInstancesPerConnectionFlag caps the instances on
// one connection, and featureflags.RemoteClusterMinConnectionsFlag sets the
// smallest pool.
func newRemoteConnections(tel *telemetry.Client, featureFlags *featureflags.Client, clusterID uuid.UUID, endpoint string, endpointTLS bool) *edge.Connections {
	limits := func(ctx context.Context) (int, int) {
		return featureflags.GetRemoteClusterConnectionLimits(ctx, featureFlags, featureflags.ClusterContext(clusterID))
	}

	dial := func() []grpc.DialOption {
		return dialOptions(tel, endpoint, endpointTLS, insecure.NewCredentials())
	}

	return edge.NewConnections(clusterID, endpoint, dial, limits)
}

// remoteInstanceClient returns a client for one instance, on the least-used
// connection. The instance keeps that connection until its client is closed.
func remoteInstanceClient(ctx context.Context, connections *edge.Connections, auth edge.Authorization) (*GRPCClient, error) {
	conn, release, err := connections.Acquire(ctx)
	if err != nil {
		return nil, err
	}

	return newSharedGRPCClient(conn, edge.ConnectionTarget, release, grpc.PerRPCCredentials(auth)), nil
}
