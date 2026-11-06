package nodemanager

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/clusters"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
)

// TestSync_ShutDownConnectionIsFatal: a node whose conn was closed under it
// (cluster Instance dropped and re-added with the same instance ID) fails every
// RPC forever. Sync must escalate so the caller deregisters — not retry or mark
// unhealthy.
func TestSync_ShutDownConnectionIsFatal(t *testing.T) {
	t.Parallel()

	n := NewTestNode("test-node", api.NodeStatusReady, 0, 1)
	require.NoError(t, n.client.Connection.Close())

	err := n.Sync(t.Context(), nil)
	require.Error(t, err, "a shut-down connection can never recover and must be escalated")

	// StatusInfo() derives an Unhealthy view from the conn state on its own;
	// assert the stored status, which Sync must not touch during teardown.
	assert.Equal(t, api.NodeStatusReady, n.status.Status, "stored status must not churn during teardown")
	_, unreachable := n.UnreachableSince()
	assert.False(t, unreachable, "a closed local conn is not evidence the node is unreachable")
}

// A remote cluster's instance closed on a connection the other instances
// still use reads as shut down, so Sync escalates it for deregistration even
// though the connection itself stays open.
func TestSync_ClosedSharedClientIsFatal(t *testing.T) {
	t.Parallel()

	n := NewTestNode("test-node", api.NodeStatusReady, 0, 1)
	shared := clusters.NewTestSharedGRPCClient(n.client.Connection)
	require.NoError(t, shared.Close())
	n.client = shared

	require.Error(t, n.Sync(t.Context(), nil))
	assert.NotEqual(t, connectivity.Shutdown, n.client.Connection.GetState(), "the shared connection stays open")
	assert.Equal(t, api.NodeStatusUnhealthy, n.Status())
}

// A remote cluster's node that fails its sync while its connection is
// reconnecting is kept out of placement as Connecting, not marked Unhealthy:
// one reconnecting connection fails every node on it at once. A local node has
// a connection of its own, so the same failure means the node is down.
func TestSync_ReconnectingConnection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		clusterID    uuid.UUID
		storedStatus api.NodeStatus
		status       api.NodeStatus
	}{
		{name: "remote cluster node", clusterID: uuid.New(), storedStatus: api.NodeStatusReady, status: api.NodeStatusConnecting},
		{name: "local node", clusterID: consts.LocalClusterID, storedStatus: api.NodeStatusUnhealthy, status: api.NodeStatusUnhealthy},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			n := NewTestNode("test-node", api.NodeStatusReady, 0, 1, WithSilentInfoClient())
			n.ClusterID = tt.clusterID
			// Nothing listens on port 1, so the connection fails to dial.
			conn, err := grpc.NewClient("127.0.0.1:1", grpc.WithTransportCredentials(insecure.NewCredentials()))
			require.NoError(t, err)
			t.Cleanup(func() { _ = conn.Close() })
			conn.Connect()
			for state := conn.GetState(); state != connectivity.TransientFailure; state = conn.GetState() {
				require.True(t, conn.WaitForStateChange(t.Context(), state))
			}
			n.client.Connection = conn

			require.NoError(t, n.Sync(t.Context(), nil))

			assert.Equal(t, tt.storedStatus, n.status.Status)
			assert.Equal(t, tt.status, n.Status())
			_, unreachable := n.UnreachableSince()
			assert.True(t, unreachable, "the node still did not answer")
		})
	}
}
