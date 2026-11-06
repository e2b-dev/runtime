package nodemanager

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

const syncMaxRetries = 4

// Sync refreshes the node's status and sandbox list. A non-nil error means the
// gRPC connection is permanently gone and the caller must deregister the node;
// all other failures are handled locally (retries, then unhealthy) and return nil.
func (n *Node) Sync(ctx context.Context, store *sandbox.Store) error {
	syncRetrySuccess := false

	// Tracked separately from success because the two answer different
	// questions. A sync can fail on a node this replica did reach — ServiceInfo
	// answers and the sandbox list call then fails — and a node that answered is
	// not unreachable however the rest of the cycle went.
	answered := false

	// The connection's state after the last attempt, to tell a node that is
	// down from a connection that is still reconnecting.
	var connState connectivity.State

	for range syncMaxRetries {
		client, ctx := n.GetClient(ctx)

		// A shut-down conn never recovers — e.g. the cluster Instance was
		// dropped and re-added under the same instance ID, leaving this node
		// on the dead conn. Escalate for deregistration instead of retrying.
		if client.State() == connectivity.Shutdown {
			return fmt.Errorf("grpc connection to node %s is shut down", n.ID)
		}

		requestedAt := time.Now()
		nodeInfo, err := client.Info.ServiceInfo(ctx, &emptypb.Empty{})
		connState = client.State()
		if err != nil {
			logger.L().Error(ctx, "Error getting node info", zap.Error(err), logger.WithNodeID(n.ID))

			continue
		}

		answered = true

		// update node status (if changed)
		nodeStatus, ok := OrchestratorToApiNodeStateMapper[nodeInfo.GetServiceStatus()]
		if !ok {
			logger.L().Error(ctx, "Unknown service info status", zap.String("status", nodeInfo.GetServiceStatus().String()), logger.WithNodeID(n.ID))
			nodeStatus = api.NodeStatusUnhealthy
		}

		var statusChangedAt time.Time
		if ts := nodeInfo.GetServiceStatusChangedAt(); ts.IsValid() {
			statusChangedAt = ts.AsTime()
		}

		n.setStatus(ctx, nodeStatus, statusChangedAt)
		n.setMachineInfo(nodeInfo.GetMachineInfo())
		n.setLabels(nodeInfo.GetLabels())
		n.setMetadata(
			NodeMetadata{
				ServiceInstanceID: nodeInfo.GetServiceId(),
				Commit:            nodeInfo.GetServiceCommit(),
				Version:           nodeInfo.GetServiceVersion(),
			},
		)
		// Update host metrics from service info
		n.UpdateMetricsFromSync(nodeInfo, requestedAt)

		orphanCandidates, instancesErr := n.GetOrphanCandidates(ctx)
		if instancesErr != nil {
			logger.L().Error(ctx, "Error getting instances", zap.Error(instancesErr), logger.WithNodeID(n.ID))

			continue
		}

		store.Reconcile(ctx, orphanCandidates, n.ID)

		syncRetrySuccess = true

		break
	}

	// Reachability keys off whether the node answered at all; status keys off
	// whether the cycle completed. They are set independently because they can
	// legitimately disagree.
	if answered {
		n.markReachable()
	} else {
		n.markUnreachable()
	}

	if !syncRetrySuccess {
		// A remote cluster's instances share a few connections, so one that is
		// reconnecting fails every node on it at once. StatusInfo already shows
		// those nodes as Connecting, which keeps them out of placement, and
		// they recover as soon as the connection is ready; marking them
		// Unhealthy would hold them out until the next sync. A local node has a
		// connection of its own, so one stuck reconnecting means the node is down.
		if n.IsClusterNode() && !answered && (connState == connectivity.TransientFailure || connState == connectivity.Connecting) {
			logger.L().Warn(ctx, "Failed to sync node while its connection is reconnecting",
				logger.WithNodeID(n.ID),
				zap.Stringer("connection_state", connState),
			)

			return nil
		}

		logger.L().Error(ctx, "Failed to sync node after max retries, temporarily marking as unhealthy",
			logger.WithNodeID(n.ID),
			zap.Bool("answered", answered),
		)
		// Local status change, the timestamp is the time of the first unhealthy observation.
		n.markUnhealthyLocal(ctx)
	}

	return nil
}
