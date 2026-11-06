package nodemanager

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	orchestratorinfo "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator-info"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

// ApiNodeToOrchestratorStateMapper translates an api.NodeStatus into the status
// an orchestrator understands. It has callers with different needs. Adding an entry
// purely to make a status overridable therefore also makes it routable if its
// orchestrator counterpart is. TestStatusMappersAreInverses pins this map
// against OrchestratorToApiNodeStateMapper so the two cannot drift apart.
var ApiNodeToOrchestratorStateMapper = map[api.NodeStatus]orchestratorinfo.ServiceInfoStatus{
	api.NodeStatusReady:        orchestratorinfo.ServiceInfoStatus_Healthy,
	api.NodeStatusDraining:     orchestratorinfo.ServiceInfoStatus_Draining,
	api.NodeStatusUnhealthy:    orchestratorinfo.ServiceInfoStatus_Unhealthy,
	api.NodeStatusStandby:      orchestratorinfo.ServiceInfoStatus_Standby,
	api.NodeStatusShuttingDown: orchestratorinfo.ServiceInfoStatus_ShuttingDown,
	api.NodeStatusNotReady:     orchestratorinfo.ServiceInfoStatus_NotReady,
}

type StatusInfo struct {
	Status    api.NodeStatus
	ChangedAt time.Time
}

func (n *Node) Status() api.NodeStatus {
	return n.StatusInfo().Status
}

// CanAcceptNewRequests reports whether the node may be sent new requests. A
// status with no orchestrator counterpart — api.NodeStatusConnecting, which is
// derived from the local gRPC channel state — is treated as unroutable.
func (n *Node) CanAcceptNewRequests() bool {
	status, ok := ApiNodeToOrchestratorStateMapper[n.Status()]

	return ok && status.CanAcceptNewRequests()
}

func (n *Node) StatusInfo() StatusInfo {
	n.mutex.RLock()
	defer n.mutex.RUnlock()

	status := n.status.Status
	if status == api.NodeStatusReady {
		switch n.client.State() {
		case connectivity.Shutdown:
			status = api.NodeStatusUnhealthy
		case connectivity.TransientFailure, connectivity.Connecting:
			status = api.NodeStatusConnecting
		}
	}

	return StatusInfo{Status: status, ChangedAt: n.status.ChangedAt}
}

func (n *Node) setStatus(ctx context.Context, status api.NodeStatus, changedAt time.Time) {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	if n.status.Status != status {
		logger.L().Info(ctx, "NodeID status changed", logger.WithNodeID(n.ID), zap.String("status", string(status)), zap.String("backend", n.Backend))
	}

	n.status = StatusInfo{Status: status, ChangedAt: changedAt}
}

// markUnhealthyLocal marks the node as unhealthy based on a local observation.
// If the node is already unhealthy, the original transition time is preserved
// so the status change timestamp reflects when the node first became unhealthy.
func (n *Node) markUnhealthyLocal(ctx context.Context) {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	if n.status.Status == api.NodeStatusUnhealthy {
		return
	}

	logger.L().Info(ctx, "NodeID status changed", logger.WithNodeID(n.ID), zap.String("status", string(api.NodeStatusUnhealthy)), zap.String("backend", n.Backend))
	n.status = StatusInfo{Status: api.NodeStatusUnhealthy, ChangedAt: time.Now()}
}

// markReachable records that the node answered this replica, clearing any
// unreachable observation.
func (n *Node) markReachable() {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	n.unreachableSince = time.Time{}
}

// markUnreachable records that the node did not answer this replica at all.
//
// First write wins, so the unreachable duration keeps accumulating across
// consecutive silent cycles — unlike status.ChangedAt, which is set on the
// transition and would restart the clock on every retry.
//
// Deliberately not called from markUnhealthyLocal, even though a sync failure
// usually implies both. A sync can fail on a node this replica did reach:
// ServiceInfo answers and the sandbox list call then fails. The node has
// demonstrably answered, so it is not unreachable, and conflating the two would
// let one failed call present a live node as a candidate for reclamation.
func (n *Node) markUnreachable() {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	if n.unreachableSince.IsZero() {
		n.unreachableSince = time.Now()
	}
}

// UnreachableSince reports when this replica first failed a full sync cycle
// against the node, and whether it is currently unreachable at all.
//
// Deliberately separate from Status, because the two answer different
// questions. A node that answers the sync and reports itself Unhealthy is
// responsive — it is telling us something. A node this replica cannot reach at
// all is not telling us anything. Both currently collapse into
// api.NodeStatusUnhealthy, and anything reasoning about whether a node is
// still there needs them apart.
//
// A local, single-replica signal: it says nothing about whether other replicas
// can reach the node, so it is only ever evidence about this replica's view.
func (n *Node) UnreachableSince() (time.Time, bool) {
	n.mutex.RLock()
	defer n.mutex.RUnlock()

	return n.unreachableSince, !n.unreachableSince.IsZero()
}

func (n *Node) SendStatusChange(ctx context.Context, s api.NodeStatus) error {
	if s == api.NodeStatusShuttingDown {
		return grpcstatus.Error(codes.FailedPrecondition, "shutting_down can only be entered during process shutdown")
	}
	if s == api.NodeStatusNotReady {
		return grpcstatus.Error(codes.FailedPrecondition, "not_ready can only be entered during process startup")
	}

	nodeStatus, ok := ApiNodeToOrchestratorStateMapper[s]
	if !ok {
		logger.L().Error(ctx, "Unknown service info status", zap.String("status", string(s)), logger.WithNodeID(n.ID))

		return fmt.Errorf("unknown service info status: %s", s)
	}

	client, ctx := n.GetClient(ctx)
	_, err := client.Info.ServiceStatusOverride(ctx, &orchestratorinfo.ServiceStatusChangeRequest{ServiceStatus: nodeStatus})
	if err != nil {
		logger.L().Error(ctx, "Failed to send status change", zap.Error(err))

		return err
	}

	return nil
}
