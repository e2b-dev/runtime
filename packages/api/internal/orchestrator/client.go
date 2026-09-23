package orchestrator

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/api/internal/clusters"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

const nodeHealthCheckTimeout = time.Second * 2

func (o *Orchestrator) connectToNode(ctx context.Context, discovered nodemanager.NodePlaneInstance) error {
	ctx, childSpan := tracer.Start(ctx, "connect-to-node")
	defer childSpan.End()

	_, err, _ := o.connectGroup.Do(discovered.WorkloadID, func() (any, error) {
		// Re-check inside the singleflight to prevent race issues due to overwriting existing nodes in the map
		if o.GetNodeByWorkloadID(discovered.WorkloadID) != nil {
			return nil, nil
		}

		connectCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), nodeConnectTimeout)
		defer cancel()

		orchestratorNode, err := nodemanager.New(connectCtx, o.tel.TracerProvider, o.tel.MeterProvider, discovered)
		if err != nil {
			return nil, err
		}

		o.registerNode(orchestratorNode)

		return nil, nil
	})

	return err
}

// registersClusterOrchestrators reports whether instances discovered through
// the clusters registry of the given cluster may be registered as orchestrator
// nodes.
//
// Unless the node discovery loop is disabled (see
// localClusterOwnsOrchestrators), local-cluster orchestrators are owned by the
// node discovery path (connectToNode), which identifies nodes by the ID they
// report over the Info RPC. The local clusters registry only exists to find
// template builders and identifies instances by their discovery item ID, so an
// instance serving both roles — a single process started with
// ORCHESTRATOR_SERVICES=orchestrator,template-manager, as in local dev — would
// otherwise register twice under two different node IDs and have its capacity
// and sandboxes counted twice.
//
// Remote clusters are always registered from their own registry.
func (o *Orchestrator) registersClusterOrchestrators(clusterID uuid.UUID) bool {
	return clusterID != consts.LocalClusterID || o.localClusterOwnsOrchestrators
}

func (o *Orchestrator) connectToClusterNode(ctx context.Context, cluster *clusters.Cluster, i *clusters.Instance) {
	ctx, span := tracer.Start(ctx, "connect-to-cluster-node")
	defer span.End()

	if !o.registersClusterOrchestrators(cluster.ID) {
		return
	}

	// connectGroup is keyed by scopedNodeID so that concurrent callers targeting
	// the same cluster instance share a single dial attempt.
	scopedKey := o.scopedNodeID(cluster.ID, i.NodeID)

	o.connectGroup.Do(scopedKey, func() (any, error) { //nolint:errcheck
		// Re-check inside the singleflight for the same reason as connectToNode.
		if o.GetNode(cluster.ID, i.NodeID) != nil {
			return nil, nil
		}

		connectCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), nodeConnectTimeout)
		defer cancel()

		orchestratorNode, err := nodemanager.NewClusterNode(connectCtx, i.GetClient(), cluster.ID, cluster.SandboxDomain, i)
		if err != nil {
			logger.L().Error(ctx, "Failed to create node", zap.Error(err))

			return nil, nil
		}

		o.registerNode(orchestratorNode)

		return nil, nil
	})
}

// registerNode adds the given node to the in-memory map of nodes
// It has to be called only once per node
func (o *Orchestrator) registerNode(node *nodemanager.Node) {
	scopedKey := o.scopedNodeID(node.ClusterID, node.ID)
	o.nodes.Insert(scopedKey, node)
	if node.ClusterID == consts.LocalClusterID && o.startup != nil {
		o.startup.signalLocalNode()
	}
}

func (o *Orchestrator) deregisterNode(node *nodemanager.Node) {
	scopedKey := o.scopedNodeID(node.ClusterID, node.ID)
	o.nodes.Remove(scopedKey)
}

// When prefixed with cluster ID, node is unique in the map containing nodes from multiple clusters
func (o *Orchestrator) scopedNodeID(clusterID uuid.UUID, nodeID string) string {
	if clusterID == consts.LocalClusterID {
		return nodeID
	}

	return fmt.Sprintf("%s-%s", clusterID.String(), nodeID)
}

// listNomadNodes is the legacy name for the orchestrator-listing call. It now
// dispatches to whatever Discovery the Orchestrator was constructed with
// (Nomad, Kubernetes, ...). The returned slice is in the nodemanager shape
// because callers use it directly to dial the orchestrator gRPC server.
//
// (Name kept for blast-radius reasons; renaming touches >20 sites.)
func (o *Orchestrator) listNomadNodes(ctx context.Context) ([]nodemanager.NodePlaneInstance, error) {
	instances, err := o.nodeDiscovery.ListInstances(ctx)
	if err != nil {
		return nil, err
	}

	result := make([]nodemanager.NodePlaneInstance, 0, len(instances))
	for _, i := range instances {
		result = append(result, nodemanager.NodePlaneInstance{
			WorkloadID:          i.WorkloadID,
			OrchestratorAddress: i.Address(),
			IPAddress:           i.IPAddress,
			Backend:             i.Backend,
		})
	}

	return result, nil
}

func (o *Orchestrator) GetNode(clusterID uuid.UUID, nodeID string) *nodemanager.Node {
	scopedKey := o.scopedNodeID(clusterID, nodeID)
	n, _ := o.nodes.Get(scopedKey)

	return n
}

// getOrConnectNode returns a node from the in-memory cache. When the node is absent it
// performs a targeted on-demand discovery and connection attempt, handling the race
// condition where a new orchestrator joined the cluster after this API instance's last
// sync cycle but another API instance already routed a sandbox there.
//
// There are two distinct gaps that must be covered:
//   - Gap 1 (0–5 s for clusters, 0–20 s for Nomad): the node exists in the upstream
//     source (Nomad / remote service discovery) but has not yet been pulled into the
//     local instance map by the background sync loop.
//   - Gap 2 (0–20 s): the node is in the local instance map but has not yet been
//     promoted into o.nodes by keepInSync.
//
// discoveryGroup ensures that concurrent requests targeting the same missing
// node share a single discovery attempt rather than fanning out.
func (o *Orchestrator) getOrConnectNode(ctx context.Context, clusterID uuid.UUID, nodeID string) *nodemanager.Node {
	ctx, span := tracer.Start(ctx, "get-or-connect-node")
	defer span.End()

	if node := o.GetNode(clusterID, nodeID); node != nil {
		return node
	}

	logger.L().Warn(ctx, "Node not found in cache, attempting on-demand connection",
		logger.WithNodeID(nodeID),
		zap.String("cluster_id", clusterID.String()),
	)

	scopedKey := o.scopedNodeID(clusterID, nodeID)

	o.discoveryGroup.Do(scopedKey, func() (any, error) { //nolint:errcheck
		// Re-check inside the singleflight
		if node := o.GetNode(clusterID, nodeID); node != nil {
			return nil, nil
		}

		connectCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cacheSyncTime)
		defer cancel()

		// The periodic loop already treats the local clusters registry as the
		// only source of orchestrator nodes when it owns them; this path has to
		// agree, or the same process registers twice — once under the node ID
		// it reports over gRPC and once under its discovery item ID — and has
		// its capacity and sandboxes counted twice.
		if clusterID == consts.LocalClusterID && !o.localClusterOwnsOrchestrators {
			o.discoverNomadNodes(connectCtx)
		} else {
			o.discoverClusterNode(connectCtx, clusterID)
		}

		return nil, nil
	})

	return o.GetNode(clusterID, nodeID)
}

// discoverNomadNodes lists all ready Nomad nodes and connects any that are not yet in the pool.
// Once a new node is connected its orchestrator ID becomes the map key, making subsequent GetNode calls succeed.
func (o *Orchestrator) discoverNomadNodes(ctx context.Context) {
	ctx, span := tracer.Start(ctx, "discover-nomad-nodes")
	defer span.End()

	nomadNodes, err := o.listNomadNodes(ctx)
	if err != nil {
		logger.L().Error(ctx, "Error listing Nomad nodes during on-demand discovery", zap.Error(err))

		return
	}

	var wg sync.WaitGroup
	defer wg.Wait()

	for _, n := range nomadNodes {
		if o.GetNodeByWorkloadID(n.WorkloadID) == nil {
			wg.Go(func() {
				if err := o.connectToNode(ctx, n); err != nil {
					logger.L().Error(ctx, "Error connecting to Nomad node on demand",
						zap.Error(err), zap.String("nomad_short_id", n.WorkloadID))
				}
			})
		}
	}
}

// discoverClusterNode forces a fresh service discovery query so that nodes which joined after the
// last periodic sync are pulled into cluster.instances, then opportunistically connects all
// unknown nodes into o.nodes (not just the target), avoiding repeated on-demand discoveries.
func (o *Orchestrator) discoverClusterNode(ctx context.Context, clusterID uuid.UUID) {
	ctx, span := tracer.Start(ctx, "discover-cluster-node")
	defer span.End()

	cluster, found := o.clusters.GetClusterById(clusterID)
	if !found {
		logger.L().Error(ctx, "Cluster not found during on-demand node discovery", logger.WithClusterID(clusterID))

		return
	}

	if err := cluster.SyncInstances(ctx); err != nil {
		logger.L().Error(ctx, "Error syncing cluster instances during on-demand node discovery", zap.Error(err), logger.WithClusterID(clusterID))

		return
	}

	var wg sync.WaitGroup
	defer wg.Wait()

	for _, instance := range cluster.GetOrchestrators() {
		wg.Go(func() {
			o.connectToClusterNode(ctx, cluster, instance)
		})
	}
}

func (o *Orchestrator) GetClusterNodes(clusterID uuid.UUID) []*nodemanager.Node {
	clusterNodes := make([]*nodemanager.Node, 0)
	for _, n := range o.nodes.Items() {
		if n.ClusterID == clusterID {
			clusterNodes = append(clusterNodes, n)
		}
	}

	return clusterNodes
}

// Deprecated: use GetNode instead
func (o *Orchestrator) GetNodeByWorkloadID(id string) *nodemanager.Node {
	for _, n := range o.nodes.Items() {
		if n.WorkloadID == id {
			return n
		}
	}

	return nil
}

func (o *Orchestrator) NodeCount() int {
	return o.nodes.Count()
}
