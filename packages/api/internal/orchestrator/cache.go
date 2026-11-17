package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

var tracer = otel.Tracer("github.com/e2b-dev/infra/packages/api/internal/orchestrator")

// cacheSyncTime is the time to sync the cache with the actual instances in Orchestrator.
const cacheSyncTime = 20 * time.Second

const nodeConnectTimeout = 5 * time.Second

func (o *Orchestrator) GetSandbox(ctx context.Context, teamID uuid.UUID, sandboxID string) (sandbox.Sandbox, error) {
	return o.sandboxStore.Get(ctx, teamID, sandboxID)
}

// keepInSync the cache with the actual instances in Orchestrator to handle instances that died.
func (o *Orchestrator) keepInSync(ctx context.Context, store *sandbox.Store, skipSyncingWithNomad bool) {
	// A stop ends the loop between syncs. A sync cut short would report every
	// node it had not reached as failing and mark it unhealthy; it ends on
	// its own timeout instead.
	syncCtx := context.WithoutCancel(ctx)

	// Run the first sync immediately
	logger.L().Info(ctx, "Running the initial node sync")
	o.syncNodes(syncCtx, store, skipSyncingWithNomad)

	// Sync the nodes every cacheSyncTime
	ticker := time.NewTicker(cacheSyncTime)
	defer ticker.Stop()

	for {
		// Between syncs, refresh as often as the flag asks. A nil channel never fires.
		var refresh <-chan time.Time
		if every := o.nodeRefreshInterval(ctx); every > 0 {
			refresh = time.After(every)
		}

		select {
		case <-ctx.Done():
			logger.L().Info(ctx, "Stopping keepInSync")

			return
		// A tick can be ready together with the stop, and select then picks
		// either: no round starts once stopped.
		case <-ticker.C:
			if ctx.Err() == nil {
				o.syncNodes(syncCtx, store, skipSyncingWithNomad)
			}
		case <-refresh:
			if ctx.Err() == nil {
				o.refreshNodes(syncCtx)
			}
		}
	}
}

// nodeRefreshInterval is how often the flag asks for a refresh between syncs;
// zero is never. A flag that does not exist yet reads as zero.
func (o *Orchestrator) nodeRefreshInterval(ctx context.Context) time.Duration {
	seconds, _ := o.featureFlagsClient.IntFlagWithError(ctx, featureflags.NodeInfoRefreshIntervalSecondsFlag)

	return time.Duration(seconds) * time.Second
}

// refreshNodes asks the local cluster's nodes for their status and metrics.
// A remote cluster's nodes are left to the sync: they are reached through the
// cluster's proxy, and its registry already polls each of them.
func (o *Orchestrator) refreshNodes(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, nodeHealthCheckTimeout)
	defer cancel()

	ctx, span := tracer.Start(ctx, "refresh-nodes")
	defer span.End()

	var wg sync.WaitGroup
	defer wg.Wait()

	for _, n := range o.nodes.Items() {
		if !n.IsClusterNode() {
			wg.Go(func() { n.RefreshInfo(ctx) })
		}
	}
}

func (o *Orchestrator) syncNodes(ctx context.Context, store *sandbox.Store, skipSyncingWithNomad bool) {
	ctxTimeout, cancel := context.WithTimeout(ctx, cacheSyncTime)
	defer cancel()

	ctx, span := tracer.Start(ctxTimeout, "keep-in-sync")
	defer span.End()

	var wg sync.WaitGroup

	nomadNodes := make([]nodemanager.NodePlaneInstance, 0)

	// Optionally, skip syncing from Nomad service discovery
	if !skipSyncingWithNomad {
		nomadSD, err := o.listNomadNodes(ctx)
		if err != nil {
			logger.L().Error(ctx, "Error listing orchestrator nodes", zap.Error(err))

			return
		}

		nomadNodes = nomadSD
	}

	wg.Go(func() {
		o.syncLocalDiscoveredNodes(ctx, nomadNodes)
	})

	wg.Go(func() {
		o.syncClusterDiscoveredNodes(ctx)
	})

	// Wait for nodes discovery to finish
	wg.Wait()

	// Sync state of all nodes currently in the pool
	ctx, syncNodesSpan := tracer.Start(ctx, "keep-in-sync-existing")
	defer syncNodesSpan.End()

	defer wg.Wait()
	for _, n := range o.nodes.Items() {
		wg.Go(func() {
			// cluster and local nodes needs to by synced differently,
			// because each of them is taken from different source pool
			var err error
			switch {
			case !n.DiscoveredByNodePlane():
				err = o.syncClusterNode(ctx, n, store)
			case skipSyncingWithNomad:
				// In local mode there is no Nomad discovery list to validate
				// membership against, so sync the statically-connected node
				// directly instead of evicting it every cycle. node.Sync still
				// marks the node unhealthy if the orchestrator is unreachable,
				// and only errors when the conn is shut down for good.
				err = n.Sync(ctx, store)
			default:
				err = o.syncNode(ctx, n, nomadNodes, store)
			}
			if err != nil {
				logger.L().Error(ctx, "Error syncing node", zap.Error(err))
				err = n.Close(context.WithoutCancel(ctx))
				if err != nil {
					logger.L().Error(ctx, "Error closing grpc connection", zap.Error(err))
				}

				o.deregisterNode(n)
			}
		})
	}
}

func (o *Orchestrator) syncLocalDiscoveredNodes(ctx context.Context, discovered []nodemanager.NodePlaneInstance) {
	// Connect local nodes that are not in the list, yet
	ctx, span := tracer.Start(ctx, "keep-in-sync-connect-local-nodes")
	defer span.End()

	var wg sync.WaitGroup
	defer wg.Wait()

	for _, n := range discovered {
		// If the node is not in the list, connect to it
		if o.GetNodeByWorkloadID(n.WorkloadID) == nil {
			wg.Go(func() {
				// Make sure slow/failed connections don't block the whole sync loop
				connectCtx, connectCancel := context.WithTimeout(ctx, nodeConnectTimeout)
				defer connectCancel()

				err := o.connectToNode(connectCtx, n)
				if err != nil {
					logger.L().Error(connectCtx, "Error connecting to node", zap.Error(err))
				}
			})
		}
	}
}

func (o *Orchestrator) syncClusterDiscoveredNodes(ctx context.Context) {
	var wg sync.WaitGroup
	defer wg.Wait()

	ctx, span := tracer.Start(ctx, "keep-in-sync-connect-clustered-nodes")
	defer span.End()

	// Connect clustered nodes that are not in the list, yet
	// We need to iterate over all clusters and their nodes
	for _, cluster := range o.clusters.GetClusters() {
		for _, n := range cluster.GetOrchestrators() {
			// If the node is not in the list, connect to it
			if o.GetNode(cluster.ID, n.NodeID) == nil {
				wg.Go(func() {
					// Make sure slow/failed connections don't block the whole sync loop
					connectCtx, connectCancel := context.WithTimeout(ctx, nodeConnectTimeout)
					defer connectCancel()

					o.connectToClusterNode(connectCtx, cluster, n)
				})
			}
		}
	}
}

func (o *Orchestrator) syncClusterNode(ctx context.Context, node *nodemanager.Node, store *sandbox.Store) error {
	ctx, childSpan := tracer.Start(ctx, "sync-cluster-node")
	telemetry.SetAttributes(ctx, telemetry.WithNodeID(node.ID), telemetry.WithClusterID(node.ClusterID))
	defer childSpan.End()

	cluster, clusterFound := o.clusters.GetClusterById(node.ClusterID)
	if !clusterFound {
		return errors.New("cluster not found")
	}

	// We want to find not just node, but explicitly node with expected service instance ID
	// because node can stay same and instance id will change after node restart, witch should trigger node de-registration from pool.
	instanceID := node.Metadata().ServiceInstanceID
	_, found := cluster.GetByServiceInstanceID(instanceID)
	if !found {
		return fmt.Errorf("node instance not found with instance ID '%s'", instanceID)
	}

	// Unified call for syncing node state across different node types
	return node.Sync(ctx, store)
}

func (o *Orchestrator) syncNode(ctx context.Context, node *nodemanager.Node, discovered []nodemanager.NodePlaneInstance, store *sandbox.Store) error {
	ctx, childSpan := tracer.Start(ctx, "sync-node")
	telemetry.SetAttributes(ctx, telemetry.WithNodeID(node.ID))
	defer childSpan.End()

	found := false
	for _, activeNode := range discovered {
		if node.WorkloadID == activeNode.WorkloadID {
			found = true

			break
		}
	}

	if !found {
		return fmt.Errorf("node '%s' not found in the discovered nodes", node.WorkloadID)
	}

	// Unified call for syncing node state across different node types
	return node.Sync(ctx, store)
}
