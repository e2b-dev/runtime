package nodemanager

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/clusters"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/machineinfo"
	"github.com/e2b-dev/infra/packages/shared/pkg/smap"
	"github.com/e2b-dev/infra/packages/shared/pkg/units"
)

// nodeSource is the discovery source a node came from. The zero value is
// deliberately invalid: a node has to say where it came from, rather than
// having it inferred from whichever identity field happens to be unset.
type nodeSource uint8

const (
	sourceUnset nodeSource = iota
	sourceNodePlane
	sourceClusterRegistry
)

type NodePlaneInstance struct {
	WorkloadID string

	OrchestratorAddress string
	IPAddress           string
	Backend             string
}

type Node struct {
	source nodeSource

	// WorkloadID is set only by the node plane; the cluster registry keys on
	// the machine instead.
	WorkloadID string

	ID            string
	ClusterID     uuid.UUID
	IPAddress     string
	SandboxDomain *string

	// Backend is the discovery lister this node was found through.
	Backend string

	client *clusters.GRPCClient
	status StatusInfo

	// unreachableSince is the time of the first local observation of a full
	// sync-cycle failure against this node, cleared by the next successful
	// cycle. Deliberately separate from status: a node whose orchestrator
	// answers the sync and self-reports Unhealthy is responsive, and must not
	// count as unreachable.
	unreachableSince time.Time

	metrics             Metrics
	metricsMu           sync.RWMutex
	lastHugePageAdd     time.Time
	lastOrdinaryPageAdd time.Time

	machineInfo  machineinfo.MachineInfo
	labels       map[string]struct{}
	capabilities []string
	meta         NodeMetadata

	PlacementMetrics PlacementMetrics

	mutex sync.RWMutex
}

func New(
	ctx context.Context,
	tracerProvider trace.TracerProvider,
	meterProvider metric.MeterProvider,
	discoveredNode NodePlaneInstance,
	creds credentials.TransportCredentials,
) (*Node, error) {
	client, err := NewClient(tracerProvider, meterProvider, discoveredNode.OrchestratorAddress, creds)
	if err != nil {
		return nil, err
	}
	client.Init(ctx)

	nodeInfo, err := client.Info.ServiceInfo(ctx, &emptypb.Empty{})
	if err != nil {
		_ = client.Close()

		return nil, fmt.Errorf("failed to get node service info: %w", err)
	}

	nodeMetadata := NodeMetadata{
		ServiceInstanceID: nodeInfo.GetServiceId(),
		Commit:            nodeInfo.GetServiceCommit(),
		Version:           nodeInfo.GetServiceVersion(),
	}

	n := &Node{
		source:        sourceNodePlane,
		WorkloadID:    discoveredNode.WorkloadID,
		ClusterID:     consts.LocalClusterID,
		ID:            nodeInfo.GetNodeId(),
		IPAddress:     discoveredNode.IPAddress,
		SandboxDomain: nil,
		Backend:       discoveredNode.Backend,

		client: client,
		status: reportedStatus(ctx, nodeInfo.GetNodeId(), nodeInfo),
		meta:   nodeMetadata,

		PlacementMetrics: PlacementMetrics{
			sandboxesInProgress: smap.New[SandboxResources](),
			createSuccess:       atomic.Uint64{},
			createFails:         atomic.Uint64{},
		},
	}

	n.UpdateMetricsFromServiceInfoResponse(nodeInfo)
	n.setMachineInfo(nodeInfo.GetMachineInfo())
	n.setLabels(nodeInfo.GetLabels())
	n.setCapabilities(nodeInfo.GetCapabilities())

	return n, nil
}

func NewClusterNode(ctx context.Context, client *clusters.GRPCClient, clusterID uuid.UUID, sandboxDomain *string, i *clusters.Instance) (*Node, error) {
	info := i.GetInfo()
	status, ok := OrchestratorToApiNodeStateMapper[info.Status]
	if !ok {
		logger.L().Error(ctx, "Unknown service info status", zap.String("status", info.Status.String()), logger.WithNodeID(i.NodeID))
		status = api.NodeStatusUnhealthy
	}

	nodeMetadata := NodeMetadata{
		ServiceInstanceID: info.ServiceInstanceID,
		Commit:            info.ServiceVersionCommit,
		Version:           info.ServiceVersion,
	}

	n := &Node{
		source:    sourceClusterRegistry,
		ClusterID: clusterID,
		Backend:   i.Backend,
		ID:        i.NodeID,
		// API control-plane calls still use the cluster gRPC proxy, but edge/client
		// proxies need the node IP address for data-plane sandbox traffic.
		IPAddress:     i.LocalIPAddress,
		SandboxDomain: sandboxDomain,
		PlacementMetrics: PlacementMetrics{
			sandboxesInProgress: smap.New[SandboxResources](),
			createSuccess:       atomic.Uint64{},
			createFails:         atomic.Uint64{},
		},

		client: client,
		status: StatusInfo{Status: status, ChangedAt: info.StatusChangedAt},
		meta:   nodeMetadata,
	}

	nodeClient, ctx := n.GetClient(ctx)
	nodeInfo, err := nodeClient.Info.ServiceInfo(ctx, &emptypb.Empty{})
	if err != nil {
		logger.L().Error(ctx, "Failed to get node service info", zap.Error(err), logger.WithNodeID(n.ID))

		return n, nil
	}

	n.UpdateMetricsFromServiceInfoResponse(nodeInfo)
	n.setMachineInfo(nodeInfo.GetMachineInfo())
	n.setLabels(nodeInfo.GetLabels())
	n.setCapabilities(nodeInfo.GetCapabilities())
	// The report is fresher than the registry entry the node was built from.
	reported := reportedStatus(ctx, n.ID, nodeInfo)
	n.setStatus(ctx, reported.Status, reported.Owner, reported.ChangedAt)

	return n, nil
}

func (n *Node) Close(ctx context.Context) error {
	if n.DiscoveredByNodePlane() {
		logger.L().Info(ctx, "Closing local node", logger.WithNodeID(n.ID))
		if err := n.client.Close(); err != nil {
			logger.L().Error(ctx, "Error closing client to node", zap.Error(err), logger.WithNodeID(n.ID))
		}
	} else {
		logger.L().Info(ctx, "Closing cluster node", logger.WithNodeID(n.ID), logger.WithClusterID(n.ClusterID))
		// We are not closing grpc client, because it is managed by cluster instance
	}

	return nil
}

// Ensures that GRPC client request context always has the latest service instance ID
func (n *Node) GetClient(ctx context.Context) (*clusters.GRPCClient, context.Context) {
	return n.client, ctx
}

// DiscoveredByNodePlane reports whether the node came from the node-plane
// listing rather than the cluster registry. Only the node plane can validate
// membership against a fresh listing, so only it may evict on absence.
func (n *Node) DiscoveredByNodePlane() bool {
	return n.source == sourceNodePlane
}

func (n *Node) IsClusterNode() bool {
	return n.ClusterID != consts.LocalClusterID
}

func (n *Node) OptimisticAdd(res SandboxResources) {
	n.metricsMu.Lock()
	defer n.metricsMu.Unlock()

	// Directly accumulate to the current metrics view
	n.metrics.CpuAllocated += uint32(res.CPUs)
	n.metrics.MemoryAllocatedBytes += uint64(res.MiBMemory) * 1024 * 1024 // Note: CpuPercent is difficult to estimate, usually just updating Allocated is sufficient for the scheduling algorithm
	if res.HugePages {
		n.metrics.HugePagesReserved += HugePagesFor(res.MiBMemory, n.metrics.HugePageSizeBytes)
		n.lastHugePageAdd = time.Now()
	} else {
		// The whole RAM, though ordinary pages fault in on use: the next
		// sync replaces it with what the guest has touched.
		n.metrics.MemoryUsedBytes += uint64(units.MBToBytes(res.MiBMemory))
		n.lastOrdinaryPageAdd = time.Now()
	}
}

// CompletePlacement moves a successful placement from in flight into the node
// view under the lock StartPlacingIf checks under, so a check made there counts
// it exactly once.
func (n *Node) CompletePlacement(sandboxID string, res SandboxResources) {
	n.PlacementMetrics.startMu.Lock()
	defer n.PlacementMetrics.startMu.Unlock()

	n.OptimisticAdd(res)
	n.PlacementMetrics.Success(sandboxID)
}

func (n *Node) OptimisticRemove(ctx context.Context, res SandboxResources) {
	n.metricsMu.Lock()
	defer n.metricsMu.Unlock()

	cpu := uint32(res.CPUs)
	memory := uint64(res.MiBMemory) * 1024 * 1024

	// Hugepage counts and used memory wait for the next sync: a sandbox whose
	// balloon returned free pages holds fewer than its RAM, so its full size
	// would undercount.
	// Each counter subtracts on its own, so one underflow cannot stick the other.
	cpuSkipped := cpu > n.metrics.CpuAllocated
	if !cpuSkipped {
		n.metrics.CpuAllocated -= cpu
	}

	memorySkipped := memory > n.metrics.MemoryAllocatedBytes
	if !memorySkipped {
		n.metrics.MemoryAllocatedBytes -= memory
	}

	if cpuSkipped || memorySkipped {
		logger.L().Warn(ctx, "OptimisticRemove skipped counters that would underflow", logger.WithNodeID(n.ID), zap.Bool("cpuSkipped", cpuSkipped), zap.Bool("memorySkipped", memorySkipped), zap.Uint32("cpuAllocated", n.metrics.CpuAllocated), zap.Uint64("memoryAllocatedBytes", n.metrics.MemoryAllocatedBytes), zap.Uint32("cpuToRemove", cpu), zap.Uint64("memoryToRemove", memory))
	}
}

// HugePagesFor is the page count a sandbox of miB commits against the pool.
// Zero page size means the node reported no pool.
func HugePagesFor(miB int64, pageSize uint64) uint64 {
	if pageSize == 0 || miB <= 0 {
		return 0
	}

	bytes := uint64(units.MBToBytes(miB))

	return (bytes + pageSize - 1) / pageSize
}
