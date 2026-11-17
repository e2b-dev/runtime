package clusters

import (
	"context"
	"math"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/e2b-dev/infra/packages/api/internal/utils"
	infogrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator-info"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/machineinfo"
	"github.com/e2b-dev/infra/packages/shared/pkg/servicediscovery"
)

const (
	// maxSyncFailuresBeforeUnhealthy defines the number of consecutive sync failures
	// before an instance is marked as unhealthy.
	maxSyncFailuresBeforeUnhealthy = 3

	maxInstanceSyncCallTimeout = 1 * time.Second
)

type Instance struct {
	// Identifier that uniquely identifies the instance so it will not be registered multiple times.
	// Depending on service discovery used, it can be combination of different parameters, what service discovery gives us.
	workloadID string

	ClusterID uuid.UUID
	NodeID    string

	LocalIPAddress string

	// Backend is the discovery lister the instance came from, carried through
	// so a node built from this one can still say how it was found.
	Backend string

	serviceInstanceID    string
	serviceVersion       string
	serviceVersionCommit string

	client          *GRPCClient
	status          infogrpc.ServiceInfoStatus
	statusChangedAt time.Time
	machine         machineinfo.MachineInfo
	roles           []infogrpc.ServiceInfoRole
	isBuilder       bool
	isOrchestrator  bool

	// Host CPU as last reported, read by builder placement.
	cpuPercent   uint32
	cpuCount     uint32
	cpuAllocated uint32
	// Builds this API placed here recently, which the reports may not show yet.
	reservations []buildReservation

	syncFailCount int

	mutex sync.RWMutex
}

// InstanceInfo contains synchronized instance information
type InstanceInfo struct {
	ServiceInstanceID    string
	ServiceVersion       string
	ServiceVersionCommit string
	Status               infogrpc.ServiceInfoStatus
	StatusChangedAt      time.Time
	IsOrchestrator       bool
	IsBuilder            bool
}

// instanceFrom projects a discovered instance onto a pool entry. The process
// identity becomes the pool's key, so a restart replaces rather than shadows;
// the machine is kept because a build persists the one it was placed on and
// resolves it again later.
func instanceFrom(clusterID uuid.UUID, sd servicediscovery.Instance, client *GRPCClient) *Instance {
	return &Instance{
		workloadID:     sd.WorkloadID,
		NodeID:         sd.NodeID,
		ClusterID:      clusterID,
		LocalIPAddress: sd.IPAddress,
		Backend:        sd.Backend,

		client: client,
	}
}

func newInstance(ctx context.Context, clusterID uuid.UUID, sd servicediscovery.Instance, client *GRPCClient) (*Instance, error) {
	client.Init(ctx)

	// Create with default values that will be updated on sync before returning the instance,
	// so we will never have uninitialized instance status or roles.
	//
	// For case with local cluster we will not receive instance ID from service discovery, but its not needed for proxy routing,
	// so it can be empty and will be filled after first sync.
	i := instanceFrom(clusterID, sd, client)

	err := i.Sync(ctx)
	if err != nil {
		closeErr := client.Close()
		if closeErr != nil {
			logger.L().Error(
				ctx, "Failed to close gRPC Connection after instance sync failure",
				zap.Error(closeErr),
				logger.WithNodeID(i.NodeID),
				logger.WithClusterID(i.ClusterID),
				logger.WithServiceInstanceID(i.serviceInstanceID),
			)
		}

		return nil, err
	}

	return i, nil
}

// Sync function can be called on freshly initialized instance to populate its data
// In initial case its possible that service instance id needed for proper remote cluster routing is not yet set.
func (i *Instance) Sync(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, maxInstanceSyncCallTimeout)
	defer cancel()

	info, err := i.client.Info.ServiceInfo(ctx, &emptypb.Empty{})
	err = utils.UnwrapGRPCError(err)
	if err != nil {
		i.mutex.Lock()
		defer i.mutex.Unlock()

		// Increase fail count and set unhealthy status if needed
		i.syncFailCount++
		if i.syncFailCount >= maxSyncFailuresBeforeUnhealthy {
			logger.L().Warn(ctx, "Instance sync failed multiple times, marking instance as unhealthy",
				logger.WithNodeID(i.NodeID),
				logger.WithClusterID(i.ClusterID),
				logger.WithServiceInstanceID(i.serviceInstanceID),
				zap.Int("counter", i.syncFailCount),
				zap.Error(err),
			)

			if i.status != infogrpc.ServiceInfoStatus_Unhealthy {
				i.status = infogrpc.ServiceInfoStatus_Unhealthy
				i.statusChangedAt = time.Now()
			}
		}

		return err
	}

	i.mutex.Lock()
	defer i.mutex.Unlock()

	// Reset fail count on successful sync
	i.syncFailCount = 0

	if ts := info.GetServiceStatusChangedAt(); ts.IsValid() {
		i.statusChangedAt = ts.AsTime()
	}

	i.status = info.GetServiceStatus()
	i.roles = info.GetServiceRoles()
	i.machine = machineinfo.FromGRPCInfo(info.GetMachineInfo())

	i.serviceInstanceID = info.GetServiceId()
	i.serviceVersion = info.GetServiceVersion()
	i.serviceVersionCommit = info.GetServiceCommit()

	i.cpuPercent = info.GetMetricCpuPercent()
	i.cpuCount = info.GetMetricCpuCount()
	i.cpuAllocated = info.GetMetricCpuAllocated()

	// We don't want to check array every time,
	// this is why we are caching value during instance sync.
	i.isBuilder = slices.Contains(i.roles, infogrpc.ServiceInfoRole_TemplateBuilder)
	i.isOrchestrator = slices.Contains(i.roles, infogrpc.ServiceInfoRole_Orchestrator)

	return nil
}

func (i *Instance) GetMachineInfo() machineinfo.MachineInfo {
	i.mutex.RLock()
	defer i.mutex.RUnlock()

	return i.machine
}

func (i *Instance) GetInfo() InstanceInfo {
	i.mutex.RLock()
	defer i.mutex.RUnlock()

	return InstanceInfo{
		ServiceInstanceID:    i.serviceInstanceID,
		ServiceVersion:       i.serviceVersion,
		ServiceVersionCommit: i.serviceVersionCommit,
		Status:               i.status,
		StatusChangedAt:      i.statusChangedAt,
		IsOrchestrator:       i.isOrchestrator,
		IsBuilder:            i.isBuilder,
	}
}

// buildReservationTTL is how long a build placed on a builder counts toward
// its load. The builder reports every instance sync, but a build's VM, and so
// its vCPUs, appear only once the builder has pulled and unpacked its image.
// Expiry rather than an explicit release keeps a build that never starts from
// holding capacity.
const buildReservationTTL = time.Minute

type buildReservation struct {
	vcpus   int64
	expires time.Time
}

// reserveBuild counts vcpus against this instance for buildReservationTTL.
func (i *Instance) reserveBuild(vcpus int64) {
	if vcpus <= 0 {
		return
	}

	i.mutex.Lock()
	defer i.mutex.Unlock()

	i.reservations = append(i.reservations, buildReservation{vcpus: vcpus, expires: time.Now().Add(buildReservationTTL)})
}

// cpuLoad is the host's CPU demand per core: vCPUs allocated to its VMs and
// reserved by recent builds, plus cores busy right now, over its core count.
// An instance that has not reported a core count scores +Inf, so any instance
// reporting load is preferred to it.
func (i *Instance) cpuLoad() float64 {
	i.mutex.Lock()
	defer i.mutex.Unlock()

	if i.cpuCount == 0 {
		return math.Inf(1)
	}

	now := time.Now()
	i.reservations = slices.DeleteFunc(i.reservations, func(r buildReservation) bool { return now.After(r.expires) })

	reserved := int64(0)
	for _, r := range i.reservations {
		reserved += r.vcpus
	}

	return (float64(i.cpuAllocated)+float64(reserved))/float64(i.cpuCount) + float64(i.cpuPercent)/100
}

func (i *Instance) GetClient() *GRPCClient {
	return i.client
}

func (i *Instance) Close() error {
	return i.client.Close()
}
