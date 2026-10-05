package clusters

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/api/internal/cfg"
	clickhouse "github.com/e2b-dev/infra/packages/clickhouse/pkg"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	infogrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator-info"
	api "github.com/e2b-dev/infra/packages/shared/pkg/http/edge"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/logs/loki"
	"github.com/e2b-dev/infra/packages/shared/pkg/machineinfo"
	"github.com/e2b-dev/infra/packages/shared/pkg/servicediscovery"
	"github.com/e2b-dev/infra/packages/shared/pkg/smap"
	"github.com/e2b-dev/infra/packages/shared/pkg/synchronization"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

var tracer = otel.Tracer("github.com/e2b-dev/infra/packages/api/internal/clusters")

const (
	instancesSyncInterval = 5 * time.Second
	instancesSyncTimeout  = 5 * time.Second
)

type Cluster struct {
	ID            uuid.UUID
	SandboxDomain *string
	AuthOrgID     string

	instances       *smap.Map[*Instance]
	synchronization *synchronization.Synchronize[servicediscovery.Instance, *Instance]
	resources       ClusterResource
}

var (
	ErrTemplateBuilderNotFound          = errors.New("template builder not found")
	ErrAvailableTemplateBuilderNotFound = errors.New("available template builder not found")
)

func NewCluster(
	clusterID uuid.UUID,
	domain *string,
	authOrgID string,
	sandboxes *smap.Map[*Instance],
	synchronization *synchronization.Synchronize[servicediscovery.Instance, *Instance],
	resources ClusterResource,
) *Cluster {
	return &Cluster{
		ID:              clusterID,
		SandboxDomain:   domain,
		AuthOrgID:       authOrgID,
		instances:       sandboxes,
		synchronization: synchronization,
		resources:       resources,
	}
}

func newLocalCluster(
	tel *telemetry.Client,
	storeDiscovery servicediscovery.Discoverer,
	clickhouse clickhouse.Clickhouse,
	queryLogsProvider *loki.LokiQueryProvider,
	sandboxLogsReader ClickhouseLogsReader,
	featureFlags *featureflags.Client,
	config cfg.Config,
) *Cluster {
	clusterID := consts.LocalClusterID

	instances := smap.New[*Instance]()
	instanceCreation := func(ctx context.Context, item servicediscovery.Instance) (*Instance, error) {
		// For local cluster we are doing direct connection to instance IP and API port and without additional cluster auth.
		return newInstance(ctx, tel, nil, clusterID, item, item.Address(), false)
	}

	store := instancesSyncStore{clusterID: clusterID, instances: instances, discovery: storeDiscovery, instanceCreation: instanceCreation}

	c := NewCluster(
		clusterID,
		nil,
		"",
		instances,
		synchronization.NewSynchronize("cluster-instances", "Cluster instances", store),
		newLocalClusterResourceProvider(clickhouse, queryLogsProvider, sandboxLogsReader, featureFlags, instances, config),
	)

	return c
}

// remoteInstanceAuthorization routes a call through the remote proxy to one
// instance. The discovery ID is that instance's service id: the edge API
// reports it per run, which is what the proxy keys on — the machine would send
// the call to whatever is on it now.
func remoteInstanceAuthorization(secret string, tls bool, sd servicediscovery.Instance) *instanceAuthorization {
	return &instanceAuthorization{secret: secret, tls: tls, serviceInstanceID: sd.WorkloadID}
}

func newRemoteCluster(
	tel *telemetry.Client,
	endpoint string,
	endpointTLS bool,
	secret string,
	clusterID uuid.UUID,
	sandboxDomain *string,
	authOrgID string,
) (*Cluster, error) {
	scheme := "http"
	if endpointTLS {
		scheme = "https"
	}

	endpointBaseUrl := fmt.Sprintf("%s://%s", scheme, endpoint)

	httpClient, err := api.NewClientWithResponses(
		endpointBaseUrl,
		func(c *api.Client) error {
			c.RequestEditors = append(
				c.RequestEditors,
				func(_ context.Context, req *http.Request) error {
					req.Header.Set(consts.EdgeApiAuthHeader, secret)

					return nil
				},
			)

			return nil
		},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create http client: %w", err)
	}

	instances := smap.New[*Instance]()
	instanceCreation := func(ctx context.Context, item servicediscovery.Instance) (*Instance, error) {
		// A remote cluster is reached through an endpoint acting as a gRPC proxy,
		// which handles auth and routing for us.
		return newInstance(ctx, tel, remoteInstanceAuthorization(secret, endpointTLS, item), clusterID, item, endpoint, endpointTLS)
	}

	storeDiscovery := servicediscovery.NewRemote(httpClient)
	store := instancesSyncStore{clusterID: clusterID, instances: instances, instanceCreation: instanceCreation, discovery: storeDiscovery}

	c := NewCluster(
		clusterID,
		sandboxDomain,
		authOrgID,
		instances,
		synchronization.NewSynchronize("cluster-instances", "Cluster instances", store),
		newRemoteClusterResourceProvider(clusterID, instances, httpClient),
	)

	return c, nil
}

// initialSyncMode says whether a cluster should run a discovery round as soon
// as it starts, or wait for the first scheduled tick. A cluster that already
// applied a snapshot during startup must not immediately run a second,
// overlapping discovery for the same cluster.
type initialSyncMode int

const (
	syncImmediately initialSyncMode = iota
	syncOnNextTick
)

// Start begins the periodic discovery loop after construction.
func (c *Cluster) Start(ctx context.Context, mode initialSyncMode) {
	if ctx.Err() != nil {
		return
	}

	go c.synchronization.Start(ctx, instancesSyncInterval, instancesSyncTimeout, mode == syncImmediately)
}

func (c *Cluster) Close(ctx context.Context) error {
	c.synchronization.Close()

	instances := c.instances.Items()
	wg := sync.WaitGroup{}

	for _, instance := range instances {
		wg.Go(func() {
			if closeErr := instance.Close(); closeErr != nil {
				logger.L().Error(ctx, "Failed to close cluster instance during cluster closing",
					zap.Error(closeErr),
					logger.WithClusterID(c.ID),
					logger.WithNodeID(instance.NodeID),
				)
			}
		})
	}

	// Wait for all instances to be closed
	wg.Wait()

	return nil
}

// Scanned rather than looked up: the pool is keyed by the running process, and
// a build persists the machine it was placed on (env_builds.cluster_node_id),
// so this resolves the machine to whichever process is on it now.
func (c *Cluster) GetTemplateBuilderByNodeID(nodeID string) (*Instance, error) {
	instance, found := builderOnNode(c.instances, nodeID)
	if !found {
		return nil, ErrTemplateBuilderNotFound
	}

	return instance, nil
}

// instanceOnNode finds an instance running on a machine, preferring one that is
// not unhealthy. Like builderOnNode this cannot be a map lookup, but it applies
// no further filter: callers that only need to reach the machine should not
// start rejecting instances the old lookup accepted.
func instanceOnNode(instances *smap.Map[*Instance], nodeID string) (*Instance, bool) {
	var unhealthy *Instance

	for _, instance := range instances.Items() {
		if instance.NodeID != nodeID {
			continue
		}

		if instance.GetInfo().Status == infogrpc.ServiceInfoStatus_Unhealthy {
			unhealthy = instance

			continue
		}

		return instance, true
	}

	return unhealthy, unhealthy != nil
}

// builderOnNode finds the usable template builder running on a machine. The
// pool is keyed by process, so this cannot be a map lookup, and every entry on
// the machine has to be considered rather than the first: a restart leaves the
// dead run and the live one sharing a machine for a sync round, and stopping
// early would answer with whichever the map happened to yield.
func builderOnNode(instances *smap.Map[*Instance], nodeID string) (*Instance, bool) {
	for _, instance := range instances.Items() {
		if instance.NodeID != nodeID {
			continue
		}

		if info := instance.GetInfo(); info.Status == infogrpc.ServiceInfoStatus_Unhealthy || !info.IsBuilder {
			continue
		}

		return instance, true
	}

	return nil, false
}

func (c *Cluster) GetByServiceInstanceID(serviceInstanceID string) (*Instance, bool) {
	for _, instance := range c.instances.Items() {
		info := instance.GetInfo()
		if info.ServiceInstanceID == serviceInstanceID {
			return instance, true
		}
	}

	return nil, false
}

func (c *Cluster) getRandomInstance(isMatch func(InstanceInfo, machineinfo.MachineInfo) bool) (*Instance, bool) {
	var instances []*Instance
	for _, instance := range c.instances.Items() {
		instances = append(instances, instance)
	}

	// Make sure we will always iterate in different order and when there is bigger amount of builders, we will not always pick the same one
	rand.Shuffle(len(instances), func(i, j int) { instances[i], instances[j] = instances[j], instances[i] })

	for _, instance := range instances {
		if isMatch(instance.GetInfo(), instance.GetMachineInfo()) {
			return instance, true
		}
	}

	return nil, false
}

func (c *Cluster) GetAvailableTemplateBuilder(ctx context.Context, expectedInfo machineinfo.MachineInfo) (*Instance, error) {
	_, span := tracer.Start(ctx, "template-builder-get-available-instance")
	span.SetAttributes(telemetry.WithClusterID(c.ID))
	defer span.End()

	instance, ok := c.getRandomInstance(func(info InstanceInfo, machineInfo machineinfo.MachineInfo) bool {
		// Check availability and builder role
		if !info.Status.CanAcceptNewRequests() || !info.IsBuilder {
			return false
		}

		// Require an exact CPU match for the template builder (no
		// cross-generation compatibility) so builds run on the configured CPU.
		if expectedInfo.CPUArchitecture != "" && !expectedInfo.IsExactMatch(machineInfo) {
			return false
		}

		return true
	})
	if !ok {
		return nil, ErrAvailableTemplateBuilderNotFound
	}

	return instance, nil
}

func (c *Cluster) GetOrchestrators() []*Instance {
	instances := make([]*Instance, 0)
	for _, i := range c.instances.Items() {
		if i.GetInfo().IsOrchestrator {
			instances = append(instances, i)
		}
	}

	return instances
}

func (c *Cluster) GetTemplateBuilders() []*Instance {
	instances := make([]*Instance, 0)
	for _, i := range c.instances.Items() {
		if i != nil && i.GetInfo().IsBuilder {
			instances = append(instances, i)
		}
	}

	return instances
}

func (c *Cluster) GetResources() ClusterResource {
	return c.resources
}

// SyncInstances performs an immediate synchronization of cluster instances from
// the service discovery source. It is called on-demand when a node lookup fails,
// to handle newly joined orchestrators that may not yet be in the in-memory pool.
func (c *Cluster) SyncInstances(ctx context.Context) error {
	return c.synchronization.Sync(ctx)
}
