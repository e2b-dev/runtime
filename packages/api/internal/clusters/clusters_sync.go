package clusters

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc/credentials"

	"github.com/e2b-dev/infra/packages/api/internal/cfg"
	"github.com/e2b-dev/infra/packages/api/internal/db/client"
	clickhouse "github.com/e2b-dev/infra/packages/clickhouse/pkg"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	clustersgrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/clusters/management"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/logs/loki"
	"github.com/e2b-dev/infra/packages/shared/pkg/servicediscovery"
	"github.com/e2b-dev/infra/packages/shared/pkg/smap"
	"github.com/e2b-dev/infra/packages/shared/pkg/synchronization"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

const (
	clustersSyncInterval = 15 * time.Second
	clusterSyncTimeout   = 5 * time.Second
)

// Record is a remote cluster as the cluster service stores it.
type Record struct {
	ID                 uuid.UUID
	Endpoint           string
	EndpointTls        bool
	Token              string
	SandboxProxyDomain *string
	AuthOrgID          *string
}

type Pool struct {
	tel *telemetry.Client

	clusters        *smap.Map[*Cluster]
	synchronization *synchronization.Synchronize[Record, *Cluster]
	store           clustersSyncStore

	cancel           context.CancelFunc
	lifecycleDone    <-chan struct{}
	startupReady     chan struct{}
	startupReadyOnce sync.Once
	lifecycleMu      *sync.Mutex

	// localInstances carries one token per batch of joins to the local
	// cluster's registry.
	localInstances chan struct{}
}

func localClusterConfig() *Record {
	return &Record{
		ID:                 consts.LocalClusterID,
		EndpointTls:        false,
		SandboxProxyDomain: nil,
	}
}

func NewPool(
	ctx context.Context,
	tel *telemetry.Client,
	db *client.Client,
	clusterService clustersgrpc.ClusterServiceClient,
	localDiscovery servicediscovery.Discoverer,
	queryMetricsProvider clickhouse.Clickhouse,
	queryLogsProvider *loki.LokiQueryProvider,
	sandboxLogsReader ClickhouseLogsReader,
	featureFlags *featureflags.Client,
	config cfg.Config,
	localCreds credentials.TransportCredentials,
) (*Pool, error) {
	clusters := smap.New[*Cluster]()
	poolCtx, cancel := context.WithCancel(ctx)
	p := &Pool{
		tel:            tel,
		clusters:       clusters,
		cancel:         cancel,
		lifecycleDone:  poolCtx.Done(),
		startupReady:   make(chan struct{}),
		lifecycleMu:    &sync.Mutex{},
		localInstances: make(chan struct{}, 1),
	}
	p.store = clustersSyncStore{
		config:               config,
		db:                   db,
		clusterService:       clusterService,
		tel:                  tel,
		clusters:             clusters,
		local:                localClusterConfig(),
		localDiscovery:       localDiscovery,
		queryLogsProvider:    queryLogsProvider,
		queryMetricsProvider: queryMetricsProvider,
		sandboxLogsReader:    sandboxLogsReader,
		featureFlags:         featureFlags,
		localCreds:           localCreds,
		onLocalInstance:      p.notifyLocalInstance,
		activateCluster: func(cluster *Cluster, mode initialSyncMode) error {
			return p.activateCluster(poolCtx, cluster, mode)
		},
	}
	p.synchronization = synchronization.NewSynchronize("clusters-pool", "Clusters pool", p.store)

	source := &startupSource{list: p.store.SourceList, local: p.store.local}
	coordinator := newStartupCoordinator(
		func(inventoryCtx context.Context) ([]Record, error) {
			return p.initialInventory(poolCtx, inventoryCtx, source.inventory)
		},
		func(attemptCtx context.Context, cluster Record) error {
			return p.startInitialRemoteCluster(poolCtx, attemptCtx, cluster)
		},
		p.finishStartup,
	)
	go coordinator.Start(poolCtx)

	return p, nil
}

func (p *Pool) GetClusterById(id uuid.UUID) (*Cluster, bool) {
	return p.clusters.Get(id.String())
}

func (p *Pool) GetClusters() map[string]*Cluster {
	return p.clusters.Items()
}

// LocalInstanceJoined delivers a token after an instance joins the local
// cluster's registry. Tokens coalesce, so one receive can stand for several
// joins, and a token sent before anyone receives waits for the first receiver.
func (p *Pool) LocalInstanceJoined() <-chan struct{} {
	return p.localInstances
}

func (p *Pool) notifyLocalInstance() {
	select {
	case p.localInstances <- struct{}{}:
	default:
	}
}

// StartupReady closes after the first successful inventory and every remote
// target in that inventory has applied a snapshot or exhausted startup retries.
func (p *Pool) StartupReady() <-chan struct{} {
	return p.startupReady
}

func (p *Pool) Close(ctx context.Context) {
	p.lifecycleMu.Lock()
	p.cancel()
	p.lifecycleMu.Unlock()
	p.synchronization.Close()

	wg := &sync.WaitGroup{}
	for _, cluster := range p.clusters.Items() {
		wg.Go(func() {
			logger.L().Info(ctx, "Closing cluster", logger.WithClusterID(cluster.ID))
			err := cluster.Close(ctx)
			if err != nil {
				logger.L().Error(ctx, "Error closing cluster", zap.Error(err), logger.WithClusterID(cluster.ID))
			}
		})
	}
	wg.Wait()
}

// SynchronizationStore is an interface that defines methods for synchronizing the clusters pool with the database
type clustersSyncStore struct {
	db                   *client.Client
	tel                  *telemetry.Client
	clusters             *smap.Map[*Cluster]
	local                *Record
	localDiscovery       servicediscovery.Discoverer
	queryMetricsProvider clickhouse.Clickhouse
	queryLogsProvider    *loki.LokiQueryProvider
	sandboxLogsReader    ClickhouseLogsReader
	featureFlags         *featureflags.Client
	config               cfg.Config
	localCreds           credentials.TransportCredentials
	activateCluster      func(*Cluster, initialSyncMode) error
	onLocalInstance      func()

	// clusterService is nil when none is configured, which leaves the local
	// cluster as the only one.
	clusterService clustersgrpc.ClusterServiceClient
}

func (d clustersSyncStore) SourceList(ctx context.Context) ([]Record, error) {
	entries := make([]Record, 0)
	if d.clusterService != nil {
		remote, err := assignedRemoteClusters(ctx, d.db, d.clusterService)
		if err != nil {
			return nil, err
		}
		entries = append(entries, remote...)
	}

	// Append local cluster if provided
	if d.local != nil {
		entries = append(entries, *d.local)
	}

	return entries, nil
}

func (d clustersSyncStore) SourceExists(_ context.Context, s []Record, p *Cluster) bool {
	for _, item := range s {
		if item.ID == p.ID {
			// A replacement deployment keeps its cluster ID but rotates the edge
			// credential, and a cluster that moves domain changes its endpoint
			// and sandbox domain. Retire its old clients so the next sync
			// recreates them.
			sameDomain := (item.SandboxProxyDomain == nil) == (p.SandboxDomain == nil) &&
				(item.SandboxProxyDomain == nil || *item.SandboxProxyDomain == *p.SandboxDomain)

			return item.ID == consts.LocalClusterID || (item.Token == p.edgeToken && item.Endpoint == p.endpoint &&
				item.EndpointTls == p.endpointTLS && sameDomain)
		}
	}

	return false
}

func (d clustersSyncStore) PoolList(_ context.Context) []*Cluster {
	items := make([]*Cluster, 0)
	for _, item := range d.clusters.Items() {
		items = append(items, item)
	}

	return items
}

func (d clustersSyncStore) PoolExists(_ context.Context, cluster Record) bool {
	_, found := d.clusters.Get(cluster.ID.String())

	return found
}

func (d clustersSyncStore) PoolInsert(ctx context.Context, cluster Record) {
	logger.L().Info(ctx, "Initializing newly discovered cluster", logger.WithClusterID(cluster.ID))

	c, err := d.newCluster(cluster)
	if err != nil {
		logger.L().Error(ctx, "Initializing cluster failed", zap.Error(err), logger.WithClusterID(cluster.ID))

		return
	}

	if err := d.activateCluster(c, syncImmediately); err != nil {
		logger.L().Info(ctx, "Skipped cluster initialization during shutdown", zap.Error(err), logger.WithClusterID(cluster.ID))

		return
	}

	logger.L().Info(ctx, "Cluster initialized successfully", logger.WithClusterID(cluster.ID))
}

func (d clustersSyncStore) newCluster(cluster Record) (*Cluster, error) {
	if cluster.ID == consts.LocalClusterID {
		return newLocalCluster(d.tel, d.localDiscovery, d.queryMetricsProvider, d.queryLogsProvider, d.sandboxLogsReader, d.featureFlags, d.config, d.localCreds, d.onLocalInstance), nil
	}

	return newRemoteCluster(d.tel, d.featureFlags, cluster)
}

func (p *Pool) initialInventory(lifecycleCtx context.Context, ctx context.Context, list func(context.Context) ([]Record, error)) ([]Record, error) {
	clusters, err := list(ctx)
	if err != nil {
		return nil, err
	}

	targets := make([]Record, 0, len(clusters))
	for _, cluster := range clusters {
		if cluster.ID != consts.LocalClusterID {
			targets = append(targets, cluster)

			continue
		}

		local, localErr := p.store.newCluster(cluster)
		if localErr != nil {
			return nil, localErr
		}
		if err := p.activateCluster(lifecycleCtx, local, syncImmediately); err != nil {
			return nil, err
		}
	}

	return targets, nil
}

func (p *Pool) startInitialRemoteCluster(lifecycleCtx context.Context, ctx context.Context, cluster Record) error {
	c, err := p.store.newCluster(cluster)
	if err != nil {
		return err
	}

	if err := c.SyncInstances(ctx); err != nil {
		_ = c.Close(ctx)

		return err
	}

	// The snapshot is already applied, so the periodic loop starts at its first
	// scheduled tick rather than repeating discovery now.
	if err := p.activateCluster(lifecycleCtx, c, syncOnNextTick); err != nil {
		_ = c.Close(ctx)

		return err
	}

	return nil
}

// activateCluster publishes a constructed cluster: it adds the cluster to the
// pool and starts the cluster's periodic discovery. Both happen under
// lifecycleMu, the mutex Close holds while cancelling, so a cluster can never
// start or become visible after shutdown began. The insert comes first so a
// registry join reported by the first discovery round finds the cluster in
// the pool. Callers construct the cluster and run any network calls outside
// this method, both to keep those off the mutex and because the caller owns
// the cluster until activation succeeds.
func (p *Pool) activateCluster(lifecycleCtx context.Context, cluster *Cluster, mode initialSyncMode) error {
	p.lifecycleMu.Lock()
	defer p.lifecycleMu.Unlock()

	if err := lifecycleCtx.Err(); err != nil {
		return err
	}

	p.clusters.Insert(cluster.ID.String(), cluster)
	cluster.Start(lifecycleCtx, mode)

	return nil
}

func (p *Pool) finishStartup(lifecycleCtx context.Context) {
	p.lifecycleMu.Lock()
	defer p.lifecycleMu.Unlock()

	if lifecycleCtx.Err() != nil {
		return
	}

	p.startupReadyOnce.Do(func() {
		close(p.startupReady)
		go p.synchronization.Start(lifecycleCtx, clustersSyncInterval, clusterSyncTimeout, false)
	})
}

func (d clustersSyncStore) PoolUpdate(_ context.Context, _ *Cluster) {
	// Clusters pool currently does not do something special during synchronization
}

// PoolReplace swaps a cluster whose endpoint, TLS setting, sandbox domain or
// credential changed for one built from its current row, and only then closes
// the old clients, so GetClusterById finds the cluster throughout the move.
func (d clustersSyncStore) PoolReplace(ctx context.Context, rows []Record, old *Cluster) bool {
	for _, row := range rows {
		if row.ID != old.ID {
			continue
		}

		replacement, err := d.newCluster(row)
		if err != nil {
			logger.L().Error(ctx, "Initializing changed cluster failed", zap.Error(err), logger.WithClusterID(row.ID))

			return false
		}
		if err := d.activateCluster(replacement, syncImmediately); err != nil {
			logger.L().Info(ctx, "Skipped changed cluster replacement during shutdown", zap.Error(err), logger.WithClusterID(row.ID))

			return false
		}
		if err := old.Close(ctx); err != nil {
			logger.L().Error(ctx, "Error closing replaced cluster", zap.Error(err), logger.WithClusterID(row.ID))
		}
		logger.L().Info(ctx, "Replaced changed cluster", logger.WithClusterID(row.ID))

		return true
	}

	return false
}

func (d clustersSyncStore) PoolRemove(ctx context.Context, cluster *Cluster) {
	logger.L().Info(ctx, "Removing cluster from pool", logger.WithClusterID(cluster.ID))

	err := cluster.Close(ctx)
	if err != nil {
		logger.L().Error(ctx, "Error during removing cluster from pool", zap.Error(err), logger.WithClusterID(cluster.ID))
	}

	d.clusters.Remove(cluster.ID.String())
}

// errClusterList marks a cluster list the cluster service failed to answer.
var errClusterList = errors.New("the cluster service did not list the clusters")

// assignedRemoteClusters lists the stored clusters a team references: a
// cluster no team uses has nothing for the pool to schedule.
func assignedRemoteClusters(ctx context.Context, db *client.Client, clusterService clustersgrpc.ClusterServiceClient) ([]Record, error) {
	assigned, err := db.ListAssignedClusterIDs(ctx)
	if err != nil {
		return nil, fmt.Errorf("list assigned cluster IDs: %w", err)
	}
	if len(assigned) == 0 {
		return nil, nil
	}
	isAssigned := make(map[uuid.UUID]struct{}, len(assigned))
	for _, id := range assigned {
		isAssigned[id] = struct{}{}
	}

	resp, err := clusterService.ListClusters(ctx, clustersgrpc.ListClustersRequest_builder{}.Build())
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errClusterList, err)
	}

	records := make([]Record, 0, len(assigned))
	for _, cluster := range resp.GetClusters() {
		id, err := uuid.Parse(cluster.GetClusterId())
		if err != nil {
			return nil, fmt.Errorf("parse cluster ID %q: %w", cluster.GetClusterId(), err)
		}
		if _, ok := isAssigned[id]; !ok {
			continue
		}
		records = append(records, recordFromCluster(id, cluster))
	}

	return records, nil
}

func recordFromCluster(id uuid.UUID, cluster *clustersgrpc.Cluster) Record {
	record := Record{
		ID:          id,
		Endpoint:    cluster.GetEndpoint(),
		EndpointTls: cluster.GetEndpointTls(),
		Token:       cluster.GetToken(),
	}
	if cluster.HasSandboxProxyDomain() {
		record.SandboxProxyDomain = new(cluster.GetSandboxProxyDomain())
	}
	if cluster.HasAuthOrgId() {
		record.AuthOrgID = new(cluster.GetAuthOrgId())
	}

	return record
}
