package clusters

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/api/internal/cfg"
	clickhouse "github.com/e2b-dev/infra/packages/clickhouse/pkg"
	"github.com/e2b-dev/infra/packages/db/client"
	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
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

type Pool struct {
	db  *client.Client
	tel *telemetry.Client

	clusters        *smap.Map[*Cluster]
	synchronization *synchronization.Synchronize[queries.Cluster, *Cluster]
	store           clustersSyncStore

	cancel           context.CancelFunc
	lifecycleDone    <-chan struct{}
	startupReady     chan struct{}
	startupReadyOnce sync.Once
	lifecycleMu      *sync.Mutex
}

func localClusterConfig() *queries.Cluster {
	return &queries.Cluster{
		ID:                 consts.LocalClusterID,
		EndpointTls:        false,
		SandboxProxyDomain: nil,
	}
}

func NewPool(
	ctx context.Context,
	tel *telemetry.Client,
	db *client.Client,
	localDiscovery servicediscovery.Discoverer,
	queryMetricsProvider clickhouse.Clickhouse,
	queryLogsProvider *loki.LokiQueryProvider,
	sandboxLogsReader ClickhouseLogsReader,
	featureFlags *featureflags.Client,
	config cfg.Config,
) (*Pool, error) {
	clusters := smap.New[*Cluster]()
	poolCtx, cancel := context.WithCancel(ctx)
	p := &Pool{
		db:            db,
		tel:           tel,
		clusters:      clusters,
		cancel:        cancel,
		lifecycleDone: poolCtx.Done(),
		startupReady:  make(chan struct{}),
		lifecycleMu:   &sync.Mutex{},
	}
	p.store = clustersSyncStore{
		config:               config,
		db:                   db,
		tel:                  tel,
		clusters:             clusters,
		local:                localClusterConfig(),
		localDiscovery:       localDiscovery,
		queryLogsProvider:    queryLogsProvider,
		queryMetricsProvider: queryMetricsProvider,
		sandboxLogsReader:    sandboxLogsReader,
		featureFlags:         featureFlags,
		activateCluster: func(cluster *Cluster, mode initialSyncMode) error {
			return p.activateCluster(poolCtx, cluster, mode)
		},
	}
	p.synchronization = synchronization.NewSynchronize("clusters-pool", "Clusters pool", p.store)

	coordinator := newStartupCoordinator(
		func(inventoryCtx context.Context) ([]queries.Cluster, error) {
			return p.initialInventory(poolCtx, inventoryCtx)
		},
		func(attemptCtx context.Context, cluster queries.Cluster) error {
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
	local                *queries.Cluster
	localDiscovery       servicediscovery.Discoverer
	queryMetricsProvider clickhouse.Clickhouse
	queryLogsProvider    *loki.LokiQueryProvider
	sandboxLogsReader    ClickhouseLogsReader
	featureFlags         *featureflags.Client
	config               cfg.Config
	activateCluster      func(*Cluster, initialSyncMode) error
}

func (d clustersSyncStore) SourceList(ctx context.Context) ([]queries.Cluster, error) {
	db, err := d.db.GetActiveClusters(ctx)
	if err != nil {
		return nil, err
	}

	entries := make([]queries.Cluster, 0)
	for _, row := range db {
		entries = append(entries, row.Cluster)
	}

	// Append local cluster if provided
	if d.local != nil {
		entries = append(entries, *d.local)
	}

	return entries, nil
}

func (d clustersSyncStore) SourceExists(_ context.Context, s []queries.Cluster, p *Cluster) bool {
	for _, item := range s {
		if item.ID == p.ID {
			return true
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

func (d clustersSyncStore) PoolExists(_ context.Context, cluster queries.Cluster) bool {
	_, found := d.clusters.Get(cluster.ID.String())

	return found
}

func (d clustersSyncStore) PoolInsert(ctx context.Context, cluster queries.Cluster) {
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

func (d clustersSyncStore) newCluster(cluster queries.Cluster) (*Cluster, error) {
	if cluster.ID == consts.LocalClusterID {
		return newLocalCluster(d.tel, d.localDiscovery, d.queryMetricsProvider, d.queryLogsProvider, d.sandboxLogsReader, d.featureFlags, d.config), nil
	}

	authOrgID := ""
	if cluster.AuthOrgID != nil {
		authOrgID = *cluster.AuthOrgID
	}

	return newRemoteCluster(
		d.tel,
		cluster.Endpoint,
		cluster.EndpointTls,
		cluster.Token,
		cluster.ID,
		cluster.SandboxProxyDomain,
		authOrgID,
	)
}

func (p *Pool) initialInventory(lifecycleCtx context.Context, ctx context.Context) ([]queries.Cluster, error) {
	clusters, err := p.store.SourceList(ctx)
	if err != nil {
		return nil, err
	}

	targets := make([]queries.Cluster, 0, len(clusters))
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

func (p *Pool) startInitialRemoteCluster(lifecycleCtx context.Context, ctx context.Context, cluster queries.Cluster) error {
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

// activateCluster publishes a constructed cluster: it starts the cluster's
// periodic discovery and adds it to the pool. Both happen under lifecycleMu,
// the mutex Close holds while cancelling, so a cluster can never start or
// become visible after shutdown began. Callers construct the cluster and run
// any network calls outside this method, both to keep those off the mutex and
// because the caller owns the cluster until activation succeeds.
func (p *Pool) activateCluster(lifecycleCtx context.Context, cluster *Cluster, mode initialSyncMode) error {
	p.lifecycleMu.Lock()
	defer p.lifecycleMu.Unlock()

	if err := lifecycleCtx.Err(); err != nil {
		return err
	}

	cluster.Start(lifecycleCtx, mode)
	p.clusters.Insert(cluster.ID.String(), cluster)

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

func (d clustersSyncStore) PoolRemove(ctx context.Context, cluster *Cluster) {
	logger.L().Info(ctx, "Removing cluster from pool", logger.WithClusterID(cluster.ID))

	err := cluster.Close(ctx)
	if err != nil {
		logger.L().Error(ctx, "Error during removing cluster from pool", zap.Error(err), logger.WithClusterID(cluster.ID))
	}

	d.clusters.Remove(cluster.ID.String())
}
