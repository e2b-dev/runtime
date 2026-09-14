package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"

	analyticscollector "github.com/e2b-dev/infra/packages/api/internal/analytics_collector"
	"github.com/e2b-dev/infra/packages/api/internal/cfg"
	"github.com/e2b-dev/infra/packages/api/internal/clusters"
	"github.com/e2b-dev/infra/packages/api/internal/metrics"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/evictor"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/placement"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	redisreservations "github.com/e2b-dev/infra/packages/api/internal/sandbox/reservations/redis"
	redisbackend "github.com/e2b-dev/infra/packages/api/internal/sandbox/storage/redis"
	sqlcdb "github.com/e2b-dev/infra/packages/db/client"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	e2bcatalog "github.com/e2b-dev/infra/packages/shared/pkg/sandbox-catalog"
	"github.com/e2b-dev/infra/packages/shared/pkg/servicediscovery"
	"github.com/e2b-dev/infra/packages/shared/pkg/smap"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

const statusLogInterval = time.Second * 20

var ErrNodeNotFound = errors.New("node not found")

// SnapshotCacheInvalidator invalidates cached snapshot entries.
type SnapshotCacheInvalidator interface {
	Invalidate(ctx context.Context, sandboxID string)
}

type Orchestrator struct {
	httpClient                    *http.Client
	nodeDiscovery                 servicediscovery.Discoverer
	sandboxStore                  *sandbox.Store
	nodes                         *smap.Map[*nodemanager.Node]
	placementAlgorithm            *placement.BestOfK
	featureFlagsClient            *featureflags.Client
	analytics                     *analyticscollector.Analytics
	posthogClient                 *analyticscollector.PosthogClient
	routingCatalog                e2bcatalog.SandboxesCatalog
	sqlcDB                        *sqlcdb.Client
	tel                           *telemetry.Client
	clusters                      *clusters.Pool
	metricsRegistration           metric.Registration
	sandboxCountGaugeRegistration metric.Registration
	createdSandboxesCounter       metric.Int64Counter
	resumeOriginNodeRemapCounter  metric.Int64Counter
	pauseRefusalRestoreCounter    metric.Int64Counter
	teamMetricsObserver           *metrics.TeamObserver
	accessTokenGenerator          *sandbox.AccessTokenGenerator
	createdCounter                metric.Int64Counter
	snapshotCache                 SnapshotCacheInvalidator

	snapshotUpsertSem *utils.AdjustableSemaphore
	redisStorage      *redisbackend.Storage

	// localClusterOwnsOrchestrators makes connectToClusterNode register
	// local-cluster instances that report the Orchestrator role as nodes.
	//
	// It is only set when the node discovery loop is disabled (see
	// skipNomadSync in New), which is the case in the local environment: there
	// the local clusters registry is the single source of orchestrator nodes.
	//
	// Otherwise local-cluster orchestrators are owned by the node discovery
	// path (connectToNode), which identifies nodes by the ID they report over
	// the Info RPC, while the clusters registry identifies instances by their
	// discovery item ID. An instance serving both the orchestrator and the
	// template-builder role would then register twice under two different node
	// IDs and have its capacity and sandboxes counted twice.
	localClusterOwnsOrchestrators bool

	// connectGroup deduplicates concurrent dial+register attempts for the same
	// physical node. It is keyed by WorkloadID (Nomad-managed nodes) or
	// scopedNodeID(clusterID, instanceNodeID) (cluster nodes) and is held inside
	// connectToNode / connectToClusterNode, so it guards every connection path
	// regardless of what triggered the attempt.
	connectGroup singleflight.Group

	// discoveryGroup deduplicates concurrent on-demand discovery attempts in
	// getOrConnectNode that target the same missing orchestrator node. It is
	// intentionally separate from connectGroup to avoid a deadlock: for cluster
	// nodes the outer discoveryGroup key and the inner connectGroup key are the
	// same string, and nesting Do calls for the same key on the same Group would
	// block forever.
	discoveryGroup singleflight.Group
}

func New(
	ctx context.Context,
	config cfg.Config,
	tel *telemetry.Client,
	nodeDiscovery servicediscovery.Discoverer,
	localRegistryOwnsOrchestrators bool,
	posthogClient *analyticscollector.PosthogClient,
	redisClient redis.UniversalClient,
	sqlcDB *sqlcdb.Client,
	clusters *clusters.Pool,
	featureFlags *featureflags.Client,
	accessTokenGenerator *sandbox.AccessTokenGenerator,
	snapshotCache SnapshotCacheInvalidator,
	snapshotUpsertSem *utils.AdjustableSemaphore,
) (*Orchestrator, error) {
	analyticsInstance, err := analyticscollector.NewAnalytics(
		config.AnalyticsCollectorHost,
		config.AnalyticsCollectorAPIToken,
		config.AnalyticsCollectorTLS,
	)
	if err != nil {
		logger.L().Error(ctx, "Error initializing Analytics client", zap.Error(err))

		return nil, err
	}
	analyticsInstance.Init(ctx)

	routingCatalog := e2bcatalog.NewRedisSandboxCatalog(redisClient)

	// We will need to either use Redis or Consul's KV for storing active sandboxes to keep everything in sync,
	// right now we load them from Orchestrator
	meter := tel.MeterProvider.Meter("github.com/e2b-dev/infra/packages/api/internal/orchestrator")

	createdCounter, err := telemetry.GetCounter(meter, telemetry.SandboxCreateMeterName)
	if err != nil {
		logger.L().Error(ctx, "error getting counter", zap.Error(err))

		return nil, err
	}

	httpClient := &http.Client{
		Timeout: nodeHealthCheckTimeout,
	}

	bestOfKAlgorithm := placement.NewBestOfK(getBestOfKConfig(ctx, featureFlags)).(*placement.BestOfK)

	redisStorage, err := redisbackend.NewStorage(redisClient, tel.MeterProvider, featureFlags)
	if err != nil {
		return nil, fmt.Errorf("failed to create redis sandbox storage: %w", err)
	}
	go redisStorage.Start(ctx)

	// The local clusters registry is the only source of orchestrator nodes
	// exactly when the discovery provider is the static local one: there both
	// planes are the same single instance and the registry registers it. Under
	// nomad or kubernetes the node plane lists orchestrators itself, local
	// environment or not — deriving this from the environment instead of the
	// resolved provider builds a node plane nothing ever consults.
	skipNomadSync := localRegistryOwnsOrchestrators

	o := Orchestrator{
		httpClient:           httpClient,
		analytics:            analyticsInstance,
		posthogClient:        posthogClient,
		nodeDiscovery:        nodeDiscovery,
		nodes:                smap.New[*nodemanager.Node](),
		placementAlgorithm:   bestOfKAlgorithm,
		featureFlagsClient:   featureFlags,
		accessTokenGenerator: accessTokenGenerator,
		routingCatalog:       routingCatalog,
		sqlcDB:               sqlcDB,
		snapshotCache:        snapshotCache,
		tel:                  tel,
		clusters:             clusters,
		redisStorage:         redisStorage,

		createdCounter: createdCounter,

		snapshotUpsertSem: snapshotUpsertSem,

		// Without the node discovery loop, the local clusters registry is the
		// only source of orchestrator nodes.
		localClusterOwnsOrchestrators: skipNomadSync,
	}

	o.sandboxStore = sandbox.NewStore(
		redisStorage,
		redisreservations.NewReservationStorage(redisClient, redisStorage.Notifier()),
		sandbox.Callbacks{
			AddSandboxToRoutingTable: o.addSandboxToRoutingTableOrLog,
			AsyncNewlyCreatedSandbox: o.handleNewlyCreatedSandbox,
			KillOrphanSandbox:        o.killOrphanSandbox,
		},
	)

	// Evict old sandboxes
	sandboxEvictor, err := evictor.New(ctx, o.sandboxStore, o.RemoveSandbox, o.featureFlagsClient, meter)
	if err != nil {
		return nil, fmt.Errorf("failed to create sandbox evictor: %w", err)
	}
	go sandboxEvictor.Start(ctx)

	teamMetricsObserver, err := metrics.NewTeamObserver(ctx, o.sandboxStore)
	if err != nil {
		logger.L().Error(ctx, "Failed to create team metrics observer", zap.Error(err))

		return nil, fmt.Errorf("failed to create team metrics observer: %w", err)
	}

	o.teamMetricsObserver = teamMetricsObserver

	go o.keepInSync(ctx, o.sandboxStore, skipNomadSync)

	if err := o.setupMetrics(tel.MeterProvider); err != nil {
		logger.L().Error(ctx, "Failed to setup metrics", zap.Error(err))

		return nil, fmt.Errorf("failed to setup metrics: %w", err)
	}

	go o.startStatusLogging(ctx)
	go o.updateBestOfKConfig(ctx)

	return &o, nil
}

func (o *Orchestrator) startStatusLogging(ctx context.Context) {
	ticker := time.NewTicker(statusLogInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.L().Info(ctx, "Stopping status logging")

			return
		case <-ticker.C:
			connectedNodes := make([]map[string]any, 0, o.nodes.Count())
			templateManagers := make([]map[string]any, 0)

			for _, nodeItem := range o.nodes.Items() {
				if nodeItem == nil {
					connectedNodes = append(connectedNodes, map[string]any{
						"id": "nil",
					})
				} else {
					connectedNodes = append(connectedNodes, map[string]any{
						"id":        nodeItem.ID,
						"status":    nodeItem.Status(),
						"sandboxes": nodeItem.Metrics().SandboxCount,
					})
				}
			}

			for _, cluster := range o.clusters.GetClusters() {
				for _, templateManager := range cluster.GetTemplateBuilders() {
					info := templateManager.GetInfo()
					templateManagers = append(templateManagers, map[string]any{
						"cluster_id":          templateManager.ClusterID,
						"node_id":             templateManager.NodeID,
						"service_instance_id": info.ServiceInstanceID,
						"status":              info.Status.String(),
					})
				}
			}

			logger.L().Info(ctx, "API internal status",
				zap.Int("nodes_count", o.nodes.Count()),
				zap.Any("nodes", connectedNodes),
				zap.Int("template_managers_count", len(templateManagers)),
				zap.Any("template_managers", templateManagers),
			)
		}
	}
}

func (o *Orchestrator) Close(ctx context.Context) error {
	var errs []error

	connectedNodes := o.nodes.Items()
	for _, node := range connectedNodes {
		if err := node.Close(ctx); err != nil {
			errs = append(errs, err)
		}
	}

	logger.L().Info(ctx, "Shutting down node clients", zap.Int("error_count", len(errs)), zap.Int("node_count", len(connectedNodes)))

	if o.metricsRegistration != nil {
		if err := o.metricsRegistration.Unregister(); err != nil {
			errs = append(errs, fmt.Errorf("failed to unregister metrics: %w", err))
		}
	}

	if o.sandboxCountGaugeRegistration != nil {
		if err := o.sandboxCountGaugeRegistration.Unregister(); err != nil {
			errs = append(errs, fmt.Errorf("failed to unregister sandbox count gauge: %w", err))
		}
	}

	if o.teamMetricsObserver != nil {
		if err := o.teamMetricsObserver.Close(ctx); err != nil {
			errs = append(errs, fmt.Errorf("failed to close team metrics observer: %w", err))
		}
	}

	if err := o.analytics.Close(); err != nil {
		errs = append(errs, err)
	}

	if err := o.routingCatalog.Close(ctx); err != nil {
		errs = append(errs, err)
	}

	o.redisStorage.Close(ctx)

	return errors.Join(errs...)
}

// updateBestOfKConfig periodically updates the BestOfK algorithm configuration from feature flags
func (o *Orchestrator) updateBestOfKConfig(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second) // Check for config updates every 30 seconds
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			config := getBestOfKConfig(ctx, o.featureFlagsClient)

			// Update the config
			o.placementAlgorithm.UpdateConfig(config)
		}
	}
}

func getBestOfKConfig(ctx context.Context, featureFlagsClient *featureflags.Client) placement.BestOfKConfig {
	k := featureFlagsClient.IntFlag(ctx, featureflags.BestOfKSampleSize)

	maxOvercommitPercent := featureFlagsClient.IntFlag(ctx, featureflags.BestOfKMaxOvercommit)

	alphaPercent := featureFlagsClient.IntFlag(ctx, featureflags.BestOfKAlpha)

	// Convert percentage to decimal
	alpha := float64(alphaPercent) / 100.0
	maxOvercommit := float64(maxOvercommitPercent) / 100.0

	return placement.BestOfKConfig{
		R:              maxOvercommit,
		K:              k,
		Alpha:          alpha,
		ScoreHugepages: featureFlagsClient.BoolFlag(ctx, featureflags.BestOfKHugepageMemory),
	}
}
