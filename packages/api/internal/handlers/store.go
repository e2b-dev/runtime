package handlers

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	nomadapi "github.com/hashicorp/nomad/api"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"k8s.io/client-go/kubernetes"

	analyticscollector "github.com/e2b-dev/infra/packages/api/internal/analytics_collector"
	"github.com/e2b-dev/infra/packages/api/internal/api"
	sandboxcountscache "github.com/e2b-dev/infra/packages/api/internal/cache/sandboxcounts"
	snapshotcache "github.com/e2b-dev/infra/packages/api/internal/cache/snapshots"
	templatecache "github.com/e2b-dev/infra/packages/api/internal/cache/templates"
	"github.com/e2b-dev/infra/packages/api/internal/cfg"
	"github.com/e2b-dev/infra/packages/api/internal/clusters"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	managementv1 "github.com/e2b-dev/infra/packages/api/internal/secretsstore/management/v1"
	template_manager "github.com/e2b-dev/infra/packages/api/internal/template-manager"
	"github.com/e2b-dev/infra/packages/api/internal/utils"
	sharedauth "github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
	clickhouse "github.com/e2b-dev/infra/packages/clickhouse/pkg"
	"github.com/e2b-dev/infra/packages/clickhouse/pkg/sandboxlogs"
	sqlcdb "github.com/e2b-dev/infra/packages/db/client"
	authdb "github.com/e2b-dev/infra/packages/db/pkg/auth"
	"github.com/e2b-dev/infra/packages/db/pkg/dberrors"
	"github.com/e2b-dev/infra/packages/db/pkg/pool"
	"github.com/e2b-dev/infra/packages/shared/pkg/apierrors"
	sharedclusters "github.com/e2b-dev/infra/packages/shared/pkg/clusters/discovery"
	"github.com/e2b-dev/infra/packages/shared/pkg/env"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	webhookevents "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/webhooks/events"
	webhookmanagement "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/webhooks/management"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/logs/loki"
	"github.com/e2b-dev/infra/packages/shared/pkg/servicediscovery"
	"github.com/e2b-dev/infra/packages/shared/pkg/servicediscovery/kube"
	"github.com/e2b-dev/infra/packages/shared/pkg/servicediscovery/nomad"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	sharedutils "github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// kubeClientFactory builds the client the Kubernetes discovery backends list
// pods with. Injected so the provider wiring is testable without a cluster.
type kubeClientFactory func(ctx context.Context, endpoint string) (kubernetes.Interface, error)

// serviceDiscovery is the pair of discovery backends the API runs on: the node
// plane (orchestrators) and the template-builder plane.
// Both planes are the same interface over the same package now; they differ in
// what they list, not in how.
type serviceDiscovery struct {
	nodes            servicediscovery.Discoverer
	templateBuilders servicediscovery.Discoverer
}

// newServiceDiscovery builds both discovery planes for
// cfg.ServiceDiscoveryProvider:
//
//	nomad            - both go through the local Nomad agent
//	kubernetes       - both list pods via the K8s API
//	nomad+kubernetes - both are the deduplicated union of the two above
//	local            - both point at one statically configured address
func newServiceDiscovery(ctx context.Context, config cfg.Config, newKube kubeClientFactory, provider string) (serviceDiscovery, error) {
	switch provider {
	case cfg.ServiceDiscoveryProviderKubernetes:
		return newKubernetesServiceDiscovery(ctx, config, newKube)
	case cfg.ServiceDiscoveryProviderNomadKubernetes:
		return newComposedServiceDiscovery(ctx, config, newKube)
	case cfg.ServiceDiscoveryProviderLocal:
		return newLocalServiceDiscovery(config)
	default: // ServiceDiscoveryProviderNomad
		return newNomadServiceDiscovery(config)
	}
}

// newComposedServiceDiscovery unions the Nomad and Kubernetes backends on both
// planes. Nomad is the primary, so its entries win a dedup conflict and its own
// union over the legacy node listing stays nested intact inside this one.
func newComposedServiceDiscovery(ctx context.Context, config cfg.Config, newKube kubeClientFactory) (serviceDiscovery, error) {
	nomadPlanes, err := newNomadServiceDiscovery(config)
	if err != nil {
		return serviceDiscovery{}, err
	}

	kubePlanes, err := newKubernetesServiceDiscovery(ctx, config, newKube)
	if err != nil {
		return serviceDiscovery{}, err
	}

	return serviceDiscovery{
		nodes:            servicediscovery.NewMerged(nomadPlanes.nodes, kubePlanes.nodes),
		templateBuilders: servicediscovery.NewMerged(nomadPlanes.templateBuilders, kubePlanes.templateBuilders),
	}, nil
}

// serviceDiscoveryProvider resolves which provider to build. A provider the
// operator named always wins, including nomad: someone running a local Nomad
// agent has to be able to say so. Only an unset provider defaults, and it
// defaults to local in a local environment because no Nomad agent runs there
// and the builder plane would otherwise dial nothing.
func serviceDiscoveryProvider(config cfg.Config, localEnv bool) string {
	if config.ServiceDiscoveryProvider != "" {
		return config.ServiceDiscoveryProvider
	}

	if localEnv {
		return cfg.ServiceDiscoveryProviderLocal
	}

	return cfg.ServiceDiscoveryProviderNomad
}

func newKubernetesServiceDiscovery(ctx context.Context, config cfg.Config, newKube kubeClientFactory) (serviceDiscovery, error) {
	client, err := newKube(ctx, config.K8sAPIEndpoint)
	if err != nil {
		return serviceDiscovery{}, fmt.Errorf("kubernetes client: %w", err)
	}

	return serviceDiscovery{
		nodes: kube.NewPods(
			client,
			config.K8sNamespace,
			config.K8sOrchestratorPodLabelSelector,
		),
		templateBuilders: kube.NewPods(
			client,
			config.K8sNamespace,
			config.K8sTemplateManagerPodLabelSelector,
		),
	}, nil
}

func newLocalServiceDiscovery(config cfg.Config) (serviceDiscovery, error) {
	nodes, err := servicediscovery.NewLocal(config.LocalOrchestratorAddress)
	if err != nil {
		return serviceDiscovery{}, fmt.Errorf("local orchestrator discovery: %w", err)
	}

	// The local orchestrator doubles as the template builder when it is started
	// with ORCHESTRATOR_SERVICES=orchestrator,template-manager, so both planes
	// point at the same address — the same backend, now that there is only one.
	// An instance that does not report the TemplateBuilder role (the darwin
	// dummy) registers with IsBuilder=false and is never selected for builds.
	return serviceDiscovery{nodes: nodes, templateBuilders: nodes}, nil
}

func newNomadServiceDiscovery(config cfg.Config) (serviceDiscovery, error) {
	client, err := nomadapi.NewClient(&nomadapi.Config{
		Address:  config.NomadAddress,
		SecretID: config.NomadToken,
	})
	if err != nil {
		return serviceDiscovery{}, fmt.Errorf("nomad client: %w", err)
	}

	nodes := nomad.NewServices(client, config.NomadOrchestratorServiceNames)
	// Migration fallback: orchestrator jobs deployed from jobspecs that
	// predate the service port-label fix register their service with an
	// empty Address, so service discovery alone would miss them until
	// they are redeployed. Union in the legacy node-pool listing (service
	// entries win on conflict) so the API flip has no rollout ordering
	// constraint. Disable via NOMAD_ORCHESTRATOR_LEGACY_DISCOVERY_ENABLED
	// once no legacy jobs remain. The pool is hardcoded: legacy jobs only
	// ever ran on the "default" pool.
	if config.NomadOrchestratorLegacyDiscoveryEnabled {
		nodes = servicediscovery.NewMerged(nodes, nomad.NewNodePool(client, "default"))
	}

	return serviceDiscovery{
		nodes:            nodes,
		templateBuilders: nomad.NewAllocations(client, sharedclusters.FilterTemplateBuilders),
	}, nil
}

var _ api.ServerInterface = (*APIStore)(nil)

type teamRunningSandboxCounter interface {
	TeamRunningSandboxCounts(ctx context.Context) (map[uuid.UUID]int64, error)
}

type APIStore struct {
	Healthy      atomic.Bool
	config       cfg.Config
	posthog      *analyticscollector.PosthogClient
	Telemetry    *telemetry.Client
	orchestrator *orchestrator.Orchestrator
	// pauseBackendOverride, when non-nil, replaces the orchestrator for the
	// pause handler's two calls — tests use it to assert the gate's wiring
	// (refusal before RemoveSandbox) without a real orchestrator.
	pauseBackendOverride             pauseOrchestrator
	resumeBackendOverride            resumeWaitOrchestrator
	connectBackendOverride           connectOrchestrator
	lifecycleBackendOverride         cathedralLifecycleOrchestrator
	lifecycleSnapshotCleanupOverride func(context.Context, string, uuid.UUID) error
	teamSandboxCounter               teamRunningSandboxCounter
	templateManager                  *template_manager.TemplateManager
	sqlcDB                           *sqlcdb.Client
	authDB                           *authdb.Client
	redisClient                      redis.UniversalClient
	templateCache                    *templatecache.TemplateCache
	templateBuildsCache              *templatecache.TemplatesBuildCache
	snapshotCache                    *snapshotcache.SnapshotCache
	authService                      sharedauth.Service
	templateSpawnCounter             *utils.TemplateSpawnCounter
	clickhouseStore                  clickhouse.Clickhouse
	sandboxLogsReader                *sandboxlogs.Reader
	accessTokenGenerator             *sandbox.AccessTokenGenerator
	featureFlags                     *featureflags.Client
	clusters                         *clusters.Pool
	snapshotUpsertSem                *sharedutils.AdjustableSemaphore
	sandboxListSem                   *sharedutils.AdjustableSemaphore
	snapshotBuildQuerySem            *sharedutils.AdjustableSemaphore

	// secretsConn and secretsManagement are nil when no secrets store backend
	// address is configured. The routes stay registered either way and answer
	// as they do when the feature gate is closed.
	secretsConn       *grpc.ClientConn
	secretsManagement managementv1.SecretManagementServiceClient

	// The sandbox events and webhook management backend, dialed only when an
	// address is configured. Its routes stay registered either way.
	webhooksConn      *grpc.ClientConn
	webhookManagement webhookmanagement.WebhookManagementServiceClient
	sandboxEvents     webhookevents.SandboxEventsServiceClient
}

func NewAPIStore(ctx context.Context, tel *telemetry.Client, redisClient redis.UniversalClient, featureFlags *featureflags.Client, config cfg.Config) *APIStore {
	logger.L().Info(ctx, "Initializing API store and services")

	sqlcDB, err := sqlcdb.NewClient(ctx, config.PostgresConnectionString, pool.WithMaxConnections(config.DBMaxOpenConnections), pool.WithMinIdle(config.DBMinIdleConnections))
	if err != nil {
		logger.L().Fatal(ctx, "Initializing SQLC client", zap.Error(err))
	}

	authDB, err := authdb.NewClient(
		ctx,
		config.AuthDBConnectionString,
		pool.WithMaxConnections(config.AuthDBMaxOpenConnections),
		pool.WithMinIdle(config.AuthDBMinIdleConnections),
	)
	if err != nil {
		logger.L().Fatal(ctx, "Initializing auth DB client", zap.Error(err))
	}

	logger.L().Info(ctx, "Created database client")

	// LD-gated switcher: empty flag → singular DSN (self-managed); "0", "1", …
	// → alternates from CLICKHOUSE_CONNECTION_STRINGS. Lets reads shift between
	// clusters per-query without restarts. Empty singular DSN falls back to a
	// noop client.
	clickhouseStore, err := clickhouse.NewSwitchingClient(
		ctx,
		featureFlags,
		config.ClickhouseConnectionString,
		config.ClickhouseConnectionStrings,
		clickhouse.WithAllowNoopDefault(true),
	)
	if err != nil {
		logger.L().Fatal(ctx, "initializing ClickHouse switching client", zap.Error(err))
	}

	// ClickHouse-backed sandbox/build log reader for the local cluster, gated at
	// read time by the logs-read-config flag. Built from the singular DSN; when
	// it is unset, the local cluster stays on Loki.
	var sandboxLogsReader *sandboxlogs.Reader
	// clusterLogsReader carries the reader into the clusters pool as an
	// interface. It is left as a nil interface (not a typed-nil pointer boxed in
	// an interface) when no reader is configured, so the nil check in the local
	// cluster resource provider fires correctly and reads stay on Loki.
	var clusterLogsReader clusters.ClickhouseLogsReader
	if config.ClickhouseConnectionString != "" {
		conn, readerErr := clickhouse.NewDriver(config.ClickhouseConnectionString)
		if readerErr != nil {
			logger.L().Fatal(ctx, "initializing ClickHouse sandbox logs reader", zap.Error(readerErr))
		}
		sandboxLogsReader = sandboxlogs.NewReader(conn)
		clusterLogsReader = sandboxLogsReader
	}

	posthogClient, posthogErr := analyticscollector.NewPosthogClient(ctx, config.PosthogAPIKey)
	if posthogErr != nil {
		logger.L().Fatal(ctx, "Initializing Posthog client", zap.Error(posthogErr))
	}

	provider := serviceDiscoveryProvider(config, env.IsLocal())

	sd, err := newServiceDiscovery(ctx, config, kube.NewClient, provider)
	if err != nil {
		logger.L().Fatal(ctx, "Initializing service discovery", zap.Error(err))
	}

	if err := noLogStoreError(config.LokiURL, config.ClickhouseConnectionString); err != nil {
		logger.L().Fatal(ctx, "no log store configured", zap.Error(err))
	}

	var queryLogsProvider *loki.LokiQueryProvider
	if config.LokiURL == "" {
		logger.L().Warn(ctx, "LOKI_URL is not set: sandbox and build log reads need logs-read-config on (ClickHouse) or they fail")
	} else {
		queryLogsProvider, err = loki.NewLokiQueryProvider(config.LokiURL, config.LokiUser, config.LokiPassword)
		if err != nil {
			logger.L().Fatal(ctx, "error when getting logs query provider", zap.Error(err))
		}
	}

	clusters, err := clusters.NewPool(ctx, tel, sqlcDB, sd.templateBuilders, clickhouseStore, queryLogsProvider, clusterLogsReader, featureFlags, config)
	if err != nil {
		logger.L().Fatal(ctx, "initializing edge clusters pool failed", zap.Error(err))
	}

	accessTokenGenerator, err := sandbox.NewAccessTokenGenerator(config.SandboxAccessTokenHashSeed)
	if err != nil {
		logger.L().Fatal(ctx, "Initializing access token generator failed", zap.Error(err))
	}

	snapshotCache := snapshotcache.NewSnapshotCache(sqlcDB, redisClient)

	snapshotUpsertSem, err := sharedutils.NewAdjustableSemaphore(dbThrottleLimit(featureFlags.IntFlag(ctx, featureflags.MaxConcurrentSnapshotUpserts)))
	if err != nil {
		logger.L().Fatal(ctx, "failed to create snapshot upsert semaphore", zap.Error(err))
	}

	sandboxListSem, err := sharedutils.NewAdjustableSemaphore(dbThrottleLimit(featureFlags.IntFlag(ctx, featureflags.MaxConcurrentSandboxListQueries)))
	if err != nil {
		logger.L().Fatal(ctx, "failed to create sandbox list semaphore", zap.Error(err))
	}

	snapshotBuildQuerySem, err := sharedutils.NewAdjustableSemaphore(dbThrottleLimit(featureFlags.IntFlag(ctx, featureflags.MaxConcurrentSnapshotBuildQueries)))
	if err != nil {
		logger.L().Fatal(ctx, "failed to create snapshot build query semaphore", zap.Error(err))
	}

	orch, err := orchestrator.New(ctx, config, tel, sd.nodes, provider == cfg.ServiceDiscoveryProviderLocal, posthogClient, redisClient, sqlcDB, clusters, featureFlags, accessTokenGenerator, snapshotCache, snapshotUpsertSem)
	if err != nil {
		logger.L().Fatal(ctx, "Initializing Orchestrator client", zap.Error(err))
	}

	authClient := &http.Client{
		Transport: otelhttp.NewTransport(http.DefaultTransport),
	}
	authService, err := sharedauth.NewAuthService(ctx, redisClient, authDB, config.AuthProvider, authClient)
	if err != nil {
		logger.L().Fatal(ctx, "Initializing auth service", zap.Error(err))
	}
	templateCache := templatecache.NewTemplateCache(sqlcDB, redisClient)
	templateSpawnCounter := utils.NewTemplateSpawnCounter(ctx, time.Minute, sqlcDB)

	templateBuildsCache := templatecache.NewTemplateBuildCache(sqlcDB, redisClient)
	templateManager, err := template_manager.New(sqlcDB, clusters, templateBuildsCache, templateCache, featureFlags)
	if err != nil {
		logger.L().Fatal(ctx, "Initializing Template manager client", zap.Error(err))
	}

	// Start the periodic sync of template builds statuses
	go templateManager.BuildsStatusPeriodicalSync(ctx)

	// An unset address leaves the secret management routes registered and
	// answering as they do when the feature gate is closed.
	var (
		secretsConn       *grpc.ClientConn
		secretsManagement managementv1.SecretManagementServiceClient
	)
	if config.SecretsStoreBackendGrpcAddress != "" {
		secretsConn, err = newSecretsManagementClient(config.SecretsStoreBackendGrpcAddress)
		if err != nil {
			logger.L().Fatal(ctx, "Initializing secrets store management client", zap.Error(err))
		}

		secretsManagement = managementv1.NewSecretManagementServiceClient(secretsConn)
	}

	var (
		webhooksConn      *grpc.ClientConn
		webhookManagement webhookmanagement.WebhookManagementServiceClient
		sandboxEvents     webhookevents.SandboxEventsServiceClient
	)
	if config.WebhooksBackendGrpcAddress != "" {
		webhooksConn, err = newWebhooksClient(config.WebhooksBackendGrpcAddress)
		if err != nil {
			logger.L().Fatal(ctx, "Initializing webhooks backend client", zap.Error(err))
		}

		webhookManagement = webhookmanagement.NewWebhookManagementServiceClient(webhooksConn)
		sandboxEvents = webhookevents.NewSandboxEventsServiceClient(webhooksConn)
	}

	a := &APIStore{
		config:                config,
		orchestrator:          orch,
		teamSandboxCounter:    sandboxcountscache.NewCountsCache(orch, redisClient),
		templateManager:       templateManager,
		sqlcDB:                sqlcDB,
		authDB:                authDB,
		Telemetry:             tel,
		posthog:               posthogClient,
		templateCache:         templateCache,
		templateBuildsCache:   templateBuildsCache,
		snapshotCache:         snapshotCache,
		authService:           authService,
		templateSpawnCounter:  templateSpawnCounter,
		clickhouseStore:       clickhouseStore,
		sandboxLogsReader:     sandboxLogsReader,
		accessTokenGenerator:  accessTokenGenerator,
		clusters:              clusters,
		featureFlags:          featureFlags,
		redisClient:           redisClient,
		snapshotUpsertSem:     snapshotUpsertSem,
		sandboxListSem:        sandboxListSem,
		snapshotBuildQuerySem: snapshotBuildQuerySem,
		secretsConn:           secretsConn,
		webhooksConn:          webhooksConn,
		webhookManagement:     webhookManagement,
		sandboxEvents:         sandboxEvents,
		secretsManagement:     secretsManagement,
	}

	go a.updateDBThrottleLimits(ctx)

	// Wait till there's at least one, otherwise we can't create sandboxes yet
	go func() {
		ticker := time.NewTicker(5 * time.Millisecond)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if orch.NodeCount() != 0 {
					logger.L().Info(ctx, "Nodes are ready, setting API as healthy")
					a.Healthy.Store(true)

					return
				}
			}
		}
	}()

	return a
}

// Drain stops admitting sandbox work that outlives its request and waits for
// what is in flight. It runs before Close, which tears down the clients that
// work uses.
func (a *APIStore) Drain(ctx context.Context) error {
	if a.orchestrator == nil {
		return nil
	}

	return a.orchestrator.Drain(ctx)
}

func (a *APIStore) Close(ctx context.Context) error {
	a.templateSpawnCounter.Close(ctx)

	errs := []error{}
	if err := a.posthog.Close(); err != nil {
		errs = append(errs, fmt.Errorf("closing Posthog client: %w", err))
	}

	if err := a.orchestrator.Close(ctx); err != nil {
		errs = append(errs, fmt.Errorf("closing Orchestrator client: %w", err))
	}

	if a.templateCache != nil {
		if err := a.templateCache.Close(ctx); err != nil {
			errs = append(errs, fmt.Errorf("closing template cache: %w", err))
		}
	}

	if a.authService != nil {
		if err := a.authService.Close(ctx); err != nil {
			errs = append(errs, fmt.Errorf("closing auth service: %w", err))
		}
	}

	a.clusters.Close(ctx)

	if err := a.authDB.Close(); err != nil {
		errs = append(errs, fmt.Errorf("closing auth database client: %w", err))
	}

	if err := a.sqlcDB.Close(); err != nil {
		errs = append(errs, fmt.Errorf("closing sqlc database client: %w", err))
	}

	if a.templateBuildsCache != nil {
		if err := a.templateBuildsCache.Close(ctx); err != nil {
			errs = append(errs, fmt.Errorf("closing template build cache: %w", err))
		}
	}

	if err := a.snapshotCache.Close(ctx); err != nil {
		errs = append(errs, fmt.Errorf("closing snapshot cache: %w", err))
	}

	if a.clickhouseStore != nil {
		if err := a.clickhouseStore.Close(ctx); err != nil {
			errs = append(errs, fmt.Errorf("closing ClickHouse store: %w", err))
		}
	}
	if a.sandboxLogsReader != nil {
		if err := a.sandboxLogsReader.Close(ctx); err != nil {
			errs = append(errs, fmt.Errorf("closing ClickHouse sandbox logs reader: %w", err))
		}
	}

	if a.secretsConn != nil {
		if err := a.secretsConn.Close(); err != nil {
			errs = append(errs, fmt.Errorf("closing secrets store management client: %w", err))
		}
	}

	if a.webhooksConn != nil {
		if err := a.webhooksConn.Close(); err != nil {
			errs = append(errs, fmt.Errorf("closing webhooks backend client: %w", err))
		}
	}

	return errors.Join(errs...)
}

// dbThrottleLimit returns the semaphore limit for a feature flag value.
// A non-positive value means "disabled" and maps to math.MaxInt32 to effectively bypass throttling.
func dbThrottleLimit(flagValue int) int64 {
	if flagValue <= 0 {
		return math.MaxInt32
	}

	return int64(flagValue)
}

// updateDBThrottleLimits periodically syncs DB throttle semaphore limits from feature flags.
func (a *APIStore) updateDBThrottleLimits(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = a.snapshotUpsertSem.SetLimit(dbThrottleLimit(a.featureFlags.IntFlag(ctx, featureflags.MaxConcurrentSnapshotUpserts)))
			_ = a.sandboxListSem.SetLimit(dbThrottleLimit(a.featureFlags.IntFlag(ctx, featureflags.MaxConcurrentSandboxListQueries)))
			_ = a.snapshotBuildQuerySem.SetLimit(dbThrottleLimit(a.featureFlags.IntFlag(ctx, featureflags.MaxConcurrentSnapshotBuildQueries)))
		}
	}
}

// sendAPIStoreError wraps sending of an error in the Error format.
func (a *APIStore) sendAPIStoreError(c *gin.Context, code int, message string) {
	apierrors.SendAPIStoreError(c, code, message)
}

func (a *APIStore) GetHealth(c *gin.Context) {
	if a.Healthy.Load() {
		c.String(http.StatusOK, "Health check successful")

		return
	}

	c.String(http.StatusServiceUnavailable, "Service is unavailable")
}

func (a *APIStore) GetTeamFromAPIKey(ctx context.Context, ginCtx *gin.Context, apiKey string) (*types.Team, *api.APIError) {
	ctx, span := tracer.Start(ctx, "get team from api key")
	defer span.End()

	return a.authService.ValidateAPIKey(ctx, ginCtx, apiKey)
}

func (a *APIStore) GetUserIDFromAuthProviderToken(ctx context.Context, ginCtx *gin.Context, token string) (uuid.UUID, *api.APIError) {
	ctx, span := tracer.Start(ctx, "get user id from auth provider token")
	defer span.End()

	return a.authService.ValidateAuthProviderToken(ctx, ginCtx, token)
}

func (a *APIStore) GetTeamFromAuthProviderToken(ctx context.Context, ginCtx *gin.Context, teamID string) (*types.Team, *api.APIError) {
	ctx, span := tracer.Start(ctx, "get team from auth provider token")
	defer span.End()

	return a.authService.ValidateAuthProviderTeam(ctx, ginCtx, teamID)
}

func (a *APIStore) GetTeamFromAdminToken(ctx context.Context, _ *gin.Context, teamID string) (*types.Team, *api.APIError) {
	ctx, span := tracer.Start(ctx, "get team from admin token")
	defer span.End()

	teamUUID, err := uuid.Parse(teamID)
	if err != nil {
		return nil, &api.APIError{
			Code:      http.StatusBadRequest,
			ClientMsg: "Invalid team ID",
			Err:       fmt.Errorf("failed to parse team ID: %w", err),
		}
	}

	team, err := a.authService.GetTeamByID(ctx, teamUUID)
	if err != nil {
		if _, ok := errors.AsType[*sharedauth.TeamForbiddenError](err); ok {
			return nil, &api.APIError{
				Code:      http.StatusForbidden,
				ClientMsg: err.Error(),
				Err:       fmt.Errorf("failed getting team: %w", err),
			}
		}

		if dberrors.IsNotFoundError(err) {
			return nil, &api.APIError{
				Code:      http.StatusNotFound,
				ClientMsg: "Team not found",
				Err:       fmt.Errorf("failed getting team: %w", err),
			}
		}

		return nil, &api.APIError{
			Code:      http.StatusInternalServerError,
			ClientMsg: "Backend authentication failed",
			Err:       fmt.Errorf("failed getting team: %w", err),
		}
	}
	if team == nil {
		return nil, &api.APIError{
			Code:      http.StatusNotFound,
			ClientMsg: "Team not found",
			Err:       errors.New("team not found"),
		}
	}

	return team, nil
}
