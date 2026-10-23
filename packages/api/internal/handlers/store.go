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
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	analyticscollector "github.com/e2b-dev/infra/packages/api/internal/analytics_collector"
	"github.com/e2b-dev/infra/packages/api/internal/api"
	snapshotcache "github.com/e2b-dev/infra/packages/api/internal/cache/snapshots"
	templatecache "github.com/e2b-dev/infra/packages/api/internal/cache/templates"
	"github.com/e2b-dev/infra/packages/api/internal/cfg"
	"github.com/e2b-dev/infra/packages/api/internal/clusters"
	clustersdiscovery "github.com/e2b-dev/infra/packages/api/internal/clusters/discovery"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator"
	orchdiscovery "github.com/e2b-dev/infra/packages/api/internal/orchestrator/discovery"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
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
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/logs/loki"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	sharedutils "github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// newInClusterKubeClient builds a Kubernetes API client using the pod's
// in-cluster ServiceAccount token. The api Pod must be running in K8s with a
// projected SA token (the default for any pod with a ServiceAccount).
func newInClusterKubeClient() (kubernetes.Interface, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("rest.InClusterConfig: %w", err)
	}
	c, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("kubernetes.NewForConfig: %w", err)
	}

	return c, nil
}

var _ api.ServerInterface = (*APIStore)(nil)

type APIStore struct {
	Healthy               atomic.Bool
	config                cfg.Config
	posthog               *analyticscollector.PosthogClient
	Telemetry             *telemetry.Client
	orchestrator          *orchestrator.Orchestrator
	templateManager       *template_manager.TemplateManager
	sqlcDB                *sqlcdb.Client
	authDB                *authdb.Client
	redisClient           redis.UniversalClient
	templateCache         *templatecache.TemplateCache
	templateBuildsCache   *templatecache.TemplatesBuildCache
	snapshotCache         *snapshotcache.SnapshotCache
	authService           sharedauth.Service
	templateSpawnCounter  *utils.TemplateSpawnCounter
	clickhouseStore       clickhouse.Clickhouse
	sandboxLogsReader     *sandboxlogs.Reader
	accessTokenGenerator  *sandbox.AccessTokenGenerator
	featureFlags          *featureflags.Client
	clusters              *clusters.Pool
	snapshotUpsertSem     *sharedutils.AdjustableSemaphore
	sandboxListSem        *sharedutils.AdjustableSemaphore
	snapshotBuildQuerySem *sharedutils.AdjustableSemaphore
	// buildAncestors resolves a build's header-mapping ancestor closure for the
	// template-deletion reachability guard. Nil when template storage is not
	// configured for the API, in which case artifact cleanup fails closed.
	buildAncestors buildAncestorReader
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

	// Build the orchestrator-discovery and template-builder-discovery backends
	// based on cfg.ServiceDiscoveryProvider:
	//   nomad      - both go through the local Nomad agent
	//   kubernetes - both list pods via the in-cluster K8s API
	var (
		nodeDiscovery            orchdiscovery.Discovery
		templateBuilderDiscovery clustersdiscovery.Discovery
	)
	switch config.ServiceDiscoveryProvider {
	case cfg.ServiceDiscoveryProviderKubernetes:
		k8sClient, k8sErr := newInClusterKubeClient()
		if k8sErr != nil {
			logger.L().Fatal(ctx, "Initializing in-cluster Kubernetes client", zap.Error(k8sErr))
		}
		nodeDiscovery = orchdiscovery.NewKubernetes(
			k8sClient,
			config.K8sNamespace,
			config.K8sOrchestratorPodLabelSelector,
		)
		templateBuilderDiscovery = clustersdiscovery.NewKubernetesDiscovery(
			consts.LocalClusterID,
			k8sClient,
			config.K8sNamespace,
			config.K8sTemplateManagerPodLabelSelector,
		)
	case cfg.ServiceDiscoveryProviderLocal:
		localND, localErr := orchdiscovery.NewLocal(config.LocalOrchestratorAddress)
		if localErr != nil {
			logger.L().Fatal(ctx, "Initializing local orchestrator discovery", zap.Error(localErr))
		}
		nodeDiscovery = localND
		// No template builders in local dev — return an empty list so the
		// clusters pool initializes cleanly.
		templateBuilderDiscovery = clustersdiscovery.NewStaticDiscovery(nil)
	default: // ServiceDiscoveryProviderNomad
		nomadClient, nomadErr := nomadapi.NewClient(&nomadapi.Config{
			Address:  config.NomadAddress,
			SecretID: config.NomadToken,
		})
		if nomadErr != nil {
			logger.L().Fatal(ctx, "Initializing Nomad client", zap.Error(nomadErr))
		}
		nodeDiscovery = orchdiscovery.NewNomad(nomadClient, config.NomadOrchestratorServiceNames)
		// Migration fallback: orchestrator jobs deployed from jobspecs that
		// predate the service port-label fix register their service with an
		// empty Address, so service discovery alone would miss them until
		// they are redeployed. Union in the legacy node-pool listing (service
		// entries win on conflict) so the API flip has no rollout ordering
		// constraint. Disable via NOMAD_ORCHESTRATOR_LEGACY_DISCOVERY_ENABLED
		// once no legacy jobs remain. The pool is hardcoded: legacy jobs only
		// ever ran on the "default" pool.
		if config.NomadOrchestratorLegacyDiscoveryEnabled {
			nodeDiscovery = orchdiscovery.NewMerged(
				nodeDiscovery,
				orchdiscovery.NewNomadNodePool(nomadClient, "default"),
			)
		}
		templateBuilderDiscovery = clustersdiscovery.NewLocalDiscovery(consts.LocalClusterID, nomadClient)
	}

	queryLogsProvider, err := loki.NewLokiQueryProvider(config.LokiURL, config.LokiUser, config.LokiPassword)
	if err != nil {
		logger.L().Fatal(ctx, "error when getting logs query provider", zap.Error(err))
	}

	clusters, err := clusters.NewPool(ctx, tel, sqlcDB, templateBuilderDiscovery, clickhouseStore, queryLogsProvider, clusterLogsReader, featureFlags, config)
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

	orch, err := orchestrator.New(ctx, config, tel, nodeDiscovery, posthogClient, redisClient, sqlcDB, clusters, featureFlags, accessTokenGenerator, snapshotCache, snapshotUpsertSem)
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

	a := &APIStore{
		config:                config,
		orchestrator:          orch,
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
		buildAncestors:        newTemplateBuildAncestorReader(ctx),
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

func (a *APIStore) GetUserFromAccessToken(ctx context.Context, ginCtx *gin.Context, accessToken string) (uuid.UUID, *api.APIError) {
	ctx, span := tracer.Start(ctx, "get user from access token")
	defer span.End()

	userID, apiErr := a.authService.ValidateAccessToken(ctx, ginCtx, accessToken)
	if apiErr != nil {
		return uuid.UUID{}, apiErr
	}

	// Access token acceptance is gated after validation so the flag can be
	// rolled out per-user via LD targeting during the deprecation cutover.
	if a.featureFlags.BoolFlag(ctx, featureflags.DisableE2BAccessTokenAuthFlag, featureflags.UserContext(userID.String())) {
		return uuid.UUID{}, &api.APIError{
			Err:       errors.New("access token authentication is disabled"),
			ClientMsg: "E2B_ACCESS_TOKEN is deprecated and no longer accepted. Use an API key (E2B_API_KEY) instead. See https://e2b.dev/docs/migration/access-token-deprecation",
			Code:      http.StatusUnauthorized,
		}
	}

	return userID, nil
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
		var forbiddenErr *sharedauth.TeamForbiddenError
		if errors.As(err, &forbiddenErr) {
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
