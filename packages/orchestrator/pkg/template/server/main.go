//go:build linux

package server

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/proxy"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	sbxtemplate "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/service"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/core/envd"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/metrics"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/cache"
	artifactsregistry "github.com/e2b-dev/infra/packages/shared/pkg/artifacts-registry"
	"github.com/e2b-dev/infra/packages/shared/pkg/dockerhub"
	"github.com/e2b-dev/infra/packages/shared/pkg/env"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	templatemanager "github.com/e2b-dev/infra/packages/shared/pkg/grpc/template-manager"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// buildLimiterRefreshInterval is how often the template build limit is refreshed.
const buildLimiterRefreshInterval = 30 * time.Second

// buildLimiterUnboundedLimit disables the template build cap.
const buildLimiterUnboundedLimit = math.MaxInt32

type closeable interface {
	Close() error
}

// consumerStatusCheckGracePeriod is how long the graceful drain waits, after
// in-flight builds finish, for consumers to read the final build status before
// the server shuts down.
const consumerStatusCheckGracePeriod = 15 * time.Second

type ServerStore struct {
	templatemanager.UnimplementedTemplateServiceServer

	logger            logger.Logger
	info              *service.ServiceInfo
	builder           *build.Builder
	buildCache        *cache.BuildCache
	buildLogger       logger.Logger
	featureFlags      *featureflags.Client
	artifactsregistry artifactsregistry.ArtifactsRegistry
	templateStorage   storage.StorageProvider
	buildStorage      storage.StorageProvider

	wg           *sync.WaitGroup // wait group for running builds
	activeBuilds atomic.Int64    // counter for active builds (for debugging)

	// buildLimiter bounds concurrent template builds on this node.
	buildLimiter *utils.AdjustableSemaphore
	// buildLimitEnabled gates the limiter acquire path.
	buildLimitEnabled atomic.Bool

	closers []closeable
}

func New(
	ctx context.Context,
	config cfg.Config,
	info *service.ServiceInfo,
	featureFlags *featureflags.Client,
	meterProvider metric.MeterProvider,
	logger logger.Logger,
	buildLogger logger.Logger,
	sandboxFactory *sandbox.Factory,
	proxy *proxy.SandboxProxy,
	templateCache *sbxtemplate.Cache,
	templatePersistence storage.StorageProvider,
	buildPersistence storage.StorageProvider,
	uploads *sandbox.Uploads,
) (s *ServerStore, e error) {
	logger.Info(ctx, "Initializing template manager")

	// Validate the build envd up front: a template-manager whose envd is
	// unresolvable can only produce failed builds. On a build-only node,
	// refuse to start — the deploy's health gate then surfaces a broken
	// /fc-envd mount or a misordered envd-version rollout before the node
	// takes any build. On a node that also serves sandboxes, log loudly and
	// keep running: each build still fails with the same error, but a
	// build-only defect must never cost sandbox capacity. This validates the
	// flag's value at startup; per-build resolution re-reads it, so a flag
	// change after startup is still picked up (and re-validated) per build.
	// Bound the validation: it runs before any listener is up and both calls
	// can exec the envd binary, and the very defect this check exists to
	// surface (a broken /fc-envd mount) can hang the exec on I/O instead of
	// failing fast — an unbounded hang here would block the whole node, not
	// just template builds.
	envdCtx, envdCancel := context.WithTimeout(ctx, 30*time.Second)
	defer envdCancel()
	envdTarget := featureFlags.StringFlag(ctx, featureflags.BuildEnvdVersion)
	envdPath, envdErr := envd.ResolveBuildBinary(envdCtx, envdTarget, config.HostEnvdPath)
	if envdErr == nil {
		var envdVersion string
		if envdVersion, envdErr = envd.GetEnvdVersion(envdCtx, envdPath); envdErr == nil {
			logger.Info(ctx, fmt.Sprintf("Template builds will bake envd v%s", envdVersion),
				zap.String("envd_path", envdPath))
		}
	}
	if envdErr != nil {
		if !cfg.GetServices(config).Has(cfg.Orchestrator) {
			return nil, fmt.Errorf("envd for template builds is unresolvable: %w", envdErr)
		}
		logger.Error(ctx, "envd for template builds is unresolvable — every template build on this node will fail until it is fixed", zap.Error(envdErr))
		telemetry.ReportCriticalError(ctx, "envd for template builds is unresolvable", envdErr)
	}

	closers := make([]closeable, 0)
	defer func() {
		if e == nil {
			return
		}

		for _, closer := range closers {
			if err := closer.Close(); err != nil {
				logger.Error(ctx, "error closing resource", zap.Error(err))
			}
		}
	}()

	artifactsRegistry, err := artifactsregistry.GetArtifactsRegistryProvider(ctx)
	if err != nil {
		return nil, fmt.Errorf("error getting artifacts registry provider: %w", err)
	}

	dockerhubRepository, err := dockerhub.GetRemoteRepository(ctx)
	if err != nil {
		return nil, fmt.Errorf("error getting docker remote repository provider: %w", err)
	}
	closers = append(closers, dockerhubRepository)

	buildCache := cache.NewBuildCache(ctx, meterProvider)
	buildMetrics, err := metrics.NewBuildMetrics(meterProvider)
	if err != nil {
		return nil, fmt.Errorf("failed to create build metrics: %w", err)
	}

	builder := build.NewBuilder(
		config.BuilderConfig,
		logger,
		featureFlags,
		sandboxFactory,
		templatePersistence,
		buildPersistence,
		dockerhubRepository,
		proxy,
		sandboxFactory.Sandboxes,
		templateCache,
		buildMetrics,
		uploads,
	)

	// Use the service context so the node-wide cap is not team-targeted.
	flagLimit := featureFlags.IntFlag(ctx, featureflags.MaxConcurrentTemplateBuilds)
	enabled := flagLimit > 0
	initialLimit := int64(flagLimit)
	if !enabled {
		initialLimit = buildLimiterUnboundedLimit
	}
	buildLimiter, err := utils.NewAdjustableSemaphore(initialLimit)
	if err != nil {
		return nil, fmt.Errorf("failed to create build limiter: %w", err)
	}

	store := &ServerStore{
		logger:            logger,
		info:              info,
		builder:           builder,
		buildCache:        buildCache,
		buildLogger:       buildLogger,
		featureFlags:      featureFlags,
		artifactsregistry: artifactsRegistry,
		templateStorage:   templatePersistence,
		buildStorage:      buildPersistence,
		wg:                &sync.WaitGroup{},
		buildLimiter:      buildLimiter,
		closers:           closers,
	}
	store.buildLimitEnabled.Store(enabled)

	go store.refreshBuildLimit(ctx)

	return store, nil
}

// refreshBuildLimit keeps the template build cap in sync with the feature flag.
func (s *ServerStore) refreshBuildLimit(ctx context.Context) {
	ticker := time.NewTicker(buildLimiterRefreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			flagLimit := s.featureFlags.IntFlag(ctx, featureflags.MaxConcurrentTemplateBuilds)

			limit := int64(flagLimit)
			enabled := flagLimit > 0
			if !enabled {
				limit = buildLimiterUnboundedLimit
			}

			if err := s.buildLimiter.SetLimit(limit); err != nil {
				s.logger.Error(ctx, "failed to adjust build limiter",
					zap.Int64("limit", limit), zap.Bool("enabled", enabled), zap.Error(err))
				continue
			}
			s.buildLimitEnabled.Store(enabled)
		}
	}
}

func (s *ServerStore) Close(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return errors.New("force exit, not waiting for builds to finish")
	default:
		var closersErr error
		for _, closer := range s.closers {
			err := closer.Close()
			if err != nil {
				closersErr = errors.Join(closersErr, err)
			}
		}
		if closersErr != nil {
			return fmt.Errorf("failed to close services: %w", closersErr)
		}

		return nil
	}
}

// Wait gracefully drains in-flight template builds during shutdown. It waits
// for running builds to finish, bounded by ctx, then gives consumers a grace
// period to read the final build status. It returns ctx.Err() if ctx is
// cancelled before the drain completes.
func (s *ServerStore) Wait(ctx context.Context) error {
	s.logger.Info(ctx, "Waiting for all build jobs to finish", zap.Int64("active_builds", s.activeBuilds.Load()))
	if err := utils.WaitGroupWait(ctx, s.wg); err != nil {
		return fmt.Errorf("waiting for template builds: %w", err)
	}

	if !env.IsLocal() {
		s.logger.Info(ctx, "Waiting for consumers to check build status")
		select {
		case <-time.After(consumerStatusCheckGracePeriod):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	s.logger.Info(ctx, "Template build queue cleaned")

	return nil
}
