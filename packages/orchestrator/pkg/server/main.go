//go:build linux

package server

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jellydator/ttlcache/v3"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/events"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/proxy"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/nbd"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template/peerclient"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/service"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// Matches the template cache TTL so entries live as long as the
// templates they refer to and are cleaned up automatically.
const uploadedBuildsTTL = 1 * time.Hour

// startingSandboxesLimitRefreshInterval is how often we re-read the
// MaxStartingInstancesPerNode feature flag and resize the semaphore.
const startingSandboxesLimitRefreshInterval = 30 * time.Second

const maxSandboxesLimitRefreshInterval = 30 * time.Second

// uploadDrainLogInterval is how often Close logs progress while waiting for
// in-flight snapshot uploads to finish during shutdown.
const uploadDrainLogInterval = 10 * time.Second

// sandboxDrainPollInterval is how often the graceful sandbox drain re-checks
// the live sandbox count during shutdown.
const sandboxDrainPollInterval = 5 * time.Second

// sandboxDrainLogInterval backs off how often the graceful sandbox drain logs
// progress so a long-lived node draining for hours does not spam the logs.
func sandboxDrainLogInterval(elapsed time.Duration) time.Duration {
	switch {
	case elapsed < time.Minute:
		return 5 * time.Second
	case elapsed < time.Hour:
		return time.Minute
	default:
		return 15 * time.Minute
	}
}

type Server struct {
	orchestrator.UnimplementedSandboxServiceServer
	orchestrator.UnimplementedChunkServiceServer

	config                    cfg.Config
	sandboxFactory            *sandbox.Factory
	info                      *service.ServiceInfo
	proxy                     *proxy.SandboxProxy
	networkPool               network.PoolInterface
	templateCache             *template.Cache
	devicePool                *nbd.DevicePool
	persistence               storage.StorageProvider
	featureFlags              *featureflags.Client
	sbxEventsService          *events.EventsService
	startingSandboxes         *utils.AdjustableSemaphore
	peerRegistry              peerclient.Registry
	uploadedBuilds            *ttlcache.Cache[string, struct{}]
	uploads                   *sandbox.Uploads
	sandboxCreateDuration     metric.Int64Histogram
	sandboxExecutionDuration  metric.Int64Histogram
	sandboxPauseDuration      metric.Int64Histogram
	sandboxKilledCounter      metric.Int64Counter
	sandboxCrashedCounter     metric.Int64Counter
	sandboxCheckpointCounter  metric.Int64Counter
	sandboxCheckpointDuration metric.Int64Histogram
	uploadFailedCounter       metric.Int64Counter
	envdUpgradeAttempts       metric.Int64Counter
	envdUpgradeGated          metric.Int64Counter
	envdUpgradeHandover       metric.Int64Counter
	envdUpgradeDuration       metric.Int64Histogram
	envdUpgradePhaseDuration  metric.Int64Histogram

	pauseAdmissionCounter      metric.Int64Counter
	pauseAdmissionWaitDuration metric.Int64Histogram

	// uploadsWG tracks in-flight async snapshot uploads so a graceful shutdown
	// can wait for them to finish instead of dropping them. uploadsInFlight is
	// the live count, used to log drain progress during shutdown.
	uploadsWG       sync.WaitGroup
	uploadsInFlight atomic.Int64

	done      chan struct{}
	closeOnce sync.Once
}

type ServiceConfig struct {
	Config           cfg.Config
	Tel              *telemetry.Client
	NetworkPool      network.PoolInterface
	DevicePool       *nbd.DevicePool
	TemplateCache    *template.Cache
	Info             *service.ServiceInfo
	Proxy            *proxy.SandboxProxy
	SandboxFactory   *sandbox.Factory
	Persistence      storage.StorageProvider
	FeatureFlags     *featureflags.Client
	SbxEventsService *events.EventsService
	PeerRegistry     peerclient.Registry
	Uploads          *sandbox.Uploads
}

func New(ctx context.Context, cfg ServiceConfig) (*Server, error) {
	uploadedBuilds := ttlcache.New[string, struct{}](
		ttlcache.WithTTL[string, struct{}](uploadedBuildsTTL),
	)
	go uploadedBuilds.Start()

	startingLimit := cfg.FeatureFlags.IntFlag(ctx, featureflags.MaxStartingInstancesPerNode)
	startingSandboxes, err := utils.NewAdjustableSemaphore(int64(startingLimit))
	if err != nil {
		return nil, fmt.Errorf("failed to create starting sandboxes semaphore: %w", err)
	}

	server := &Server{
		config:            cfg.Config,
		sandboxFactory:    cfg.SandboxFactory,
		info:              cfg.Info,
		proxy:             cfg.Proxy,
		networkPool:       cfg.NetworkPool,
		templateCache:     cfg.TemplateCache,
		devicePool:        cfg.DevicePool,
		persistence:       cfg.Persistence,
		featureFlags:      cfg.FeatureFlags,
		sbxEventsService:  cfg.SbxEventsService,
		startingSandboxes: startingSandboxes,
		peerRegistry:      cfg.PeerRegistry,
		uploadedBuilds:    uploadedBuilds,
		uploads:           cfg.Uploads,
		done:              make(chan struct{}),
	}
	server.updateMaxSandboxesLimit(ctx)

	meter := cfg.Tel.MeterProvider.Meter("github.com/e2b-dev/infra/packages/orchestrator/pkg/server")

	sandboxCreateDuration, err := telemetry.GetHistogram(meter, telemetry.OrchestratorSandboxCreateDurationName)
	if err != nil {
		return nil, fmt.Errorf("failed to register sandbox create duration histogram: %w", err)
	}
	server.sandboxCreateDuration = sandboxCreateDuration

	sandboxExecutionDuration, err := telemetry.GetHistogram(meter, telemetry.OrchestratorSandboxExecutionDurationName)
	if err != nil {
		return nil, fmt.Errorf("failed to register sandbox execution duration histogram: %w", err)
	}
	server.sandboxExecutionDuration = sandboxExecutionDuration

	sandboxPauseDuration, err := telemetry.GetHistogram(meter, telemetry.PauseDurationHistogramName)
	if err != nil {
		return nil, fmt.Errorf("failed to register sandbox pause duration histogram: %w", err)
	}
	server.sandboxPauseDuration = sandboxPauseDuration

	sandboxKilledCounter, err := telemetry.GetCounter(meter, telemetry.OrchestratorSandboxKilledCounterName)
	if err != nil {
		return nil, fmt.Errorf("failed to register sandbox kills counter: %w", err)
	}
	server.sandboxKilledCounter = sandboxKilledCounter

	sandboxCrashedCounter, err := telemetry.GetCounter(meter, telemetry.OrchestratorSandboxCrashedCounterName)
	if err != nil {
		return nil, fmt.Errorf("failed to register sandbox crashes counter: %w", err)
	}
	server.sandboxCrashedCounter = sandboxCrashedCounter

	sandboxCheckpointCounter, err := telemetry.GetCounter(meter, telemetry.OrchestratorSandboxCheckpointCounterName)
	if err != nil {
		return nil, fmt.Errorf("failed to register sandbox checkpoint counter: %w", err)
	}
	server.sandboxCheckpointCounter = sandboxCheckpointCounter

	sandboxCheckpointDuration, err := telemetry.GetHistogram(meter, telemetry.CheckpointDurationName)
	if err != nil {
		return nil, fmt.Errorf("failed to register sandbox checkpoint duration histogram: %w", err)
	}
	server.sandboxCheckpointDuration = sandboxCheckpointDuration

	pauseAdmissionCounter, err := telemetry.GetCounter(meter, telemetry.OrchestratorSandboxPauseAdmissionCounterName)
	if err != nil {
		return nil, fmt.Errorf("failed to register pause admission counter: %w", err)
	}
	server.pauseAdmissionCounter = pauseAdmissionCounter

	pauseAdmissionWaitDuration, err := telemetry.GetHistogram(meter, telemetry.OrchestratorSandboxPauseAdmissionWaitDurationName)
	if err != nil {
		return nil, fmt.Errorf("failed to register pause admission wait duration histogram: %w", err)
	}
	server.pauseAdmissionWaitDuration = pauseAdmissionWaitDuration

	uploadFailedCounter, err := telemetry.GetCounter(meter, telemetry.OrchestratorSnapshotUploadFailedCounterName)
	if err != nil {
		return nil, fmt.Errorf("failed to register snapshot upload failed counter: %w", err)
	}
	server.uploadFailedCounter = uploadFailedCounter

	envdUpgradeAttempts, err := telemetry.GetCounter(meter, telemetry.OrchestratorEnvdUpgradeAttempts)
	if err != nil {
		return nil, fmt.Errorf("failed to register envd upgrade attempts counter: %w", err)
	}
	server.envdUpgradeAttempts = envdUpgradeAttempts

	envdUpgradeGated, err := telemetry.GetCounter(meter, telemetry.OrchestratorEnvdUpgradeGated)
	if err != nil {
		return nil, fmt.Errorf("failed to register envd upgrade gated counter: %w", err)
	}
	server.envdUpgradeGated = envdUpgradeGated

	envdUpgradeHandover, err := telemetry.GetCounter(meter, telemetry.OrchestratorEnvdUpgradeHandover)
	if err != nil {
		return nil, fmt.Errorf("failed to register envd upgrade handover counter: %w", err)
	}
	server.envdUpgradeHandover = envdUpgradeHandover

	envdUpgradeDuration, err := telemetry.GetHistogram(meter, telemetry.OrchestratorEnvdUpgradeDurationName)
	if err != nil {
		return nil, fmt.Errorf("failed to register envd upgrade duration histogram: %w", err)
	}
	server.envdUpgradeDuration = envdUpgradeDuration

	envdUpgradePhaseDuration, err := telemetry.GetHistogram(meter, telemetry.OrchestratorEnvdUpgradePhaseDurationName)
	if err != nil {
		return nil, fmt.Errorf("failed to register envd upgrade phase duration histogram: %w", err)
	}
	server.envdUpgradePhaseDuration = envdUpgradePhaseDuration

	_, err = telemetry.GetObservableUpDownCounter(meter, telemetry.OrchestratorSandboxCountMeterName, func(_ context.Context, observer metric.Int64Observer) error {
		observer.Observe(int64(server.sandboxFactory.Sandboxes.Count()))

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to register sandbox count metric: %w", err)
	}

	sandboxLimitGauge, err := telemetry.GetGaugeInt(meter, telemetry.OrchestratorSandboxLimitGaugeName)
	if err != nil {
		return nil, fmt.Errorf("failed to create sandbox limit gauge: %w", err)
	}

	_, err = meter.RegisterCallback(func(_ context.Context, obs metric.Observer) error {
		obs.ObserveInt64(sandboxLimitGauge, server.info.MaxSandboxes.Load())

		return nil
	}, sandboxLimitGauge)
	if err != nil {
		return nil, fmt.Errorf("failed to register sandbox limit gauge: %w", err)
	}

	statusGauge, err := telemetry.GetGaugeInt(meter, telemetry.OrchestratorStatusGaugeName)
	if err != nil {
		return nil, fmt.Errorf("failed to create orchestrator status gauge: %w", err)
	}

	_, err = meter.RegisterCallback(
		func(_ context.Context, obs metric.Observer) error {
			obs.ObserveInt64(statusGauge, 1, metric.WithAttributes(
				attribute.String("status", server.info.GetStatus().Status.String()),
				attribute.String("version", server.info.SourceVersion),
				attribute.String("commit", server.info.SourceCommit),
			))

			return nil
		}, statusGauge)
	if err != nil {
		return nil, fmt.Errorf("failed to register orchestrator status gauge: %w", err)
	}

	cpuAllocatedGauge, err := telemetry.GetGaugeInt(meter, telemetry.OrchestratorCpuAllocatedGaugeName)
	if err != nil {
		return nil, fmt.Errorf("failed to create orchestrator CPU allocated gauge: %w", err)
	}

	memoryAllocatedGauge, err := telemetry.GetGaugeInt(meter, telemetry.OrchestratorMemoryAllocatedGaugeName)
	if err != nil {
		return nil, fmt.Errorf("failed to create orchestrator memory allocated gauge: %w", err)
	}

	diskAllocatedGauge, err := telemetry.GetGaugeInt(meter, telemetry.OrchestratorDiskAllocatedGaugeName)
	if err != nil {
		return nil, fmt.Errorf("failed to create orchestrator disk allocated gauge: %w", err)
	}

	_, err = meter.RegisterCallback(
		func(_ context.Context, obs metric.Observer) error {
			var (
				cpuAllocated    int64
				memoryAllocated int64
				diskAllocated   int64
			)

			for _, item := range server.sandboxFactory.Sandboxes.Items() {
				cpuAllocated += item.Config.Vcpu
				memoryAllocated += item.Config.RamMB * 1024 * 1024
				diskAllocated += item.Config.TotalDiskSizeMB * 1024 * 1024
			}

			obs.ObserveInt64(cpuAllocatedGauge, cpuAllocated)
			obs.ObserveInt64(memoryAllocatedGauge, memoryAllocated)
			obs.ObserveInt64(diskAllocatedGauge, diskAllocated)

			return nil
		}, cpuAllocatedGauge, memoryAllocatedGauge, diskAllocatedGauge)
	if err != nil {
		return nil, fmt.Errorf("failed to register orchestrator sandbox resource gauges: %w", err)
	}

	go server.refreshStartingSandboxesLimit(ctx)
	go server.refreshMaxSandboxesLimit(ctx)

	return server, nil
}

func (s *Server) Close(ctx context.Context) error {
	s.closeOnce.Do(func() {
		close(s.done)
	})

	// Wait for in-flight snapshot uploads to finish so a graceful shutdown
	// doesn't drop a snapshot that is still uploading. ctx is cancelled on a
	// forced stop, in which case we stop waiting and let the process exit.
	uploadsDone := make(chan struct{})
	go func() {
		s.uploadsWG.Wait()
		close(uploadsDone)
	}()

	s.drainUploads(ctx, uploadsDone)

	s.uploadedBuilds.Stop()

	return nil
}

// drainUploads waits for in-flight snapshot uploads to finish, logging progress
// periodically, until they complete or ctx is cancelled (forced stop).
func (s *Server) drainUploads(ctx context.Context, uploadsDone <-chan struct{}) {
	inFlight := s.uploadsInFlight.Load()
	if inFlight == 0 {
		return
	}

	logger.L().Info(ctx, "waiting for in-flight snapshot uploads to finish", zap.Int64("uploads", inFlight))

	ticker := time.NewTicker(uploadDrainLogInterval)
	defer ticker.Stop()

	for {
		select {
		case <-uploadsDone:
			logger.L().Info(ctx, "all in-flight snapshot uploads finished")

			return
		case <-ctx.Done():
			logger.L().Warn(ctx, "shutting down with snapshot uploads still in flight",
				zap.Int64("uploads", s.uploadsInFlight.Load()),
				zap.Error(context.Cause(ctx)),
			)

			return
		case <-ticker.C:
			logger.L().Info(ctx, "still waiting for in-flight snapshot uploads",
				zap.Int64("uploads", s.uploadsInFlight.Load()),
			)
		}
	}
}

// DrainSandboxes waits for the live sandboxes on this node to exit on their own
// during a graceful shutdown, then waits for their lifecycle cleanup to finish.
// It does not reject new sandbox starts; that admission gating is layered in
// separately. It returns ctx.Err() if ctx is cancelled before the node empties.
func (s *Server) DrainSandboxes(ctx context.Context) error {
	live := s.sandboxFactory.Sandboxes.Count()
	logger.L().Info(ctx, "starting graceful sandbox drain", zap.Int("live_sandboxes", live))

	ticker := time.NewTicker(sandboxDrainPollInterval)
	defer ticker.Stop()
	startedAt := time.Now()
	lastLoggedAt := startedAt

	for {
		remaining := s.sandboxFactory.Sandboxes.Count()
		if remaining == 0 {
			logger.L().Info(ctx, "graceful sandbox drain complete", zap.Int("live_sandboxes", remaining))

			return s.waitSandboxLifecycles(ctx)
		}

		select {
		case <-ctx.Done():
			logger.L().Warn(ctx, "graceful sandbox drain timed out",
				zap.Int("remaining_sandboxes", remaining),
				zap.Error(ctx.Err()),
			)

			return ctx.Err()
		case <-ticker.C:
			now := time.Now()
			remaining = s.sandboxFactory.Sandboxes.Count()
			elapsed := now.Sub(startedAt)
			if remaining > 0 && now.Sub(lastLoggedAt) >= sandboxDrainLogInterval(elapsed) {
				logger.L().Info(ctx, "waiting for sandbox drain",
					zap.Int("remaining_sandboxes", remaining),
					zap.Duration("elapsed", elapsed),
				)
				lastLoggedAt = now
			}
		}
	}
}

func (s *Server) waitSandboxLifecycles(ctx context.Context) error {
	return s.sandboxFactory.Sandboxes.WaitLifecycles(ctx)
}

func (s *Server) refreshStartingSandboxesLimit(ctx context.Context) {
	ticker := time.NewTicker(startingSandboxesLimitRefreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.done:
			return
		case <-ticker.C:
			limit := s.featureFlags.IntFlag(ctx, featureflags.MaxStartingInstancesPerNode)
			if limit <= 0 {
				continue
			}

			if err := s.startingSandboxes.SetLimit(int64(limit)); err != nil {
				logger.L().Error(ctx, "failed to adjust starting sandboxes semaphore",
					zap.Int("limit", limit), zap.Error(err))
			}
		}
	}
}

func (s *Server) updateMaxSandboxesLimit(ctx context.Context) {
	s.info.MaxSandboxes.Store(int64(s.featureFlags.IntFlag(ctx, featureflags.MaxSandboxesPerNode)))
}

func (s *Server) refreshMaxSandboxesLimit(ctx context.Context) {
	ticker := time.NewTicker(maxSandboxesLimitRefreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.done:
			return
		case <-ticker.C:
			s.updateMaxSandboxesLimit(ctx)
		}
	}
}
