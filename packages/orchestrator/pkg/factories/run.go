//go:build linux

package factories

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/gofrs/flock"
	"github.com/google/uuid"
	"github.com/soheilhy/cmux"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"

	clickhouse "github.com/e2b-dev/infra/packages/clickhouse/pkg"
	clickhouseevents "github.com/e2b-dev/infra/packages/clickhouse/pkg/events"
	clickhousehoststats "github.com/e2b-dev/infra/packages/clickhouse/pkg/hoststats"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/chrooted"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/events"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/gcpercent"
	e2bhealthcheck "github.com/e2b-dev/infra/packages/orchestrator/pkg/healthcheck"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/hyperloopserver"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/localupload"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/metrics"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/nfsproxy"
	nfscfg "github.com/e2b-dev/infra/packages/orchestrator/pkg/nfsproxy/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/portmap"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/proxy"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/routing"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	blockmetrics "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block/metrics"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/cgroup"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/nbd"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"
	networkv2 "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network/v2"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template/peerclient"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/server"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/service"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/service/machineinfo"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/startupreclaim"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/constants"
	tmplserver "github.com/e2b-dev/infra/packages/orchestrator/pkg/template/server"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/volumes"
	"github.com/e2b-dev/infra/packages/shared/pkg/env"
	event "github.com/e2b-dev/infra/packages/shared/pkg/events"
	sharedFactories "github.com/e2b-dev/infra/packages/shared/pkg/factories"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	e2bgrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	orchestratorinfo "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator-info"
	templatemanager "github.com/e2b-dev/infra/packages/shared/pkg/grpc/template-manager"
	"github.com/e2b-dev/infra/packages/shared/pkg/limit"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	sbxlogger "github.com/e2b-dev/infra/packages/shared/pkg/logger/sandbox"
	sandboxcatalog "github.com/e2b-dev/infra/packages/shared/pkg/sandbox-catalog"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// Deps holds shared infrastructure created during orchestrator init.
// Passed to factory callbacks so editions can build components using shared deps.
type Deps struct {
	Config              cfg.Config
	Tel                 *telemetry.Client
	MeterProvider       metric.MeterProvider
	Logger              logger.Logger
	Sandboxes           *sandbox.Map
	FeatureFlags        *featureflags.Client
	ClickhouseEndpoints []ClickhouseEndpoint
}

type ClickhouseEndpoint struct {
	Conn driver.Conn

	// Label is the credential-stripped host:port, empty for the primary.
	// Never the DSN: it carries the password.
	Label string

	// Primary is the endpoint from the singular connection string.
	// Its writes are ungated and a writer that fails to build on it is fatal.
	Primary bool
}

// BatcherName suffixes base with the endpoint so per-endpoint queue metrics
// stay distinct. The primary keeps the bare name existing dashboards use.
func (e ClickhouseEndpoint) BatcherName(base string) string {
	if e.Label == "" {
		return base
	}

	return base + ":" + e.Label
}

// newClickhouseEventsDelivery builds the sandbox events writer for one endpoint.
// Returns nil when a best-effort endpoint cannot take one.
func newClickhouseEventsDelivery(ctx context.Context, endpoint ClickhouseEndpoint, featureFlags *featureflags.Client) event.Delivery[event.SandboxEvent] {
	delivery, err := clickhouseevents.NewDefaultClickhouseSandboxEventsDelivery(
		ctx,
		endpoint.Conn,
		featureFlags,
		endpoint.BatcherName(clickhouseevents.DefaultBatcherName),
	)
	if err != nil {
		if endpoint.Primary {
			logger.L().Fatal(ctx, "failed to create clickhouse events delivery", zap.Error(err))
		}

		logger.L().Error(ctx, "failed to create clickhouse events delivery, skipping endpoint",
			zap.String("endpoint", endpoint.Label),
			zap.Error(err),
		)

		return nil
	}

	if endpoint.Primary {
		return delivery
	}

	return clickhouseevents.NewGatedDelivery(delivery, featureFlags)
}

// newClickhouseHostStatsDelivery builds the host stats writer for one endpoint.
// Returns nil when a best-effort endpoint cannot take one.
func newClickhouseHostStatsDelivery(ctx context.Context, endpoint ClickhouseEndpoint, featureFlags *featureflags.Client) clickhousehoststats.Delivery {
	delivery, err := clickhousehoststats.NewDefaultClickhouseHostStatsDelivery(
		ctx,
		endpoint.Conn,
		featureFlags,
		endpoint.BatcherName(clickhousehoststats.DefaultBatcherName),
	)
	if err != nil {
		if endpoint.Primary {
			logger.L().Fatal(ctx, "failed to create clickhouse host stats delivery", zap.Error(err))
		}

		logger.L().Error(ctx, "failed to create clickhouse host stats delivery, skipping endpoint",
			zap.String("endpoint", endpoint.Label),
			zap.Error(err),
		)

		return nil
	}

	if endpoint.Primary {
		return delivery
	}

	return clickhousehoststats.NewGatedDelivery(delivery, featureFlags)
}

// openClickhouseEndpoints connects to every configured endpoint, returning them
// with the closers that release them. One driver per endpoint rather than per
// writer keeps connection pools, batcher queues, and OTel metrics isolated so a
// slow/failing endpoint cannot stall the others.
func openClickhouseEndpoints(ctx context.Context, config cfg.Config) ([]ClickhouseEndpoint, []closer) {
	var (
		endpoints []ClickhouseEndpoint
		closers   []closer
	)

	// Legacy singular ClickHouse delivery path. Fatal on init error and keeps
	// the unsuffixed default batcher names to preserve pre-multi-endpoint
	// behavior and existing dashboards/alerts.
	if config.ClickhouseConnectionString != "" {
		conn, err := clickhouse.NewDriver(config.ClickhouseConnectionString)
		if err != nil {
			logger.L().Fatal(ctx, "failed to create clickhouse driver", zap.Error(err))
		}

		endpoints = append(endpoints, ClickhouseEndpoint{Conn: conn, Primary: true})
		closers = append(closers, closer{"clickhouse connection", func(context.Context) error {
			return conn.Close()
		}})
	}

	additionalEndpoints, droppedDuplicates := config.AdditionalClickhouseEndpoints()
	for _, dsn := range droppedDuplicates {
		endpoint, err := clickhouse.EndpointFromDSN(dsn)
		if err != nil {
			logger.L().Info(ctx, "dropped duplicate unparseable ClickHouse endpoint", zap.Error(err))

			continue
		}
		logger.L().Info(ctx, "dropped duplicate ClickHouse endpoint", zap.String("endpoint", endpoint))
	}

	if len(additionalEndpoints) > 0 {
		logger.L().Info(ctx, "resolved additional ClickHouse delivery endpoints",
			zap.Int("count", len(additionalEndpoints)),
		)
	}

	// Additional ClickHouse delivery endpoints are best-effort.
	for _, dsn := range additionalEndpoints {
		label, err := clickhouse.EndpointFromDSN(dsn)
		if err != nil {
			logger.L().Error(ctx, "failed to parse clickhouse DSN, skipping endpoint", zap.Error(err))

			continue
		}

		conn, err := clickhouse.NewDriver(dsn)
		if err != nil {
			logger.L().Error(ctx, "failed to create clickhouse driver, skipping endpoint",
				zap.String("endpoint", label),
				zap.Error(err),
			)

			continue
		}

		endpoints = append(endpoints, ClickhouseEndpoint{Conn: conn, Label: label})
		closers = append(closers, closer{"clickhouse connection " + label, func(context.Context) error {
			return conn.Close()
		}})
	}

	return endpoints, closers
}

// EgressSetup is returned by EgressFactory with the proxy implementation
// and optional lifecycle hooks.
type EgressSetup struct {
	// Proxy is the network egress proxy for slot creation/deletion.
	Proxy network.EgressProxy
	// NetworkAssignHook runs right after the sandbox's network is assigned
	// and strictly before its guest can run (or resume running).
	NetworkAssignHook sandbox.NetworkAssignHook

	// Start is called as a managed service (optional).
	// If nil, no service is started for the egress proxy.
	Start func(ctx context.Context) error

	// Close is called during shutdown in reverse order (optional).
	Close func(ctx context.Context) error
}

// EgressFactory builds an edition-specific egress proxy.
// It receives fully initialized shared deps and a context.
type EgressFactory func(ctx context.Context, deps *Deps) (*EgressSetup, error)

// Options configures the orchestrator with edition-specific behavior.
type Options struct {
	Version       string
	CommitSHA     string
	EgressFactory EgressFactory
}

type closer struct {
	name  string
	close func(ctx context.Context) error
}

type serviceDoneError struct {
	name string
}

func (e serviceDoneError) Error() string {
	return fmt.Sprintf("service %s finished", e.name)
}

// Run starts the orchestrator, blocking until shutdown.
// Returns true on clean shutdown.
func Run(opts Options) bool {
	config, err := cfg.Parse()
	if err != nil {
		log.Fatalf("failed to parse config: %v", err)
	}

	if err = ensureDirs(config); err != nil {
		log.Fatalf("failed to create dirs: %v", err)
	}

	if opts.EgressFactory == nil {
		log.Fatalf("EgressFactory must be set in Options")
	}

	success := run(config, opts)

	log.Println("Stopping orchestrator, success:", success)

	if !success {
		os.Exit(1)
	}

	return success
}

func ensureDirs(c cfg.Config) error {
	for _, dir := range []string{
		c.DefaultCacheDir,
		c.OrchestratorBaseDir,
		c.StorageConfig.SandboxCacheDir,
		c.SandboxDir,
		c.SharedChunkCacheDir,
		c.StorageConfig.TemplateCacheDir,
		c.TemplatesDir,
	} {
		if dir == "" {
			continue
		}

		if err := os.MkdirAll(dir, 0o700); err != nil {
			return fmt.Errorf("failed to make %q: %w", dir, err)
		}
	}

	return nil
}

func acquireOrchestratorLock(path string) (*flock.Flock, error) {
	fileLock := flock.New(path, flock.SetPermissions(0o644))
	locked, err := fileLock.TryLock()
	if err != nil {
		return nil, fmt.Errorf("lock file: %w", err)
	}
	if !locked {
		// flock(2) is released by the kernel on crash, so reaching here means a
		// live process holds the lock. Surface the PID it recorded, if any.
		if pid, perr := readLockHolderPID(path); perr == nil && pid > 0 {
			return nil, fmt.Errorf("another instance is running with pid %d", pid)
		}

		return nil, errors.New("another instance is running")
	}

	// Record our PID so a future conflicting instance can report it. We hold the
	// exclusive advisory lock, so no other process writes this file concurrently.
	if err := writeLockHolderPID(path); err != nil {
		_ = fileLock.Unlock()

		return nil, fmt.Errorf("write lock holder pid: %w", err)
	}

	return fileLock, nil
}

func writeLockHolderPID(path string) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("open lock file: %w", err)
	}
	defer f.Close()

	if _, err := fmt.Fprintf(f, "%d\n", os.Getpid()); err != nil {
		return fmt.Errorf("write pid: %w", err)
	}

	return nil
}

func readLockHolderPID(path string) (int, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, fmt.Errorf("read lock file: %w", err)
	}

	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return 0, fmt.Errorf("parse pid: %w", err)
	}

	return pid, nil
}

func run(config cfg.Config, opts Options) (success bool) {
	success = true

	version := opts.Version
	commitSHA := opts.CommitSHA

	services := cfg.GetServices(config)

	usesSandboxRuntime := services.UsesSandboxRuntime()

	// Enforce a single host-level sandbox runtime instance.
	// Skip this check in development mode.
	if !env.IsDevelopment() && usesSandboxRuntime {
		f, err := acquireOrchestratorLock(config.OrchestratorLockPath)
		if err != nil {
			log.Fatalf("Failed to acquire orchestrator lock %s: %v", config.OrchestratorLockPath, err)
		}
		defer func() {
			fileErr := f.Close()
			if fileErr != nil {
				log.Printf("Failed to close lock file %s: %v", config.OrchestratorLockPath, fileErr)
			}
			// Remove the lock file on clean shutdown so a rollback to the older
			// stat-based release can start: that guard exits whenever the lock
			// file exists and cannot tell that this process is already gone.
			// TODO: Remove this os.Remove once all hosts run a flock-based
			// release and rollback to the stat-based guard is no longer possible.
			if rmErr := os.Remove(config.OrchestratorLockPath); rmErr != nil && !os.IsNotExist(rmErr) {
				log.Printf("Failed to remove lock file %s: %v", config.OrchestratorLockPath, rmErr)
			}
		}()
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sig, sigCancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM, syscall.SIGUSR1)
	defer sigCancel()

	nodeID := env.GetNodeID()
	serviceName := cfg.GetServiceName(services)
	serviceInstanceID := uuid.NewString()

	// Detect CPU platform for orchestrator pool matching
	machineInfo, err := machineinfo.Detect()
	if err != nil {
		log.Printf("failed to detect machine info: %v", err)

		return false
	}

	serviceInfo := service.NewInfoContainer(nodeID, version, commitSHA, serviceInstanceID, machineInfo, config)

	serviceError := make(chan error)
	defer close(serviceError)

	var g errgroup.Group
	// defer waiting on the group so that this runs even when
	// there's a panic.
	defer func(g *errgroup.Group) {
		err := g.Wait()
		if err != nil {
			log.Printf("error while shutting down: %v", err)
			success = false
		}
	}(&g)

	// Setup telemetry
	tel, err := telemetry.New(
		ctx,
		nodeID,
		serviceName,
		commitSHA,
		version,
		serviceInstanceID,
		attribute.Key("host.labels").StringSlice(config.NodeLabels),
	)
	if err != nil {
		logger.L().Fatal(ctx, "failed to init telemetry", zap.Error(err))
	}
	e2bgrpc.StartChannelzSampler(ctx)
	defer func() {
		err := tel.Shutdown(ctx)
		if err != nil {
			log.Printf("error while shutting down telemetry: %v", err)
			success = false
		}
	}()

	if err := tel.StartRuntimeInstrumentation(); err != nil {
		log.Printf("failed to start runtime instrumentation: %v", err)
	}

	globalLogger := utils.Must(logger.NewLogger(logger.LoggerConfig{
		ServiceName:   serviceName,
		IsInternal:    true,
		IsDebug:       env.IsDebug(),
		Cores:         []zapcore.Core{logger.GetOTELCore(tel.LogsProvider, serviceName)},
		EnableConsole: true,
	}))
	defer func(l logger.Logger) {
		err := l.Sync()
		if err != nil {
			log.Printf("error while shutting down logger: %v", err)
			success = false
		}
	}(globalLogger)
	logger.ReplaceGlobals(ctx, globalLogger)

	sbxLoggerInternal := sbxlogger.NewLogger(
		ctx,
		tel.LogsProvider,
		sbxlogger.SandboxLoggerConfig{
			ServiceName:      serviceName,
			IsInternal:       true,
			CollectorAddress: env.LogsCollectorAddress(),
		},
	)
	defer func(l logger.Logger) {
		err := l.Sync()
		if err != nil {
			log.Printf("error while shutting down sandbox logger: %v", err)
			success = false
		}
	}(sbxLoggerInternal)
	sbxlogger.SetSandboxLoggerInternal(sbxLoggerInternal)

	globalLogger.Info(ctx, "Starting orchestrator",
		zap.String("version", version),
		zap.String("commit", commitSHA),
		zap.Strings("labels", config.NodeLabels),
		zap.String("host_kernel", telemetry.HostKernelVersion()),
		logger.WithServiceInstanceID(serviceInstanceID),
	)

	startService := func(name string, f func() error) {
		g.Go(func() error {
			l := globalLogger.With(zap.String("service", name))
			l.Info(ctx, "starting service")

			err := f()
			if err != nil {
				l.Error(ctx, "service returned an error", zap.Error(err))
			}

			select {
			case serviceError <- err:
			default:
				// Don't block if the serviceError channel is already closed
				// or if the error is already sent
			}

			return serviceDoneError{name: name}
		})
	}

	var closers []closer

	pprofServer := telemetry.NewPprofServer()
	go func() {
		logger.L().Info(ctx, "pprof server starting", zap.Int("port", telemetry.PprofPort()))

		if err := pprofServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.L().Error(ctx, "pprof server encountered error", zap.Error(err))
		}
	}()
	// Closers run in reverse order; retain diagnostics through service teardown.
	closers = append(closers, closer{"pprof server", func(ctx context.Context) error {
		return closePprofServer(ctx, pprofServer)
	}})

	// The sandbox map is shared between the server and the proxy
	// to propagate information about sandbox routing.
	sandboxes := sandbox.NewSandboxesMap()

	// feature flags
	featureFlags, err := featureflags.NewClient(config.DeploymentEnvironment, "")
	if err != nil {
		logger.L().Fatal(ctx, "failed to create feature flags client", zap.Error(err))
	}
	closers = append(closers, closer{"feature flags", featureFlags.Close})

	featureFlags.RegisterContextProvider(orchestratorContextProvider(nodeID, commitSHA))
	featureFlags.RegisterContextProvider(instanceGroupContextProvider(config.InstanceGroupName))

	// Not a service and no closer: pacing stays in force through the sandbox
	// drain and ends with the process.
	if usesSandboxRuntime {
		gcController, err := gcpercent.New(tel.MeterProvider, featureFlags)
		if err != nil {
			logger.L().Error(ctx, "failed to create GC percent controller", zap.Error(err))
		} else {
			go gcController.Run(context.WithoutCancel(ctx))
		}
	}

	// External sandbox logger routes through LaunchDarkly (LogsWriteConfigFlag),
	// falling back to the fixed collector address. Created here so it can use the
	// feature flags client.
	sbxLoggerExternal := sbxlogger.NewLogger(
		ctx,
		tel.LogsProvider,
		sbxlogger.SandboxLoggerConfig{
			ServiceName:      serviceName,
			IsInternal:       false,
			CollectorAddress: env.LogsCollectorAddress(),
			FeatureFlags:     featureFlags,
		},
	)
	defer func(l logger.Logger) {
		err := l.Sync()
		if err != nil {
			log.Printf("error while shutting down sandbox logger: %v", err)
			success = false
		}
	}(sbxLoggerExternal)
	sbxlogger.SetSandboxLoggerExternal(sbxLoggerExternal)

	// gcp concurrent upload limiter
	limiter, err := limit.New(ctx, featureFlags)
	if err != nil {
		logger.L().Fatal(ctx, "failed to create limiter", zap.Error(err))
	}
	closers = append(closers, closer{"limiter", limiter.Close})

	templateSpec, err := cfg.TemplateStorage()
	if err != nil {
		logger.L().Fatal(ctx, "failed to resolve template storage", zap.Error(err))
	}

	persistence, err := storage.NewProvider(ctx, templateSpec, storage.WithLimiter(limiter))
	if err != nil {
		logger.L().Fatal(ctx, "failed to create template storage provider", zap.Error(err))
	}

	blockMetrics, err := blockmetrics.NewMetrics(tel.MeterProvider)
	if err != nil {
		logger.L().Fatal(ctx, "failed to create metrics provider", zap.Error(err))
	}

	// redis (initialized before template cache so the peer registry can be passed to NewCache)
	redisClient, err := sharedFactories.NewRedisClient(ctx, sharedFactories.RedisConfig{
		RedisURL:         config.RedisURL,
		RedisClusterURL:  config.RedisClusterURL,
		RedisTLSCABase64: config.RedisTLSCABase64,
		RedisTLSEnabled:  config.RedisTLSEnabled,
		RedisPassword:    config.RedisPassword,
		PoolSize:         config.RedisPoolSize,
		MinIdleConns:     config.RedisMinIdleConns,
	})
	if err != nil && !errors.Is(err, sharedFactories.ErrRedisDisabled) {
		logger.L().Fatal(ctx, "Could not connect to Redis", zap.Error(err))
	} else if err == nil {
		closers = append(closers, closer{"redis client", func(context.Context) error {
			return sharedFactories.CloseCleanly(redisClient)
		}})
	}

	peerRegistry := peerclient.NopRegistry()
	peerResolver := peerclient.NopResolver()
	if nodeAddress := config.NodeAddress(); redisClient != nil && nodeAddress != nil {
		peerRegistry = peerclient.NewRedisRegistry(redisClient, *nodeAddress)
		peerResolver = peerclient.NewResolver(peerRegistry, *nodeAddress)
	}

	templateCache, err := template.NewCache(config, featureFlags, persistence, blockMetrics, peerResolver)
	if err != nil {
		logger.L().Fatal(ctx, "failed to create template cache", zap.Error(err))
	}
	templateCache.Start(ctx)
	closers = append(closers, closer{"template cache", func(context.Context) error {
		templateCache.Stop()

		return nil
	}})

	// clickhouse delivery endpoints
	clickhouseEndpoints, clickhouseClosers := openClickhouseEndpoints(ctx, config)
	closers = append(closers, clickhouseClosers...)

	sbxEventsDeliveryTargets := make([]event.Delivery[event.SandboxEvent], 0, len(clickhouseEndpoints))
	hostStatsTargets := make([]clickhousehoststats.Delivery, 0, len(clickhouseEndpoints))

	for _, endpoint := range clickhouseEndpoints {
		if delivery := newClickhouseEventsDelivery(ctx, endpoint, featureFlags); delivery != nil {
			sbxEventsDeliveryTargets = append(sbxEventsDeliveryTargets, delivery)
		}

		if delivery := newClickhouseHostStatsDelivery(ctx, endpoint, featureFlags); delivery != nil {
			hostStatsTargets = append(hostStatsTargets, delivery)
		}
	}

	hostStatsDelivery := clickhousehoststats.NewMultiDelivery(hostStatsTargets...)

	// cgroup manager for resource accounting
	cgroupManager, err := cgroup.NewManager()
	if err != nil {
		logger.L().Fatal(ctx, "failed to initialize cgroup manager", zap.Error(err))
	}

	if err := cgroupManager.Initialize(ctx); err != nil {
		logger.L().Fatal(ctx, "failed to initialize root cgroup", zap.Error(err))
	}

	logger.L().Info(ctx, "cgroup accounting enabled", zap.String("root", cgroup.RootCgroupPath))

	// Redis sandbox events delivery target
	if redisClient != nil {
		sbxEventsDeliveryRedis := event.NewRedisStreamsDelivery[event.SandboxEvent](redisClient, event.SandboxEventsStreamName)
		sbxEventsDeliveryTargets = append(sbxEventsDeliveryTargets, sbxEventsDeliveryRedis)
	}

	// Orchestrator-owned sandbox routing record (sandbox:routing:{id}).
	if redisClient != nil {
		routingPublisher, err := routing.New(
			tel.MeterProvider,
			sandboxcatalog.NewRedisSandboxCatalog(redisClient),
			serviceInstanceID,
			config.NodeIP,
		)
		if err != nil {
			logger.L().Fatal(ctx, "failed to create sandbox routing publisher", zap.Error(err))
		}
		sandboxes.Subscribe(routingPublisher)
	} else {
		logger.L().Warn(ctx, "redis disabled; orchestrator sandbox routing records are not published")
	}

	// Wrapper closers run before per-driver closers (deliveries write through the drivers).
	eventsService := events.NewEventsService(sbxEventsDeliveryTargets)
	closers = append(closers, closer{"sandbox host stats deliveries (all)", hostStatsDelivery.Close})
	closers = append(closers, closer{"sandbox events deliveries (all)", eventsService.Close})

	// sandbox observer
	sandboxObserver, err := metrics.NewSandboxObserver(ctx, nodeID, serviceName, commitSHA, version, serviceInstanceID, sandboxes)
	if err != nil {
		logger.L().Fatal(ctx, "failed to create sandbox observer", zap.Error(err))
	}
	closers = append(closers, closer{"sandbox observer", sandboxObserver.Close})

	// host metrics — samples CPU in the background so GetCPUMetrics is a
	// non-blocking cache read on the request path.
	hostMetrics := metrics.NewHostMetrics()
	startService("host metrics poller", func() error {
		return hostMetrics.Start()
	})
	closers = append(closers, closer{"host metrics poller", hostMetrics.Close})

	// sandbox proxy
	sandboxProxy, err := proxy.NewSandboxProxy(tel.MeterProvider, config.ProxyPort, sandboxes, featureFlags)
	if err != nil {
		logger.L().Fatal(ctx, "failed to create sandbox proxy", zap.Error(err))
	}
	startService("sandbox proxy", func() error {
		err := sandboxProxy.Start(ctx)
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}

		return err
	})
	closers = append(closers, closer{"sandbox proxy", sandboxProxy.Close})

	// egress proxy — built by the edition-specific factory
	deps := &Deps{
		Config:              config,
		Tel:                 tel,
		MeterProvider:       tel.MeterProvider,
		Logger:              globalLogger,
		Sandboxes:           sandboxes,
		FeatureFlags:        featureFlags,
		ClickhouseEndpoints: clickhouseEndpoints,
	}

	egressSetup, err := opts.EgressFactory(ctx, deps)
	if err != nil {
		logger.L().Fatal(ctx, "failed to create egress proxy", zap.Error(err))
	}
	if egressSetup == nil {
		logger.L().Fatal(ctx, "EgressFactory returned nil EgressSetup without error")
	}
	if egressSetup.Start != nil {
		startService("egress proxy", func() error {
			return egressSetup.Start(ctx)
		})
	}
	if egressSetup.Close != nil {
		closers = append(closers, closer{"egress proxy", egressSetup.Close})
	}

	// Sandbox-runtime reclaim must run before NewStorageLocal below: reclaim
	// deletes leaked ns-* from /run/netns, and NewStorageLocal snapshots the
	// remaining namespaces as foreign at construction.
	// reclaimClean is true only when reclaim ran AND tore every slot down; a
	// partial failure leaves anchor namespaces that must keep their v2 set entries.
	reclaimClean := false
	reclaimRan := false
	if usesSandboxRuntime && !config.DisableStartupReclaim {
		reclaimRan = true
		summary := startupreclaim.Run(ctx, startupreclaim.Config{
			NetworkConfig: config.NetworkConfig,
			EgressProxy:   egressSetup.Proxy,
			CgroupManager: cgroupManager,
			StorageConfig: config.StorageConfig,
		})
		reclaimClean = !summary.HasFailures()
	}

	if usesSandboxRuntime {
		tracker, err := metrics.NewFirecrackerTracker(tel.MeterProvider, sandboxes)
		if err != nil {
			logger.L().Fatal(ctx, "failed to create Firecracker process tracker", zap.Error(err))
		}
		startService("Firecracker process tracker", func() error { return tracker.Start(ctx) })
		closers = append(closers, closer{"Firecracker process tracker", tracker.Close})
	}

	// device pool
	devicePool, err := nbd.NewDevicePool(config.NBDPoolSize)
	if err != nil {
		logger.L().Fatal(ctx, "failed to create device pool", zap.Error(err))
	}
	startService("nbd device pool", func() error {
		devicePool.Populate(ctx)

		return nil
	})
	closers = append(closers, closer{"device pool", devicePool.Close})

	// network pool
	slotStorage, err := network.NewStorageLocal(ctx, config.NetworkConfig, egressSetup.Proxy)
	if err != nil {
		logger.L().Fatal(ctx, "failed to create network pool", zap.Error(err))
	}
	var networkPool network.PoolInterface
	selectedNetworkVersion := 0
	switch config.NetworkConfig.NetworkVersion {
	case 2:
		logger.L().Info(ctx, "using v2 network pool (nftables)")
		v2Metrics, metricsErr := networkv2.NewMetrics(tel.MeterProvider)
		if metricsErr != nil {
			logger.L().Fatal(ctx, "failed to create v2 network metrics", zap.Error(metricsErr))
		}

		if err := networkv2.ValidateV2Prerequisites(); err != nil {
			logger.L().Fatal(ctx, "v2 network prerequisites not met", zap.Error(err))
		}

		hostFw, hfErr := networkv2.NewHostFirewallWithMetrics(network.DefaultGateway(), config.NetworkConfig, v2Metrics)
		if hfErr != nil {
			logger.L().Fatal(ctx, "failed to create v2 host firewall", zap.Error(hfErr))
		}

		// Flush to empty only when reclaim tore every slot down; a partial
		// failure leaves anchor namespaces whose set entries must survive.
		if reclaimClean {
			if err := hostFw.ReconcileSlots(ctx, nil); err != nil {
				logger.L().Fatal(ctx, "failed to reconcile v2 host firewall slots", zap.Error(err))
			}
		} else if reclaimRan {
			v2Metrics.RecordReconciliationSkipped(ctx)
		}

		observer, obsErr := networkv2.NewVethObserver()
		if obsErr != nil {
			logger.L().Fatal(ctx, "failed to create v2 veth observer", zap.Error(obsErr))
		}

		v2Pool := networkv2.NewV2Pool(slotStorage, config.NetworkConfig, hostFw, observer, networkv2.WithPoolMetrics(v2Metrics))
		startService("network pool", func() error {
			v2Pool.Populate(ctx)

			return nil
		})
		networkPool = v2Pool
		selectedNetworkVersion = 2
	case 1:
		if usesSandboxRuntime && !config.DisableStartupReclaim {
			if err := networkv2.PurgeHostFirewallTable(); err != nil {
				logger.L().Fatal(ctx, "refusing to start v1: stale v2 host firewall table could hijack v1 sandbox egress", zap.Error(err))
			}
		}
		v1Pool := network.NewPool(network.NewSlotsPoolSize, network.ReusedSlotsPoolSize, slotStorage, config.NetworkConfig)
		startService("network pool", func() error {
			v1Pool.Populate(ctx)

			return nil
		})
		networkPool = v1Pool
		selectedNetworkVersion = 1
	default:
		// Config validation runs before startup side effects. Keep this exhaustive
		// defense so future parsing and selection logic cannot silently diverge.
		logger.L().Fatal(ctx, "unsupported NETWORK_VERSION after validated config", zap.Int("network_version", config.NetworkConfig.NetworkVersion))
	}
	closers = append(closers, closer{"network pool", networkPool.Close})

	datapathRegistration, err := network.RegisterDatapathMetric(tel.MeterProvider, selectedNetworkVersion)
	if err != nil {
		logger.L().Fatal(ctx, "failed to register network datapath metric", zap.Error(err))
	}
	closers = append(closers, closer{"network datapath metric", func(context.Context) error {
		return datapathRegistration.Unregister()
	}})

	// sandbox factory
	networkAssignHook := egressSetup.NetworkAssignHook
	if networkAssignHook == nil {
		networkAssignHook = sandbox.NoopNetworkAssignHook{}
	}
	sandboxFactory := sandbox.NewFactory(ctx, config.BuilderConfig, networkPool, devicePool, featureFlags, hostStatsDelivery, cgroupManager, egressSetup.Proxy, networkAssignHook, sandboxes)

	// confined volume filesystems (for the volume service and nfs proxy)
	if err := chrooted.CheckSupport(); err != nil {
		logger.L().Fatal(ctx, "volume confinement prerequisites not met", zap.Error(err))
	}
	builder := chrooted.NewBuilder(config)
	volumeService := volumes.New(config, builder)

	uploads := sandbox.NewUploads(templateCache, persistence, peerResolver, redisClient, featureFlags)
	closers = append(closers, closer{"pending uploads", func(context.Context) error {
		uploads.Stop()

		return nil
	}})

	orchestratorService, err := server.New(ctx, server.ServiceConfig{
		Config:           config,
		SandboxFactory:   sandboxFactory,
		Tel:              tel,
		NetworkPool:      networkPool,
		DevicePool:       devicePool,
		TemplateCache:    templateCache,
		Info:             serviceInfo,
		Proxy:            sandboxProxy,
		Persistence:      persistence,
		FeatureFlags:     featureFlags,
		SbxEventsService: eventsService,
		PeerRegistry:     peerRegistry,
		Uploads:          uploads,
	})
	if err != nil {
		logger.L().Fatal(ctx, "failed to create orchestrator server", zap.Error(err))
	}
	closers = append(closers, closer{"orchestrator server", func(ctx context.Context) error {
		return orchestratorService.Close(ctx)
	}})

	// template manager sandbox logger
	tmplSbxLoggerExternal := sbxlogger.NewLogger(
		ctx,
		tel.LogsProvider,
		sbxlogger.SandboxLoggerConfig{
			ServiceName:      constants.ServiceNameTemplate,
			IsInternal:       false,
			CollectorAddress: env.LogsCollectorAddress(),
			FeatureFlags:     featureFlags,
		},
	)
	closers = append(closers, closer{
		"template manager sandbox logger", func(context.Context) error {
			// Sync returns EINVAL when path is /dev/stdout (for example)
			if err := tmplSbxLoggerExternal.Sync(); err != nil && !errors.Is(err, syscall.EINVAL) {
				return err
			}

			return nil
		},
	})

	// nfs proxy server
	if len(config.PersistentVolumeMounts) > 0 {
		nfsClosers, err := startNFSProxy(ctx, config, builder, startService, sandboxes)
		if err != nil {
			logger.L().Fatal(ctx, "failed to start nfs proxy", zap.Error(err))
		}
		closers = append(closers, nfsClosers...)
	}

	// hyperloop server
	hyperloopSrv, err := hyperloopserver.NewHyperloopServer(ctx, config.NetworkConfig.HyperloopProxyPort, globalLogger, sandboxes, featureFlags)
	if err != nil {
		logger.L().Fatal(ctx, "failed to create hyperloop server", zap.Error(err))
	}
	startService("hyperloop server", func() error {
		err := hyperloopSrv.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}

		return err
	})
	closers = append(closers, closer{"hyperloop server", hyperloopSrv.Shutdown})

	grpcServer := e2bgrpc.NewGRPCServer(tel, e2bgrpc.WithSandboxResumeMetrics())
	orchestrator.RegisterSandboxServiceServer(grpcServer, orchestratorService)
	orchestrator.RegisterVolumeServiceServer(grpcServer, volumeService)
	orchestrator.RegisterChunkServiceServer(grpcServer, orchestratorService)

	// template manager
	var tmpl *tmplserver.ServerStore
	var localUploadHandler *localupload.Handler
	if services.RunsTemplateManager() {
		buildPersistence, uploadHandler, err := setupBuildStorage(ctx, limiter, config)
		if err != nil {
			logger.L().Fatal(ctx, "failed to setup build storage", zap.Error(err))
		}

		localUploadHandler = uploadHandler

		tmpl, err = tmplserver.New(
			ctx,
			config,
			serviceInfo,
			featureFlags,
			tel.MeterProvider,
			globalLogger,
			tmplSbxLoggerExternal,
			sandboxFactory,
			sandboxProxy,
			templateCache,
			persistence,
			buildPersistence,
			uploads,
		)
		if err != nil {
			logger.L().Fatal(ctx, "failed to create template manager", zap.Error(err))
		}

		templatemanager.RegisterTemplateServiceServer(grpcServer, tmpl)

		closers = append(closers, closer{"template server", tmpl.Close})
	}

	infoService := service.NewInfoService(serviceInfo, sandboxes, hostMetrics)
	orchestratorinfo.RegisterInfoServiceServer(grpcServer, infoService)

	grpcHealth := health.NewServer()
	grpc_health_v1.RegisterHealthServer(grpcServer, grpcHealth)

	// cmux server, allows us to reuse the same TCP port between grpc and HTTP requests
	cmuxServer, err := NewCMUXServer(ctx, config.GRPCPort, tel.MeterProvider)
	if err != nil {
		logger.L().Fatal(ctx, "failed to create cmux server", zap.Error(err))
	}

	// Create all matchers BEFORE starting Serve() to avoid data race.
	// cmux.Match() modifies internal state that Serve() reads from.
	httpListener := cmuxServer.Match(cmux.HTTP1Fast())
	grpcListener := cmuxServer.Match(cmux.Any()) // the rest are GRPC requests

	startService("cmux server", func() error {
		logger.L().Info(ctx, "Starting network server", zap.Uint16("port", config.GRPCPort))
		err := cmuxServer.Serve()
		if err != nil && strings.Contains(err.Error(), "use of closed network connection") {
			return nil
		}

		return err
	})
	closers = append(closers, closer{"cmux server", func(context.Context) error {
		logger.L().Info(ctx, "Shutting down cmux server")
		cmuxServer.Close()

		return nil
	}})

	// http server
	healthcheck, err := e2bhealthcheck.NewHealthcheck(serviceInfo)
	if err != nil {
		logger.L().Fatal(ctx, "failed to create healthcheck", zap.Error(err))
	}

	httpMux := http.NewServeMux()
	httpMux.Handle("/health", healthcheck.CreateHandler())

	if localUploadHandler != nil {
		httpMux.Handle("/upload", localUploadHandler)
	}

	httpServer := NewHTTPServer()
	httpServer.Handler = httpMux

	startService("http server", func() error {
		err := httpServer.Serve(httpListener)
		switch {
		case errors.Is(err, cmux.ErrServerClosed):
			return nil
		case errors.Is(err, http.ErrServerClosed):
			return nil
		default:
			return err
		}
	})
	closers = append(closers, closer{"http server", httpServer.Shutdown})

	// grpc server
	startService("grpc server", func() error {
		return grpcServer.Serve(grpcListener)
	})
	closers = append(closers, closer{"grpc server", func(context.Context) error {
		logger.L().Info(ctx, "Shutting down grpc server")
		grpcServer.GracefulStop()

		return nil
	}})

	// Wait for the shutdown signal or if some service fails
	select {
	case <-sig.Done():
		logger.L().Info(ctx, "Shutdown signal received")
	case serviceErr := <-serviceError:
		logger.L().Error(ctx, "Service error", zap.Error(serviceErr))
	}

	closeCtx, cancelCloseCtx := context.WithCancel(context.Background())
	defer cancelCloseCtx()
	if config.ForceStop {
		cancelCloseCtx()
	}

	logger.L().Info(ctx, "Starting drain phase", zap.Int("sandbox_count", sandboxes.Count()))
	serviceInfo.SetStatus(ctx, orchestratorinfo.ServiceInfoStatus_ShuttingDown)

	// Consumers must stop assigning work before services drain.
	if !env.IsLocal() {
		time.Sleep(15 * time.Second)
	}

	// Wait for services to be drained before closing them
	if tmpl != nil {
		err := tmpl.Wait(closeCtx)
		if err != nil {
			logger.L().Error(ctx, "error while waiting for template manager to drain", zap.Error(err))
			success = false
		}
	}

	// Gracefully wait for live sandboxes to exit before closing the services they
	// depend on. The forced-stop path skips this and tears sandboxes down later.
	if !config.ForceStop {
		logger.L().Info(ctx, "Starting sandbox drain phase", zap.Int("sandbox_count", sandboxes.Count()))
		if err := orchestratorService.DrainSandboxes(closeCtx); err != nil {
			logger.L().Error(ctx, "error while draining sandboxes", zap.Error(err))
			success = false
		}
	}

	slices.Reverse(closers)
	for _, closer := range closers {
		clog := globalLogger.With(zap.String("service", closer.name), zap.Bool("forced", config.ForceStop))
		clog.Info(ctx, "closing")
		if err := closer.close(closeCtx); err != nil {
			clog.Error(ctx, "error during shutdown", zap.Error(err))
			success = false
		}
	}

	logger.L().Info(ctx, "Waiting for services to finish")
	var sde serviceDoneError
	if err := g.Wait(); err != nil && !errors.As(err, &sde) {
		logger.L().Error(ctx, "service group error", zap.Error(err))
		success = false
	}

	return success
}

func startNFSProxy(
	ctx context.Context,
	config cfg.Config,
	builder *chrooted.Builder,
	startService func(name string, f func() error),
	sandboxes *sandbox.Map,
) ([]closer, error) {
	var closers []closer

	// portmapper listener
	var pmConfig net.ListenConfig
	pmLis, err := pmConfig.Listen(ctx, "tcp", fmt.Sprintf(":%d", config.NetworkConfig.PortmapperPort))
	if err != nil {
		return nil, fmt.Errorf("failed to listen on portmapper port: %w", err)
	}

	// portmapper implementation
	pm := portmap.NewPortMap(ctx)
	pm.RegisterPort(ctx, 2049)
	startService("portmapper server", func() error {
		return pm.Serve(ctx, pmLis)
	})
	closers = append(closers, closer{"portmapper server", func(_ context.Context) error { return pmLis.Close() }})

	// nfs proxy listener
	var nfsConfig net.ListenConfig
	lis, err := nfsConfig.Listen(ctx, "tcp", fmt.Sprintf(":%d", config.NetworkConfig.NFSProxyPort))
	if err != nil {
		return nil, fmt.Errorf("failed to listen on nfs port: %w", err)
	}

	// nfs proxy implementation
	nfsServer, err := nfsproxy.NewProxy(ctx, builder, sandboxes, nfscfg.Config{
		Logging:           config.NFSProxyLogging,
		Tracing:           config.NFSProxyTracing,
		Metrics:           config.NFSProxyMetrics,
		RecordHandleCalls: config.NFSProxyRecordHandleCalls,
		RecordStatCalls:   config.NFSProxyRecordStatCalls,
		NFSLogLevel:       config.NFSProxyLogLevel,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create nfs proxy: %w", err)
	}
	startService("nfs proxy", func() error {
		return nfsServer.Serve(lis)
	})
	closers = append(closers, closer{
		"nfs proxy server", func(_ context.Context) error {
			return lis.Close()
		},
	})

	return closers, nil
}

func setupBuildStorage(ctx context.Context, limiter *limit.Limiter, orchConfig cfg.Config) (storage.StorageProvider, *localupload.Handler, error) {
	spec, err := cfg.BuildCacheStorage()
	if err != nil {
		return nil, nil, fmt.Errorf("resolve build cache storage: %w", err)
	}

	opts := []storage.Option{storage.WithLimiter(limiter)}

	var uploadHandler *localupload.Handler

	if spec.Provider == storage.LocalStorageProvider {
		hmacKey := make([]byte, 32)
		if _, err := rand.Read(hmacKey); err != nil {
			return nil, nil, fmt.Errorf("generate HMAC key: %w", err)
		}

		uploadBaseURL := orchConfig.LocalUploadBaseURL
		if uploadBaseURL == "" {
			uploadBaseURL = fmt.Sprintf("http://localhost:%d", orchConfig.GRPCPort)
		}

		opts = append(opts, storage.WithLocalUpload(uploadBaseURL, hmacKey))
		uploadHandler = localupload.NewHandler(spec.BasePath, hmacKey)

		logger.L().Info(ctx, "Local upload endpoint enabled for filesystem storage",
			zap.String("upload_base_url", uploadBaseURL),
			zap.String("base_path", spec.BasePath))
	}

	provider, err := storage.NewProvider(ctx, spec, opts...)
	if err != nil {
		return nil, nil, fmt.Errorf("create build cache storage provider: %w", err)
	}

	return provider, uploadHandler, nil
}
