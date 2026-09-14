package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	limits "github.com/gin-contrib/size"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	middleware "github.com/oapi-codegen/gin-middleware"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/cfg"
	"github.com/e2b-dev/infra/packages/api/internal/handlers"
	customMiddleware "github.com/e2b-dev/infra/packages/api/internal/middleware"
	"github.com/e2b-dev/infra/packages/api/internal/middleware/ratelimit"
	"github.com/e2b-dev/infra/packages/api/internal/oauth"
	"github.com/e2b-dev/infra/packages/api/internal/utils"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	sqlcdb "github.com/e2b-dev/infra/packages/db/client"
	"github.com/e2b-dev/infra/packages/shared/pkg/apierrors"
	"github.com/e2b-dev/infra/packages/shared/pkg/env"
	"github.com/e2b-dev/infra/packages/shared/pkg/factories"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	e2bgrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc"
	proxygrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/proxy"
	"github.com/e2b-dev/infra/packages/shared/pkg/httpserver"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	sbxlogger "github.com/e2b-dev/infra/packages/shared/pkg/logger/sandbox"
	sharedmiddleware "github.com/e2b-dev/infra/packages/shared/pkg/middleware"
	metricsMiddleware "github.com/e2b-dev/infra/packages/shared/pkg/middleware/otel/metrics"
	tracingMiddleware "github.com/e2b-dev/infra/packages/shared/pkg/middleware/otel/tracing"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	sharedutils "github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

const (
	serviceName        = "orchestration-api"
	maxMultipartMemory = 1 << 23 // 8 MiB
	maxUploadLimit     = 1 << 24 // 16 MiB

	maxReadHeaderTimeout = 5 * time.Second
	maxReadTimeout       = 10 * time.Second
	maxWriteTimeout      = 75 * time.Second

	// requestTimeout is the context deadline applied to each request.
	// Must be less than maxWriteTimeout so the context cancels before the
	// server's write deadline kills the connection (WriteTimeout does NOT
	// cancel r.Context(); see https://github.com/golang/go/issues/59602).
	requestTimeout = 70 * time.Second

	// This timeout should be > 600 (GCP LB upstream idle timeout) to prevent race condition
	// https://cloud.google.com/load-balancing/docs/https#timeouts_and_retries%23:~:text=The%20load%20balancer%27s%20backend%20keepalive,is%20greater%20than%20600%20seconds
	idleTimeout = 620 * time.Second

	defaultPort = 80

	// shutdownDrainWait is how long /health returns 503 before we begin
	// stopping the HTTP listener, giving the load balancer time to drain
	// us off active backends.
	shutdownDrainWait = 15 * time.Second

	// shutdownTimeout caps how long s.Shutdown waits for in-flight
	// requests to complete. Must be >= requestTimeout so a request, that
	// arrived before load balancer stopped sending new traffic,
	// has its full deadline to finish
	// (e.g. a slow sandbox pause / snapshot RPC).
	//
	// Also caps grpc.Server.GracefulStop. After this we
	// fall back to Stop() so a stuck stream cannot block the process
	// past Nomad's kill_timeout.
	shutdownTimeout = requestTimeout + 5*time.Second

	// pprofShutdownTimeout is a best-effort bound for pprof drain.
	pprofShutdownTimeout = 5 * time.Second
)

var (
	commitSHA                  string
	expectedMigrationTimestamp string
	serviceVersion             = "0.14.1" // x-release-please-version
)

func NewGinServer(ctx context.Context, config cfg.Config, tel *telemetry.Client, l logger.Logger, apiStore *handlers.APIStore, adminJWTVerifier *auth.JWKSVerifier, redisClient redis.UniversalClient, ff *featureflags.Client, swagger *openapi3.T, port int) *http.Server {
	// Clear out the servers array in the swagger spec, that skips validating
	// that server names match. We don't know how this thing will be run.
	swagger.Servers = nil

	r := gin.New()
	// Use the raw (percent-encoded) URL path for route matching so that encoded slashes (%2F)
	// in path params are treated as part of the segment, not as path separators.
	// This is needed for template IDs that contain namespace/alias (e.g. "team-slug/my-template").
	// Param values are still unescaped before reaching handlers (UnescapePathValues defaults to true).
	r.UseRawPath = true

	r.Use(gin.Recovery())

	// Every secrets response is uncacheable, whichever layer answers. Must run
	// before anything that can write a response.
	r.Use(customMiddleware.NoStoreSecrets())

	r.Use(
		// We use custom otel gin middleware because we want to log 4xx errors in the otel
		sharedmiddleware.ExcludeRoutes(
			tracingMiddleware.Middleware(tel.TracerProvider, serviceName), //nolint:contextcheck // TODO: fix this later
			"/health",
			"/sandboxes/:sandboxID/refreshes",
			"/templates/:templateID/builds/:buildID/logs",
			"/templates/:templateID/builds/:buildID/status",
		),
		sharedmiddleware.IncludeRoutes(
			metricsMiddleware.Middleware(tel.MeterProvider, serviceName), //nolint:contextcheck // TODO: fix this later
			"/sandboxes",
			"/sandboxes/:sandboxID",
			"/sandboxes/:sandboxID/pause",
			"/sandboxes/:sandboxID/connect",
			"/sandboxes/:sandboxID/resume",
			"/sandboxes/:sandboxID/snapshots",
		),
		sharedmiddleware.LoggingMiddleware(l, sharedmiddleware.Config{ //nolint:contextcheck // ctx is captured before c.Next() intentionally to avoid seeing child context cancellations from inner middleware
			TimeFormat:   time.RFC3339Nano,
			UTC:          true,
			DefaultLevel: zap.InfoLevel,
			Skipper: func(c *gin.Context) bool {
				switch c.FullPath() {
				case "/health",
					"/sandboxes/:sandboxID/refreshes",
					"/templates/:templateID/builds/:buildID/logs",
					"/templates/:templateID/builds/:buildID/status":
					return true
				}

				return false
			},
			Context: func(c *gin.Context) []zapcore.Field {
				if teamInfo, ok := auth.GetTeamInfo(c); ok {
					return []zapcore.Field{logger.WithTeamID(teamInfo.ID.String())}
				}

				return nil
			},
		}),
		gin.Recovery(),
		sharedmiddleware.RequestTimeout(requestTimeout), //nolint:contextcheck // Gin middleware sets context via c.Request.WithContext
	)

	r.Use(customMiddleware.CORS())

	// Removed routes are registered before the OpenAPI validator middleware
	// (which rejects paths missing from the spec) so old clients get a clear
	// 410 instead of a 404.
	gone := func(msg string) gin.HandlerFunc {
		return func(c *gin.Context) { apierrors.SendAPIStoreError(c, http.StatusGone, msg) }
	}
	accessTokensGone := gone("E2B_ACCESS_TOKEN is deprecated and no longer supported. Use an API key (E2B_API_KEY) instead. See https://e2b.dev/docs/migration/access-token-deprecation")
	r.POST("/access-tokens", accessTokensGone)
	r.DELETE("/access-tokens/:accessTokenID", accessTokensGone)

	templateBuildV1Gone := gone("The v1 template build API is no longer supported. Upgrade the CLI and migrate to v2 templates. See https://e2b.dev/docs/template/migration-v2")
	r.POST("/templates", templateBuildV1Gone)
	r.POST("/templates/:templateID", templateBuildV1Gone)
	r.POST("/templates/:templateID/builds/:buildID", templateBuildV1Gone)
	r.POST("/v2/templates", templateBuildV1Gone)

	// Create a team API Key auth validator
	AuthenticationFunc := auth.CreateAuthenticationFunc(
		[]auth.Authenticator{
			auth.NewApiKeyAuthenticator(apiStore.GetTeamFromAPIKey),
			auth.NewAuthProviderBearerAuthenticator(apiStore.GetUserIDFromAuthProviderToken),
			auth.NewAuthProviderTeamAuthenticator(apiStore.GetTeamFromAuthProviderToken),
			auth.NewAdminApiKeyAuthenticator(config.AdminToken),
			auth.NewAdminJWTAuthenticator(adminJWTVerifier),
			auth.NewAdminTeamAuthenticator(apiStore.GetTeamFromAdminToken),
		},
		metricsMiddleware.SetProcessingStartTime,
	)

	// Use our validation middleware to check all requests against the
	// OpenAPI schema.
	r.Use(
		limits.RequestSizeLimiter(maxUploadLimit),
		middleware.OapiRequestValidatorWithOptions(swagger,
			&middleware.Options{
				ErrorHandler: func(c *gin.Context, message string, fallbackStatusCode int) {
					// Override the status code provided by the oapi-codegen/gin-middleware as that is always set to 400 or 404.
					statusCode := max(c.Writer.Status(), fallbackStatusCode)
					utils.ErrorHandler(c, message, statusCode)
				},
				MultiErrorHandler: utils.MultiErrorHandler,
				Options: openapi3filter.Options{
					AuthenticationFunc: AuthenticationFunc,
					// Handle multiple errors as MultiError type
					MultiError: true,
				},
			}),
	)

	r.Use(customMiddleware.InitLaunchDarklyContext)

	// Per-team rate limiting (after auth + LD context, before handlers).
	// Only applied to connect and resume endpoints. Gated by feature flag.
	limiter := ratelimit.NewLimiter(redisClient)
	r.Use(ratelimit.Middleware(limiter, ratelimit.Config{FailOpen: true}, ff)) //nolint:contextcheck // Gin middleware sets context via c.Request.WithContext

	// Deny blocked teams on every mutating route unless allowlisted in
	// EnforceBlockedTeam. Must run after auth (which populates team info on
	// the gin context) and before the handlers.
	r.Use(customMiddleware.EnforceBlockedTeam())

	// We now register our store above as the handler for the interface
	api.RegisterHandlersWithOptions(r, apiStore, api.GinServerOptions{
		ErrorHandler: func(c *gin.Context, err error, statusCode int) {
			utils.ErrorHandler(c, err.Error(), statusCode)
		},
	})

	r.MaxMultipartMemory = maxMultipartMemory

	s := &http.Server{
		Handler: r,
		Addr:    fmt.Sprintf("0.0.0.0:%d", port),

		// Configure request timeouts.
		ReadHeaderTimeout: maxReadHeaderTimeout,
		ReadTimeout:       maxReadTimeout,
		WriteTimeout:      maxWriteTimeout,

		// Configure timeouts to be greater than the proxy timeouts.
		IdleTimeout: idleTimeout,

		// BaseContext is the parent of every incoming request's r.Context().
		// It MUST NOT be derived from a context that the serve goroutines
		// (HTTP/gRPC) cancel on exit: s.Shutdown stops the listener, the
		// serve goroutine returns, and its `defer serveErrCancel()` fires
		// while in-flight requests (sandbox pause/snapshot/delete) are
		// still running. Per-request deadlines come from middleware.
		BaseContext: func(net.Listener) context.Context { return ctx },
	}
	httpserver.ConfigureH2C(s)

	return s
}

func run() int {
	ctx, cancel := context.WithCancel(context.Background()) // root context
	defer cancel()

	// TODO: additional improvements to signal handling/shutdown:
	//   - provide access to root context in the signal handling
	//     context so request scoped work can start background tasks
	//     without needing to make an unattached context.
	//   - provide mechanism to inform shutdown that background
	//     work has completed (waitgroup, counter, etc.) to avoid
	//     exiting early.

	var port int
	flag.IntVar(&port, "port", defaultPort, "Port for test HTTP server")
	flag.Parse()

	serviceInstanceID := uuid.New().String()
	nodeID := env.GetNodeID()

	tel, err := telemetry.New(ctx, nodeID, serviceName, commitSHA, serviceVersion, serviceInstanceID)
	if err != nil {
		logger.L().Fatal(ctx, "failed to create metrics exporter", zap.Error(err))
	}
	e2bgrpc.StartChannelzSampler(ctx)
	defer func() {
		err := tel.Shutdown(ctx)
		if err != nil {
			log.Printf("telemetry shutdown:%v\n", err)
		}
	}()

	l := sharedutils.Must(logger.NewLogger(logger.LoggerConfig{
		ServiceName:   serviceName,
		IsInternal:    true,
		IsDebug:       env.IsDebug(),
		Cores:         []zapcore.Core{logger.GetOTELCore(tel.LogsProvider, serviceName)},
		EnableConsole: true,
	}))
	defer l.Sync()
	logger.ReplaceGlobals(ctx, l)

	sbxLoggerInternal := sbxlogger.NewLogger(
		ctx,
		tel.LogsProvider,
		sbxlogger.SandboxLoggerConfig{
			ServiceName:      serviceName,
			IsInternal:       true,
			CollectorAddress: env.LogsCollectorAddress(),
		},
	)
	defer sbxLoggerInternal.Sync()
	sbxlogger.SetSandboxLoggerInternal(sbxLoggerInternal)

	// Register Go runtime metric callbacks (goroutines, heap, GC, etc.)
	if err := tel.StartRuntimeInstrumentation(); err != nil {
		l.Warn(ctx, "failed to start runtime instrumentation", zap.Error(err))
	}

	// Convert the string expectedMigrationTimestamp  to a int64
	expectedMigration, err := strconv.ParseInt(expectedMigrationTimestamp, 10, 64)
	if err != nil {
		// If expectedMigrationTimestamp is not set, we set it to 0
		l.Warn(ctx, "Failed to parse expected migration timestamp", zap.Error(err))
		expectedMigration = 0
	}

	config, err := cfg.Parse()
	if err != nil {
		fields := []zap.Field{zap.Error(err)}
		if condition, ok := cfg.ParseFailureCondition(err); ok {
			fields = append(fields, zap.String("config_failure_condition", string(condition)))
		}

		logger.L().Fatal(ctx, "Error parsing config", fields...)
	}

	err = sqlcdb.CheckMigrationVersion(ctx, config.PostgresConnectionString, expectedMigration)
	if err != nil {
		l.Fatal(ctx, "failed to check migration version", zap.Error(err))
	}

	l.Info(ctx, "Starting API service...", zap.String("commit_sha", commitSHA), logger.WithServiceInstanceID(serviceInstanceID))

	swagger, err := api.GetSwagger()
	if err != nil {
		// this will call os.Exit: defers won't run, but none
		// need to yet. Change this if this is called later.
		l.Error(ctx, "Error loading swagger spec", zap.Error(err))

		return 1
	}

	var cleanupFns []func(context.Context) error
	exitCode := &atomic.Int32{}
	cleanupOp := func() {
		// some cleanup functions do work that requires a context. passing shutdown a
		// specific context here so that all timeout configuration is in one place.
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		start := time.Now()
		// doing shutdown in parallel to avoid
		// unintentionally: creating shutdown ordering
		// effects.
		cwg := &sync.WaitGroup{}
		count := 0
		for idx := range cleanupFns {
			if cleanup := cleanupFns[idx]; cleanup != nil {
				cwg.Add(1)
				count++
				go func(
					op func(context.Context) error,
					idx int,
				) {
					defer cwg.Done()
					if err := op(ctx); err != nil {
						exitCode.Add(1)
						l.Error(ctx, "Cleanup operation error", zap.Int("index", idx), zap.Error(err))
					}
				}(cleanup, idx)

				cleanupFns[idx] = nil
			}
		}
		if count == 0 {
			l.Info(ctx, "no cleanup operations")

			return
		}
		l.Info(ctx, "Running cleanup operations", zap.Int("count", count))
		cwg.Wait() // this doesn't have a timeout
		l.Info(ctx, "Cleanup operations completed", zap.Int("count", count), zap.Duration("duration", time.Since(start)))
	}
	cleanupOnce := &sync.Once{}
	cleanup := func() { cleanupOnce.Do(cleanupOp) }
	defer cleanup()

	redisClient, err := factories.NewRedisClient(ctx, factories.RedisConfig{
		RedisURL:         config.RedisURL,
		RedisClusterURL:  config.RedisClusterURL,
		RedisTLSCABase64: config.RedisTLSCABase64,
		RedisTLSEnabled:  config.RedisTLSEnabled,
		RedisPassword:    config.RedisPassword,
		PoolSize:         config.RedisPoolSize,
	})
	if err != nil {
		logger.L().Fatal(ctx, "Initializing Redis client", zap.Error(err))
	}
	cleanupFns = append(cleanupFns, func(_ context.Context) error {
		return redisClient.Close()
	})

	featureFlags, err := featureflags.NewClient()
	if err != nil {
		logger.L().Fatal(ctx, "failed to create feature flags client", zap.Error(err))
	}
	cleanupFns = append(cleanupFns, featureFlags.Close)

	featureFlags.SetServiceName(serviceName)
	featureFlags.SetDeploymentName(config.DomainName)

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
	defer sbxLoggerExternal.Sync()
	sbxlogger.SetSandboxLoggerExternal(sbxLoggerExternal)

	// Create an instance of our handler which satisfies the generated interface
	//  (use the outer context rather than the signal handling
	//   context so it doesn't exit first.)
	apiStore := handlers.NewAPIStore(ctx, tel, redisClient, featureFlags, config)
	cleanupFns = append(cleanupFns, apiStore.Close)

	adminJWTVerifier, err := auth.NewJWKSVerifier(ctx, config.AdminAuthProvider, http.DefaultClient)
	if err != nil {
		l.Error(ctx, "initializing admin JWT verifier", zap.Error(err))

		return 1
	}
	if adminJWTVerifier == nil {
		l.Warn(ctx, "ADMIN_AUTH_PROVIDER_CONFIG is not configured; admin JWT requests will return 401")
	}

	grpcAddr := fmt.Sprintf("0.0.0.0:%d", config.APIInternalGrpcPort)
	grpcListener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", grpcAddr)
	if err != nil {
		l.Fatal(ctx, "failed to create proxy grpc listener", zap.Error(err))
	}

	grpcServer := e2bgrpc.NewGRPCServer(tel)
	proxygrpc.RegisterSandboxServiceServer(grpcServer, handlers.NewSandboxService(apiStore, false, nil))

	edgeGrpcAddr := fmt.Sprintf("0.0.0.0:%d", config.APIEdgeGrpcPort)
	edgeGrpcListener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", edgeGrpcAddr)
	if err != nil {
		l.Fatal(ctx, "failed to create edge proxy grpc listener", zap.Error(err))
	}

	edgeGrpcServer := e2bgrpc.NewGRPCServer(tel)
	clientProxyOAuthVerifier, err := oauth.NewVerifier(ctx, config.ClientProxyOIDCIssuerURL)
	if err != nil {
		l.Fatal(ctx, "failed to create client proxy OIDC verifier", zap.Error(err))
	}
	if !oauth.Configured(config.ClientProxyOIDCIssuerURL) {
		l.Warn(ctx, "client proxy OIDC is not configured; edge proxy gRPC requests will be rejected")
	}
	proxygrpc.RegisterSandboxServiceServer(edgeGrpcServer, handlers.NewSandboxService(apiStore, true, clientProxyOAuthVerifier))

	// Pass ctx so in-flight requests survive the serve goroutines' exit during graceful shutdown.
	s := NewGinServer(ctx, config, tel, l, apiStore, adminJWTVerifier, redisClient, featureFlags, swagger, port)

	// ////////////////////////
	//
	// Start the HTTP service

	// signalCtx is cancelled when the process receives SIGTERM/SIGINT.
	// It is the trigger for the shutdown watcher below.
	signalCtx, sigCancel := signal.NotifyContext(ctx, syscall.SIGTERM, syscall.SIGINT)
	defer sigCancel()

	// serveErrCtx is cancelled by any serve goroutine when it exits — both
	// on fatal Serve() error and on the normal "listener closed by Shutdown"
	// exit. The shutdown watcher selects on this so that a startup-time
	// listener failure (e.g. port in use) also triggers the drain sequence.
	serveErrCtx, serveErrCancel := context.WithCancel(ctx)
	defer serveErrCancel()

	wg := &sync.WaitGroup{}

	// in the event of an unhandled panic *still* wait for the
	// HTTP service to terminate:
	defer wg.Wait()

	wg.Go(func() {
		// Signal sibling goroutines via serveErrCtx (NOT the root ctx) so
		// that a startup error or a normal Shutdown-triggered exit wakes
		// the shutdown watcher without aborting the drain.
		defer serveErrCancel()

		l.Info(ctx, "Http service starting", zap.Int("port", port))

		// Serve HTTP until shutdown.
		err := s.ListenAndServe()

		switch {
		case errors.Is(err, http.ErrServerClosed):
			l.Info(ctx, "Http service shutdown successfully", zap.Int("port", port))
		case err != nil:
			exitCode.Add(1)
			l.Error(ctx, "Http service encountered error", zap.Int("port", port), zap.Error(err))
		default:
			// this probably shouldn't happen...
			l.Info(ctx, "Http service exited without error", zap.Int("port", port))
		}
	})

	wg.Go(func() {
		defer serveErrCancel()

		l.Info(ctx, "internal gRPC service starting", zap.Uint16("port", config.APIInternalGrpcPort))
		err := grpcServer.Serve(grpcListener)
		if err != nil {
			exitCode.Add(1)
			l.Error(ctx, "internal gRPC service encountered error", zap.Error(err))
		}
	})

	wg.Go(func() {
		defer serveErrCancel()

		l.Info(ctx, "edge gRPC service starting", zap.Uint16("port", config.APIEdgeGrpcPort))
		err := edgeGrpcServer.Serve(edgeGrpcListener)
		if err != nil {
			exitCode.Add(1)
			l.Error(ctx, "edge gRPC service encountered error", zap.Error(err))
		}
	})

	pprofServer := telemetry.NewPprofServer()

	wg.Go(func() {
		l.Info(ctx, "pprof server starting", zap.Int("port", telemetry.PprofPort()))

		if err := pprofServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			l.Error(ctx, "pprof server encountered error", zap.Error(err))
		}
	})

	wg.Go(func() {
		// Wake on signal OR on a serve goroutine exiting unexpectedly at
		// startup. Either way, run the full drain sequence.
		select {
		case <-signalCtx.Done():
			l.Info(ctx, "shutdown signal received, beginning graceful shutdown")
		case <-serveErrCtx.Done():
			l.Info(ctx, "serve goroutine exited, beginning graceful shutdown")
		}

		// Start returning 503s for health checks
		// to signal that the service is shutting down.
		// This is a bit of a hack, but this way we can properly propagate
		// the health status to the load balancer.
		apiStore.Healthy.Store(false)

		// Skip the delay in local environment for instant shutdown
		if !env.IsLocal() {
			time.Sleep(shutdownDrainWait)
		}

		// Drain HTTP, gRPC in parallel.
		drainWG := &sync.WaitGroup{}

		drainWG.Go(func() {
			httpShutdownCtx, httpShutdownCancel := context.WithTimeout(ctx, shutdownTimeout)
			defer httpShutdownCancel()
			if err := s.Shutdown(httpShutdownCtx); err != nil {
				exitCode.Add(1)
				l.Error(ctx, "Http service shutdown error", zap.Int("port", port), zap.Error(err))
			}
		})

		// Bounded gRPC stop: GracefulStop has no built-in deadline, so a
		// stuck stream would block past Nomad's kill_timeout. Fall back to
		// Stop() after the budget elapses.
		drainWG.Go(func() {
			if !e2bgrpc.GracefulStopWithTimeout(grpcServer, shutdownTimeout) {
				l.Warn(ctx, "internal gRPC forced stop after graceful timeout", zap.Duration("budget", shutdownTimeout))
			}
		})
		drainWG.Go(func() {
			if !e2bgrpc.GracefulStopWithTimeout(edgeGrpcServer, shutdownTimeout) {
				l.Warn(ctx, "edge gRPC forced stop after graceful timeout", zap.Duration("budget", shutdownTimeout))
			}
		})
		drainWG.Wait()

		// Drain pprof after, so that it is still available during the shutdown process for debugging if needed.
		pprofShutdownCtx, pprofCancel := context.WithTimeout(ctx, pprofShutdownTimeout)
		defer pprofCancel()
		if err := pprofServer.Shutdown(pprofShutdownCtx); err != nil {
			l.Error(ctx, "pprof server shutdown error", zap.Error(err))
		}
	})

	// wait for the HTTP service to complete shutting down first
	// before doing other cleanup, we're listening for the signal
	// termination in one of these background threads.
	wg.Wait()

	// call cleanup explicitly because defers (from above) do not
	// run on os.Exit.
	cleanup()

	// TODO: wait for additional work to coalesce
	//
	// currently we only wait for the HTTP handlers to return, and
	// then cancel the remaining context and run all of the
	// cleanup functions. Background go routines at this point
	// terminate. Would need to have a goroutine pool or worker
	// coordinator running to manage and track that work.

	// Exit, with appropriate code.
	return int(exitCode.Load())
}

func main() {
	os.Exit(run())
}
