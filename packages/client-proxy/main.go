package main

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/e2b-dev/infra/packages/proxy/internal"
	"github.com/e2b-dev/infra/packages/proxy/internal/cfg"
	e2bproxy "github.com/e2b-dev/infra/packages/proxy/internal/proxy"
	"github.com/e2b-dev/infra/packages/shared/pkg/env"
	"github.com/e2b-dev/infra/packages/shared/pkg/factories"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	e2bgrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	e2bcatalog "github.com/e2b-dev/infra/packages/shared/pkg/sandbox-catalog"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

type Closeable interface {
	Close(ctx context.Context) error
}

const (
	serviceName = "client-proxy"

	shutdownDrainingWait  = 15 * time.Second
	shutdownUnhealthyWait = 15 * time.Second
)

var (
	commitSHA string
	version   = "0.3.0" // x-release-please-version
)

func run() int {
	config, err := cfg.Parse()
	if err != nil {
		log.Fatalf("failed to parse config: %v\n", err)
	}

	ctx, ctxCancel := context.WithCancel(context.Background())
	defer ctxCancel()

	instanceID := uuid.New().String()
	nodeID := env.GetNodeID()

	// Setup telemetry
	tel, err := telemetry.New(ctx, nodeID, serviceName, commitSHA, version, instanceID)
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

	l := utils.Must(
		logger.NewLogger(logger.LoggerConfig{
			ServiceName:   serviceName,
			IsInternal:    true,
			IsDebug:       env.IsDebug(),
			Cores:         []zapcore.Core{logger.GetOTELCore(tel.LogsProvider, serviceName)},
			EnableConsole: true,
		}),
	)

	defer func() {
		err := l.Sync()
		if err != nil {
			log.Printf("logger sync error: %v\n", err)
		}
	}()
	logger.ReplaceGlobals(ctx, l)

	exitCode := atomic.Int32{}

	wg := sync.WaitGroup{}

	signalCtx, sigCancel := signal.NotifyContext(ctx, syscall.SIGTERM, syscall.SIGINT)
	defer sigCancel()

	l.Info(ctx, "Starting client proxy", zap.String("commit", commitSHA), zap.String("instance_id", instanceID))

	featureFlagsClient, err := featureflags.NewClient()
	if err != nil {
		l.Error(ctx, "Failed to create feature flags client", zap.Error(err))

		return 1
	}
	featureFlagsClient.SetServiceName(serviceName)

	redisClient, err := factories.NewRedisClient(ctx, factories.RedisConfig{
		RedisURL:         config.RedisURL,
		RedisClusterURL:  config.RedisClusterURL,
		RedisTLSCABase64: config.RedisTLSCABase64,
		RedisTLSEnabled:  config.RedisTLSEnabled,
		RedisPassword:    config.RedisPassword,
		PoolSize:         config.RedisPoolSize,
	})
	if err != nil {
		l.Error(ctx, "Failed to create redis client", zap.Error(err))

		return 1
	}
	defer func() {
		err := factories.CloseCleanly(redisClient)
		if err != nil {
			l.Error(ctx, "Failed to close redis client", zap.Error(err))
		}
	}()

	catalog := e2bcatalog.NewRedisSandboxCatalog(redisClient)
	orchestratorCatalog := e2bcatalog.NewRedisSandboxRoutingCatalog(redisClient)

	info := &internal.ServiceInfo{}
	info.SetStatus(ctx, internal.Healthy)

	var pausedSandboxResumer e2bproxy.PausedSandboxResumer
	apiGRPCAddress := strings.TrimSpace(config.APIInternalGRPCAddress)
	apiGRPCOAuthConfig := e2bproxy.GRPCOAuthConfig{}
	apiGRPCUseTLS := false
	if apiGRPCAddress == "" {
		apiGRPCAddress = strings.TrimSpace(config.APIEdgeGRPCAddress)
		apiGRPCUseTLS = true
		apiGRPCOAuthConfig = e2bproxy.GRPCOAuthConfig{
			ClientID:     config.APIEdgeGRPCOAuthClientID,
			ClientSecret: config.APIEdgeGRPCOAuthClientSecret,
			TokenURL:     config.APIEdgeGRPCOAuthTokenURL,
		}
	}

	if apiGRPCAddress != "" {
		pausedSandboxResumer, err = e2bproxy.NewGRPCPausedSandboxResumer(ctx, apiGRPCAddress, apiGRPCOAuthConfig, apiGRPCUseTLS)
		if err != nil {
			l.Error(ctx, "Failed to create paused sandbox checker", zap.Error(err))

			return 1
		}

		pausedSandboxResumer.Init(ctx)
	} else {
		l.Warn(ctx, "API gRPC address not set; paused sandbox checks disabled")
	}

	// Proxy sandbox http traffic to orchestrator nodes
	trafficProxy, err := e2bproxy.NewClientProxy(
		tel.MeterProvider,
		serviceName,
		config.ProxyPort,
		config.OrchestratorProxyPort,
		catalog,
		orchestratorCatalog,
		pausedSandboxResumer,
		featureFlagsClient,
	)
	if err != nil {
		l.Error(ctx, "Failed to create client proxy", zap.Error(err))

		return 1
	}

	// Health check server
	healthAddr := fmt.Sprintf("0.0.0.0:%d", config.HealthPort)
	healthHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if info.GetStatus() == internal.Healthy {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("healthy"))

			return
		}

		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("unhealthy"))
	})

	healthServer := &http.Server{
		Addr:    healthAddr,
		Handler: healthHandler,
	}

	var closers []Closeable
	closers = append(closers, featureFlagsClient, catalog)
	if closeable, ok := pausedSandboxResumer.(Closeable); ok {
		closers = append(closers, closeable)
	}

	wg.Go(func() {
		// make sure to cancel the parent context before this
		// goroutine returns, so that in the case of a panic
		// or error here, the other thread won't block until
		// signaled.
		defer sigCancel()

		proxyRunLogger := l.With(zap.Uint16("port", config.ProxyPort))
		proxyRunLogger.Info(ctx, "Http proxy starting")

		err := trafficProxy.ListenAndServe(ctx)

		// Add different handling for the error
		switch {
		case errors.Is(err, http.ErrServerClosed):
			proxyRunLogger.Info(ctx, "Http proxy closed successfully")
		case err != nil:
			exitCode.Add(1)
			proxyRunLogger.Error(ctx, "Http proxy encountered error", zap.Error(err))
		default:
			// this probably shouldn't happen...
			proxyRunLogger.Error(ctx, "Http proxy exited without error")
		}
	})

	wg.Go(func() {
		defer sigCancel()

		healthLogger := l.With(zap.Uint16("port", config.HealthPort))
		healthLogger.Info(ctx, "Health server starting")

		err := healthServer.ListenAndServe()
		switch {
		case errors.Is(err, http.ErrServerClosed):
			healthLogger.Info(ctx, "Health server closed successfully")
		case err != nil:
			exitCode.Add(1)
			healthLogger.Error(ctx, "Health server encountered error", zap.Error(err))
		default:
			healthLogger.Error(ctx, "Health server exited without error")
		}
	})

	// Service gracefully shutdown flow
	//
	// When service shut-downs we need to info all services that depends on us gracefully shutting down existing connections.
	// Shutdown phase starts with marking sandbox traffic as draining.
	// After that we will wait some time so all dependent services will recognize that we are draining and will stop sending new requests.
	// Following phase marks the service as unhealthy, we are waiting for some time to let dependent services recognize new state.
	// After some wait proxy server is closed with followed close of health server and calling all registered closers.
	wg.Go(func() {
		<-signalCtx.Done()

		shutdownLogger := l.With(zap.Uint16("proxy_port", config.ProxyPort), zap.Uint16("health_port", config.HealthPort))
		shutdownLogger.Info(ctx, "Shutting down proxy")

		info.SetStatus(ctx, internal.Draining)

		// We should wait for health check manager to notice that we are not ready for new traffic
		shutdownLogger.Info(ctx, "Waiting for draining state propagation", zap.Float64("wait_in_seconds", shutdownDrainingWait.Seconds()))
		time.Sleep(shutdownDrainingWait)

		proxyShutdownCtx, proxyShutdownCtxCancel := context.WithTimeout(ctx, 24*time.Hour)
		defer proxyShutdownCtxCancel()

		// Gracefully shutdown the proxy http server
		err := trafficProxy.Shutdown(proxyShutdownCtx)
		if err != nil {
			exitCode.Add(1)
			shutdownLogger.Error(ctx, "Http proxy shutdown error", zap.Error(err))
		} else {
			shutdownLogger.Info(ctx, "Http proxy shutdown successfully")
		}

		info.SetStatus(ctx, internal.Unhealthy)

		// Wait for the health check manager to notice that we are not healthy at all
		shutdownLogger.Info(ctx, "Waiting for unhealthy state propagation", zap.Float64("wait_in_seconds", shutdownUnhealthyWait.Seconds()))
		time.Sleep(shutdownUnhealthyWait)

		// Gracefully shutdown the health server
		healthShutdownCtx, healthShutdownCtxCancel := context.WithTimeout(ctx, 5*time.Second)
		defer healthShutdownCtxCancel()

		err = healthServer.Shutdown(healthShutdownCtx)
		if err != nil {
			exitCode.Add(1)
			shutdownLogger.Error(ctx, "Health server shutdown error", zap.Error(err))
		} else {
			shutdownLogger.Info(ctx, "Health server shutdown successfully")
		}

		closeCtx, cancelCloseCtx := context.WithCancel(context.Background())
		defer cancelCloseCtx()

		// Close all resources that needs to be closed gracefully
		for _, c := range closers {
			logger.L().Info(ctx, fmt.Sprintf("Closing %T", c))
			if err := c.Close(closeCtx); err != nil { //nolint:contextcheck // TODO: fix this later
				logger.L().Error(ctx, "error during shutdown", zap.Error(err))
			}
		}
	})

	wg.Wait()

	return int(exitCode.Load())
}

func main() {
	// Exit, with appropriate code.
	os.Exit(run())
}
