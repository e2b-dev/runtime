// Command scheduling-api serves the scheduling gRPC API.
package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/e2b-dev/infra/packages/scheduling-api/internal/config"
	"github.com/e2b-dev/infra/packages/scheduling-api/internal/server"
	"github.com/e2b-dev/infra/packages/shared/pkg/env"
	e2bgrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/mtls"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	sharedutils "github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

const (
	serviceName = "scheduling-api"

	// shutdownDrain keeps the listener serving after health is withdrawn:
	// Kubernetes signals a terminating pod while the Service still routes to
	// it. shutdownDrain plus shutdownTimeout stays inside the pod's 30 s
	// termination grace period.
	shutdownDrain   = 10 * time.Second
	shutdownTimeout = 10 * time.Second
)

var (
	commitSHA string
	version   = "0.0.0" // x-release-please-version
)

func main() {
	os.Exit(run())
}

func run() int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Parse()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", serviceName, err)

		return 1
	}

	tel, err := telemetry.New(ctx, env.GetNodeID(), serviceName, commitSHA, version, uuid.NewString())
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: telemetry initialization failed: %v\n", serviceName, err)

		return 1
	}

	log := sharedutils.Must(logger.NewLogger(logger.LoggerConfig{
		ServiceName:   serviceName,
		IsInternal:    true,
		IsDebug:       env.IsDebug(),
		Cores:         []zapcore.Core{logger.GetOTELCore(tel.LogsProvider, serviceName)},
		EnableConsole: true,
	}))
	logger.ReplaceGlobals(ctx, log)
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
		defer cancel()
		if err := tel.Shutdown(shutdownCtx); err != nil {
			log.Error(shutdownCtx, serviceName+" telemetry shutdown failed", zap.Error(err))
		}
		_ = log.Sync()
	}()

	drain := shutdownDrain
	if env.IsLocal() {
		drain = 0
	}
	log.Info(ctx, "starting "+serviceName, zap.String("commit_sha", commitSHA), zap.String("version", version))
	if err := serve(ctx, cfg, tel, drain); err != nil {
		log.Error(ctx, serviceName+" stopped with an error", zap.Error(err))

		return 1
	}

	return 0
}

func serve(ctx context.Context, cfg config.Config, tel *telemetry.Client, drain time.Duration) error {
	proc, err := mtls.NewProcess(ctx, tel.MeterProvider, nil, logger.L())
	if err != nil {
		return err
	}
	tlsConfig, err := config.ServerTLS(proc, cfg.TLS)
	if err != nil {
		return fmt.Errorf("gRPC TLS initialization failed: %w", err)
	}
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", cfg.GRPCAddress)
	if err != nil {
		return fmt.Errorf("gRPC listener initialization failed: %w", err)
	}
	grpcServer, healthServer := newServer(tel, tlsConfig)

	serveDone := make(chan error, 1)
	go func() { serveDone <- grpcServer.Serve(listener) }()
	logger.L().Info(ctx, serviceName+" gRPC listener is serving", zap.String("address", listener.Addr().String()))

	var serveErr error
	select {
	case <-ctx.Done():
	case err := <-serveDone:
		serveErr = fmt.Errorf("gRPC listener stopped unexpectedly: %w", err)
	}

	healthServer.Shutdown()
	if serveErr == nil {
		logger.L().Info(context.WithoutCancel(ctx), serviceName+" draining before shutdown", zap.Duration("drain", drain))
		timer := time.NewTimer(drain)
		select {
		case <-timer.C:
		case err := <-serveDone:
			timer.Stop()
			serveErr = fmt.Errorf("gRPC listener stopped unexpectedly: %w", err)
		}
	}
	if !e2bgrpc.GracefulStopWithTimeout(grpcServer, shutdownTimeout) {
		logger.L().Warn(context.WithoutCancel(ctx), serviceName+" gRPC listener required a forced stop")
	}
	if serveErr == nil {
		<-serveDone
	}

	return serveErr
}

func newServer(tel *telemetry.Client, tlsConfig mtls.ServerConfig) (*grpc.Server, *health.Server) {
	grpcServer := server.New(tel, tlsConfig)
	healthServer := health.NewServer()
	healthv1.RegisterHealthServer(grpcServer, healthServer)

	return grpcServer, healthServer
}
