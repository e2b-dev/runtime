// Command scheduling-api serves the scheduling gRPC API.
package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/exaring/otelpgx"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/e2b-dev/infra/packages/scheduling-api/internal/cleanup"
	"github.com/e2b-dev/infra/packages/scheduling-api/internal/config"
	"github.com/e2b-dev/infra/packages/scheduling-api/internal/server"
	"github.com/e2b-dev/infra/packages/scheduling-api/internal/store"
	"github.com/e2b-dev/infra/packages/shared/pkg/env"
	e2bgrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/scheduling"
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

	databasePingTimeout = 10 * time.Second
)

var (
	commitSHA string
	version   = "0.0.0" // x-release-please-version
)

var (
	meter = otel.Meter("github.com/e2b-dev/infra/packages/scheduling-api")

	statementDuration = sharedutils.Must(meter.Float64Histogram("api.scheduling.db.statement.duration",
		metric.WithDescription("Duration of the SQL statements, by pool, statement name and failure."), metric.WithUnit("s")))
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
	if err := serve(ctx, cfg, tel, drain, cleanup.DefaultConfig); err != nil {
		log.Error(ctx, serviceName+" stopped with an error", zap.Error(err))

		return 1
	}

	return 0
}

func serve(ctx context.Context, cfg config.Config, tel *telemetry.Client, drain time.Duration, cleanupConfig cleanup.Config) error {
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
	writePool, err := openDatabase(ctx, "write", cfg.DatabaseURL, cfg.WritePoolMaxConns)
	if err != nil {
		_ = listener.Close()

		return fmt.Errorf("write database initialization failed: %w", err)
	}
	defer writePool.Close()
	readPool, err := openDatabase(ctx, "read", cfg.DatabaseURL, cfg.ReadPoolMaxConns)
	if err != nil {
		_ = listener.Close()

		return fmt.Errorf("read database initialization failed: %w", err)
	}
	defer readPool.Close()
	st := store.New(writePool, readPool)
	grpcServer, healthServer, err := newServer(tel, st, tlsConfig) //nolint:contextcheck // TLS callbacks use the handshake context.
	if err != nil {
		_ = listener.Close()

		return fmt.Errorf("gRPC server initialization failed: %w", err)
	}
	cleanupConnConfig := writePool.Config().ConnConfig
	cleanupConnConfig.Tracer = statementTimer{pool: attribute.String("pool.name", "cleanup")}
	worker, err := cleanup.New(st, cleanupConnConfig, cleanupConfig)
	if err != nil {
		_ = listener.Close()

		return fmt.Errorf("cleanup worker initialization failed: %w", err)
	}

	workerCtx, stopWorker := context.WithCancel(ctx)
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		worker.Run(workerCtx)
	}()
	defer func() {
		stopWorker()
		<-workerDone
	}()

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

func openDatabase(ctx context.Context, name, databaseURL string, maxConns int32) (*pgxpool.Pool, error) {
	if maxConns <= 0 {
		return nil, errors.New("pool maximum connections must be positive")
	}
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse the database URL: %w", err)
	}
	config.MaxConns = maxConns
	if config.MinConns > maxConns || config.MinIdleConns > maxConns {
		return nil, fmt.Errorf("pool minimum connections exceed maximum %d", maxConns)
	}
	config.ConnConfig.RuntimeParams["synchronous_commit"] = "on"
	config.ConnConfig.Tracer = statementTimer{pool: attribute.String("pool.name", name)}
	config.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		var writablePrimary bool
		if err := conn.QueryRow(ctx, `SELECT NOT pg_is_in_recovery() AND NOT current_setting('transaction_read_only')::boolean`).Scan(&writablePrimary); err != nil {
			return fmt.Errorf("check the database primary: %w", err)
		}
		if !writablePrimary {
			return errors.New("the database must be a writable primary")
		}

		return nil
	}
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("create the pool: %w", err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, databasePingTimeout)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()

		return nil, fmt.Errorf("ping the database: %w", err)
	}
	if err := otelpgx.RecordStats(pool, otelpgx.WithStatsAttributes(attribute.String("pool.name", name))); err != nil {
		pool.Close()

		return nil, fmt.Errorf("record the pool statistics: %w", err)
	}

	return pool, nil
}

func newServer(tel *telemetry.Client, st *store.Store, tlsConfig mtls.ServerConfig) (*grpc.Server, *health.Server, error) {
	grpcServer, err := server.New(tel, st, tlsConfig)
	if err != nil {
		return nil, nil, err
	}
	healthServer := health.NewServer()
	healthv1.RegisterHealthServer(grpcServer, healthServer)
	healthServer.SetServingStatus(scheduling.SchedulingService_ServiceDesc.ServiceName, healthv1.HealthCheckResponse_SERVING)

	return grpcServer, healthServer, nil
}

type statementTimer struct {
	pool attribute.KeyValue
}

type statementStart struct {
	name string
	at   time.Time
}

type statementStartKey struct{}

func (t statementTimer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	return context.WithValue(ctx, statementStartKey{}, statementStart{name: statementName(data.SQL), at: time.Now()})
}

func (t statementTimer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	start, ok := ctx.Value(statementStartKey{}).(statementStart)
	if !ok {
		return
	}
	failed := data.Err != nil || (start.name == "commit" && data.CommandTag.String() == "ROLLBACK")
	statementDuration.Record(ctx, time.Since(start.at).Seconds(), metric.WithAttributes(
		t.pool, attribute.String("statement", start.name), attribute.Bool("error", failed),
	))
}

// statementName is a metric attribute: it must return a sqlc query name, a
// transaction control statement or "other", never SQL text.
func statementName(sql string) string {
	if rest, ok := strings.CutPrefix(sql, "-- name: "); ok {
		if name, _, found := strings.Cut(rest, " "); found {
			return name
		}

		return "other"
	}
	switch {
	case strings.HasPrefix(sql, "begin"):
		return "begin"
	case sql == "commit":
		return "commit"
	case sql == "rollback":
		return "rollback"
	default:
		return "other"
	}
}
