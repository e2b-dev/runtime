package grpc

import (
	"context"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/recovery"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/selector"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// ServerOption configures NewGRPCServer.
type ServerOption func(*serverOptions)

type serverOptions struct {
	maxMessageSize           *int
	withSandboxResumeMetrics bool
	withoutPayloadLogging    bool
	recoveryHandler          recovery.RecoveryHandlerFunc
	unaryDeadline            grpc.UnaryServerInterceptor
	unaryInterceptors        []grpc.UnaryServerInterceptor
	maxConnectionAge         time.Duration
	maxConnectionAgeGrace    time.Duration
}

// WithMaxMessageSize sets both send and receive message limits in bytes.
func WithMaxMessageSize(size int) ServerOption {
	return func(o *serverOptions) { o.maxMessageSize = &size }
}

// WithSandboxResumeMetrics adds sandbox.resume attribute to otelgrpc metrics,
// read from incoming gRPC metadata.
func WithSandboxResumeMetrics() ServerOption {
	return func(o *serverOptions) { o.withSandboxResumeMetrics = true }
}

// WithRecoveryHandler configures the unary panic recovery handler.
func WithRecoveryHandler(handler recovery.RecoveryHandlerFunc) ServerOption {
	return func(o *serverOptions) { o.recoveryHandler = handler }
}

// WithoutPayloadLogging omits request and response payloads from server logs.
func WithoutPayloadLogging() ServerOption {
	return func(o *serverOptions) { o.withoutPayloadLogging = true }
}

// WithUnaryInterceptors appends interceptors after recovery, logging, and the
// unary deadline, so they run with a bounded context and inside the recovery.
func WithUnaryInterceptors(interceptors ...grpc.UnaryServerInterceptor) ServerOption {
	return func(o *serverOptions) { o.unaryInterceptors = append(o.unaryInterceptors, interceptors...) }
}

// WithUnaryDeadline bounds unary requests while preserving an earlier caller deadline.
func WithUnaryDeadline(timeout time.Duration) ServerOption {
	return func(o *serverOptions) {
		o.unaryDeadline = func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			ctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()

			return handler(ctx, req)
		}
	}
}

// WithMaxConnectionAge gracefully closes every connection after about age,
// which gRPC jitters by up to 10%, and forcibly closes it grace later. A client
// that resolves its target again when a connection closes uses the rotation to
// find servers added after it connected. Calls in flight at rotation keep
// running on the old connection until they finish or grace ends. Without this
// option connections have no age limit.
func WithMaxConnectionAge(age, grace time.Duration) ServerOption {
	return func(o *serverOptions) {
		o.maxConnectionAge = age
		o.maxConnectionAgeGrace = grace
	}
}

func NewGRPCServer(tel *telemetry.Client, opts ...ServerOption) *grpc.Server {
	var cfg serverOptions
	for _, o := range opts {
		o(&cfg)
	}

	logEvents := []logging.LoggableEvent{logging.StartCall, logging.FinishCall}
	if !cfg.withoutPayloadLogging {
		logEvents = append(logEvents, logging.PayloadReceived, logging.PayloadSent)
	}
	logOpts := []logging.Option{
		logging.WithLogOnEvents(logEvents...),
		logging.WithLevels(logging.DefaultServerCodeToLevel),
		logging.WithFieldsFromContext(logging.ExtractFields),
	}

	ignoredLoggingRoutes := logger.WithoutRoutes(
		logger.HealthCheckRoute,
		"/TemplateService/TemplateBuildStatus",
		"/TemplateService/HealthStatus",
		"/InfoService/ServiceInfo",
	)

	otelOpts := []otelgrpc.Option{
		otelgrpc.WithTracerProvider(tel.TracerProvider),
		otelgrpc.WithMeterProvider(tel.MeterProvider),
	}
	if cfg.withSandboxResumeMetrics {
		otelOpts = append(otelOpts, otelgrpc.WithMetricAttributesFn(extractSandboxResumeAttrs))
	}

	var recoveryOpts []recovery.Option
	if cfg.recoveryHandler != nil {
		recoveryOpts = append(recoveryOpts, recovery.WithRecoveryHandler(cfg.recoveryHandler))
	}

	unaryInterceptors := []grpc.UnaryServerInterceptor{
		recovery.UnaryServerInterceptor(recoveryOpts...),
		selector.UnaryServerInterceptor(
			logging.UnaryServerInterceptor(logger.GRPCLogger(logger.L()), logOpts...),
			ignoredLoggingRoutes,
		),
	}
	if cfg.unaryDeadline != nil {
		unaryInterceptors = append(unaryInterceptors, cfg.unaryDeadline)
	}
	unaryInterceptors = append(unaryInterceptors, cfg.unaryInterceptors...)

	serverOpts := []grpc.ServerOption{
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             5 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			// Zero age and grace are gRPC's defaults: no age limit.
			MaxConnectionAge:      cfg.maxConnectionAge,
			MaxConnectionAgeGrace: cfg.maxConnectionAgeGrace,
			Time:                  15 * time.Second,
			Timeout:               5 * time.Second,
		}),
		grpc.StatsHandler(
			NewStatsWrapper(
				otelgrpc.NewServerHandler(otelOpts...))),
		grpc.ChainUnaryInterceptor(unaryInterceptors...),
		grpc.ChainStreamInterceptor(
			selector.StreamServerInterceptor(
				logging.StreamServerInterceptor(logger.GRPCLogger(logger.L()), logOpts...),
				ignoredLoggingRoutes,
			),
		),
	}
	if cfg.maxMessageSize != nil {
		serverOpts = append(serverOpts, grpc.MaxRecvMsgSize(*cfg.maxMessageSize), grpc.MaxSendMsgSize(*cfg.maxMessageSize))
	}

	return grpc.NewServer(serverOpts...)
}

// extractSandboxResumeAttrs reads sandbox.resume from gRPC metadata set by the
// API client. Called by otelgrpc during TagRPC — before the request payload is
// deserialized — so we use metadata instead of the payload.
func extractSandboxResumeAttrs(ctx context.Context) []attribute.KeyValue {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil
	}

	values := md.Get(IsResumeMetadataKey)
	if len(values) == 0 {
		return nil
	}

	return []attribute.KeyValue{
		attribute.Bool("sandbox.resume", values[0] == "true"),
	}
}
