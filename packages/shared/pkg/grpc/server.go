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
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// The connection age applied when WithMaxConnectionAge is not passed.
//
// Every connection rotates, so a client finds replicas added after it
// connected and a transport-security change reaches long-lived connections
// within minutes. The grace exceeds every unary deadline in use, so a call
// in flight at rotation completes; a stream still open when age plus grace
// elapses is cut. A server whose streams may outlive that, or that must never
// rotate, passes WithMaxConnectionAge(0, 0).
const (
	DefaultMaxConnectionAge      = 5 * time.Minute
	DefaultMaxConnectionAgeGrace = 5 * time.Minute
)

// ServerOption configures NewGRPCServer.
type ServerOption func(*serverOptions)

type serverOptions struct {
	maxMessageSize              *int
	withSandboxMetricAttributes bool
	recoveryHandler             recovery.RecoveryHandlerFunc
	unaryDeadline               grpc.UnaryServerInterceptor
	unaryInterceptors           []grpc.UnaryServerInterceptor
	streamInterceptors          []grpc.StreamServerInterceptor
	creds                       credentials.TransportCredentials
	maxConnectionAge            time.Duration
	maxConnectionAgeGrace       time.Duration
	maxConnectionAgeSet         bool
	waitForHandlers             bool
}

// connectionAge returns the age and grace to apply: the explicit values when
// WithMaxConnectionAge was passed, the defaults otherwise.
func (o serverOptions) connectionAge() (time.Duration, time.Duration) {
	if o.maxConnectionAgeSet {
		return o.maxConnectionAge, o.maxConnectionAgeGrace
	}

	return DefaultMaxConnectionAge, DefaultMaxConnectionAgeGrace
}

// WithMaxMessageSize sets both send and receive message limits in bytes.
func WithMaxMessageSize(size int) ServerOption {
	return func(o *serverOptions) { o.maxMessageSize = &size }
}

// WithSandboxMetricAttributes adds the sandbox.resume and team.id attributes
// to otelgrpc metrics, read from incoming gRPC metadata.
func WithSandboxMetricAttributes() ServerOption {
	return func(o *serverOptions) { o.withSandboxMetricAttributes = true }
}

// WithRecoveryHandler configures the unary panic recovery handler.
func WithRecoveryHandler(handler recovery.RecoveryHandlerFunc) ServerOption {
	return func(o *serverOptions) { o.recoveryHandler = handler }
}

// WithUnaryInterceptors appends interceptors after logging, recovery, and the
// unary deadline, so they run with a bounded context and inside the recovery.
func WithUnaryInterceptors(interceptors ...grpc.UnaryServerInterceptor) ServerOption {
	return func(o *serverOptions) { o.unaryInterceptors = append(o.unaryInterceptors, interceptors...) }
}

// WithStreamInterceptors appends interceptors after the logging interceptor,
// so streams see the ordering WithUnaryInterceptors promises for unary calls.
func WithStreamInterceptors(interceptors ...grpc.StreamServerInterceptor) ServerOption {
	return func(o *serverOptions) { o.streamInterceptors = append(o.streamInterceptors, interceptors...) }
}

// WithTransportCredentials sets the server's transport credentials. Without
// it the server speaks plaintext, as before.
func WithTransportCredentials(creds credentials.TransportCredentials) ServerOption {
	return func(o *serverOptions) { o.creds = creds }
}

// WithWaitForHandlers makes Stop, including the forced fallback of
// GracefulStopWithTimeout, return only after every method handler has
// returned. Stop still closes connections and cancels handler contexts first.
// Without it, Stop can return while handlers run, so a caller that closes
// their dependencies afterwards races them.
func WithWaitForHandlers() ServerOption {
	return func(o *serverOptions) { o.waitForHandlers = true }
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
// option DefaultMaxConnectionAge and DefaultMaxConnectionAgeGrace apply;
// zero for both removes the age limit.
func WithMaxConnectionAge(age, grace time.Duration) ServerOption {
	return func(o *serverOptions) {
		o.maxConnectionAge = age
		o.maxConnectionAgeGrace = grace
		o.maxConnectionAgeSet = true
	}
}

func NewGRPCServer(tel *telemetry.Client, opts ...ServerOption) *grpc.Server {
	var cfg serverOptions
	for _, o := range opts {
		o(&cfg)
	}

	callEvents := []logging.LoggableEvent{logging.StartCall, logging.FinishCall}
	logOpts := []logging.Option{
		// One line per call: FinishCall carries the method, status code and duration.
		logging.WithLogOnEvents(logging.FinishCall),
		logging.WithLevels(logging.DefaultServerCodeToLevel),
	}
	// A stream logs a payload event per message, so one call carrying
	// thousands of messages writes thousands of lines, enough to flood the
	// node's log pipeline. Streams log their start and finish only.
	streamLogOpts := []logging.Option{
		logging.WithLogOnEvents(callEvents...),
		logging.WithLevels(logging.DefaultServerCodeToLevel),
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
	if cfg.withSandboxMetricAttributes {
		otelOpts = append(otelOpts, otelgrpc.WithMetricAttributesFn(extractSandboxMetricAttrs))
	}

	var recoveryOpts []recovery.Option
	if cfg.recoveryHandler != nil {
		recoveryOpts = append(recoveryOpts, recovery.WithRecoveryHandler(cfg.recoveryHandler))
	}

	// Recovery runs inside logging, so a recovered panic still logs its finished call.
	unaryInterceptors := []grpc.UnaryServerInterceptor{
		selector.UnaryServerInterceptor(
			logging.UnaryServerInterceptor(logger.GRPCLogger(logger.L()), logOpts...),
			ignoredLoggingRoutes,
		),
		recovery.UnaryServerInterceptor(recoveryOpts...),
	}
	if cfg.unaryDeadline != nil {
		unaryInterceptors = append(unaryInterceptors, cfg.unaryDeadline)
	}
	unaryInterceptors = append(unaryInterceptors, cfg.unaryInterceptors...)

	streamInterceptors := []grpc.StreamServerInterceptor{
		selector.StreamServerInterceptor(
			logging.StreamServerInterceptor(logger.GRPCLogger(logger.L()), streamLogOpts...),
			ignoredLoggingRoutes,
		),
	}
	streamInterceptors = append(streamInterceptors, cfg.streamInterceptors...)

	age, grace := cfg.connectionAge()
	serverOpts := []grpc.ServerOption{
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             5 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			// Zero age and grace mean no age limit, gRPC's own default.
			MaxConnectionAge:      age,
			MaxConnectionAgeGrace: grace,
			Time:                  15 * time.Second,
			Timeout:               5 * time.Second,
		}),
		grpc.StatsHandler(
			NewStatsWrapper(
				otelgrpc.NewServerHandler(otelOpts...))),
		grpc.ChainUnaryInterceptor(unaryInterceptors...),
		grpc.ChainStreamInterceptor(streamInterceptors...),
	}
	if cfg.maxMessageSize != nil {
		serverOpts = append(serverOpts, grpc.MaxRecvMsgSize(*cfg.maxMessageSize), grpc.MaxSendMsgSize(*cfg.maxMessageSize))
	}
	if cfg.creds != nil {
		serverOpts = append(serverOpts, grpc.Creds(cfg.creds))
	}
	if cfg.waitForHandlers {
		serverOpts = append(serverOpts, grpc.WaitForHandlers(true))
	}

	return grpc.NewServer(serverOpts...)
}

// extractSandboxMetricAttrs reads sandbox.resume and team.id from gRPC
// metadata set by the API client. Called by otelgrpc during TagRPC — before the
// request payload is deserialized — so we use metadata instead of the payload.
func extractSandboxMetricAttrs(ctx context.Context) []attribute.KeyValue {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil
	}

	return sandboxMetricAttrs(md)
}

// sandboxMetricAttrs maps the sandbox metadata keys to metric attributes. An
// absent key adds nothing, so a call without them stays an unlabelled series.
func sandboxMetricAttrs(md metadata.MD) []attribute.KeyValue {
	var attrs []attribute.KeyValue
	if values := md.Get(IsResumeMetadataKey); len(values) > 0 {
		attrs = append(attrs, attribute.Bool("sandbox.resume", values[0] == "true"))
	}
	if values := md.Get(TeamIDMetadataKey); len(values) > 0 && values[0] != "" {
		attrs = append(attrs, telemetry.WithTeamID(values[0]))
	}

	return attrs
}
