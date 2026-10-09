package grpc

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"math/big"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	proxygrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/proxy"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

const panicSentinel = "panic-sentinel-not-for-callers-4b7e"

const fixedPanicMessage = "internal error"

type panickingService struct {
	proxygrpc.UnimplementedSandboxServiceServer
}

func (panickingService) ResumeSandbox(context.Context, *proxygrpc.SandboxResumeRequest) (*proxygrpc.SandboxResumeResponse, error) {
	panic(panicSentinel)
}

type respondingService struct {
	proxygrpc.UnimplementedSandboxServiceServer
}

func (respondingService) ResumeSandbox(context.Context, *proxygrpc.SandboxResumeRequest) (*proxygrpc.SandboxResumeResponse, error) {
	return &proxygrpc.SandboxResumeResponse{}, nil
}

//nolint:paralleltest // the middleware logs through the global logger, which this test replaces
func TestNewGRPCServerRecoveryHandler(t *testing.T) {
	logs := captureLogs(t)

	client := serveSandboxService(t, panickingService{}, WithRecoveryHandler(func(any) error {
		return status.Error(codes.Internal, fixedPanicMessage)
	}))

	_, err := client.ResumeSandbox(t.Context(), &proxygrpc.SandboxResumeRequest{})

	require.Equal(t, codes.Internal, status.Code(err))
	require.Equal(t, fixedPanicMessage, status.Convert(err).Message())
	require.NotContains(t, err.Error(), panicSentinel)
	require.NotContains(t, flattenLogs(logs), panicSentinel)
}

//nolint:paralleltest // the middleware logs through the global logger, which this test replaces
func TestNewGRPCServerWithoutPayloadLogging(t *testing.T) {
	logs := captureLogs(t)
	client := serveSandboxService(t, respondingService{}, WithoutPayloadLogging())

	_, err := client.ResumeSandbox(t.Context(), &proxygrpc.SandboxResumeRequest{SandboxId: panicSentinel})

	require.NoError(t, err)
	require.NotContains(t, flattenLogs(logs), panicSentinel)
}

//nolint:paralleltest // the middleware logs through the global logger, which this test replaces
func TestNewGRPCServerWithUnaryInterceptors(t *testing.T) {
	captureLogs(t)
	rejected := status.Error(codes.InvalidArgument, "rejected by interceptor")
	client := serveSandboxService(t, respondingService{}, WithUnaryInterceptors(
		func(context.Context, any, *grpc.UnaryServerInfo, grpc.UnaryHandler) (any, error) {
			return nil, rejected
		},
	))

	_, err := client.ResumeSandbox(t.Context(), &proxygrpc.SandboxResumeRequest{})

	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Equal(t, "rejected by interceptor", status.Convert(err).Message())
}

// heldService holds every call until the test releases it or the call ends,
// and counts how many times its application code ran.
type heldService struct {
	proxygrpc.UnimplementedSandboxServiceServer

	entered chan struct{}
	release chan struct{}
	calls   atomic.Int32
}

func (s *heldService) ResumeSandbox(ctx context.Context, _ *proxygrpc.SandboxResumeRequest) (*proxygrpc.SandboxResumeResponse, error) {
	s.calls.Add(1)
	s.entered <- struct{}{}

	select {
	case <-s.release:
		return &proxygrpc.SandboxResumeResponse{}, nil
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
}

func TestNewGRPCServerWithMaxConnectionAge(t *testing.T) {
	t.Parallel()

	// The age is short so the connection rotates during the held call. The
	// grace is long so only the rotation, never the forced close, is exercised.
	service := &heldService{entered: make(chan struct{}, 2), release: make(chan struct{})}
	server, conn := startSandboxServer(t, service, WithMaxConnectionAge(100*time.Millisecond, time.Minute))
	client := proxygrpc.NewSandboxServiceClient(conn)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	held := make(chan error, 1)
	go func() {
		_, err := client.ResumeSandbox(ctx, &proxygrpc.SandboxResumeRequest{})
		held <- err
	}()
	receive(ctx, t, service.entered)

	// GOAWAY takes the connection out of service while the call still runs on it.
	for conn.GetState() == connectivity.Ready {
		require.True(t, conn.WaitForStateChange(ctx, connectivity.Ready), "the aged connection was never closed")
	}

	service.release <- struct{}{}
	require.NoError(t, receive(ctx, t, held), "a call in flight at rotation completes")
	require.EqualValues(t, 1, service.calls.Load(), "rotation does not run the call again")

	// A new call opens a fresh connection. Shutdown with that call still held
	// falls back to a forced stop within its timeout instead of hanging.
	go func() {
		_, err := client.ResumeSandbox(ctx, &proxygrpc.SandboxResumeRequest{})
		held <- err
	}()
	receive(ctx, t, service.entered)

	start := time.Now()
	require.False(t, GracefulStopWithTimeout(server, 200*time.Millisecond), "the held call prevents a graceful stop")
	require.Less(t, time.Since(start), 5*time.Second)
	require.Equal(t, codes.Unavailable, status.Code(receive(ctx, t, held)))
	require.EqualValues(t, 2, service.calls.Load())
}

// cleanupService keeps running after its call is cancelled, until the test
// releases it, as a handler still releasing a database session would.
type cleanupService struct {
	proxygrpc.UnimplementedSandboxServiceServer

	entered   chan struct{}
	cancelled chan struct{}
	release   chan struct{}
	returned  atomic.Bool
}

func (s *cleanupService) ResumeSandbox(ctx context.Context, _ *proxygrpc.SandboxResumeRequest) (*proxygrpc.SandboxResumeResponse, error) {
	s.entered <- struct{}{}
	<-ctx.Done()
	s.cancelled <- struct{}{}
	<-s.release
	s.returned.Store(true)

	return nil, status.FromContextError(ctx.Err()).Err()
}

func TestNewGRPCServerWithWaitForHandlers(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		stop func(*testing.T, *grpc.Server)
	}{
		{"Stop", func(_ *testing.T, server *grpc.Server) { server.Stop() }},
		{"GracefulStopWithTimeout fallback", func(t *testing.T, server *grpc.Server) {
			t.Helper()
			assert.False(t, GracefulStopWithTimeout(server, 50*time.Millisecond), "the held call prevents a graceful stop")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			service := &cleanupService{entered: make(chan struct{}, 1), cancelled: make(chan struct{}, 1), release: make(chan struct{})}
			server, conn := startSandboxServer(t, service, WithWaitForHandlers())
			// Runs before the server's cleanup, so a failed run cannot hang it.
			release := sync.OnceFunc(func() { close(service.release) })
			t.Cleanup(release)
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			go func() {
				_, _ = proxygrpc.NewSandboxServiceClient(conn).ResumeSandbox(ctx, &proxygrpc.SandboxResumeRequest{})
			}()
			receive(ctx, t, service.entered)

			stopped := make(chan struct{})
			go func() {
				tc.stop(t, server)
				close(stopped)
			}()
			// The stop cancels the admitted handler, which then holds on
			// until released. The window only gives an early return a
			// chance to show. A passing run cannot depend on it.
			receive(ctx, t, service.cancelled)
			require.Never(t, func() bool {
				select {
				case <-stopped:
					return true
				default:
					return false
				}
			}, 100*time.Millisecond, 5*time.Millisecond, "the stop returned while the cancelled handler was still running")
			release()
			receive(ctx, t, stopped)
			require.True(t, service.returned.Load())
		})
	}
}

func receive[T any](ctx context.Context, t *testing.T, values <-chan T) T {
	t.Helper()

	select {
	case value := <-values:
		return value
	case <-ctx.Done():
		t.Fatal("timed out waiting for the server")

		var zero T

		return zero
	}
}

func serveSandboxService(t *testing.T, service proxygrpc.SandboxServiceServer, opts ...ServerOption) proxygrpc.SandboxServiceClient {
	t.Helper()

	_, conn := startSandboxServer(t, service, opts...)

	return proxygrpc.NewSandboxServiceClient(conn)
}

func startSandboxServer(t *testing.T, service proxygrpc.SandboxServiceServer, opts ...ServerOption) (*grpc.Server, *grpc.ClientConn) {
	t.Helper()

	server := NewGRPCServer(&telemetry.Client{
		TracerProvider: tracenoop.NewTracerProvider(),
		MeterProvider:  metricnoop.NewMeterProvider(),
	}, opts...)
	proxygrpc.RegisterSandboxServiceServer(server, service)

	var listenConfig net.ListenConfig
	listener, err := listenConfig.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	conn, err := grpc.NewClient(listener.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	return server, conn
}

func captureLogs(t *testing.T) *observer.ObservedLogs {
	t.Helper()

	core, logs := observer.New(zap.DebugLevel)
	restore := logger.ReplaceGlobals(t.Context(), logger.NewTracedLoggerFromCore(core))
	t.Cleanup(restore)

	return logs
}

func flattenLogs(logs *observer.ObservedLogs) string {
	var out strings.Builder
	for _, entry := range logs.All() {
		out.WriteString(entry.Message)

		for key, value := range entry.ContextMap() {
			out.WriteString(key)
			fmt.Fprint(&out, value)
		}
	}

	return out.String()
}

func noopTelemetry() *telemetry.Client {
	return &telemetry.Client{
		TracerProvider: tracenoop.NewTracerProvider(),
		MeterProvider:  metricnoop.NewMeterProvider(),
	}
}

// serve listens on a loopback port, serves server there until the test ends
// and returns the address.
func serve(t *testing.T, server *grpc.Server) string {
	t.Helper()

	var listenConfig net.ListenConfig
	listener, err := listenConfig.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	return listener.Addr().String()
}

// startHealthServer builds a server with the gRPC health service and a
// plaintext client to it.
func startHealthServer(t *testing.T, opts ...ServerOption) *grpc.ClientConn {
	t.Helper()

	server := NewGRPCServer(noopTelemetry(), opts...)
	healthpb.RegisterHealthServer(server, health.NewServer())
	addr := serve(t, server)

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	return conn
}

func TestServerOptionsConnectionAge(t *testing.T) {
	t.Parallel()

	var defaults serverOptions
	age, grace := defaults.connectionAge()
	require.Equal(t, DefaultMaxConnectionAge, age)
	require.Equal(t, DefaultMaxConnectionAgeGrace, grace)
	require.Greater(t, DefaultMaxConnectionAgeGrace, 70*time.Second, "longer than the longest unary deadline in use")

	var explicit serverOptions
	WithMaxConnectionAge(5*time.Minute, time.Minute)(&explicit)
	age, grace = explicit.connectionAge()
	require.Equal(t, 5*time.Minute, age)
	require.Equal(t, time.Minute, grace)

	var unlimited serverOptions
	WithMaxConnectionAge(0, 0)(&unlimited)
	age, grace = unlimited.connectionAge()
	require.Zero(t, age, "zero for both is the explicit opt-out")
	require.Zero(t, grace)
}

func TestNewGRPCServerWithStreamInterceptors(t *testing.T) {
	t.Parallel()

	rejected := status.Error(codes.FailedPrecondition, "rejected by stream interceptor")
	var unaryCalls atomic.Int32
	conn := startHealthServer(t,
		WithStreamInterceptors(func(any, grpc.ServerStream, *grpc.StreamServerInfo, grpc.StreamHandler) error {
			return rejected
		}),
		WithUnaryInterceptors(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			unaryCalls.Add(1)

			return handler(ctx, req)
		}),
	)
	client := healthpb.NewHealthClient(conn)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	_, err := client.Check(ctx, &healthpb.HealthCheckRequest{})
	require.NoError(t, err, "unary RPCs are untouched by a stream interceptor")
	require.EqualValues(t, 1, unaryCalls.Load())

	watch, err := client.Watch(ctx, &healthpb.HealthCheckRequest{})
	require.NoError(t, err)
	_, err = watch.Recv()
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Equal(t, "rejected by stream interceptor", status.Convert(err).Message())
}

// selfSignedServer mints a certificate for localhost and the pool that trusts
// it, so the credentials test needs no certificate authority.
func selfSignedServer(t *testing.T) (tls.Certificate, *x509.CertPool) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "localhost"},
		DNSNames:              []string{"localhost"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	roots := x509.NewCertPool()
	roots.AddCert(cert)

	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: cert}, roots
}

func TestNewGRPCServerWithTransportCredentials(t *testing.T) {
	t.Parallel()

	cert, roots := selfSignedServer(t)
	server := NewGRPCServer(noopTelemetry(), WithTransportCredentials(credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS13,
	})))
	healthpb.RegisterHealthServer(server, health.NewServer())
	addr := serve(t, server)

	secure, err := grpc.NewClient(addr, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
		RootCAs:    roots,
		ServerName: "localhost",
		MinVersion: tls.VersionTLS13,
	})))
	require.NoError(t, err)
	t.Cleanup(func() { _ = secure.Close() })

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	_, err = healthpb.NewHealthClient(secure).Check(ctx, &healthpb.HealthCheckRequest{})
	require.NoError(t, err, "a TLS client reaches a server built with credentials")

	plain, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = plain.Close() })
	shortCtx, shortCancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer shortCancel()
	_, err = healthpb.NewHealthClient(plain).Check(shortCtx, &healthpb.HealthCheckRequest{})
	require.Error(t, err, "a plaintext client cannot complete the server's TLS handshake")
}
