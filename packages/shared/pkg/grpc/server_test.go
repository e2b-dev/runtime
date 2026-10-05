package grpc

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
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
