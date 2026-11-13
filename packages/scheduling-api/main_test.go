package main

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/e2b-dev/infra/packages/scheduling-api/internal/config"
	"github.com/e2b-dev/infra/packages/shared/pkg/mtls"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

func TestServe(t *testing.T) {
	t.Parallel()
	cfg := config.Config{TLS: mtls.ListenerSettings{Mode: "off"}, GRPCAddress: freeAddress(t)}
	ctx, stop := context.WithCancel(t.Context())
	defer stop()
	served := make(chan error, 1)
	go func() { served <- serve(ctx, cfg, telemetry.NewNoopClient(), 2*time.Second) }()

	conn, err := grpc.NewClient(cfg.GRPCAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	healthClient := healthv1.NewHealthClient(conn)
	healthStatus := func(service string) healthv1.HealthCheckResponse_ServingStatus {
		response, err := healthClient.Check(t.Context(), &healthv1.HealthCheckRequest{Service: service}, grpc.WaitForReady(true))
		require.NoError(t, err)

		return response.GetStatus()
	}
	require.Equal(t, healthv1.HealthCheckResponse_SERVING, healthStatus(""))

	err = conn.Invoke(t.Context(), "/test.Missing/Missing", &emptypb.Empty{}, &emptypb.Empty{})
	require.Equal(t, codes.Unimplemented, status.Code(err))

	stop()
	// Health is withdrawn first while the listener keeps answering.
	require.Eventually(t, func() bool {
		return healthStatus("") == healthv1.HealthCheckResponse_NOT_SERVING
	}, time.Second, 10*time.Millisecond)

	select {
	case err := <-served:
		require.NoError(t, err)
	case <-time.After(shutdownTimeout + 5*time.Second):
		require.Fail(t, "serve did not return after shutdown")
	}
}

func TestServeListenerInUse(t *testing.T) {
	t.Parallel()
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, listener.Close()) })

	err = serve(t.Context(), config.Config{TLS: mtls.ListenerSettings{Mode: "off"}, GRPCAddress: listener.Addr().String()}, telemetry.NewNoopClient(), 0)
	require.ErrorContains(t, err, "gRPC listener initialization failed")
}

func freeAddress(t *testing.T) string {
	t.Helper()
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := listener.Addr().String()
	require.NoError(t, listener.Close())

	return address
}
