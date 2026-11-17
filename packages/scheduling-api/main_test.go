package main

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/e2b-dev/infra/packages/scheduling-api/internal/config"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/scheduling"
	"github.com/e2b-dev/infra/packages/shared/pkg/mtls"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

const hangTimeout = 30 * time.Second

var reservationServiceName = scheduling.ReservationService_ServiceDesc.ServiceName

func TestServe(t *testing.T) {
	t.Parallel()
	redisClient := redis_utils.SetupInstance(t)
	cfg := config.Config{TLS: mtls.ListenerSettings{Mode: "off"}, GRPCAddress: freeAddress(t)}
	ctx, stop := context.WithCancel(t.Context())
	defer stop()
	served := make(chan error, 1)
	go func() { served <- serve(ctx, cfg, telemetry.NewNoopClient(), 2*time.Second, redisClient) }()

	conn, err := grpc.NewClient(cfg.GRPCAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	callCtx, cancelCalls := context.WithTimeout(t.Context(), hangTimeout)
	defer cancelCalls()

	// Watches, not Check polling: the result must not depend on the drain's length.
	healthClient := healthv1.NewHealthClient(conn)
	watches := map[string]healthv1.Health_WatchClient{}
	for _, service := range []string{"", reservationServiceName} {
		watch, err := healthClient.Watch(callCtx, &healthv1.HealthCheckRequest{Service: service}, grpc.WaitForReady(true))
		require.NoError(t, err)
		response, err := watch.Recv()
		require.NoError(t, err)
		require.Equal(t, healthv1.HealthCheckResponse_SERVING, response.GetStatus(), service)
		watches[service] = watch
	}

	client := scheduling.NewReservationServiceClient(conn)
	team, sandbox := uuid.NewString(), "sbx-"+uuid.NewString()
	reserved, err := client.Reserve(callCtx, scheduling.ReserveRequest_builder{TeamId: team, SandboxId: sandbox, Limit: 10}.Build())
	require.NoError(t, err)
	require.Equal(t, scheduling.ReserveOutcome_RESERVE_OUTCOME_RESERVED, reserved.GetOutcome())
	_, err = client.FinishStart(callCtx, scheduling.FinishStartRequest_builder{TeamId: team, SandboxId: sandbox, Result: []byte("result")}.Build())
	require.NoError(t, err)
	waited, err := client.WaitForStart(callCtx, scheduling.WaitForStartRequest_builder{TeamId: team, SandboxId: sandbox}.Build())
	require.NoError(t, err)
	require.Equal(t, scheduling.WaitForStartOutcome_WAIT_FOR_START_OUTCOME_FINISHED, waited.GetOutcome())
	require.Equal(t, []byte("result"), waited.GetResult())

	stop()
	for service, watch := range watches {
		response, err := watch.Recv()
		require.NoError(t, err, service)
		assert.Equal(t, healthv1.HealthCheckResponse_NOT_SERVING, response.GetStatus(), service)
	}
	cancelCalls()

	select {
	case err := <-served:
		require.NoError(t, err)
	case <-time.After(shutdownTimeout + hangTimeout):
		require.Fail(t, "serve did not return after shutdown")
	}
}

func TestServeListenerInUse(t *testing.T) {
	t.Parallel()
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, listener.Close()) })

	err = serve(t.Context(), config.Config{TLS: mtls.ListenerSettings{Mode: "off"}, GRPCAddress: listener.Addr().String()}, telemetry.NewNoopClient(), 0, nil)
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
