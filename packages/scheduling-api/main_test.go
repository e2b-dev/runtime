package main

import (
	"context"
	"net"
	"net/url"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/e2b-dev/infra/packages/scheduling-api/internal/cleanup"
	"github.com/e2b-dev/infra/packages/scheduling-api/internal/config"
	"github.com/e2b-dev/infra/packages/scheduling-api/internal/db/dbtest"
	"github.com/e2b-dev/infra/packages/scheduling-api/internal/store"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/scheduling"
	"github.com/e2b-dev/infra/packages/shared/pkg/mtls"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

var schedulingServiceName = scheduling.SchedulingService_ServiceDesc.ServiceName

func TestServe(t *testing.T) {
	t.Parallel()
	cfg := config.Config{TLS: mtls.ListenerSettings{Mode: "off"}, GRPCAddress: freeAddress(t), DatabaseURL: dbtest.ConnString(t, dbtest.MigratedPool(t)), WritePoolMaxConns: 2, ReadPoolMaxConns: 1}
	ctx, stop := context.WithCancel(t.Context())
	defer stop()
	served := make(chan error, 1)
	go func() { served <- serve(ctx, cfg, telemetry.NewNoopClient(), 2*time.Second, cleanup.DefaultConfig) }()

	conn, err := grpc.NewClient(cfg.GRPCAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	healthClient := healthv1.NewHealthClient(conn)
	healthStatus := func(service string) healthv1.HealthCheckResponse_ServingStatus {
		response, err := healthClient.Check(t.Context(), &healthv1.HealthCheckRequest{Service: service}, grpc.WaitForReady(true))
		require.NoError(t, err)

		return response.GetStatus()
	}
	for _, service := range []string{"", schedulingServiceName} {
		require.Equal(t, healthv1.HealthCheckResponse_SERVING, healthStatus(service), service)
	}

	err = conn.Invoke(t.Context(), "/"+schedulingServiceName+"/Missing", &emptypb.Empty{}, &emptypb.Empty{})
	require.Equal(t, codes.Unimplemented, status.Code(err))

	got, err := scheduling.NewSchedulingServiceClient(conn).Get(t.Context(), scheduling.GetRequest_builder{
		ProjectId: "6f1c2c0e-4a8e-4d4f-9a59-2f7c8c1f0b11",
		SandboxId: "i5f3k9q2m8x1c7v4b6n0p",
	}.Build())
	require.NoError(t, err)
	require.False(t, got.HasSandbox())

	stop()
	// Health is withdrawn first while the listener keeps answering.
	require.Eventually(t, func() bool {
		return healthStatus(schedulingServiceName) == healthv1.HealthCheckResponse_NOT_SERVING
	}, time.Second, 10*time.Millisecond)
	require.Equal(t, healthv1.HealthCheckResponse_NOT_SERVING, healthStatus(""))

	select {
	case err := <-served:
		require.NoError(t, err)
	case <-time.After(shutdownTimeout + 5*time.Second):
		t.Fatal("serve did not return after shutdown")
	}
}

func TestServeCleanupWithOneConnection(t *testing.T) {
	t.Parallel()
	pool := dbtest.MigratedPool(t)
	databaseURL, err := url.Parse(dbtest.ConnString(t, pool))
	require.NoError(t, err)
	query := databaseURL.Query()
	query.Set("pool_max_conns", "1")
	databaseURL.RawQuery = query.Encode()
	cfg := config.Config{TLS: mtls.ListenerSettings{Mode: "off"}, GRPCAddress: freeAddress(t), DatabaseURL: databaseURL.String(), WritePoolMaxConns: 1, ReadPoolMaxConns: 1}

	project, actor := uuid.New(), uuid.New()
	reserved, err := store.New(pool, pool).Reserve(t.Context(), store.Reservation{
		ProjectID: project, ActorID: actor, ReservationID: uuid.New(), Limit: 10, EndTime: time.Now().Add(time.Hour),
	})
	require.NoError(t, err)
	require.Equal(t, store.ReserveReserved, reserved.Outcome)
	_, err = pool.Exec(t.Context(), `UPDATE public.scheduling_actor_run SET reserved_at = now() - interval '1 hour' WHERE actor_id = $1`, actor)
	require.NoError(t, err)

	ctx, stop := context.WithCancel(t.Context())
	defer stop()
	served := make(chan error, 1)
	fast := cleanup.Config{Interval: 20 * time.Millisecond, BatchSize: 1000, Yield: time.Millisecond, IdleSessionTimeout: time.Minute}
	go func() { served <- serve(ctx, cfg, telemetry.NewNoopClient(), 0, fast) }()

	conn, err := grpc.NewClient(cfg.GRPCAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	client := scheduling.NewSchedulingServiceClient(conn)
	require.Eventually(t, func() bool {
		var state store.State
		err := pool.QueryRow(t.Context(), `SELECT state FROM public.scheduling_actor_run WHERE actor_id = $1`, actor).Scan(&state)
		if err != nil {
			return false
		}
		_, err = client.Get(t.Context(), scheduling.GetRequest_builder{
			ProjectId: project.String(),
			SandboxId: "i5f3k9q2m8x1c7v4b6n0p",
		}.Build(), grpc.WaitForReady(true))

		return err == nil && state == store.StateGone
	}, 5*time.Second, 10*time.Millisecond)

	stop()
	select {
	case err := <-served:
		require.NoError(t, err)
	case <-time.After(shutdownTimeout + 5*time.Second):
		t.Fatal("serve did not return after shutdown")
	}
	require.Eventually(t, func() bool {
		var holders int
		err := pool.QueryRow(t.Context(), `
SELECT count(*) FROM pg_locks
 WHERE locktype = 'advisory' AND database = (SELECT oid FROM pg_database WHERE datname = current_database())`).Scan(&holders)

		return err == nil && holders == 0
	}, 5*time.Second, 10*time.Millisecond, "shutdown released the cleanup lock")

	seen := statementSeries(t)
	cleanupStatements := 0
	for s := range seen {
		if s.pool != "cleanup" {
			continue
		}
		cleanupStatements++
		for _, pool := range []string{"write", "read"} {
			for _, failed := range []bool{false, true} {
				require.False(t, seen[series{pool, s.statement, failed}], "the pools never run the lock statement %s", s.statement)
			}
		}
	}
	require.Positive(t, cleanupStatements, "the lock connection reports its own statements")
}

func TestServeListenerInUse(t *testing.T) {
	t.Parallel()
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, listener.Close()) })

	err = serve(t.Context(), config.Config{TLS: mtls.ListenerSettings{Mode: "off"}, GRPCAddress: listener.Addr().String()}, telemetry.NewNoopClient(), 0, cleanup.DefaultConfig)
	require.ErrorContains(t, err, "gRPC listener initialization failed")
}

func TestServeDatabaseUnavailable(t *testing.T) {
	t.Parallel()
	cfg := config.Config{
		TLS:               mtls.ListenerSettings{Mode: "off"},
		GRPCAddress:       freeAddress(t),
		DatabaseURL:       "postgres://postgres@" + freeAddress(t) + "/scheduling?sslmode=disable&connect_timeout=5",
		WritePoolMaxConns: 2,
		ReadPoolMaxConns:  1,
	}

	err := serve(t.Context(), cfg, telemetry.NewNoopClient(), 0, cleanup.DefaultConfig)
	require.ErrorContains(t, err, "database initialization failed")

	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", cfg.GRPCAddress)
	require.NoError(t, err, "serve released the gRPC address")
	require.NoError(t, listener.Close())
}

func TestServeReadPoolFailureClosesWritePool(t *testing.T) {
	t.Parallel()
	pool := dbtest.EmptyPool(t)
	databaseURL, err := url.Parse(dbtest.ConnString(t, pool))
	require.NoError(t, err)
	application := "scheduling-pool-test-" + uuid.NewString()
	query := databaseURL.Query()
	query.Set("application_name", application)
	query.Set("pool_min_conns", "2")
	databaseURL.RawQuery = query.Encode()
	cfg := config.Config{
		TLS:               mtls.ListenerSettings{Mode: "off"},
		GRPCAddress:       freeAddress(t),
		DatabaseURL:       databaseURL.String(),
		WritePoolMaxConns: 2,
		ReadPoolMaxConns:  1,
	}

	err = serve(t.Context(), cfg, telemetry.NewNoopClient(), 0, cleanup.DefaultConfig)
	require.ErrorContains(t, err, "read database initialization failed")
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", cfg.GRPCAddress)
	require.NoError(t, err, "serve released the gRPC address")
	require.NoError(t, listener.Close())
	require.Eventually(t, func() bool {
		var connections int
		err := pool.QueryRow(t.Context(), `SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND application_name = $1`, application).Scan(&connections)

		return err == nil && connections == 0
	}, 5*time.Second, 10*time.Millisecond, "serve closed every write connection")
}

func freeAddress(t *testing.T) string {
	t.Helper()
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := listener.Addr().String()
	require.NoError(t, listener.Close())

	return address
}
