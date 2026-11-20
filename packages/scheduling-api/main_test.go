package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
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
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxstore/sandboxtypes"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

const hangTimeout = 30 * time.Second

var (
	reservationServiceName = scheduling.ReservationService_ServiceDesc.ServiceName
	stateServiceName       = scheduling.StateService_ServiceDesc.ServiceName
)

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
	for _, service := range []string{"", reservationServiceName, stateServiceName} {
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

	state := scheduling.NewStateServiceClient(conn)
	teamID := uuid.New()
	start := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	record, err := json.Marshal(sandboxtypes.Sandbox{
		SandboxID:   sandbox,
		TeamID:      teamID,
		ExecutionID: uuid.NewString(),
		StartTime:   start,
		EndTime:     start.Add(time.Hour),
		State:       sandboxtypes.StateRunning,
	})
	require.NoError(t, err)
	added, err := state.Add(callCtx, scheduling.AddRequest_builder{Record: record}.Build())
	require.NoError(t, err)
	require.Equal(t, scheduling.StateOutcome_STATE_OUTCOME_OK, added.GetOutcome())
	got, err := state.Get(callCtx, scheduling.GetRequest_builder{TeamId: teamID.String(), SandboxId: sandbox}.Build())
	require.NoError(t, err)
	require.Equal(t, scheduling.StateOutcome_STATE_OUTCOME_OK, got.GetOutcome())
	require.JSONEq(t, string(record), string(got.GetRecord()), "Get returns the added record")

	items, err := state.TeamItems(callCtx, scheduling.TeamItemsRequest_builder{TeamId: teamID.String()}.Build())
	require.NoError(t, err)
	var streamed [][]byte
	for {
		resp, err := items.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		streamed = append(streamed, resp.GetRecords()...)
	}
	require.Len(t, streamed, 1, "TeamItems streams the added record")
	require.JSONEq(t, string(record), string(streamed[0]))

	begun, err := state.BeginRemoving(callCtx, scheduling.BeginRemovingRequest_builder{
		TeamId:    teamID.String(),
		SandboxId: sandbox,
		Options:   scheduling.RemoveOptions_builder{Action: scheduling.RemoveAction_REMOVE_ACTION_KILL}.Build(),
	}.Build())
	require.NoError(t, err)
	require.Equal(t, scheduling.StateOutcome_STATE_OUTCOME_OK, begun.GetOutcome())
	transitionID := begun.GetTransition().GetId()
	require.NotEmpty(t, transitionID, "this call started the kill")
	finished, err := state.FinishTransition(callCtx, scheduling.FinishTransitionRequest_builder{
		TeamId:       teamID.String(),
		SandboxId:    sandbox,
		TransitionId: transitionID,
		Action:       scheduling.RemoveAction_REMOVE_ACTION_KILL,
		Result:       scheduling.TransitionResult_builder{}.Build(),
	}.Build())
	require.NoError(t, err)
	require.Equal(t, scheduling.StateOutcome_STATE_OUTCOME_OK, finished.GetOutcome())
	transitionWait, err := state.WaitForTransition(callCtx, scheduling.WaitForTransitionRequest_builder{TeamId: teamID.String(), SandboxId: sandbox, TransitionId: transitionID}.Build())
	require.NoError(t, err)
	require.Equal(t, scheduling.WaitForTransitionOutcome_WAIT_FOR_TRANSITION_OUTCOME_FINISHED, transitionWait.GetOutcome())

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
