package handlers

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/resolver"

	reservationpb "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/scheduling/reservation"
	statepb "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/scheduling/state"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxstore/remote"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxstore/sandboxtypes"
)

var schedulingResolver = registerSecretsTestResolver("scheduling-test-replicas", nil)

const schedulingBatch = 10

type fakeStateService struct {
	statepb.UnimplementedStateServiceServer

	calls atomic.Int64
}

func (s *fakeStateService) Get(context.Context, *statepb.GetRequest) (*statepb.GetResponse, error) {
	s.calls.Add(1)

	return statepb.GetResponse_builder{Outcome: statepb.StateOutcome_STATE_OUTCOME_NOT_FOUND}.Build(), nil
}

type schedulingReplica struct {
	state   *fakeStateService
	health  *health.Server
	address string
}

func startSchedulingReplica(t *testing.T) *schedulingReplica {
	t.Helper()

	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	replica := &schedulingReplica{state: &fakeStateService{}, health: health.NewServer(), address: listener.Addr().String()}
	statepb.RegisterStateServiceServer(server, replica.state)
	for _, name := range []string{statepb.StateService_ServiceDesc.ServiceName, reservationpb.ReservationService_ServiceDesc.ServiceName} {
		replica.health.SetServingStatus(name, healthv1.HealthCheckResponse_SERVING)
	}
	healthv1.RegisterHealthServer(server, replica.health)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	return replica
}

func (r *schedulingReplica) setStateServing(serving healthv1.HealthCheckResponse_ServingStatus) {
	r.health.SetServingStatus(statepb.StateService_ServiceDesc.ServiceName, serving)
}

func awaitSchedulingBatch(t *testing.T, storage *remote.Storage, a, b *schedulingReplica, done func(fromA, fromB int64) bool) {
	t.Helper()

	deadline := time.Now().Add(30 * time.Second)
	for !schedulingBatchMatches(t, storage, a, b, done) {
		require.True(t, time.Now().Before(deadline), "timed out waiting for the expected distribution of calls")
	}
}

func schedulingBatchMatches(t *testing.T, storage *remote.Storage, a, b *schedulingReplica, done func(fromA, fromB int64) bool) bool {
	t.Helper()

	beforeA, beforeB := a.state.calls.Load(), b.state.calls.Load()
	for range schedulingBatch {
		_, err := storage.Get(t.Context(), uuid.New(), "sandbox")
		require.ErrorIs(t, err, sandboxtypes.ErrNotFound)
	}

	return done(a.state.calls.Load()-beforeA, b.state.calls.Load()-beforeB)
}

func TestSchedulingClientFollowsServingReplicas(t *testing.T) {
	t.Parallel()

	a, b := startSchedulingReplica(t), startSchedulingReplica(t)
	schedulingResolver.UpdateState(resolver.State{Addresses: []resolver.Address{{Addr: a.address}, {Addr: b.address}}})
	conn, err := newSchedulingClient(schedulingResolver.Scheme()+":///scheduling-api", insecure.NewCredentials())
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	require.Equal(t, connectivity.Idle, conn.GetState())
	storage := remote.NewStorage(conn)

	both := func(fromA, fromB int64) bool { return fromA > 0 && fromB > 0 }
	onlyA := func(fromA, fromB int64) bool { return fromA == schedulingBatch && fromB == 0 }

	awaitSchedulingBatch(t, storage, a, b, both)

	b.setStateServing(healthv1.HealthCheckResponse_NOT_SERVING)
	awaitSchedulingBatch(t, storage, a, b, onlyA)
	assert.True(t, schedulingBatchMatches(t, storage, a, b, onlyA), "a replica that is not serving gets no calls")

	b.setStateServing(healthv1.HealthCheckResponse_SERVING)
	awaitSchedulingBatch(t, storage, a, b, both)
}
