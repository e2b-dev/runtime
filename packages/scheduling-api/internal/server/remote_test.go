package server_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/e2b-dev/infra/packages/scheduling-api/internal/server"
	statepb "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/scheduling/state"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxstore/remote"
	reservations_redis "github.com/e2b-dev/infra/packages/shared/pkg/sandboxstore/reservations/redis"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxstore/sandboxtypes"
	storage_redis "github.com/e2b-dev/infra/packages/shared/pkg/sandboxstore/storage/redis"
)

type callDeadline struct {
	deadline time.Time
	ok       bool
	at       time.Time
}

func recordDeadlines(seen chan<- callDeadline) grpc.DialOption {
	return grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		deadline, ok := ctx.Deadline()
		seen <- callDeadline{deadline: deadline, ok: ok, at: time.Now()}

		return invoker(ctx, method, req, reply, cc, opts...)
	})
}

func TestRemoteClientDeadlinesMatchTheServer(t *testing.T) {
	t.Parallel()

	seen := make(chan callDeadline, 2)
	reservations := &fakeStorage{wait: func(context.Context) ([]byte, error) { return []byte("result"), nil }}
	conn := newConn(t, reservations, &fakeState{}, recordDeadlines(seen))
	ctx := context.WithoutCancel(t.Context())

	before := time.Now()
	_, err := remote.NewStorage(conn).Get(ctx, uuid.New(), "sbx")
	require.ErrorIs(t, err, sandboxtypes.ErrNotFound)
	unary := <-seen
	require.True(t, unary.ok, "the client sets a deadline")
	assert.LessOrEqual(t, unary.deadline.Sub(unary.at), server.UnaryDeadline, "the client's default deadline is the server's unary deadline")
	assert.GreaterOrEqual(t, unary.deadline.Sub(before), server.UnaryDeadline, "the client's default deadline is the server's unary deadline")

	before = time.Now()
	result, err := remote.NewReservationStorage(conn).WaitForStart(ctx, uuid.New(), "sbx")
	require.NoError(t, err)
	assert.Equal(t, []byte("result"), result)
	poll := <-seen
	require.True(t, poll.ok, "the client sets a deadline")
	assert.GreaterOrEqual(t, poll.deadline.Sub(poll.at), server.MaxWait+time.Second, "a long poll's deadline leaves the server's maximum wait a margin")
	assert.LessOrEqual(t, poll.deadline.Sub(before), server.MaxWait+10*time.Second, "a long poll's deadline is a few seconds above the server's maximum wait")
	assert.Less(t, poll.deadline.Sub(before), server.UnaryDeadline)
}

func TestRemoteClientReceivesTheServersLargestMessage(t *testing.T) {
	t.Parallel()

	team := uuid.New()
	sbx := testRecord(team)
	size := func(sbx sandboxtypes.Sandbox) int {
		return proto.Size(statepb.GetResponse_builder{Outcome: statepb.StateOutcome_STATE_OUTCOME_OK, Record: encodeRecord(t, sbx)}.Build())
	}
	padding := server.MaxMessageSize - size(sbx)
	for range 4 {
		sbx.TemplateID = strings.Repeat("a", padding)
		padding += server.MaxMessageSize - size(sbx)
	}
	require.Equal(t, server.MaxMessageSize, size(sbx), "the response is exactly the server's message limit")

	fake := &fakeState{get: func(context.Context) (sandboxtypes.Sandbox, error) { return sbx, nil }}
	got, err := remote.NewStorage(newConn(t, &fakeStorage{}, fake)).Get(callContext(t), team, sbx.SandboxID)
	require.NoError(t, err)
	assert.Equal(t, sbx, got)
}

func TestRemoteLargeRecordThroughRedis(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	records, err := storage_redis.NewStorage(client, noop.NewMeterProvider())
	require.NoError(t, err)
	go records.Start(t.Context())
	t.Cleanup(func() { records.Close(context.WithoutCancel(t.Context())) })
	store := remote.NewStorage(newConn(t, reservations_redis.NewReservationStorage(client, records.Notifier()), records))

	ctx := callContext(t)
	sbx := testRecord(uuid.New())
	sbx.Metadata = map[string]string{"pad": strings.Repeat("a", 5<<20)}
	require.NoError(t, store.Add(ctx, sbx))
	got, err := store.Get(ctx, sbx.TeamID, sbx.SandboxID)
	require.NoError(t, err)
	assert.Equal(t, sbx, got, "a record above gRPC's default 4 MiB comes back whole")
	items, err := store.TeamItems(ctx, sbx.TeamID, nil)
	require.NoError(t, err)
	assert.Equal(t, []sandboxtypes.Sandbox{sbx}, items)
}
