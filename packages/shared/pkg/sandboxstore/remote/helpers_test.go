package remote_test

import (
	"context"
	"encoding/json"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"

	reservationpb "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/scheduling/reservation"
	statepb "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/scheduling/state"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxstore/remote"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxstore/sandboxtypes"
)

const hangTimeout = 30 * time.Second

type handler func(ctx context.Context, method string, req proto.Message, send func(proto.Message) error) (proto.Message, error)

var streamRequests = map[string]func() proto.Message{
	statepb.StateService_TeamItems_FullMethodName:    func() proto.Message { return &statepb.TeamItemsRequest{} },
	statepb.StateService_ExpiredItems_FullMethodName: func() proto.Message { return &statepb.ExpiredItemsRequest{} },
}

func dial(t *testing.T, h handler, options ...grpc.DialOption) *grpc.ClientConn {
	t.Helper()

	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer(
		grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, _ grpc.UnaryHandler) (any, error) {
			return h(ctx, info.FullMethod, req.(proto.Message), nil)
		}),
		grpc.StreamInterceptor(func(_ any, stream grpc.ServerStream, info *grpc.StreamServerInfo, _ grpc.StreamHandler) error {
			req := streamRequests[info.FullMethod]()
			if err := stream.RecvMsg(req); err != nil {
				return err
			}
			_, err := h(stream.Context(), info.FullMethod, req, func(resp proto.Message) error { return stream.SendMsg(resp) })

			return err
		}),
	)
	statepb.RegisterStateServiceServer(server, statepb.UnimplementedStateServiceServer{})
	reservationpb.RegisterReservationServiceServer(server, reservationpb.UnimplementedReservationServiceServer{})
	go func() {
		assert.NoError(t, server.Serve(listener))
	}()
	t.Cleanup(server.Stop)

	conn, err := grpc.NewClient("passthrough:///bufconn", append([]grpc.DialOption{
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}, options...)...)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, conn.Close()) })

	return conn
}

func respond(resp proto.Message, err error) handler {
	return func(context.Context, string, proto.Message, func(proto.Message) error) (proto.Message, error) {
		return resp, err
	}
}

type recorder struct {
	mu       sync.Mutex
	requests []proto.Message
}

func (r *recorder) record(req proto.Message) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.requests = append(r.requests, req)
}

func (r *recorder) all() []proto.Message {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]proto.Message(nil), r.requests...)
}

func (r *recorder) last(t *testing.T) proto.Message {
	t.Helper()

	all := r.all()
	require.NotEmpty(t, all, "the server received a request")

	return all[len(all)-1]
}

func callContext(t *testing.T) context.Context {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), hangTimeout)
	t.Cleanup(cancel)

	return ctx
}

var (
	teamID    = uuid.MustParse("6c1f7c5e-8a33-4a8e-9d7b-1f7e0a6b2c11")
	sandboxID = "sbx-remote"
)

func testRecord() sandboxtypes.Sandbox {
	at := time.Date(2026, 11, 13, 10, 11, 12, 123456789, time.UTC)
	token := "envd-token"

	return sandboxtypes.Sandbox{
		SandboxID:       sandboxID,
		TemplateID:      "template",
		ExecutionID:     "1b4e28ba-2fa1-41d2-883f-0016d3cca427",
		TeamID:          teamID,
		Metadata:        map[string]string{"key": "value"},
		StartTime:       at,
		EndTime:         at.Add(time.Hour),
		EnvdAccessToken: &token,
		NodeID:          "node",
		State:           sandboxtypes.StateRunning,
	}
}

func encode(t *testing.T, sbx sandboxtypes.Sandbox) []byte {
	t.Helper()

	data, err := json.Marshal(sbx)
	require.NoError(t, err)

	return data
}

func requireProtoEqual(t *testing.T, want, got proto.Message) {
	t.Helper()

	require.True(t, proto.Equal(want, got), "want %v\ngot  %v", want, got)
}

func requireNoZeroField(t *testing.T, value any) {
	t.Helper()

	v := reflect.ValueOf(value)
	for i := range v.NumField() {
		require.False(t, v.Field(i).IsZero(), "field %s is set so the test covers it", v.Type().Field(i).Name)
	}
}

var sentinels = []error{
	sandboxtypes.ErrNotFound,
	sandboxtypes.ErrAlreadyExists,
	sandboxtypes.ErrEvictionInProgress,
	sandboxtypes.ErrEvictionNotNeeded,
	sandboxtypes.ErrTransitionRestored,
	sandboxtypes.ErrSkipRestore,
	sandboxtypes.ErrDraining,
	sandboxtypes.ErrExecutionMismatch,
	sandboxtypes.ErrLifecycleHeld,
	sandboxtypes.ErrHoldConflict,
	sandboxtypes.ErrLockLost,
	sandboxtypes.ErrInvalidUpdateToken,
	sandboxtypes.ErrNotHeld,
	sandboxtypes.ErrSandboxKilled,
	sandboxtypes.ErrRestoreConflict,
	sandboxtypes.PauseQueueExhaustedError{},
	sandboxtypes.PauseUnsupportedError{},
	context.Canceled,
	context.DeadlineExceeded,
}

func requireNoStoreError(t *testing.T, err error) {
	t.Helper()

	require.Error(t, err)
	for _, sentinel := range sentinels {
		require.NotErrorIs(t, err, sentinel)
	}
	var (
		limit      *sandboxtypes.LimitExceededError
		missing    *sandboxtypes.StartResultMissingError
		invalid    *sandboxtypes.InvalidStateTransitionError
		notRunning *sandboxtypes.NotRunningError
		failed     *sandboxtypes.TransitionFailedError
		restored   *sandboxtypes.RestoredTransitionError
	)
	for _, target := range []any{&limit, &missing, &invalid, &notRunning, &failed, &restored} {
		require.NotErrorAs(t, err, target)
	}
}

func newStores(conn grpc.ClientConnInterface) (*remote.Storage, *remote.ReservationStorage) {
	return remote.NewStorage(conn), remote.NewReservationStorage(conn)
}
