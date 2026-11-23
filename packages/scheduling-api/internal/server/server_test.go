package server_test

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"
	"buf.build/go/protovalidate"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/e2b-dev/infra/packages/scheduling-api/internal/server"
	reservationpb "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/scheduling/reservation"
	statepb "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/scheduling/state"
	"github.com/e2b-dev/infra/packages/shared/pkg/mtls"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxstore/sandboxtypes"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

const hangTimeout = 30 * time.Second

type reserveCall struct {
	teamID    uuid.UUID
	sandboxID string
	limit     int
}

type fakeStorage struct {
	calls atomic.Int32

	reserve func(ctx context.Context, call reserveCall) (bool, error)
	wait    func(ctx context.Context) ([]byte, error)
}

var _ sandboxtypes.ReservationStorage = (*fakeStorage)(nil)

func (f *fakeStorage) Reserve(ctx context.Context, teamID uuid.UUID, sandboxID string, limit int) (bool, error) {
	f.calls.Add(1)

	return f.reserve(ctx, reserveCall{teamID: teamID, sandboxID: sandboxID, limit: limit})
}

func (f *fakeStorage) FinishStart(context.Context, uuid.UUID, string, []byte) error {
	f.calls.Add(1)

	return nil
}

func (f *fakeStorage) WaitForStart(ctx context.Context, _ uuid.UUID, _ string) ([]byte, error) {
	f.calls.Add(1)

	return f.wait(ctx)
}

func (f *fakeStorage) Release(context.Context, uuid.UUID, string) error {
	f.calls.Add(1)

	return nil
}

func (f *fakeStorage) ClaimKill(context.Context, uuid.UUID, string) (string, bool, error) {
	f.calls.Add(1)

	return "", false, nil
}

func (f *fakeStorage) ReleaseKillClaim(context.Context, uuid.UUID, string, string) error {
	f.calls.Add(1)

	return nil
}

func TestInvalidRequests(t *testing.T) {
	t.Parallel()
	team := uuid.NewString()

	for name, tc := range map[string]struct {
		call  func(context.Context, reservationpb.ReservationServiceClient) error
		field string
	}{
		"invalid team_id": {func(ctx context.Context, c reservationpb.ReservationServiceClient) error {
			_, err := c.Reserve(ctx, reservationpb.ReserveRequest_builder{TeamId: "team", SandboxId: "sbx", Limit: 10}.Build())

			return err
		}, "team_id"},
		"empty sandbox_id": {func(ctx context.Context, c reservationpb.ReservationServiceClient) error {
			_, err := c.FinishStart(ctx, reservationpb.FinishStartRequest_builder{TeamId: team, SandboxId: ""}.Build())

			return err
		}, "sandbox_id"},
		"sandbox_id with a brace": {func(ctx context.Context, c reservationpb.ReservationServiceClient) error {
			_, err := c.WaitForStart(ctx, reservationpb.WaitForStartRequest_builder{TeamId: team, SandboxId: "a{b"}.Build())

			return err
		}, "sandbox_id"},
		"empty claim_id": {func(ctx context.Context, c reservationpb.ReservationServiceClient) error {
			_, err := c.ReleaseKillClaim(ctx, reservationpb.ReleaseKillClaimRequest_builder{TeamId: team, SandboxId: "sbx"}.Build())

			return err
		}, "claim_id"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			fake := &fakeStorage{}
			client := newClient(t, fake)

			err := tc.call(callContext(t), client)
			require.Equal(t, codes.InvalidArgument, status.Code(err), err)
			var fields []string
			for _, detail := range status.Convert(err).Details() {
				violations, ok := detail.(*validate.Violations)
				require.True(t, ok, "detail %T", detail)
				for _, violation := range violations.GetViolations() {
					fields = append(fields, protovalidate.FieldPathString(violation.GetField()))
				}
			}
			assert.Equal(t, []string{tc.field}, fields, "the validation interceptor reports the field")
			assert.Zero(t, fake.calls.Load())
		})
	}
}

func TestReserve(t *testing.T) {
	t.Parallel()
	calls := make(chan reserveCall, 1)
	fake := &fakeStorage{reserve: func(_ context.Context, call reserveCall) (bool, error) {
		calls <- call

		return true, nil
	}}
	team := uuid.New()

	resp, err := newClient(t, fake).Reserve(callContext(t), reservationpb.ReserveRequest_builder{TeamId: team.String(), SandboxId: "sbx", Limit: 7}.Build())
	require.NoError(t, err)
	assert.Equal(t, reservationpb.ReserveOutcome_RESERVE_OUTCOME_RESERVED, resp.GetOutcome())
	assert.Equal(t, reserveCall{teamID: team, sandboxID: "sbx", limit: 7}, <-calls)
}

func TestPanicIsInternal(t *testing.T) {
	t.Parallel()
	fake := &fakeStorage{reserve: func(context.Context, reserveCall) (bool, error) { panic("backend bug") }}

	_, err := newClient(t, fake).Reserve(callContext(t), reserveRequest())
	require.Equal(t, codes.Internal, status.Code(err), err)
	assert.Equal(t, "internal error", status.Convert(err).Message())
	assert.Equal(t, int32(1), fake.calls.Load())
}

func TestUnaryDeadline(t *testing.T) {
	t.Parallel()
	deadlines := make(chan time.Time, 1)
	fake := &fakeStorage{reserve: func(ctx context.Context, _ reserveCall) (bool, error) {
		deadline, _ := ctx.Deadline()
		deadlines <- deadline
		<-ctx.Done()

		return false, ctx.Err()
	}}
	client := newClient(t, fake)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	before := time.Now()
	done := make(chan error, 1)
	go func() {
		_, err := client.Reserve(ctx, reserveRequest())
		done <- err
	}()

	var deadline time.Time
	select {
	case deadline = <-deadlines:
	case <-time.After(hangTimeout):
		require.Fail(t, "Reserve never reached the storage")
	}
	after := time.Now()
	require.False(t, deadline.IsZero(), "the handler has a deadline")
	assert.False(t, deadline.Before(before.Add(server.UnaryDeadline)), "deadline %s", deadline)
	assert.False(t, deadline.After(after.Add(server.UnaryDeadline)), "deadline %s", deadline)

	cancel()
	select {
	case err := <-done:
		assert.Equal(t, codes.Canceled, status.Code(err), err)
	case <-time.After(hangTimeout):
		require.Fail(t, "Reserve did not end after the caller cancelled")
	}
}

func TestWaitForStartWaiting(t *testing.T) {
	t.Parallel()
	assert.Less(t, server.MaxWait, server.UnaryDeadline)
	remaining := make(chan time.Duration, 1)
	fake := &fakeStorage{wait: func(ctx context.Context) ([]byte, error) {
		var left time.Duration
		if deadline, ok := ctx.Deadline(); ok {
			left = time.Until(deadline)
		}
		remaining <- left

		return nil, context.DeadlineExceeded
	}}

	ctx, cancel := context.WithTimeout(t.Context(), server.UnaryDeadline+hangTimeout)
	defer cancel()
	resp, err := newClient(t, fake).WaitForStart(ctx, reservationpb.WaitForStartRequest_builder{TeamId: uuid.NewString(), SandboxId: "sbx"}.Build())
	require.NoError(t, err)
	assert.Equal(t, reservationpb.WaitForStartOutcome_WAIT_FOR_START_OUTCOME_WAITING, resp.GetOutcome())
	left := <-remaining
	assert.Positive(t, left, "the wait has a deadline")
	assert.LessOrEqual(t, left, server.MaxWait, "the wait ends at most maxWait after it starts")
}

func newClient(t *testing.T, reservations sandboxtypes.ReservationStorage) reservationpb.ReservationServiceClient {
	t.Helper()

	return reservationpb.NewReservationServiceClient(newConn(t, reservations, &fakeState{}))
}

func newStateClient(t *testing.T, storage sandboxtypes.Storage, options ...grpc.DialOption) statepb.StateServiceClient {
	t.Helper()

	return statepb.NewStateServiceClient(newConn(t, &fakeStorage{}, storage, options...))
}

func newConn(t *testing.T, reservations sandboxtypes.ReservationStorage, storage sandboxtypes.Storage, options ...grpc.DialOption) *grpc.ClientConn {
	t.Helper()

	grpcServer, err := server.New(telemetry.NewNoopClient(), mtls.ServerConfig{Mode: mtls.StaticMode(mtls.ModeOff)}, reservations, storage)
	require.NoError(t, err)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///bufconn", append([]grpc.DialOption{
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}, options...)...)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, conn.Close()) })

	return conn
}

func callContext(t *testing.T) context.Context {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), hangTimeout)
	t.Cleanup(cancel)

	return ctx
}

func reserveRequest() *reservationpb.ReserveRequest {
	return reservationpb.ReserveRequest_builder{TeamId: uuid.NewString(), SandboxId: "sbx", Limit: 10}.Build()
}
