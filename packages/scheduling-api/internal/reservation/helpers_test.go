package reservation_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/e2b-dev/infra/packages/scheduling-api/internal/reservation"
	reservationpb "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/scheduling/reservation"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxstore/sandboxtypes"
)

const hangTimeout = 30 * time.Second

func serve(t *testing.T, reservations sandboxtypes.ReservationStorage, maxWait time.Duration) *bufconn.Listener {
	t.Helper()

	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	reservationpb.RegisterReservationServiceServer(server, reservation.NewServer(reservations, maxWait))
	go func() {
		assert.NoError(t, server.Serve(listener))
	}()
	t.Cleanup(server.Stop)

	return listener
}

func dial(t *testing.T, listener *bufconn.Listener) reservationpb.ReservationServiceClient {
	t.Helper()

	return reservationpb.NewReservationServiceClient(dialConn(t, listener))
}

func dialConn(t *testing.T, listener *bufconn.Listener, options ...grpc.DialOption) *grpc.ClientConn {
	t.Helper()

	conn, err := grpc.NewClient("passthrough:///bufconn", append([]grpc.DialOption{
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}, options...)...)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, conn.Close()) })

	return conn
}

func newClient(t *testing.T, reservations sandboxtypes.ReservationStorage, maxWait time.Duration) reservationpb.ReservationServiceClient {
	t.Helper()

	return dial(t, serve(t, reservations, maxWait))
}

func callContext(t *testing.T) context.Context {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), hangTimeout)
	t.Cleanup(cancel)

	return ctx
}

func requireCode(t *testing.T, err error, code codes.Code) *status.Status {
	t.Helper()

	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok, "not a status error: %v", err)
	require.Equal(t, code, st.Code(), st.Message())

	return st
}

type ids struct {
	team    string
	sandbox string
}

func newIDs() ids {
	return ids{team: uuid.NewString(), sandbox: "sbx-" + uuid.NewString()}
}

func reserve(ctx context.Context, t *testing.T, client reservationpb.ReservationServiceClient, id ids, limit int64) reservationpb.ReserveOutcome {
	t.Helper()

	resp, err := client.Reserve(ctx, reservationpb.ReserveRequest_builder{TeamId: id.team, SandboxId: id.sandbox, Limit: limit}.Build())
	require.NoError(t, err)

	return resp.GetOutcome()
}

func waitForStart(ctx context.Context, client reservationpb.ReservationServiceClient, id ids) (*reservationpb.WaitForStartResponse, error) {
	return client.WaitForStart(ctx, reservationpb.WaitForStartRequest_builder{TeamId: id.team, SandboxId: id.sandbox}.Build())
}

func finishStart(ctx context.Context, t *testing.T, client reservationpb.ReservationServiceClient, id ids, result []byte) {
	t.Helper()

	_, err := client.FinishStart(ctx, reservationpb.FinishStartRequest_builder{TeamId: id.team, SandboxId: id.sandbox, Result: result}.Build())
	require.NoError(t, err)
}

func claimKill(ctx context.Context, t *testing.T, client reservationpb.ReservationServiceClient, id ids) *reservationpb.ClaimKillResponse {
	t.Helper()

	resp, err := client.ClaimKill(ctx, reservationpb.ClaimKillRequest_builder{TeamId: id.team, SandboxId: id.sandbox}.Build())
	require.NoError(t, err)

	return resp
}
