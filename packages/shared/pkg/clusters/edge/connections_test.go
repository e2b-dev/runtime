package edge

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	infogrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator-info"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

type echoRoutingServer struct {
	infogrpc.UnimplementedInfoServiceServer
}

func (echoRoutingServer) ServiceInfo(ctx context.Context, _ *emptypb.Empty) (*infogrpc.ServiceInfoResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	ids := md.Get(consts.EdgeRpcServiceInstanceIDHeader)
	if len(ids) != 1 {
		return nil, status.Errorf(codes.InvalidArgument, "want one instance id, got %v", ids)
	}

	return &infogrpc.ServiceInfoResponse{ServiceId: ids[0]}, nil
}

func startEchoRoutingServer(t *testing.T) (*grpc.Server, string) {
	t.Helper()

	var lc net.ListenConfig
	lis, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	server := grpc.NewServer()
	infogrpc.RegisterInfoServiceServer(server, echoRoutingServer{})
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(server.Stop)

	return server, lis.Addr().String()
}

func newTestConnections(t *testing.T, clusterID uuid.UUID, endpoint string, minConnections, perConnection int) *Connections {
	t.Helper()

	limits := func(context.Context) (int, int) { return minConnections, perConnection }
	dialOptions := func() []grpc.DialOption {
		return []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	}
	connections := NewConnections(clusterID, endpoint, dialOptions, limits)
	t.Cleanup(func() { _ = connections.Close() })

	return connections
}

func acquire(t *testing.T, connections *Connections) (*grpc.ClientConn, func()) {
	t.Helper()

	conn, release, err := connections.Acquire(t.Context())
	require.NoError(t, err)

	return conn, release
}

func TestConnections_GrowWhenEveryConnectionIsFull(t *testing.T) {
	t.Parallel()

	_, endpoint := startEchoRoutingServer(t)
	connections := newTestConnections(t, uuid.New(), endpoint, 4, 64)

	const users = 300
	perConnection := map[*grpc.ClientConn]int{}
	for i := range users {
		conn, _ := acquire(t, connections)

		id := fmt.Sprintf("svc-%d", i)
		info, err := infogrpc.NewInfoServiceClient(conn).ServiceInfo(t.Context(), &emptypb.Empty{}, grpc.PerRPCCredentials(Authorization{Token: "shh", ServiceInstanceID: id}))
		require.NoError(t, err)
		assert.Equal(t, id, info.GetServiceId())

		perConnection[conn]++
	}

	assert.Len(t, perConnection, 5)
	for _, n := range perConnection {
		assert.LessOrEqual(t, n, 64)
	}
}

func TestConnections_SpreadAcrossTheMinimumPool(t *testing.T) {
	t.Parallel()

	_, endpoint := startEchoRoutingServer(t)
	connections := newTestConnections(t, uuid.New(), endpoint, 4, 64)

	distinct := map[*grpc.ClientConn]struct{}{}
	for range 8 {
		conn, _ := acquire(t, connections)
		distinct[conn] = struct{}{}
	}

	assert.Len(t, distinct, 4)
}

func TestConnections_LimitsBelowOneCountAsOne(t *testing.T) {
	t.Parallel()

	_, endpoint := startEchoRoutingServer(t)
	connections := newTestConnections(t, uuid.New(), endpoint, 0, 0)

	first, _ := acquire(t, connections)
	second, _ := acquire(t, connections)

	assert.NotSame(t, first, second)
}

func TestConnections_ReleaseFreesOneSlot(t *testing.T) {
	t.Parallel()

	_, endpoint := startEchoRoutingServer(t)
	connections := newTestConnections(t, uuid.New(), endpoint, 2, 2)

	freed, release := acquire(t, connections)
	for range 3 {
		acquire(t, connections)
	}

	release()
	release()

	reused, _ := acquire(t, connections)
	assert.Same(t, freed, reused)

	grown, _ := acquire(t, connections)
	assert.NotSame(t, freed, grown)
	assert.NotEqual(t, connectivity.Shutdown, freed.GetState())
}

func TestConnections_CloseShutsDownEveryConnection(t *testing.T) {
	t.Parallel()

	_, endpoint := startEchoRoutingServer(t)
	connections := newTestConnections(t, uuid.New(), endpoint, 2, 64)

	first, _ := acquire(t, connections)
	second, _ := acquire(t, connections)

	require.NoError(t, connections.Close())
	assert.Equal(t, connectivity.Shutdown, first.GetState())
	assert.Equal(t, connectivity.Shutdown, second.GetState())

	_, _, err := connections.Acquire(t.Context())
	assert.Error(t, err)
}

//nolint:paralleltest // ReplaceGlobals mutates process state
func TestConnections_LogTransientFailure(t *testing.T) {
	core, logs := observer.New(zapcore.InfoLevel)
	t.Cleanup(logger.ReplaceGlobals(t.Context(), logger.NewTracedLoggerFromCore(core)))

	server, endpoint := startEchoRoutingServer(t)
	clusterID := uuid.New()
	connections := newTestConnections(t, clusterID, endpoint, 1, 64)

	conn, _ := acquire(t, connections)
	client := infogrpc.NewInfoServiceClient(conn)
	auth := grpc.PerRPCCredentials(Authorization{ServiceInstanceID: "svc-a"})
	_, err := client.ServiceInfo(t.Context(), &emptypb.Empty{}, auth)
	require.NoError(t, err)

	server.Stop()

	var failure []observer.LoggedEntry
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancel()
		_, _ = client.ServiceInfo(ctx, &emptypb.Empty{}, auth)

		failure = logs.FilterMessage("Remote cluster connection changed state").
			FilterField(zap.Stringer("to", connectivity.TransientFailure)).All()

		return len(failure) > 0
	}, 10*time.Second, 50*time.Millisecond)

	fields := failure[0].ContextMap()
	assert.Equal(t, zapcore.WarnLevel, failure[0].Level)
	assert.Equal(t, clusterID.String(), fields["cluster.id"])
	assert.EqualValues(t, 0, fields["connection_index"])
}
