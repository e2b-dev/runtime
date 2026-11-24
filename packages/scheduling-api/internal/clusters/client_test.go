package clusters

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"

	infogrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator-info"
	"github.com/e2b-dev/infra/packages/shared/pkg/mtls/servicetest"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

type fakeInfoServer struct {
	infogrpc.UnimplementedInfoServiceServer

	response *infogrpc.ServiceInfoResponse
}

func (s *fakeInfoServer) ServiceInfo(context.Context, *emptypb.Empty) (*infogrpc.ServiceInfoResponse, error) {
	return s.response, nil
}

func serveInfo(t *testing.T, response *infogrpc.ServiceInfoResponse) string {
	t.Helper()

	var lc net.ListenConfig
	lis, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	infogrpc.RegisterInfoServiceServer(server, &fakeInfoServer{response: response})
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(server.Stop)

	return lis.Addr().String()
}

func callContext(t *testing.T) context.Context {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)

	return ctx
}

func TestServiceInfoRoundTripsThroughTheCallerCredentials(t *testing.T) {
	t.Parallel()

	want := &infogrpc.ServiceInfoResponse{
		NodeId:        "node-1",
		ServiceId:     "service-1",
		ServiceStatus: infogrpc.ServiceInfoStatus_Healthy,
		ServiceRoles:  []infogrpc.ServiceInfoRole{infogrpc.ServiceInfoRole_Orchestrator},
	}
	creds := servicetest.NewCredentials()
	client, err := NewNodeClient(telemetry.NewNoopClient(), serveInfo(t, want), creds)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	got, err := client.ServiceInfo(callContext(t))
	require.NoError(t, err)

	assert.True(t, proto.Equal(want, got), "got %v", got)
	assert.Positive(t, creds.Handshakes())
	assert.Equal(t, connectivity.Ready, client.State())
}

func TestNewNodeClientDoesNotDialUntilTheFirstCall(t *testing.T) {
	t.Parallel()

	creds := servicetest.NewCredentials()
	client, err := NewNodeClient(telemetry.NewNoopClient(), "127.0.0.1:1", creds)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	assert.Equal(t, connectivity.Idle, client.State())
	assert.Zero(t, creds.Handshakes())
}

func TestCloseShutsDownTheConnection(t *testing.T) {
	t.Parallel()

	client, err := NewNodeClient(telemetry.NewNoopClient(), serveInfo(t, &infogrpc.ServiceInfoResponse{}), servicetest.NewCredentials())
	require.NoError(t, err)

	_, err = client.ServiceInfo(callContext(t))
	require.NoError(t, err)

	require.NoError(t, client.Close())
	assert.Equal(t, connectivity.Shutdown, client.State())

	_, err = client.ServiceInfo(callContext(t))
	assert.Equal(t, codes.Canceled, status.Code(err))
}
