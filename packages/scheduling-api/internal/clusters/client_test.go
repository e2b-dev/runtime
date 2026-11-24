package clusters

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	infogrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator-info"
	"github.com/e2b-dev/infra/packages/shared/pkg/mtls/servicetest"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

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
