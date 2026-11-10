package servicetest_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/e2b-dev/infra/packages/shared/pkg/mtls"
	"github.com/e2b-dev/infra/packages/shared/pkg/mtls/servicetest"
)

func TestNewProcessIsConfiguredOverAnEmptyDirectory(t *testing.T) {
	t.Parallel()

	proc, metrics := servicetest.NewProcess(t, nil)
	require.True(t, proc.Enabled())
	cfg, err := proc.Listener("http", "", mtls.ListenerSettings{})
	require.NoError(t, err)
	_ = mtls.NewServerCredentials(cfg)

	assert.Equal(t, int64(1), servicetest.ListenerMode(t, metrics, "http", "off", mtls.SourceFallback))
	assert.Equal(t, int64(-1), servicetest.ListenerMode(t, metrics, "http", "permissive", mtls.SourceFallback), "an absent series reads -1")
}

func TestFlagsDriveAListenerAndAHop(t *testing.T) {
	t.Parallel()

	flags, source := servicetest.Flags(t)
	proc, metrics := servicetest.NewProcess(t, flags.StringReader())
	servicetest.SetFlag(source, "test-listener-mode", "permissive")
	servicetest.SetFlag(source, "test-hop-mode", "on")

	cfg, err := proc.Listener("http", "test-listener-mode", mtls.ListenerSettings{})
	require.NoError(t, err)
	_ = mtls.NewServerCredentials(cfg)
	hop, err := proc.Hop("peers", "test-hop-mode", mtls.HopSettings{Expect: []string{"spiffe://cluster-a.example.internal/ns/platform/sa/server"}})
	require.NoError(t, err)
	_ = mtls.NewClientCredentials(hop)

	assert.Equal(t, int64(1), servicetest.ListenerMode(t, metrics, "http", "permissive", mtls.SourceFlag))
	assert.Equal(t, int64(1), servicetest.HopMode(t, metrics, "peers", "on", mtls.SourceFlag))
}

func TestCredentialsCountTheHandshakesOfADialSite(t *testing.T) {
	t.Parallel()

	creds := servicetest.NewCredentials()
	conn, err := grpc.NewClient(servicetest.GRPCServer(t), grpc.WithTransportCredentials(creds))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	_, err = healthpb.NewHealthClient(conn).Check(t.Context(), &healthpb.HealthCheckRequest{})
	require.NoError(t, err)
	servicetest.AwaitHandshake(t, creds)
	assert.Equal(t, int64(1), creds.Handshakes())
}
