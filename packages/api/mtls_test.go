package main

import (
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/cfg"
	e2bgrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc"
	"github.com/e2b-dev/infra/packages/shared/pkg/httpserver"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/mtls"
	"github.com/e2b-dev/infra/packages/shared/pkg/mtls/servicetest"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// postTemplates POSTs /templates on a new connection and returns the status.
func postTemplates(t *testing.T, addr string) int {
	t.Helper()

	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}, Timeout: 5 * time.Second}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "http://"+addr+"/templates", nil)
	require.NoError(t, err)
	resp, err := client.Do(req)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	return resp.StatusCode
}

// The HTTP port serves the gin server through the listener: a plaintext
// request gets the application's own answer, the port reports its mode
// from startup, and its own flag moves it. The probe route is exempt
// without the env list.
func TestHTTPPortServesThroughTheMTLSListener(t *testing.T) {
	t.Parallel()

	flags, source := servicetest.Flags(t)
	proc, metrics := servicetest.NewProcess(t, flags.StringReader())
	tlsConfig, err := newServiceTLS(proc, cfg.TLSConfig{}, false)
	require.NoError(t, err)
	assert.Equal(t, []string{"/health"}, tlsConfig.httpHealthPaths)
	swagger, err := api.GetSpec()
	require.NoError(t, err)
	s := NewGinServer(t.Context(), cfg.Config{}, telemetry.NewNoopClient(), logger.NewNopLogger(), nil, nil, nil, flags, swagger)
	inner, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	listener := mtls.WrapHTTPServer(s, inner, tlsConfig.http, tlsConfig.httpHealthPaths)
	httpserver.ConfigureH2C(s)
	go func() { _ = s.Serve(listener) }()
	t.Cleanup(func() { _ = s.Close() })

	assert.Equal(t, int64(1), servicetest.ListenerMode(t, metrics, "http", "off", mtls.SourceFallback), "reported from startup")
	assert.Equal(t, http.StatusGone, postTemplates(t, inner.Addr().String()))
	assert.Equal(t, int64(1), servicetest.Point(t, metrics, mtls.MetricHandshakes,
		attribute.String(mtls.AttrListener, "http"), attribute.String(mtls.AttrOutcome, mtls.OutcomePlaintext)))

	servicetest.SetFlag(source, "mtls-api-http-mode", "permissive")
	assert.Equal(t, http.StatusGone, postTemplates(t, inner.Addr().String()), "permissive still serves plaintext")
	assert.Equal(t, int64(1), servicetest.ListenerMode(t, metrics, "http", "permissive", mtls.SourceFlag))
}

// serveGRPC serves the health service through the constructor and the
// port's TLS options, and returns a plaintext client.
func serveGRPC(t *testing.T, port mtls.ServerConfig) *grpc.ClientConn {
	t.Helper()

	server := e2bgrpc.NewGRPCServer(telemetry.NewNoopClient(), grpcTLSOptions(port)...)
	healthpb.RegisterHealthServer(server, health.NewServer())
	lis, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	return conn
}

func healthCheck(t *testing.T, conn *grpc.ClientConn) {
	t.Helper()

	_, err := healthpb.NewHealthClient(conn).Check(t.Context(), &healthpb.HealthCheckRequest{})
	require.NoError(t, err)
}

// Both gRPC ports take the listener's credentials and checks: a plaintext
// call is served, and each port reports and follows its own flag.
func TestGRPCPortsTakeTheMTLSCredentials(t *testing.T) {
	t.Parallel()

	flags, source := servicetest.Flags(t)
	proc, metrics := servicetest.NewProcess(t, flags.StringReader())
	tlsConfig, err := newServiceTLS(proc, cfg.TLSConfig{}, false)
	require.NoError(t, err)
	internal := serveGRPC(t, tlsConfig.internalGRPC)
	public := serveGRPC(t, tlsConfig.publicGRPC)

	healthCheck(t, internal)
	healthCheck(t, public)
	assert.Equal(t, int64(1), servicetest.ListenerMode(t, metrics, "internal_grpc", "off", mtls.SourceFallback))
	assert.Equal(t, int64(1), servicetest.ListenerMode(t, metrics, "public_grpc", "off", mtls.SourceFallback))

	servicetest.SetFlag(source, "mtls-api-public-grpc-mode", "permissive")
	healthCheck(t, public)
	healthCheck(t, internal)
	assert.Equal(t, int64(1), servicetest.ListenerMode(t, metrics, "public_grpc", "permissive", mtls.SourceFlag))
	assert.Equal(t, int64(1), servicetest.ListenerMode(t, metrics, "internal_grpc", "off", mtls.SourceFallback), "each port reads its own flag")
}

func TestNewServiceTLSRefusesAWildcardOnTheAPIsPorts(t *testing.T) {
	t.Parallel()

	proc, _ := servicetest.NewProcess(t, nil)
	_, err := newServiceTLS(proc, cfg.TLSConfig{InternalGRPC: mtls.ListenerSettings{Allow: []string{"spiffe://td.example.internal/ns/edge/sa/proxy-*"}}}, false)
	require.ErrorIs(t, err, mtls.ErrInvalidAllowList)
	assert.ErrorContains(t, err, "E2B_TLS_INTERNAL_GRPC_ALLOW")
}

// Each hop reports under its chart key from startup and follows its own flag.
func TestHopsReportUnderTheirKeysAndReadTheirFlags(t *testing.T) {
	t.Parallel()

	flags, source := servicetest.Flags(t)
	proc, metrics := servicetest.NewProcess(t, flags.StringReader())
	servicetest.SetFlag(source, "mtls-api-webhooks-client-mode", "on")
	// A hop with no expected server name refuses an on flip.
	servicetest.SetFlag(source, "mtls-api-analytics-client-mode", "on")

	_, err := newServiceTLS(proc, cfg.TLSConfig{
		SecretsStore: mtls.HopSettings{Expect: []string{"spiffe://td.example.internal/ns/store/sa/store"}},
		Webhooks:     mtls.HopSettings{Expect: []string{"spiffe://td.example.internal/ns/webhooks/sa/webhooks"}},
		Orchestrator: mtls.HopSettings{Expect: []string{"spiffe://td.example.internal/ns/orchestrator/sa/orchestrator"}},
	}, true)
	require.NoError(t, err)

	for _, hop := range []string{"secrets_store", "orchestrator"} {
		assert.Equal(t, int64(1), servicetest.HopMode(t, metrics, hop, "off", mtls.SourceFallback), hop)
	}
	assert.Equal(t, int64(1), servicetest.HopMode(t, metrics, "analytics", "off", mtls.SourceRefused))
	assert.Equal(t, int64(1), servicetest.HopMode(t, metrics, "webhooks", "on", mtls.SourceFlag))
}

// On the public TLS path the collector is not dialled through the hop, so
// none is built and no analytics series is reported.
func TestAnalyticsHopIsNotBuiltOffTheHopPath(t *testing.T) {
	t.Parallel()

	proc, metrics := servicetest.NewProcess(t, nil)
	tlsConfig, err := newServiceTLS(proc, cfg.TLSConfig{}, false)
	require.NoError(t, err)

	assert.Nil(t, tlsConfig.hops.Analytics)
	assert.Equal(t, int64(-1), servicetest.HopMode(t, metrics, "analytics", "off", mtls.SourceFallback))
	assert.Equal(t, int64(1), servicetest.HopMode(t, metrics, "orchestrator", "off", mtls.SourceFallback), "the other hops still report")
}
