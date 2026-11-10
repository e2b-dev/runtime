//go:build linux

package factories

import (
	"bufio"
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/soheilhy/cmux"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
	e2bgrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/mtls"
	"github.com/e2b-dev/infra/packages/shared/pkg/mtls/mtlstest"
	"github.com/e2b-dev/infra/packages/shared/pkg/mtls/servicetest"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

const (
	portServerID  = "spiffe://td.example.internal/ns/nodes/sa/node"
	portCallerID  = "spiffe://td.example.internal/ns/nodes/sa/caller"
	portServerDNS = "node.td.example.internal"
)

// sharedPort is the shared port as run.go builds it: the mtls listener in
// front of cmux, gRPC and HTTP behind it, both asking the listener.
type sharedPort struct {
	addr    string
	metrics *sdkmetric.ManualReader
	root    *mtlstest.CA
	inter   *mtlstest.CA
}

func startSharedPort(t *testing.T, mode string) sharedPort {
	t.Helper()

	root := mtlstest.NewRootCA(t, "root")
	inter := root.Intermediate(t, "intermediate")
	dir := mtlstest.WriteFiles(t, inter.Leaf(t, mtlstest.LeafSpec{SPIFFEID: portServerID, DNSNames: []string{portServerDNS}}), root.BundlePEM())
	flags, source := servicetest.Flags(t)
	if mode != "off" {
		servicetest.SetFlag(source, grpcModeFlag, mode)
	}
	metrics := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(metrics))
	proc, err := mtls.NewProcess(t.Context(), provider, flags.StringReader(), logger.NewNopLogger(),
		mtls.WithFileConfig(mtls.FileConfig{CertFile: dir.CertFile, KeyFile: dir.KeyFile, CAFile: dir.CAFile}),
		mtls.WithWatchInterval(time.Hour))
	require.NoError(t, err)
	portMTLS, err := newPortTLS(proc, cfg.TLSConfig{GRPC: mtls.ListenerSettings{HealthPaths: []string{"/health"}}})
	require.NoError(t, err)

	grpcServer := e2bgrpc.NewGRPCServer(telemetry.NewNoopClient(), append([]e2bgrpc.ServerOption{e2bgrpc.WithMaxConnectionAge(0, 0)}, portMTLS.grpcOptions()...)...)
	healthpb.RegisterHealthServer(grpcServer, health.NewServer())
	cmuxServer, err := NewCMUXServer(t.Context(), 0, metricnoop.NewMeterProvider(), portMTLS.wrap)
	require.NoError(t, err)
	httpListener := cmuxServer.Match(cmux.HTTP1Fast())
	grpcListener := cmuxServer.Match(cmux.Any())
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	httpServer := &http.Server{Handler: portMTLS.guard(mux), ReadHeaderTimeout: time.Second}
	go func() { _ = cmuxServer.Serve() }()
	go func() { _ = httpServer.Serve(httpListener) }()
	go func() { _ = grpcServer.Serve(grpcListener) }()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = httpServer.Close()
		cmuxServer.Close()
	})

	_, port, err := net.SplitHostPort(portMTLS.listener.Addr().String())
	require.NoError(t, err)

	return sharedPort{addr: net.JoinHostPort("127.0.0.1", port), metrics: metrics, root: root, inter: inter}
}

// probe sends the readiness probe's exact bytes and returns the status line.
func probe(t *testing.T, addr string) string {
	t.Helper()

	var dialer net.Dialer
	conn, err := dialer.DialContext(t.Context(), "tcp", addr)
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, conn.SetDeadline(time.Now().Add(5*time.Second)))
	_, err = conn.Write([]byte("GET /health HTTP/1.0\r\n\r\n"))
	require.NoError(t, err)
	line, err := bufio.NewReader(conn).ReadString('\n')
	require.NoError(t, err)

	return strings.TrimSpace(line)
}

func healthOver(t *testing.T, addr string, creds credentials.TransportCredentials) {
	t.Helper()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(creds))
	require.NoError(t, err)
	defer conn.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	res, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	require.NoError(t, err)
	assert.Equal(t, healthpb.HealthCheckResponse_SERVING, res.GetStatus())
}

// stall opens a silent, a one-byte and a fragmented connection and leaves
// them open.
func stall(t *testing.T, addr string) {
	t.Helper()

	var dialer net.Dialer
	for _, prefix := range []string{"", "G", "PR"} {
		conn, err := dialer.DialContext(t.Context(), "tcp", addr)
		require.NoError(t, err)
		t.Cleanup(func() { _ = conn.Close() })
		if prefix != "" {
			_, err = conn.Write([]byte(prefix))
			require.NoError(t, err)
		}
	}
}

func TestSharedPortCarriesHTTPAndGRPCBehindTheMTLSListener(t *testing.T) {
	t.Parallel()

	t.Run("off", func(t *testing.T) {
		t.Parallel()

		port := startSharedPort(t, "off")
		assert.Equal(t, "HTTP/1.0 200 OK", probe(t, port.addr))
		healthOver(t, port.addr, insecure.NewCredentials())
		assert.Equal(t, int64(1), servicetest.ListenerMode(t, port.metrics, "grpc", "off", mtls.SourceFallback))
		assert.Equal(t, int64(2), servicetest.Point(t, port.metrics, mtls.MetricHandshakes, attribute.String(mtls.AttrListener, "grpc"),
			attribute.String(mtls.AttrOutcome, mtls.OutcomePlaintext)), "one per connection: the gRPC credentials take the listener's verdict and do not count again")

		stall(t, port.addr)
		assert.Equal(t, "HTTP/1.0 200 OK", probe(t, port.addr), "stalled connections stall nothing else")
		healthOver(t, port.addr, insecure.NewCredentials())
	})

	t.Run("permissive", func(t *testing.T) {
		t.Parallel()

		port := startSharedPort(t, "permissive")
		callerLeaf := port.inter.Leaf(t, mtlstest.LeafSpec{SPIFFEID: portCallerID})
		callerDir := mtlstest.WriteFiles(t, callerLeaf, port.root.BundlePEM())
		callerFiles := mtls.NewFiles(t.Context(), mtls.FileConfig{CertFile: callerDir.CertFile, KeyFile: callerDir.KeyFile, CAFile: callerDir.CAFile})
		expected, err := mtls.ParseAllowList([]string{portServerID})
		require.NoError(t, err)
		onCreds := mtls.NewClientCredentials(mtls.ClientConfig{Name: "peers", Files: callerFiles, Mode: mtls.StaticClientMode(mtls.ClientOn), ServerName: portServerDNS, ExpectedServerIDs: expected})

		assert.Equal(t, "HTTP/1.0 200 OK", probe(t, port.addr), "plaintext health still matches before the TLS branch")
		healthOver(t, port.addr, insecure.NewCredentials())
		healthOver(t, port.addr, onCreds)
		assert.Equal(t, int64(1), servicetest.Point(t, port.metrics, mtls.MetricWouldReject, attribute.String(mtls.AttrListener, "grpc"),
			attribute.String(mtls.AttrReason, string(mtls.ReasonNotAllowed)), attribute.String(mtls.AttrKind, mtls.KindGRPC)))
		httpsClient := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{callerLeaf.TLS}, NextProtos: []string{"http/1.1"},
			InsecureSkipVerify: true, // the server's certificate is not under test here
		}}}
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://"+port.addr+"/health", nil)
		require.NoError(t, err)
		resp, err := httpsClient.Do(req)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		assert.Equal(t, http.StatusOK, resp.StatusCode, "HTTP over TLS still matches cmux's HTTP/1 branch")
		assert.Equal(t, int64(1), servicetest.Point(t, port.metrics, mtls.MetricWouldReject, attribute.String(mtls.AttrListener, "grpc"),
			attribute.String(mtls.AttrReason, string(mtls.ReasonNotAllowed)), attribute.String(mtls.AttrKind, mtls.KindHTTP)),
			"the guard takes the listener's TLS verdict through cmux")
		assert.Equal(t, int64(2), servicetest.Point(t, port.metrics, mtls.MetricHandshakes, attribute.String(mtls.AttrListener, "grpc"),
			attribute.String(mtls.AttrOutcome, mtls.OutcomeTLS)))
		assert.Equal(t, int64(1), servicetest.ListenerMode(t, port.metrics, "grpc", "permissive", mtls.SourceFlag))

		stall(t, port.addr)
		assert.Equal(t, "HTTP/1.0 200 OK", probe(t, port.addr), "stalled connections stall nothing else")
		healthOver(t, port.addr, onCreds)
	})
}

func TestPortTLSExemptsTheProbeRoutesWithoutTheEnvList(t *testing.T) {
	t.Parallel()

	proc, _ := servicetest.NewProcess(t, nil)
	port, err := newPortTLS(proc, cfg.TLSConfig{})
	require.NoError(t, err)
	assert.Subset(t, port.healthPaths, []string{"/health", "/live"})
}

func TestPortTLSConnStateBeforeWrapIsNotFound(t *testing.T) {
	t.Parallel()

	proc, _ := servicetest.NewProcess(t, nil)
	port, err := newPortTLS(proc, cfg.TLSConfig{})
	require.NoError(t, err)
	_, ok := port.connState("127.0.0.1:1")
	assert.False(t, ok)
}
