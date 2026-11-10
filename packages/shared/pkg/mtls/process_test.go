package mtls

import (
	"context"
	"net"
	"net/http"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

// testProvider is a meter provider the test reads, for NewProcess.
func testProvider(t *testing.T) (*sdkmetric.MeterProvider, *sdkmetric.ManualReader) {
	t.Helper()

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.WithoutCancel(t.Context())) })

	return provider, reader
}

// emptyFileConfig names certificate files in a directory that holds none,
// as on a pod whose Secret is not written yet.
func emptyFileConfig(t *testing.T) FileConfig {
	t.Helper()

	dir := t.TempDir()

	return FileConfig{CertFile: filepath.Join(dir, "tls.crt"), KeyFile: filepath.Join(dir, "tls.key"), CAFile: filepath.Join(dir, "ca.crt")}
}

// countingFlags answers every flag with its fallback and counts the reads,
// for a process that must read none.
type countingFlags struct {
	reads atomic.Int32
}

func (f *countingFlags) String(_ context.Context, _, fallback string) string {
	f.reads.Add(1)

	return fallback
}

// serveWrapped serves app behind WrapHTTPServer on a loopback port and
// returns the address.
func serveWrapped(t *testing.T, cfg ServerConfig, app http.Handler) string {
	t.Helper()

	var lc net.ListenConfig
	inner, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := &http.Server{Handler: app, ReadHeaderTimeout: time.Second}
	l := WrapHTTPServer(server, inner, cfg, []string{"/health"})
	go func() { _ = server.Serve(l) }()
	t.Cleanup(func() { _ = server.Close() })

	return inner.Addr().String()
}

func teapot() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) })
}

func TestProcessWithoutCertificateFilesIsInert(t *testing.T) { //nolint:paralleltest // cannot call t.Setenv and t.Parallel
	t.Setenv(CertFileEnv, "")
	provider, reader := testProvider(t)
	core, logs := observer.New(zapcore.InfoLevel)
	flags := &countingFlags{}

	proc, err := NewProcess(t.Context(), provider, flags, logger.NewTracedLoggerFromCore(core))
	require.NoError(t, err)
	assert.False(t, proc.Enabled())

	cfg, err := proc.Listener("http", "listener-flag", ListenerSettings{HealthPaths: []string{"/health"}})
	require.NoError(t, err)
	hop, err := proc.Hop("peers", "hop-flag", HopSettings{})
	require.NoError(t, err)
	_ = NewClientCredentials(hop)
	addr := serveWrapped(t, cfg, teapot())

	assert.Equal(t, http.StatusTeapot, get(t, httpClient(nil), "http://"+addr+"/data").status, "the port serves as before")
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	assert.Empty(t, rm.ScopeMetrics, "an unconfigured process records nothing")
	assert.Zero(t, flags.reads.Load(), "an unconfigured process reads no flag")
	assert.Zero(t, logs.Len(), "an unconfigured process logs nothing at info or above")
}

func TestProcessWithoutCertificateFilesRefusesAMode(t *testing.T) { //nolint:paralleltest // cannot call t.Setenv and t.Parallel
	t.Setenv(CertFileEnv, "")
	provider, _ := testProvider(t)
	proc, err := NewProcess(t.Context(), provider, nil, logger.NewNopLogger())
	require.NoError(t, err)

	_, err = proc.Listener("http", "", ListenerSettings{Mode: "permissive"})
	require.ErrorIs(t, err, ErrNoCertificateConfigured)
	require.ErrorContains(t, err, "E2B_TLS_HTTP_MODE")

	_, err = proc.Hop("peers", "", HopSettings{Mode: "on"})
	require.ErrorIs(t, err, ErrNoCertificateConfigured)
	require.ErrorContains(t, err, "E2B_TLS_PEERS_CLIENT_MODE")
}

func TestProcessReadsTheFilesTheChartNames(t *testing.T) { //nolint:paralleltest // cannot call t.Setenv and t.Parallel
	fx := newFixture(t)
	t.Setenv(CertFileEnv, fx.dir.CertFile)
	t.Setenv(KeyFileEnv, fx.dir.KeyFile)
	t.Setenv(CAFileEnv, fx.dir.CAFile)
	provider, reader := testProvider(t)

	proc, err := NewProcess(t.Context(), provider, nil, fx.log)
	require.NoError(t, err)
	assert.True(t, proc.Enabled())
	assert.NotNil(t, proc.files.Bundle(), "the certificate, key and bundle named by the environment loaded")
	assert.Equal(t, int64(1), mustPoint(t, reader, MetricReloads, attribute.String(AttrOutcome, ReloadLoaded)))
}

func TestProcessWithEmptyFilesStartsAndServes(t *testing.T) {
	t.Parallel()

	provider, reader := testProvider(t)
	log, logs := testLogger(t)
	proc, err := NewProcess(t.Context(), provider, nil, log, WithFileConfig(emptyFileConfig(t)))
	require.NoError(t, err)
	assert.True(t, proc.Enabled(), "configured even before the files exist")

	cfg, err := proc.Listener("http", "", ListenerSettings{})
	require.NoError(t, err)
	addr := serveWrapped(t, cfg, teapot())

	assert.Equal(t, http.StatusTeapot, get(t, httpClient(nil), "http://"+addr+"/data").status)
	assert.Equal(t, int64(1), mustPoint(t, reader, MetricReloads, attribute.String(AttrOutcome, ReloadFailed)))
	assert.Contains(t, logMessages(logs), "mtls: no certificate loaded; TLS handshakes fail until the files are fixed")
}

func TestProcessListenerFollowsTheFlagOverTheFallback(t *testing.T) {
	t.Parallel()

	provider, _ := testProvider(t)
	log, _ := testLogger(t)
	flags := newFakeFlags()
	proc, err := NewProcess(t.Context(), provider, flags, log, WithFileConfig(emptyFileConfig(t)))
	require.NoError(t, err)

	cfg, err := proc.Listener("internal_grpc", "listener-flag", ListenerSettings{Allow: []string{clientID}})
	require.NoError(t, err)
	assert.Equal(t, "internal_grpc", cfg.Name)
	assert.True(t, cfg.Allow.Allows(clientID))
	mode, source := modeWithSource(t.Context(), cfg.Mode)
	assert.Equal(t, ModeOff, mode)
	assert.Equal(t, SourceFallback, source)

	flags.set("listener-flag", "permissive")
	mode, source = modeWithSource(t.Context(), cfg.Mode)
	assert.Equal(t, ModePermissive, mode)
	assert.Equal(t, SourceFlag, source)

	static, err := proc.Listener("grpc", "", ListenerSettings{Mode: "permissive"})
	require.NoError(t, err)
	assert.Equal(t, StaticMode(ModePermissive), static.Mode, "no flag key: the env value is the mode")
}

func TestProcessHopFollowsTheFlagOverTheFallback(t *testing.T) {
	t.Parallel()

	provider, _ := testProvider(t)
	log, _ := testLogger(t)
	flags := newFakeFlags()
	proc, err := NewProcess(t.Context(), provider, flags, log, WithFileConfig(emptyFileConfig(t)))
	require.NoError(t, err)

	hop, err := proc.Hop("peers", "hop-flag", HopSettings{Expect: []string{serverID}, ServerName: " " + serverDNS + " "})
	require.NoError(t, err)
	assert.Equal(t, "peers", hop.Name)
	assert.Equal(t, serverDNS, hop.ServerName)
	assert.True(t, hop.ExpectedServerIDs.Allows(serverID))
	mode, source := clientModeWithSource(t.Context(), hop.Mode)
	assert.Equal(t, ClientOff, mode)
	assert.Equal(t, SourceFallback, source)

	flags.set("hop-flag", "on")
	mode, source = clientModeWithSource(t.Context(), hop.Mode)
	assert.Equal(t, ClientOn, mode)
	assert.Equal(t, SourceFlag, source)
}

// A refused flag value reports its own source, whichever mode it keeps, so
// a hop that read the flag is never counted as one on its env fallback. Once
// the flag stops serving it, the source is the last accepted one again; ""
// reads as an unset flag.
func TestProcessHopReportsWhereItsModeCameFrom(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		withReader bool
		served     []string
		expect     []string
		wantMode   ClientMode
		wantSource string
	}{
		{name: "on without names at start is refused", withReader: true, served: []string{"on"}, wantMode: ClientOff, wantSource: SourceRefused},
		{name: "on without names after an accepted off is refused", withReader: true, served: []string{"off", "on"}, wantMode: ClientOff, wantSource: SourceRefused},
		{name: "unset after a refusal at start", withReader: true, served: []string{"on", ""}, wantMode: ClientOff, wantSource: SourceFallback},
		{name: "unset after a refusal that followed an accepted off", withReader: true, served: []string{"off", "on", ""}, wantMode: ClientOff, wantSource: SourceFlag},
		{name: "malformed after a refusal at start", withReader: true, served: []string{"on", "bogus"}, wantMode: ClientOff, wantSource: SourceFallback},
		{name: "malformed after a refusal that followed an accepted off", withReader: true, served: []string{"off", "on", "bogus"}, wantMode: ClientOff, wantSource: SourceFlag},
		{name: "on with names is accepted", withReader: true, served: []string{"on"}, expect: []string{serverID}, wantMode: ClientOn, wantSource: SourceFlag},
		{name: "no flag reader keeps the fallback", served: []string{"on"}, expect: []string{serverID}, wantMode: ClientOff, wantSource: SourceFallback},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			provider, _ := testProvider(t)
			flags := newFakeFlags()
			var reader FlagReader
			if tc.withReader {
				reader = flags
			}
			proc, err := NewProcess(t.Context(), provider, reader, logger.NewNopLogger(), WithFileConfig(emptyFileConfig(t)))
			require.NoError(t, err)
			hop, err := proc.Hop("peers", "hop-flag", HopSettings{Expect: tc.expect})
			require.NoError(t, err)

			var mode ClientMode
			var source string
			for _, value := range tc.served {
				flags.set("hop-flag", value)
				mode, source = clientModeWithSource(t.Context(), hop.Mode)
			}
			assert.Equal(t, tc.wantMode, mode)
			assert.Equal(t, tc.wantSource, source)
		})
	}
}

// An empty expected set refuses every server, so a hop that is on from the
// environment refuses it at startup rather than at every dial. A hop that
// is off from the environment needs none.
func TestProcessHopOnFromTheEnvironmentNeedsExpectedServerNames(t *testing.T) {
	t.Parallel()

	provider, _ := testProvider(t)
	proc, err := NewProcess(t.Context(), provider, newFakeFlags(), logger.NewNopLogger(), WithFileConfig(emptyFileConfig(t)))
	require.NoError(t, err)

	_, err = proc.Hop("peers", "", HopSettings{Mode: "on", Expect: []string{" "}})
	require.ErrorContains(t, err, "E2B_TLS_PEERS_EXPECT")
	_, err = proc.Hop("peers", "", HopSettings{})
	assert.NoError(t, err, "off from the environment")
}

func TestProcessRefusesMalformedSettings(t *testing.T) {
	t.Parallel()

	provider, _ := testProvider(t)
	proc, err := NewProcess(t.Context(), provider, nil, logger.NewNopLogger(), WithFileConfig(emptyFileConfig(t)))
	require.NoError(t, err)

	_, err = proc.Listener("internal_grpc", "", ListenerSettings{Mode: "permisive"})
	require.ErrorIs(t, err, ErrInvalidMode)
	require.ErrorContains(t, err, "E2B_TLS_INTERNAL_GRPC_MODE")

	_, err = proc.Listener("internal_grpc", "", ListenerSettings{Allow: []string{"not-a-spiffe-id"}})
	require.ErrorIs(t, err, ErrInvalidAllowList)
	require.ErrorContains(t, err, "E2B_TLS_INTERNAL_GRPC_ALLOW")

	_, err = proc.Listener("internal_grpc", "", ListenerSettings{Allow: []string{"spiffe://cluster-a.example.internal/ns/platform/sa/worker-*"}}, WithoutWildcards())
	require.ErrorIs(t, err, ErrInvalidAllowList)

	_, err = proc.Hop("peers", "", HopSettings{Mode: "yes"})
	require.ErrorIs(t, err, ErrInvalidMode)
	require.ErrorContains(t, err, "E2B_TLS_PEERS_CLIENT_MODE")

	_, err = proc.Hop("peers", "", HopSettings{Expect: []string{"spiffe://cluster-a.example.internal/ns/platform/sa/server-*"}})
	require.ErrorIs(t, err, ErrInvalidAllowList)
	require.ErrorContains(t, err, "E2B_TLS_PEERS_EXPECT")
}

// A listener's gauge is written when the listener or credentials are
// built, on the provider the process was given, so a port that never
// sees traffic still reports its mode.
func TestProcessRecordsTheListenerModeBeforeAnyTraffic(t *testing.T) {
	t.Parallel()

	provider, reader := testProvider(t)
	proc, err := NewProcess(t.Context(), provider, nil, logger.NewNopLogger(), WithFileConfig(emptyFileConfig(t)))
	require.NoError(t, err)
	cfg, err := proc.Listener("public_grpc", "", ListenerSettings{})
	require.NoError(t, err)

	_ = NewServerCredentials(cfg)

	assert.Equal(t, int64(1), mustPoint(t, reader, MetricMode, attribute.String(AttrListener, "public_grpc"), attribute.String(AttrMode, "off"), attribute.String(AttrSource, SourceFallback)))
}

// Under required a plaintext request reaches only the health path, and a
// TLS request the listener verified reaches the application: that second
// answer needs the listener's lookup in the guard.
func TestWrapHTTPServerGuardsTheHandlerWithTheListenersLookup(t *testing.T) {
	t.Parallel()

	fx := newFixture(t, serverDNS)
	cfg := newServerConfig(t, fx, StaticMode(ModeRequired), clientID)
	addr := serveWrapped(t, cfg, teapot())

	plain := httpClient(nil)
	assert.Equal(t, http.StatusTeapot, get(t, plain, "http://"+addr+"/health").status)
	assert.Equal(t, http.StatusForbidden, get(t, plain, "http://"+addr+"/data").status)
	verified := httpClient(rawClientTLS(fx.peerLeaf(t, clientID), protoHTTP11))
	assert.Equal(t, http.StatusTeapot, get(t, verified, "https://"+addr+"/data").status)
}
