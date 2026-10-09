package mtls

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/embedded"

	"github.com/e2b-dev/infra/packages/shared/pkg/mtls/mtlstest"
)

func listenerAttr() attribute.KeyValue {
	return attribute.String(AttrListener, "test-listener")
}

// wouldRejectCount reads the fixture listener's would-reject counter for one reason and kind.
func wouldRejectCount(t *testing.T, fx *fixture, reason Reason, kind string) int64 {
	t.Helper()

	return mustPoint(t, fx.reader, MetricWouldReject, listenerAttr(), attribute.String(AttrReason, string(reason)), attribute.String(AttrKind, kind))
}

func TestMetricsCountHandshakesByOutcomeAndCaller(t *testing.T) {
	t.Parallel()

	fx := newFixture(t, serverDNS)
	cfg := newServerConfig(t, fx, StaticMode(ModePermissive), clientID)
	cfg.HandshakeTimeout = 300 * time.Millisecond
	_, addr := serveOn(t, cfg, &http.Server{Handler: describe(), ReadHeaderTimeout: time.Second})

	plain := httpClient(nil)
	resp := get(t, plain, "http://"+addr+"/")
	require.Equal(t, http.StatusOK, resp.status)
	assert.Equal(t, int64(1), mustPoint(t, fx.reader, MetricHandshakes, listenerAttr(), attribute.String(AttrOutcome, OutcomePlaintext)))
	assert.Equal(t, int64(1), mustPoint(t, fx.reader, MetricPlaintextOpen, listenerAttr()), "the plaintext connection is open and idle")

	plain.CloseIdleConnections()
	require.Eventually(t, func() bool {
		open, _ := pointValue(t, fx.reader, MetricPlaintextOpen, listenerAttr())

		return open == 0
	}, 5*time.Second, 10*time.Millisecond, "closing the connection takes the gauge back to zero")

	resp = get(t, httpClient(rawClientTLS(fx.peerLeaf(t, clientID), protoHTTP11)), "https://"+addr+"/")
	require.Equal(t, http.StatusOK, resp.status)
	assert.Equal(t, int64(1), mustPoint(t, fx.reader, MetricHandshakes, listenerAttr(), attribute.String(AttrOutcome, OutcomeTLS)))
	assert.Equal(t, int64(1), mustPoint(t, fx.reader, MetricConnections, listenerAttr(), attribute.String(AttrCaller, clientID)))

	resp = get(t, httpClient(rawClientTLS(fx.peerLeaf(t, strangerID), protoHTTP11)), "https://"+addr+"/")
	require.Equal(t, http.StatusOK, resp.status, "permissive admits")
	assert.Equal(t, int64(1), mustPoint(t, fx.reader, MetricWouldReject, listenerAttr(), attribute.String(AttrReason, string(ReasonNotAllowed)), attribute.String(AttrKind, KindHandshake)))
	_, counted := pointValue(t, fx.reader, MetricConnections, listenerAttr(), attribute.String(AttrCaller, strangerID))
	assert.False(t, counted, "a name that would be refused is not a connection by caller")

	var dialer net.Dialer
	silent, err := dialer.DialContext(t.Context(), "tcp", addr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = silent.Close() })
	require.Eventually(t, func() bool {
		timeouts, _ := pointValue(t, fx.reader, MetricHandshakes, listenerAttr(), attribute.String(AttrOutcome, OutcomeTimeout))

		return timeouts == 1
	}, 5*time.Second, 20*time.Millisecond)
}

func TestMetricsCountRefusalsOnARequiredListener(t *testing.T) {
	t.Parallel()

	fx := newFixture(t, serverDNS)
	cfg := newServerConfig(t, fx, StaticMode(ModeRequired), clientID)
	_, addr := serveOn(t, cfg, &http.Server{Handler: describe(), ReadHeaderTimeout: time.Second})

	// The client learns of a refusal from the alert before the listener's
	// goroutine has recorded it, so these counters are awaited, not read once.
	require.Error(t, getErr(t, httpClient(rawClientTLS(fx.peerLeaf(t, strangerID), protoHTTP11)), "https://"+addr+"/"))
	awaitPoint(t, fx.reader, 1, MetricHandshakes, listenerAttr(), attribute.String(AttrOutcome, OutcomeRefused))
	awaitPoint(t, fx.reader, 1, MetricRefused, listenerAttr(), attribute.String(AttrReason, string(ReasonNotAllowed)), attribute.String(AttrKind, KindHandshake))

	require.Error(t, getErr(t, httpClient(rawClientTLS(nil, protoHTTP11)), "https://"+addr+"/"), "no certificate under RequireAnyClientCert fails inside Go's handshake")
	awaitPoint(t, fx.reader, 1, MetricHandshakes, listenerAttr(), attribute.String(AttrOutcome, OutcomeFailed))
}

func TestMetricsCountPlaintextRequestsAndRPCs(t *testing.T) {
	t.Parallel()

	fx := newFixture(t, serverDNS)
	permissive := newServerConfig(t, fx, StaticMode(ModePermissive), clientID)

	code, _, _ := guarded(t, permissive, []string{"/health"}, plainRequest(t, http.MethodGet, "/health"))
	require.Equal(t, http.StatusNoContent, code)
	_, counted := pointValue(t, fx.reader, MetricPlaintextRequests, listenerAttr(), attribute.String(AttrKind, KindHTTP))
	assert.False(t, counted, "a health probe is not a plaintext request")

	code, _, _ = guarded(t, permissive, []string{"/health"}, plainRequest(t, http.MethodPost, "/admin"))
	require.Equal(t, http.StatusNoContent, code)
	assert.Equal(t, int64(1), mustPoint(t, fx.reader, MetricPlaintextRequests, listenerAttr(), attribute.String(AttrKind, KindHTTP)))

	_, _, err := callUnary(t, permissive, peerContext(t, plaintextAuthInfo()), businessMethod)
	require.NoError(t, err)
	_, _, err = callUnary(t, permissive, peerContext(t, plaintextAuthInfo()), healthMethod)
	require.NoError(t, err)
	assert.Equal(t, int64(1), mustPoint(t, fx.reader, MetricPlaintextRequests, listenerAttr(), attribute.String(AttrKind, KindGRPC)))

	_, _, err = callUnary(t, permissive, tlsPeer(t, fx.peerLeaf(t, strangerID).Cert), businessMethod)
	require.NoError(t, err)
	assert.Equal(t, int64(1), mustPoint(t, fx.reader, MetricWouldReject, listenerAttr(), attribute.String(AttrReason, string(ReasonNotAllowed)), attribute.String(AttrKind, KindGRPC)))

	required := newServerConfig(t, fx, StaticMode(ModeRequired), clientID)
	code, _, _ = guarded(t, required, []string{"/health"}, plainRequest(t, http.MethodPost, "/admin"))
	require.Equal(t, http.StatusForbidden, code)
	assert.Equal(t, int64(2), mustPoint(t, fx.reader, MetricPlaintextRequests, listenerAttr(), attribute.String(AttrKind, KindHTTP)), "refused plaintext is still a plaintext request")
	assert.Equal(t, int64(1), mustPoint(t, fx.reader, MetricRefused, listenerAttr(), attribute.String(AttrReason, string(ReasonPlaintext)), attribute.String(AttrKind, KindHTTP)))

	_, _, err = callUnary(t, required, tlsPeer(t, fx.peerLeaf(t, strangerID).Cert), businessMethod)
	require.Error(t, err)
	assert.Equal(t, int64(1), mustPoint(t, fx.reader, MetricRefused, listenerAttr(), attribute.String(AttrReason, string(ReasonNotAllowed)), attribute.String(AttrKind, KindGRPC)))

	// Required with an empty allow-list is served as permissive: the plaintext
	// request is admitted and counted, and the gauge reports the mode in force.
	emptyRequired := newServerConfig(t, fx, StaticMode(ModeRequired))
	emptyRequired.Name = "required-empty-metrics"
	code, _, _ = guarded(t, emptyRequired, []string{"/health"}, plainRequest(t, http.MethodPost, "/admin"))
	require.Equal(t, http.StatusNoContent, code)
	assert.Equal(t, int64(1), mustPoint(t, fx.reader, MetricPlaintextRequests, attribute.String(AttrListener, "required-empty-metrics"), attribute.String(AttrKind, KindHTTP)))
	assert.Equal(t, int64(1), mustPoint(t, fx.reader, MetricMode, attribute.String(AttrListener, "required-empty-metrics"), attribute.String(AttrMode, "permissive"), attribute.String(AttrSource, SourceFallback)))
}

func TestMetricsReportTheModeInForceAndItsSource(t *testing.T) {
	t.Parallel()

	fx := newFixture(t, serverDNS)
	flags := newFakeFlags()
	allow := mustAllow(t, clientID)
	src := NewFlagModeSource(flags, "listener-mode", ModePermissive, allow, WithModeLogger(fx.log))
	cfg := newServerConfig(t, fx, src, clientID)

	cfg.policy(t.Context())
	assert.Equal(t, int64(1), mustPoint(t, fx.reader, MetricMode, listenerAttr(), attribute.String(AttrMode, "permissive"), attribute.String(AttrSource, SourceFallback)))

	flags.set("listener-mode", "required")
	cfg.policy(t.Context())
	assert.Equal(t, int64(1), mustPoint(t, fx.reader, MetricMode, listenerAttr(), attribute.String(AttrMode, "required"), attribute.String(AttrSource, SourceFlag)))
	assert.Equal(t, int64(0), mustPoint(t, fx.reader, MetricMode, listenerAttr(), attribute.String(AttrMode, "permissive"), attribute.String(AttrSource, SourceFallback)), "the series left behind reads zero")

	hop := ClientConfig{Name: "test-hop", Files: fx.peerFiles(t, clientID), Mode: StaticClientMode(ClientOff), ExpectedServerIDs: allow, Logger: fx.log, Metrics: fx.metrics}
	creds := NewClientCredentials(hop)
	serverSide, clientSide := net.Pipe()
	t.Cleanup(func() {
		_ = serverSide.Close()
		_ = clientSide.Close()
	})
	_, _, err := creds.ClientHandshake(t.Context(), "ignored", clientSide)
	require.NoError(t, err)
	assert.Equal(t, int64(1), mustPoint(t, fx.reader, MetricClientMode, attribute.String(AttrClientHop, "test-hop"), attribute.String(AttrMode, "off"), attribute.String(AttrSource, SourceFallback)))
	assert.Equal(t, int64(1), mustPoint(t, fx.reader, MetricHandshakes, attribute.String(AttrClientHop, "test-hop"), attribute.String(AttrOutcome, OutcomePlaintext)))
}

func fingerprint(der []byte) string {
	sum := sha256.Sum256(der)

	return hex.EncodeToString(sum[:])
}

func TestMetricsReportReloadsExpiryAndRoots(t *testing.T) {
	t.Parallel()

	fx := newFixture(t, serverDNS)
	assert.Equal(t, int64(1), mustPoint(t, fx.reader, MetricReloads, attribute.String(AttrOutcome, ReloadLoaded)))
	assert.Equal(t, fx.leaf.Cert.NotAfter.Unix(), mustPoint(t, fx.reader, MetricNotAfter, attribute.String(AttrKind, KindLeaf)))
	assert.Equal(t, fx.leaf.Cert.NotAfter.Unix(), mustPoint(t, fx.reader, MetricNotAfter, attribute.String(AttrKind, KindChain)), "the leaf expires before its intermediate")
	assert.Equal(t, int64(1), mustPoint(t, fx.reader, MetricRootsLoaded, attribute.String(AttrFingerprint, fingerprint(fx.root.Cert.Raw))))

	require.NoError(t, fx.files.Reload(t.Context()))
	assert.Equal(t, int64(1), mustPoint(t, fx.reader, MetricReloads, attribute.String(AttrOutcome, ReloadUnchanged)))

	// corruptPEMBlock and concat come from files_test.go.
	mtlstest.Write(t, fx.dir.CAFile, concat(fx.root.PEM, []byte(corruptPEMBlock)))
	require.Error(t, fx.files.Reload(t.Context()))
	assert.Equal(t, int64(1), mustPoint(t, fx.reader, MetricReloads, attribute.String(AttrOutcome, ReloadFailed)))

	replacement := newFixture(t, serverDNS)
	fx.dir.Rewrite(t, fx.leaf, replacement.root.BundlePEM())
	require.NoError(t, fx.files.Reload(t.Context()))
	assert.Equal(t, int64(0), mustPoint(t, fx.reader, MetricRootsLoaded, attribute.String(AttrFingerprint, fingerprint(fx.root.Cert.Raw))), "a root that left the bundle reads zero")
	assert.Equal(t, int64(1), mustPoint(t, fx.reader, MetricRootsLoaded, attribute.String(AttrFingerprint, fingerprint(replacement.root.Cert.Raw))))
	assert.Equal(t, int64(2), mustPoint(t, fx.reader, MetricReloads, attribute.String(AttrOutcome, ReloadLoaded)))
}

func TestNilMetricsRecordNothing(t *testing.T) {
	t.Parallel()

	var m *Metrics
	m.handshake(t.Context(), "l", OutcomeTLS)
	m.clientHandshake(t.Context(), "h", OutcomeTLS)
	m.refused(t.Context(), "l", ReasonNotAllowed, KindGRPC)
	m.wouldReject(t.Context(), "l", ReasonNotAllowed, KindGRPC)
	m.plaintextRequest(t.Context(), "l", KindHTTP)
	m.plaintextConn(t.Context(), "l", 1)
	m.connection(t.Context(), "l", clientID)
	m.recordMode(t.Context(), "l", ModeOff, SourceFallback)
	m.recordClientMode(t.Context(), "h", ClientOff, SourceFallback)
	m.reload(t.Context(), ReloadLoaded)
	m.bundleLoaded(t.Context(), nil, nil)
}

// slowGauge is an Int64Gauge whose first write stalls, so a second flip of
// the same key can run while the first is still writing.
type slowGauge struct {
	embedded.Int64Gauge

	mu      sync.Mutex
	writes  int
	reading map[attribute.Distinct]int64
}

func (g *slowGauge) Record(_ context.Context, value int64, opts ...metric.RecordOption) {
	g.mu.Lock()
	g.writes++
	first := g.writes == 1
	g.mu.Unlock()
	if first {
		time.Sleep(50 * time.Millisecond)
	}

	set := metric.NewRecordConfig(opts).Attributes()
	g.mu.Lock()
	defer g.mu.Unlock()
	g.reading[set.Equivalent()] = value
}

func (g *slowGauge) Enabled(context.Context) bool { return true }

func TestMetricsFlipLeavesOneSeriesReadingOne(t *testing.T) {
	t.Parallel()

	m := &Metrics{lastMode: map[string]attribute.Set{}}
	gauge := &slowGauge{reading: map[attribute.Distinct]int64{}}
	off := attribute.NewSet(attribute.String(AttrListener, "edge"), attribute.String(AttrMode, ModeOff.String()))
	required := attribute.NewSet(attribute.String(AttrListener, "edge"), attribute.String(AttrMode, ModeRequired.String()))

	var wg sync.WaitGroup
	wg.Go(func() { m.flip(t.Context(), gauge, m.lastMode, "edge", off) })
	time.Sleep(10 * time.Millisecond) // the first flip is now inside its stalled write
	wg.Go(func() { m.flip(t.Context(), gauge, m.lastMode, "edge", required) })
	wg.Wait()

	ones := 0
	for _, v := range gauge.reading {
		if v == 1 {
			ones++
		}
	}
	assert.Equal(t, 1, ones, "one series reads 1")
	last := m.lastMode["edge"]
	assert.Equal(t, int64(1), gauge.reading[last.Equivalent()], "and it is the mode recorded last")
	assert.Equal(t, 3, gauge.writes, "a repeat of the mode in force records nothing")
	m.flip(t.Context(), gauge, m.lastMode, "edge", required)
	assert.Equal(t, 3, gauge.writes)
}
