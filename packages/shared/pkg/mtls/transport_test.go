package mtls

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"

	"github.com/e2b-dev/infra/packages/shared/pkg/mtls/mtlstest"
)

// peerEcho answers with the caller's name the guard admitted.
func peerEcho() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		peer, _ := PeerFromContext(r.Context())
		_, _ = io.WriteString(w, peer.ID)
	})
}

func TestHTTPTransportOffDialsPlaintext(t *testing.T) {
	t.Parallel()

	metrics, reader := testMetrics(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	t.Cleanup(server.Close)

	transport := NewHTTPTransport(ClientConfig{Name: "plain-hop", Mode: StaticClientMode(ClientOff), Metrics: metrics}, nil)
	assert.Equal(t, int64(1), mustPoint(t, reader, MetricClientMode, attribute.String(AttrClientHop, "plain-hop"), attribute.String(AttrMode, "off"), attribute.String(AttrSource, SourceFallback)), "reported before the first dial")

	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	assert.Equal(t, http.StatusNoContent, get(t, client, server.URL).status)
	assert.Equal(t, int64(1), mustPoint(t, reader, MetricHandshakes, attribute.String(AttrClientHop, "plain-hop"), attribute.String(AttrOutcome, OutcomePlaintext)))
}

// On, the hop completes mutual TLS under the http URL with a listener
// that requires it, and the server's guard sees the hop's name.
func TestHTTPTransportOnCompletesMutualTLSWithAListener(t *testing.T) {
	t.Parallel()

	fx := newFixture(t, serverDNS)
	addr := serveWrapped(t, newServerConfig(t, fx, StaticMode(ModeRequired), clientID), peerEcho())
	hop := newClientConfig(t, fx.peerFiles(t, clientID), fx.log, StaticClientMode(ClientOn), serverDNS, serverID)
	hop.Name = "tls-hop"
	hop.Metrics = fx.metrics
	client := &http.Client{Transport: NewHTTPTransport(hop, nil), Timeout: 5 * time.Second}

	res := get(t, client, "http://"+addr+"/data")
	assert.Equal(t, http.StatusOK, res.status)
	assert.Equal(t, clientID, res.body, "the server admitted the hop's verified name")
	assert.Equal(t, int64(1), mustPoint(t, fx.reader, MetricHandshakes, attribute.String(AttrClientHop, "tls-hop"), attribute.String(AttrOutcome, OutcomeTLS)))
}

func TestHTTPTransportRefusesAServerOutsideTheExpectedNames(t *testing.T) {
	t.Parallel()

	fx := newFixture(t, serverDNS)
	addr := serveWrapped(t, newServerConfig(t, fx, StaticMode(ModeRequired), clientID), peerEcho())
	hop := newClientConfig(t, fx.peerFiles(t, clientID), fx.log, StaticClientMode(ClientOn), serverDNS, strangerID)
	hop.Name = "picky-hop"
	hop.Metrics = fx.metrics
	client := &http.Client{Transport: NewHTTPTransport(hop, nil), Timeout: 5 * time.Second}

	require.Error(t, getErr(t, client, "http://"+addr+"/data"))
	assert.Equal(t, int64(1), mustPoint(t, fx.reader, MetricHandshakes, attribute.String(AttrClientHop, "picky-hop"), attribute.String(AttrOutcome, OutcomeRefused)))
}

// A server that never answers the handshake is given up on after the base's
// TLS handshake timeout, as net/http does for https.
func TestHTTPTransportBoundsTheHandshakeByTheBaseTimeout(t *testing.T) {
	t.Parallel()

	metrics, reader := testMetrics(t)
	var lc net.ListenConfig
	silent, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0") // never accepts, so the ClientHello goes unanswered
	require.NoError(t, err)
	t.Cleanup(func() { _ = silent.Close() })
	base := &http.Transport{TLSHandshakeTimeout: 100 * time.Millisecond}
	hop := ClientConfig{Name: "stalled-hop", Mode: StaticClientMode(ClientOn), Metrics: metrics}
	client := &http.Client{Transport: NewHTTPTransport(hop, base), Timeout: 5 * time.Second}

	require.Error(t, getErr(t, client, "http://"+silent.Addr().String()+"/"))
	assert.Equal(t, int64(1), mustPoint(t, reader, MetricHandshakes, attribute.String(AttrClientHop, "stalled-hop"), attribute.String(AttrOutcome, OutcomeTimeout)))
}

func TestHTTPTransportKeepsTheBaseTransportsSettings(t *testing.T) {
	t.Parallel()

	metrics, _ := testMetrics(t)
	base := &http.Transport{MaxIdleConnsPerHost: 7, ForceAttemptHTTP2: true}

	transport := NewHTTPTransport(ClientConfig{Name: "base-hop", Metrics: metrics}, base)

	assert.Equal(t, 7, transport.MaxIdleConnsPerHost)
	assert.False(t, transport.ForceAttemptHTTP2, "an http/1.1 hop never negotiates h2")
	assert.True(t, base.ForceAttemptHTTP2, "the base is copied, not changed")
}

// A proxy in base, as HTTP_PROXY gives http.DefaultTransport, would take
// the hop's connection and its handshake: the hop dials its target directly.
func TestHTTPTransportDialsItsTargetDirectly(t *testing.T) {
	t.Parallel()

	var lc net.ListenConfig
	gone, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	proxy := &url.URL{Scheme: "http", Host: gone.Addr().String()}
	require.NoError(t, gone.Close()) // the proxy refuses every connection
	base := &http.Transport{Proxy: http.ProxyURL(proxy), TLSHandshakeTimeout: 5 * time.Second}

	metrics, _ := testMetrics(t)
	plain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	t.Cleanup(plain.Close)
	off := &http.Client{Transport: NewHTTPTransport(ClientConfig{Name: "plain-hop", Mode: StaticClientMode(ClientOff), Metrics: metrics}, base), Timeout: 5 * time.Second}
	require.NoError(t, getErr(t, off, plain.URL), "off reached the server, not the proxy")

	fx := newFixture(t, serverDNS)
	addr := serveWrapped(t, newServerConfig(t, fx, StaticMode(ModeRequired), clientID), peerEcho())
	hop := newClientConfig(t, fx.peerFiles(t, clientID), fx.log, StaticClientMode(ClientOn), serverDNS, serverID)
	on := &http.Client{Transport: NewHTTPTransport(hop, base), Timeout: 5 * time.Second}
	assert.Equal(t, clientID, get(t, on, "http://"+addr+"/data").body, "on completed mutual TLS with the server")
}

// A pooled keep-alive connection is never dialled again, so when its hop's
// flag flips to on the transport drops the plaintext one; otherwise the hop
// would keep sending in plaintext while its gauge reads on.
func TestHTTPTransportDropsPooledConnectionsWhenItsHopFlips(t *testing.T) {
	t.Parallel()

	fx := newFixture(t, serverDNS)
	var lc net.ListenConfig
	inner, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := &http.Server{Handler: peerEcho(), ReadHeaderTimeout: time.Second}
	l := WrapHTTPServer(server, inner, newServerConfig(t, fx, StaticMode(ModePermissive), clientID), []string{"/health"})
	go func() { _ = server.Serve(l) }()
	t.Cleanup(func() { _ = server.Close() })

	dir := mtlstest.WriteFiles(t, fx.peerLeaf(t, clientID), fx.root.BundlePEM())
	provider, reader := testProvider(t)
	flags := newFakeFlags()
	proc, err := NewProcess(t.Context(), provider, flags, fx.log,
		WithFileConfig(FileConfig{CertFile: dir.CertFile, KeyFile: dir.KeyFile, CAFile: dir.CAFile}), WithWatchInterval(10*time.Millisecond))
	require.NoError(t, err)
	hop, err := proc.Hop("peers", "hop-flag", HopSettings{Expect: []string{serverID}, ServerName: serverDNS})
	require.NoError(t, err)
	dialed := make(chan net.Conn, 4)
	var dialer net.Dialer
	base := &http.Transport{TLSHandshakeTimeout: 5 * time.Second, DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := dialer.DialContext(ctx, network, addr)
		if err == nil {
			dialed <- conn
		}

		return conn, err
	}}
	client := &http.Client{Transport: NewHTTPTransport(hop, base), Timeout: 5 * time.Second}
	target := "http://" + inner.Addr().String() + "/data"

	require.Empty(t, get(t, client, target).body, "off: plaintext, so no name reached the server")
	plain := <-dialed

	flags.set("hop-flag", "on")
	assert.Eventually(t, func() bool {
		_, open := l.ConnState(plain.LocalAddr().String())

		return !open
	}, 5*time.Second, 10*time.Millisecond, "the transport dropped its pooled plaintext connection")

	assert.Equal(t, clientID, get(t, client, target).body, "the second request dialled mutual TLS")
	assert.Equal(t, int64(1), mustPoint(t, reader, MetricHandshakes, attribute.String(AttrClientHop, "peers"), attribute.String(AttrOutcome, OutcomeTLS)))
}
