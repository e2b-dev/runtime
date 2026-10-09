package mtls

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"

	"github.com/e2b-dev/infra/packages/shared/pkg/httpserver"
)

// guarded runs one request through a guard over cfg and reports the status
// and the peer the application handler saw.
func guarded(t *testing.T, cfg ServerConfig, healthPaths []string, req *http.Request, opts ...GuardOption) (int, Peer, bool) {
	t.Helper()

	var seen Peer
	var reached bool
	app := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen, reached = PeerFromContext(r.Context())
		w.WriteHeader(http.StatusNoContent)
	})
	rec := httptest.NewRecorder()
	NewHTTPGuard(cfg, healthPaths, app, opts...).ServeHTTP(rec, req)

	return rec.Code, seen, reached
}

func plainRequest(t *testing.T, method, target string) *http.Request {
	t.Helper()

	return httptest.NewRequestWithContext(t.Context(), method, target, nil)
}

// tlsRequest is a request to a business path over a connection whose peer
// presented cert and whose chain Go itself verified, as a server with
// VerifyClientCertIfGiven would record it.
func tlsRequest(t *testing.T, method string, cert *x509.Certificate) *http.Request {
	t.Helper()

	req := plainRequest(t, method, "/admin")
	req.TLS = &tls.ConnectionState{HandshakeComplete: true, PeerCertificates: []*x509.Certificate{cert}, VerifiedChains: [][]*x509.Certificate{{cert}}}

	return req
}

// unverifiedTLSRequest is a request over a connection this package's listener
// terminated: r.TLS is set, but Go recorded no verified chain, because the
// package verifies in VerifyConnection and keeps the verdict in its registry.
func unverifiedTLSRequest(t *testing.T, method, target string, cert *x509.Certificate) *http.Request {
	t.Helper()

	req := plainRequest(t, method, target)
	req.TLS = &tls.ConnectionState{HandshakeComplete: true, PeerCertificates: []*x509.Certificate{cert}}

	return req
}

func TestHTTPGuardWithoutALookupTakesOnlyGoVerdicts(t *testing.T) {
	t.Parallel()

	fx := newFixture(t, serverDNS)
	cert := fx.peerLeaf(t, clientID).Cert
	required := newServerConfig(t, fx, StaticMode(ModeRequired), clientID)

	// No lookup and no Go verdict: required cannot tell this connection from
	// one admitted under permissive with a forged chain, so it is plaintext.
	code, _, reached := guarded(t, required, []string{"/health"}, unverifiedTLSRequest(t, http.MethodPost, "/admin", cert))
	assert.Equal(t, http.StatusForbidden, code, "an unrecorded verdict is not a verified chain")
	assert.False(t, reached)
	code, seen, _ := guarded(t, required, []string{"/health"}, unverifiedTLSRequest(t, http.MethodGet, "/health", cert))
	assert.Equal(t, http.StatusNoContent, code)
	assert.Equal(t, Peer{TLS: true}, seen)

	// Go's own verdict counts: a server that let Go verify the chain is served.
	code, seen, _ = guarded(t, required, nil, tlsRequest(t, http.MethodPost, cert))
	assert.Equal(t, http.StatusNoContent, code)
	assert.Equal(t, Peer{ID: clientID, TLS: true, ChainVerified: true}, seen)

	permissive := newServerConfig(t, fx, StaticMode(ModePermissive), clientID)
	code, seen, _ = guarded(t, permissive, nil, unverifiedTLSRequest(t, http.MethodPost, "/admin", cert))
	assert.Equal(t, http.StatusNoContent, code, "permissive admits, as it did at the handshake")
	assert.Equal(t, Peer{ID: clientID, TLS: true, ChainVerified: false}, seen)
	assert.Equal(t, int64(1), wouldRejectCount(t, fx, ReasonUnverifiedChain, KindHTTP), "and counts what required would refuse")
}

func TestHTTPGuardRequiredAdmitsPlaintextOnlyToHealth(t *testing.T) {
	t.Parallel()

	fx := newFixture(t, serverDNS)
	cfg := newServerConfig(t, fx, StaticMode(ModeRequired), clientID)
	paths := []string{"/health", "/ready"}

	for _, tc := range []struct {
		method string
		target string
	}{
		{method: http.MethodGet, target: "/health"},
		{method: http.MethodHead, target: "/health"},
		{method: http.MethodGet, target: "/ready"},
		{method: http.MethodGet, target: "/health?probe=1"},
	} {
		code, seen, reached := guarded(t, cfg, paths, plainRequest(t, tc.method, tc.target))
		assert.Equal(t, http.StatusNoContent, code, "%s %s", tc.method, tc.target)
		assert.True(t, reached)
		assert.Equal(t, Peer{}, seen, "a plaintext probe has no identity")
	}

	code, _, reached := guarded(t, cfg, paths, plainRequest(t, http.MethodGet, "/other"))
	assert.Equal(t, http.StatusForbidden, code)
	assert.False(t, reached)

	code, _, reached = guarded(t, cfg, paths, plainRequest(t, http.MethodPost, "/health"))
	assert.Equal(t, http.StatusForbidden, code)
	assert.False(t, reached)
}

func TestHTTPGuardRefusesHealthLookAlikes(t *testing.T) {
	t.Parallel()

	fx := newFixture(t, serverDNS)
	cfg := newServerConfig(t, fx, StaticMode(ModeRequired), clientID)

	for _, tc := range []struct {
		name   string
		method string
		target string
	}{
		{name: "trailing slash", method: http.MethodGet, target: "/health/"},
		{name: "prefix", method: http.MethodGet, target: "/healthz"},
		{name: "dot segments", method: http.MethodGet, target: "/health/../admin"},
		{name: "double slash", method: http.MethodGet, target: "//health"},
		{name: "upper case path", method: http.MethodGet, target: "/HEALTH"},
		{name: "encoded slash", method: http.MethodGet, target: "/health%2F"},
		{name: "encoded letter", method: http.MethodGet, target: "/%68ealth"},
		{name: "lower case method", method: "get", target: "/health"},
		{name: "options", method: http.MethodOptions, target: "/health"},
		{name: "put", method: http.MethodPut, target: "/health"},
	} {
		code, _, reached := guarded(t, cfg, []string{"/health"}, plainRequest(t, tc.method, tc.target))
		assert.Equal(t, http.StatusForbidden, code, tc.name)
		assert.False(t, reached, tc.name)
	}

	// An encoded slash inside a multi-segment health path decodes to the same
	// URL.Path, but a router matching the escaped form serves it elsewhere.
	code, _, reached := guarded(t, cfg, []string{"/-/health"}, plainRequest(t, http.MethodGet, "/-%2Fhealth"))
	assert.Equal(t, http.StatusForbidden, code, "encoded slash inside a health path")
	assert.False(t, reached)
	code, _, _ = guarded(t, cfg, []string{"/-/health"}, plainRequest(t, http.MethodGet, "/-/health"))
	assert.Equal(t, http.StatusNoContent, code)
}

func TestHTTPGuardPermissiveAndOffPassPlaintextThrough(t *testing.T) {
	t.Parallel()

	fx := newFixture(t, serverDNS)
	for _, mode := range []Mode{ModePermissive, ModeOff} {
		cfg := newServerConfig(t, fx, StaticMode(mode), clientID)

		code, seen, reached := guarded(t, cfg, []string{"/health"}, plainRequest(t, http.MethodPost, "/admin"))
		assert.Equal(t, http.StatusNoContent, code, mode)
		assert.True(t, reached, mode)
		assert.Equal(t, Peer{}, seen, mode)
	}
}

func TestHTTPGuardReChecksTheNameOnEveryRequest(t *testing.T) {
	t.Parallel()

	fx := newFixture(t, serverDNS)
	cfg := newServerConfig(t, fx, StaticMode(ModeRequired), clientID)
	client := fx.peerLeaf(t, clientID).Cert
	stranger := fx.peerLeaf(t, strangerID).Cert

	code, seen, reached := guarded(t, cfg, nil, tlsRequest(t, http.MethodPost, client))
	assert.Equal(t, http.StatusNoContent, code)
	assert.True(t, reached)
	assert.Equal(t, Peer{ID: clientID, TLS: true, ChainVerified: true}, seen)

	code, _, reached = guarded(t, cfg, nil, tlsRequest(t, http.MethodPost, stranger))
	assert.Equal(t, http.StatusForbidden, code)
	assert.False(t, reached)

	require.NoError(t, cfg.Allow.Replace([]string{strangerID}))
	code, _, _ = guarded(t, cfg, nil, tlsRequest(t, http.MethodPost, client))
	assert.Equal(t, http.StatusForbidden, code, "a removed name is refused at its next request")
	code, _, _ = guarded(t, cfg, nil, tlsRequest(t, http.MethodPost, stranger))
	assert.Equal(t, http.StatusNoContent, code, "an added name is admitted at its next request")
}

func TestHTTPGuardPermissiveAdmitsATLSPeerOffTheList(t *testing.T) {
	t.Parallel()

	fx := newFixture(t, serverDNS)
	cfg := newServerConfig(t, fx, StaticMode(ModePermissive), clientID)

	code, seen, reached := guarded(t, cfg, nil, tlsRequest(t, http.MethodGet, fx.peerLeaf(t, strangerID).Cert))
	assert.Equal(t, http.StatusNoContent, code)
	assert.True(t, reached)
	assert.Equal(t, Peer{ID: strangerID, TLS: true, ChainVerified: true}, seen)
}

func TestHTTPGuardOffIgnoresTheListForTLSPeers(t *testing.T) {
	t.Parallel()

	fx := newFixture(t, serverDNS)
	cfg := newServerConfig(t, fx, StaticMode(ModeOff), clientID)

	code, seen, reached := guarded(t, cfg, nil, tlsRequest(t, http.MethodGet, fx.peerLeaf(t, strangerID).Cert))
	assert.Equal(t, http.StatusNoContent, code)
	assert.True(t, reached)
	assert.Equal(t, Peer{ID: strangerID, TLS: true, ChainVerified: true}, seen)
}

func TestHTTPGuardUsesTheLookupForMultiplexedConnections(t *testing.T) {
	t.Parallel()

	fx := newFixture(t, serverDNS)
	cfg := newServerConfig(t, fx, StaticMode(ModeRequired), clientID)
	client := fx.peerLeaf(t, clientID).Cert
	lookup := func(remote string) (ConnState, bool) {
		switch remote {
		case "10.0.0.1:40000":
			return ConnState{TLS: true, ChainVerified: true, State: tls.ConnectionState{HandshakeComplete: true, PeerCertificates: []*x509.Certificate{client}}}, true
		case "10.0.0.2:40000":
			return ConnState{}, true
		case "10.0.0.3:40000":
			return ConnState{TLS: true, State: tls.ConnectionState{HandshakeComplete: true, PeerCertificates: []*x509.Certificate{client}}}, true
		default:
			return ConnState{}, false
		}
	}

	req := plainRequest(t, http.MethodPost, "/admin")
	req.RemoteAddr = "10.0.0.1:40000"
	code, seen, _ := guarded(t, cfg, nil, req, WithGuardConnStateLookup(lookup))
	assert.Equal(t, http.StatusNoContent, code, "a connection the listener terminated is TLS even though r.TLS is nil")
	assert.Equal(t, Peer{ID: clientID, TLS: true, ChainVerified: true}, seen)

	req = plainRequest(t, http.MethodPost, "/admin")
	req.RemoteAddr = "10.0.0.2:40000"
	code, _, _ = guarded(t, cfg, nil, req, WithGuardConnStateLookup(lookup))
	assert.Equal(t, http.StatusForbidden, code, "a connection the listener admitted as plaintext stays plaintext")

	// A chain that did not verify at the handshake, admitted under permissive:
	// required treats it as plaintext, health paths included.
	req = plainRequest(t, http.MethodPost, "/admin")
	req.RemoteAddr = "10.0.0.3:40000"
	code, _, reached := guarded(t, cfg, []string{"/health"}, req, WithGuardConnStateLookup(lookup))
	assert.Equal(t, http.StatusForbidden, code, "an allow-listed name on an unverified chain is not an identity")
	assert.False(t, reached)
	req = plainRequest(t, http.MethodGet, "/health")
	req.RemoteAddr = "10.0.0.3:40000"
	code, seen, _ = guarded(t, cfg, []string{"/health"}, req, WithGuardConnStateLookup(lookup))
	assert.Equal(t, http.StatusNoContent, code)
	assert.Equal(t, Peer{TLS: true}, seen, "reachable as a probe, with no identity")

	// The lookup's verdict wins over r.TLS, which Go leaves without one.
	req = tlsRequest(t, http.MethodPost, client)
	req.RemoteAddr = "10.0.0.3:40000"
	code, _, _ = guarded(t, cfg, nil, req, WithGuardConnStateLookup(lookup))
	assert.Equal(t, http.StatusForbidden, code)
}

// guardedServer serves app behind the guard on a Listener with h2c enabled,
// the way a service wires it: the listener first, the guard with the
// listener's lookup around the application, then ConfigureH2C, then Serve.
func guardedServer(t *testing.T, cfg ServerConfig, app http.Handler) string {
	t.Helper()

	var lc net.ListenConfig
	inner, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	l := NewListener(inner, cfg)
	server := &http.Server{
		Handler:           NewHTTPGuard(cfg, []string{"/health"}, app, WithGuardConnStateLookup(l.ConnState)),
		ReadHeaderTimeout: 5 * time.Second,
	}
	httpserver.ConfigureH2C(server)
	go func() { _ = server.Serve(l) }()
	t.Cleanup(func() { _ = server.Close() })

	return inner.Addr().String()
}

func countingApp(business *atomic.Int32) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health" {
			business.Add(1)
		}
		w.WriteHeader(http.StatusNoContent)
	})
}

func TestHTTPGuardChecksEveryPriorKnowledgeH2CStream(t *testing.T) {
	t.Parallel()

	fx := newFixture(t, serverDNS)
	cfg := newServerConfig(t, fx, StaticMode(ModeRequired), clientID)
	var business atomic.Int32
	addr := guardedServer(t, cfg, countingApp(&business))

	client := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http2.Transport{
			AllowHTTP: true,
			DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, network, addr)
			},
		},
	}

	resp := get(t, client, "http://"+addr+"/health")
	assert.Equal(t, http.StatusNoContent, resp.status)
	assert.Equal(t, "HTTP/2.0", resp.proto)

	resp = get(t, client, "http://"+addr+"/admin")
	assert.Equal(t, http.StatusForbidden, resp.status, "the next stream on the same plaintext connection is refused")
	assert.Equal(t, "HTTP/2.0", resp.proto)
	assert.Zero(t, business.Load())

	resp = get(t, httpClient(rawClientTLS(fx.peerLeaf(t, clientID), protoHTTP11)), "https://"+addr+"/admin")
	assert.Equal(t, http.StatusNoContent, resp.status, "the same path over mTLS is served")
	assert.Equal(t, int32(1), business.Load())
}

func TestHTTPGuardUpgradeOnHealthCarriesNothingElse(t *testing.T) {
	t.Parallel()

	fx := newFixture(t, serverDNS)
	cfg := newServerConfig(t, fx, StaticMode(ModeRequired), clientID)
	var business atomic.Int32
	addr := guardedServer(t, cfg, countingApp(&business))

	var dialer net.Dialer
	conn, err := dialer.DialContext(t.Context(), "tcp", addr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	require.NoError(t, conn.SetDeadline(time.Now().Add(10*time.Second)))

	// Stream 1 is the GET /health that carries the upgrade.
	_, err = fmt.Fprintf(conn, "GET /health HTTP/1.1\r\nHost: example\r\nConnection: Upgrade, HTTP2-Settings\r\nUpgrade: h2c\r\nHTTP2-Settings: AAMAAABkAAQAAP__\r\n\r\n")
	require.NoError(t, err)
	reader := bufio.NewReader(conn)
	resp, err := http.ReadResponse(reader, nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	_, err = io.WriteString(conn, http2.ClientPreface)
	require.NoError(t, err)
	framer := http2.NewFramer(conn, reader)
	require.NoError(t, framer.WriteSettings())
	decoder := hpack.NewDecoder(4096, nil)
	assert.Equal(t, "204", readStatus(t, framer, decoder, 1), "the upgraded health request is served")

	// Stream 3 rides the same plaintext connection to a business path.
	var block bytes.Buffer
	encoder := hpack.NewEncoder(&block)
	for _, field := range []hpack.HeaderField{
		{Name: ":method", Value: "GET"},
		{Name: ":path", Value: "/admin"},
		{Name: ":scheme", Value: "http"},
		{Name: ":authority", Value: "example"},
	} {
		require.NoError(t, encoder.WriteField(field))
	}
	require.NoError(t, framer.WriteHeaders(http2.HeadersFrameParam{StreamID: 3, BlockFragment: block.Bytes(), EndStream: true, EndHeaders: true}))
	assert.Equal(t, "403", readStatus(t, framer, decoder, 3), "the second stream is refused")
	assert.Zero(t, business.Load(), "the application never saw the second stream")
}

// readStatus reads frames until the HEADERS of streamID and returns its
// :status, acknowledging SETTINGS and decoding every header block on the way
// so the HPACK table stays in step.
func readStatus(t *testing.T, framer *http2.Framer, decoder *hpack.Decoder, streamID uint32) string {
	t.Helper()

	for {
		frame, err := framer.ReadFrame()
		require.NoError(t, err)

		switch f := frame.(type) {
		case *http2.SettingsFrame:
			if !f.IsAck() {
				require.NoError(t, framer.WriteSettingsAck())
			}
		case *http2.HeadersFrame:
			fields, err := decoder.DecodeFull(f.HeaderBlockFragment())
			require.NoError(t, err)
			if f.StreamID != streamID {
				continue
			}
			for _, field := range fields {
				if field.Name == ":status" {
					return field.Value
				}
			}
			t.Fatalf("no :status on stream %d", streamID)
		case *http2.GoAwayFrame:
			t.Fatalf("server sent GOAWAY: %v", f.ErrCode)
		}
	}
}
