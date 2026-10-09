package mtls

import (
	"bufio"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/httpserver"
)

// serveOn serves server over a Listener built from cfg on a loopback port.
func serveOn(t *testing.T, cfg ServerConfig, server *http.Server) (*Listener, string) {
	t.Helper()

	var lc net.ListenConfig
	inner, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	l := NewListener(inner, cfg)
	go func() { _ = server.Serve(l) }()
	t.Cleanup(func() { _ = server.Close() })

	return l, inner.Addr().String()
}

// httpClient dials with tlsCfg for https URLs and in plaintext for http URLs.
func httpClient(tlsCfg *tls.Config) *http.Client {
	return &http.Client{
		Transport: &http.Transport{TLSClientConfig: tlsCfg},
		Timeout:   5 * time.Second,
	}
}

// response is what get returns: the parts of an *http.Response the tests
// inspect, with the body already read and closed.
type response struct {
	status   int
	proto    string
	header   http.Header
	tlsState *tls.ConnectionState
	body     string
}

// get performs a GET that must succeed at the transport.
func get(t *testing.T, client *http.Client, url string) response {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	require.NoError(t, err)
	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return response{status: resp.StatusCode, proto: resp.Proto, header: resp.Header, tlsState: resp.TLS, body: string(body)}
}

// getErr performs a GET expected to fail at the transport and returns the
// error, closing the body if a response arrived after all.
func getErr(t *testing.T, client *http.Client, url string) error {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	require.NoError(t, err)
	resp, err := client.Do(req)
	if err == nil {
		_ = resp.Body.Close()
	}

	return err
}

// rawHTTP sends one keep-alive HTTP/1.1 request on conn and returns the
// status, so a test can hold a connection open across a policy change.
func rawHTTP(t *testing.T, conn net.Conn) int {
	t.Helper()

	require.NoError(t, conn.SetDeadline(time.Now().Add(5*time.Second)))
	_, err := io.WriteString(conn, "GET / HTTP/1.1\r\nHost: example\r\n\r\n")
	require.NoError(t, err)
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	require.NoError(t, err)
	defer resp.Body.Close()
	_, err = io.Copy(io.Discard, resp.Body)
	require.NoError(t, err)

	return resp.StatusCode
}

// describe answers with "plain" or "tls:<peer id>" and echoes the remote address.
func describe() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Remote", r.RemoteAddr)
		if r.TLS == nil {
			_, _ = io.WriteString(w, "plain")

			return
		}
		id := ""
		if len(r.TLS.PeerCertificates) > 0 {
			id, _ = PeerID(r.TLS.PeerCertificates[0])
		}
		_, _ = io.WriteString(w, "tls:"+id)
	})
}

func TestListenerOffPassesPlaintextThroughUnread(t *testing.T) {
	t.Parallel()

	fx := newFixture(t, serverDNS)
	cfg := newServerConfig(t, fx, StaticMode(ModeOff), clientID)
	l, addr := serveOn(t, cfg, &http.Server{Handler: describe(), ReadHeaderTimeout: time.Second})

	resp := get(t, httpClient(nil), "http://"+addr+"/")
	assert.Equal(t, http.StatusOK, resp.status)
	assert.Equal(t, "plain", resp.body)
	state, known := l.ConnState(resp.header.Get("X-Remote"))
	require.True(t, known, "off mode records the connection without reading it, so CloseUnverified reaches it later")
	assert.False(t, state.TLS)

	require.Error(t, getErr(t, httpClient(rawClientTLS(fx.peerLeaf(t, clientID), protoHTTP11)), "https://"+addr+"/"), "a TLS client meets a plaintext server")
}

func TestListenerPermissiveServesPlaintextAndTLS(t *testing.T) {
	t.Parallel()

	fx := newFixture(t, serverDNS)
	cfg := newServerConfig(t, fx, StaticMode(ModePermissive), clientID)
	l, addr := serveOn(t, cfg, &http.Server{Handler: describe(), ReadHeaderTimeout: time.Second})

	resp := get(t, httpClient(nil), "http://"+addr+"/")
	assert.Equal(t, http.StatusOK, resp.status)
	assert.Equal(t, "plain", resp.body)
	state, known := l.ConnState(resp.header.Get("X-Remote"))
	require.True(t, known)
	assert.False(t, state.TLS)

	resp = get(t, httpClient(rawClientTLS(fx.peerLeaf(t, clientID), protoHTTP11)), "https://"+addr+"/")
	assert.Equal(t, http.StatusOK, resp.status)
	assert.Equal(t, "tls:"+clientID, resp.body, "r.TLS carries the peer certificate")
	state, known = l.ConnState(resp.header.Get("X-Remote"))
	require.True(t, known)
	assert.True(t, state.TLS)
	require.Len(t, state.State.PeerCertificates, 2, "the leaf and the intermediate it chains through")

	resp = get(t, httpClient(rawClientTLS(nil, protoHTTP11)), "https://"+addr+"/")
	assert.Equal(t, http.StatusOK, resp.status, "permissive admits TLS without a certificate")
	assert.Equal(t, "tls:", resp.body)

	resp = get(t, httpClient(rawClientTLS(fx.peerLeaf(t, strangerID), protoHTTP11)), "https://"+addr+"/")
	assert.Equal(t, http.StatusOK, resp.status, "permissive admits a name not on the list")
	assert.Equal(t, "tls:"+strangerID, resp.body)
}

func TestListenerRequiredRefusesAtTheHandshakeAndStillDeliversPlaintext(t *testing.T) {
	t.Parallel()

	fx := newFixture(t, serverDNS)
	cfg := newServerConfig(t, fx, StaticMode(ModeRequired), clientID)
	_, addr := serveOn(t, cfg, &http.Server{Handler: describe(), ReadHeaderTimeout: time.Second})

	// Plaintext reaches the handler: admitting or refusing it is the HTTP guard's job.
	resp := get(t, httpClient(nil), "http://"+addr+"/health")
	assert.Equal(t, http.StatusOK, resp.status)
	assert.Equal(t, "plain", resp.body)

	resp = get(t, httpClient(rawClientTLS(fx.peerLeaf(t, clientID), protoHTTP11)), "https://"+addr+"/")
	assert.Equal(t, http.StatusOK, resp.status)
	assert.Equal(t, "tls:"+clientID, resp.body)

	require.Error(t, getErr(t, httpClient(rawClientTLS(fx.peerLeaf(t, strangerID), protoHTTP11)), "https://"+addr+"/"), "a name not on the list is refused at the handshake")

	require.Error(t, getErr(t, httpClient(rawClientTLS(nil, protoHTTP11)), "https://"+addr+"/"), "no certificate is refused at the handshake")
}

func TestListenerOffersOnlyHTTP11OverTLS(t *testing.T) {
	t.Parallel()

	fx := newFixture(t, serverDNS)
	cfg := newServerConfig(t, fx, StaticMode(ModePermissive), clientID)
	server := &http.Server{Handler: describe(), ReadHeaderTimeout: time.Second}
	// ConfigureH2C registers h2 on server.TLSConfig and TLSNextProto; the
	// listener's own tls.Config must not pick it up.
	httpserver.ConfigureH2C(server)
	_, addr := serveOn(t, cfg, server)

	resp := get(t, httpClient(rawClientTLS(fx.peerLeaf(t, clientID), protoH2, protoHTTP11)), "https://"+addr+"/")
	assert.Equal(t, "HTTP/1.1", resp.proto)
	require.NotNil(t, resp.tlsState)
	assert.Equal(t, protoHTTP11, resp.tlsState.NegotiatedProtocol)
}

// stallingConn sends what tls.Client writes and never delivers a reply, so
// the client's handshake stalls after its ClientHello.
type stallingConn struct {
	net.Conn

	done chan struct{}
}

func (c *stallingConn) Read([]byte) (int, error) {
	<-c.done

	return 0, io.EOF
}

func TestListenerStalledConnectionsDelayNobody(t *testing.T) {
	t.Parallel()

	fx := newFixture(t, serverDNS)
	cfg := newServerConfig(t, fx, StaticMode(ModePermissive), clientID)
	cfg.HandshakeTimeout = 2 * time.Second
	_, addr := serveOn(t, cfg, &http.Server{Handler: describe(), ReadHeaderTimeout: time.Second})

	var dialer net.Dialer
	dial := func() net.Conn {
		conn, err := dialer.DialContext(t.Context(), "tcp", addr)
		require.NoError(t, err)
		t.Cleanup(func() { _ = conn.Close() })

		return conn
	}

	silent := dial()
	oneByte := dial()
	_, err := oneByte.Write([]byte{tlsHandshakeRecord})
	require.NoError(t, err)
	fragmented := dial()
	_, err = fragmented.Write([]byte{0x16, 0x03, 0x01, 0x00, 0x80, 0x01})
	require.NoError(t, err)

	midHandshake := dial()
	stall := &stallingConn{Conn: midHandshake, done: make(chan struct{})}
	stalledClient := rawClientTLS(fx.peerLeaf(t, clientID), protoHTTP11)
	// Cleanups run last-registered first: the channel closes before the wait,
	// so the stalled handshake goroutine returns.
	var handshakes sync.WaitGroup
	t.Cleanup(handshakes.Wait)
	handshakes.Go(func() {
		_ = tls.Client(stall, stalledClient).HandshakeContext(t.Context())
	})
	t.Cleanup(func() { close(stall.done) })

	// While four connections hang, a healthy caller is served at once.
	start := time.Now()
	resp := get(t, httpClient(nil), "http://"+addr+"/")
	assert.Equal(t, http.StatusOK, resp.status)
	assert.Equal(t, "plain", resp.body)
	assert.Less(t, time.Since(start), time.Second, "stalled connections do not delay others")

	resp = get(t, httpClient(rawClientTLS(fx.peerLeaf(t, clientID), protoHTTP11)), "https://"+addr+"/")
	assert.Equal(t, http.StatusOK, resp.status)
	assert.Equal(t, "tls:"+clientID, resp.body)

	// Each stalled connection is closed by the server once the deadline
	// passes. Drained to EOF rather than read once: the mid-handshake socket
	// still holds the server's ServerHello flight.
	for name, conn := range map[string]net.Conn{"silent": silent, "one byte": oneByte, "fragmented": fragmented, "mid-handshake": midHandshake} {
		require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
		_, err := io.Copy(io.Discard, conn)
		assert.False(t, isTimeout(err), "%s connection was closed, not left open: %v", name, err)
	}
}

func TestListenerCloseStopsAccepting(t *testing.T) {
	t.Parallel()

	fx := newFixture(t, serverDNS)
	cfg := newServerConfig(t, fx, StaticMode(ModePermissive), clientID)

	var lc net.ListenConfig
	inner, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	l := NewListener(inner, cfg)
	require.Equal(t, inner.Addr(), l.Addr())

	require.NoError(t, l.Close())
	_, err = l.Accept()
	require.ErrorIs(t, err, net.ErrClosed)
	require.NoError(t, l.Close(), "closing twice is fine")

	// A connection in the middle of classification when the listener closes is dropped, not leaked.
	inner2, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	l2 := NewListener(inner2, cfg)
	var dialer net.Dialer
	conn, err := dialer.DialContext(t.Context(), "tcp", inner2.Addr().String())
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	_, err = conn.Write([]byte("GET / HTTP/1.1\r\n"))
	require.NoError(t, err)
	require.NoError(t, l2.Close())
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	_, err = conn.Read(make([]byte, 1))
	require.Error(t, err)
	assert.False(t, isTimeout(err))
}

func TestListenerRequiredWithAnEmptyAllowListServesPermissive(t *testing.T) {
	t.Parallel()

	fx := newFixture(t, serverDNS)
	cfg := newServerConfig(t, fx, StaticMode(ModeRequired))
	l, addr := serveOn(t, cfg, &http.Server{Handler: describe(), ReadHeaderTimeout: time.Second})

	resp := get(t, httpClient(nil), "http://"+addr+"/admin")
	assert.Equal(t, http.StatusOK, resp.status, "plaintext is admitted, as permissive admits it")
	assert.Equal(t, "plain", resp.body)
	state, known := l.ConnState(resp.header.Get("X-Remote"))
	require.True(t, known, "the connection is classified and counted")
	assert.False(t, state.TLS)

	resp = get(t, httpClient(rawClientTLS(fx.peerLeaf(t, strangerID), protoHTTP11)), "https://"+addr+"/admin")
	assert.Equal(t, http.StatusOK, resp.status, "a name no empty list could hold is served")
	assert.Equal(t, "tls:"+strangerID, resp.body)
}

func TestListenerCloseUnverifiedAndCloseByPeer(t *testing.T) {
	t.Parallel()

	fx := newFixture(t, serverDNS)
	cfg := newServerConfig(t, fx, StaticMode(ModePermissive), clientID, strangerID)
	l, addr := serveOn(t, cfg, &http.Server{Handler: describe(), ReadHeaderTimeout: time.Second})

	var dialer net.Dialer
	plain, err := dialer.DialContext(t.Context(), "tcp", addr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = plain.Close() })
	require.Equal(t, http.StatusOK, rawHTTP(t, plain))

	dialTLSAs := func(tlsCfg *tls.Config) net.Conn {
		tlsDialer := tls.Dialer{NetDialer: &dialer, Config: tlsCfg}
		conn, err := tlsDialer.DialContext(t.Context(), "tcp", addr)
		require.NoError(t, err)
		t.Cleanup(func() { _ = conn.Close() })
		require.Equal(t, http.StatusOK, rawHTTP(t, conn))

		return conn
	}
	client := dialTLSAs(rawClientTLS(fx.peerLeaf(t, clientID), protoHTTP11))
	stranger := dialTLSAs(rawClientTLS(fx.peerLeaf(t, strangerID), protoHTTP11))
	// Trusted certificate, name not on the list: permissive admits it.
	outsider := dialTLSAs(rawClientTLS(fx.peerLeaf(t, serverID), protoHTTP11))
	_, known := l.ConnState(plain.LocalAddr().String())
	require.True(t, known)

	// A flip to required: the plaintext keep-alive connection and the one
	// whose name required would refuse are closed, the allowed TLS ones stay.
	closed, err := l.CloseUnverified()
	require.NoError(t, err)
	assert.Equal(t, 2, closed)
	require.NoError(t, plain.SetReadDeadline(time.Now().Add(5*time.Second)))
	_, err = io.Copy(io.Discard, plain)
	assert.False(t, isTimeout(err), "the plaintext connection was closed: %v", err)
	require.NoError(t, outsider.SetReadDeadline(time.Now().Add(5*time.Second)))
	_, err = io.Copy(io.Discard, outsider)
	assert.False(t, isTimeout(err), "the connection of a name required refuses was closed: %v", err)
	_, known = l.ConnState(plain.LocalAddr().String())
	assert.False(t, known, "a closed connection leaves the registry")
	require.Equal(t, http.StatusOK, rawHTTP(t, client), "TLS connections are untouched")

	// A name removed from the list: its keep-alive connection ends, the other name's stays.
	assert.Equal(t, 1, l.CloseByPeer(clientID))
	require.NoError(t, client.SetReadDeadline(time.Now().Add(5*time.Second)))
	_, err = io.Copy(io.Discard, client)
	assert.False(t, isTimeout(err), "the removed name's connection was closed: %v", err)
	require.Equal(t, http.StatusOK, rawHTTP(t, stranger), "the other name's connection is untouched")
	assert.Equal(t, 0, l.CloseByPeer(clientID), "nothing left to close")
}

func TestListenerConnStateForgetsAClosedConnection(t *testing.T) {
	t.Parallel()

	fx := newFixture(t, serverDNS)
	cfg := newServerConfig(t, fx, StaticMode(ModePermissive), clientID)
	l, addr := serveOn(t, cfg, &http.Server{Handler: describe(), ReadHeaderTimeout: time.Second})

	client := httpClient(rawClientTLS(fx.peerLeaf(t, clientID), protoHTTP11))
	resp := get(t, client, "https://"+addr+"/")
	remote := resp.header.Get("X-Remote")
	_, known := l.ConnState(remote)
	require.True(t, known)

	client.CloseIdleConnections()
	require.Eventually(t, func() bool {
		_, known := l.ConnState(remote)

		return !known
	}, 5*time.Second, 10*time.Millisecond, "a closed connection leaves the registry")
}
