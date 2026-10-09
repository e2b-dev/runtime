package mtls

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/e2b-dev/infra/packages/shared/pkg/mtls/mtlstest"
)

// serverOutcome is what one ServerHandshake produced.
type serverOutcome struct {
	info credentials.AuthInfo
	err  error
	// echo holds the first three application bytes of a plaintext connection.
	echo []byte
}

// connSet closes every accepted connection when the test ends.
type connSet struct {
	mu    sync.Mutex
	conns []net.Conn
}

func (s *connSet) add(c net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.conns = append(s.conns, c)
}

func (s *connSet) closeAll() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, c := range s.conns {
		_ = c.Close()
	}
}

// serveCredentials accepts loopback connections and runs ServerHandshake on
// each in its own goroutine, like gRPC does. On success it writes one byte
// so the client learns it was admitted, and for a plaintext connection it
// then reads three bytes back.
func serveCredentials(t *testing.T, creds *ServerCredentials) (string, <-chan serverOutcome) {
	t.Helper()

	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })

	accepted := &connSet{}
	t.Cleanup(accepted.closeAll)
	outcomes := make(chan serverOutcome, 64)

	go func() {
		for {
			raw, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				conn, info, err := creds.ServerHandshake(raw)
				outcome := serverOutcome{info: info, err: err}
				if err == nil {
					accepted.add(conn)
					_, _ = conn.Write([]byte{1})
					if _, plaintext := info.(PlaintextAuthInfo); plaintext {
						outcome.echo = make([]byte, 3)
						_, _ = io.ReadFull(conn, outcome.echo)
					}
				}
				outcomes <- outcome
			}()
		}
	}()

	return ln.Addr().String(), outcomes
}

// dialTLS dials addr with cfg and waits for the admission byte.
func dialTLS(t *testing.T, addr string, cfg *tls.Config) (*tls.Conn, error) {
	t.Helper()

	dialer := tls.Dialer{NetDialer: &net.Dialer{Timeout: 5 * time.Second}, Config: cfg}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	tlsConn, ok := conn.(*tls.Conn)
	require.True(t, ok)
	t.Cleanup(func() { _ = tlsConn.Close() })

	require.NoError(t, tlsConn.SetReadDeadline(time.Now().Add(5*time.Second)))
	if _, err := io.ReadFull(tlsConn, make([]byte, 1)); err != nil {
		return tlsConn, err
	}

	return tlsConn, nil
}

func awaitOutcome(t *testing.T, outcomes <-chan serverOutcome) serverOutcome {
	t.Helper()

	select {
	case outcome := <-outcomes:
		return outcome
	case <-time.After(5 * time.Second):
		t.Fatal("no handshake outcome within 5s")

		return serverOutcome{}
	}
}

func TestServerCredentialsRequiredAdmission(t *testing.T) {
	t.Parallel()

	fx := newFixture(t, serverDNS)
	for _, tc := range admissionCases(t, fx) {
		creds := NewServerCredentials(newServerConfig(t, fx, StaticMode(ModeRequired), clientID))
		addr, outcomes := serveCredentials(t, creds)

		_, err := dialTLS(t, addr, tc.client)
		outcome := awaitOutcome(t, outcomes)

		if tc.reason == "" {
			require.NoError(t, err, tc.name)
			require.NoError(t, outcome.err, tc.name)
			info, ok := outcome.info.(TLSAuthInfo)
			require.True(t, ok, tc.name)
			assert.True(t, info.ChainVerified, tc.name)
			assert.Equal(t, credentials.PrivacyAndIntegrity, info.SecurityLevel, tc.name)
			require.NotNil(t, info.SPIFFEID, tc.name)
			assert.Equal(t, clientID, info.SPIFFEID.String(), tc.name)
			assert.Equal(t, protoH2, info.State.NegotiatedProtocol, tc.name)

			continue
		}

		require.Error(t, err, tc.name)
		require.Error(t, outcome.err, tc.name)
		if tc.reason == ReasonNoCertificate {
			continue
		}
		var rejection *RejectionError
		require.ErrorAs(t, outcome.err, &rejection, tc.name)
		assert.Equal(t, tc.reason, rejection.Reason, tc.name)
	}
}

func TestServerCredentialsPermissiveAdmitsEveryTLSPeer(t *testing.T) {
	t.Parallel()

	fx := newFixture(t, serverDNS)
	for _, tc := range admissionCases(t, fx) {
		creds := NewServerCredentials(newServerConfig(t, fx, StaticMode(ModePermissive), clientID))
		addr, outcomes := serveCredentials(t, creds)

		_, err := dialTLS(t, addr, tc.client)
		require.NoError(t, err, tc.name)
		outcome := awaitOutcome(t, outcomes)
		require.NoError(t, outcome.err, tc.name)
		_, ok := outcome.info.(TLSAuthInfo)
		assert.True(t, ok, "%s: the connection is TLS even when it would be refused", tc.name)
	}
}

func TestServerCredentialsAdmitsPlaintextWithAMarker(t *testing.T) {
	t.Parallel()

	fx := newFixture(t, serverDNS)
	for _, mode := range []Mode{ModePermissive, ModeRequired} {
		creds := NewServerCredentials(newServerConfig(t, fx, StaticMode(mode), clientID))
		addr, outcomes := serveCredentials(t, creds)

		var dialer net.Dialer
		conn, err := dialer.DialContext(t.Context(), "tcp", addr)
		require.NoError(t, err)
		t.Cleanup(func() { _ = conn.Close() })
		_, err = conn.Write([]byte("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"))
		require.NoError(t, err)

		require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
		_, err = io.ReadFull(conn, make([]byte, 1))
		require.NoError(t, err, "%s: the plaintext connection is handed over, the interceptors decide", mode)

		outcome := awaitOutcome(t, outcomes)
		require.NoError(t, outcome.err)
		info, ok := outcome.info.(PlaintextAuthInfo)
		require.True(t, ok, "%s: plaintext is marked", mode)
		assert.Equal(t, credentials.NoSecurity, info.SecurityLevel)
		assert.Equal(t, "plaintext", info.AuthType())
		assert.Equal(t, "PRI", string(outcome.echo), "the peeked byte is replayed")
	}
}

func TestServerCredentialsOffPassesThroughWithoutReading(t *testing.T) {
	t.Parallel()

	fx := newFixture(t, serverDNS)
	creds := NewServerCredentials(newServerConfig(t, fx, StaticMode(ModeOff), clientID))
	addr, outcomes := serveCredentials(t, creds)

	var dialer net.Dialer
	conn, err := dialer.DialContext(t.Context(), "tcp", addr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	// Nothing has been sent, and the server has already handed the connection over.
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(time.Second)))
	_, err = io.ReadFull(conn, make([]byte, 1))
	require.NoError(t, err, "off mode does not wait for a first byte")

	_, err = conn.Write([]byte("PRI"))
	require.NoError(t, err)
	outcome := awaitOutcome(t, outcomes)
	require.NoError(t, outcome.err)
	_, ok := outcome.info.(PlaintextAuthInfo)
	assert.True(t, ok)
	assert.Equal(t, "PRI", string(outcome.echo))
}

func TestServerCredentialsRequireALPNH2(t *testing.T) {
	t.Parallel()

	fx := newFixture(t, serverDNS)
	creds := NewServerCredentials(newServerConfig(t, fx, StaticMode(ModeRequired), clientID))
	addr, outcomes := serveCredentials(t, creds)

	_, err := dialTLS(t, addr, rawClientTLS(fx.peerLeaf(t, clientID)))
	require.Error(t, err, "a client that does not advertise h2 is closed")

	outcome := awaitOutcome(t, outcomes)
	var rejection *RejectionError
	require.ErrorAs(t, outcome.err, &rejection)
	assert.Equal(t, ReasonNoALPN, rejection.Reason)
}

func TestServerCredentialsStalledConnectionsDelayNobody(t *testing.T) {
	t.Parallel()

	fx := newFixture(t, serverDNS)
	cfg := newServerConfig(t, fx, StaticMode(ModeRequired), clientID)
	cfg.HandshakeTimeout = 2 * time.Second
	addr, outcomes := serveCredentials(t, NewServerCredentials(cfg))

	var dialer net.Dialer
	silent, err := dialer.DialContext(t.Context(), "tcp", addr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = silent.Close() })
	oneByte, err := dialer.DialContext(t.Context(), "tcp", addr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = oneByte.Close() })
	_, err = oneByte.Write([]byte{tlsHandshakeRecord})
	require.NoError(t, err)

	start := time.Now()
	_, err = dialTLS(t, addr, rawClientTLS(fx.peerLeaf(t, clientID), protoH2))
	require.NoError(t, err)
	assert.Less(t, time.Since(start), time.Second, "a healthy peer is served while others stall")

	healthy := awaitOutcome(t, outcomes)
	require.NoError(t, healthy.err)
	for range 2 {
		stalled := awaitOutcome(t, outcomes)
		require.Error(t, stalled.err)
		assert.True(t, isTimeout(stalled.err), "stalled connections end at the deadline: %v", stalled.err)
	}
}

func TestServerCredentialsUseWhatAListenerEstablished(t *testing.T) {
	t.Parallel()

	fx := newFixture(t, serverDNS)
	leaf := fx.peerLeaf(t, clientID)
	known := map[string]ConnState{
		"tls":   {TLS: true, ChainVerified: true, State: tls.ConnectionState{HandshakeComplete: true, NegotiatedProtocol: protoH2, PeerCertificates: []*x509.Certificate{leaf.Cert}}},
		"plain": {},
	}
	lookup := func(remote string) (ConnState, bool) {
		st, ok := known[remote]

		return st, ok
	}
	creds := NewServerCredentials(newServerConfig(t, fx, StaticMode(ModeRequired), clientID), WithConnStateLookup(lookup))

	// Nothing is written on either pipe: a peek would block forever.
	serverSide, clientSide := net.Pipe()
	t.Cleanup(func() {
		_ = serverSide.Close()
		_ = clientSide.Close()
	})
	conn, info, err := creds.ServerHandshake(namedConn{Conn: serverSide, remote: "tls"})
	require.NoError(t, err)
	assert.NotNil(t, conn)
	tlsInfo, ok := info.(TLSAuthInfo)
	require.True(t, ok)
	assert.True(t, tlsInfo.ChainVerified, "the listener's verdict travels with the connection")
	require.NotNil(t, tlsInfo.SPIFFEID)
	assert.Equal(t, clientID, tlsInfo.SPIFFEID.String())

	_, info, err = creds.ServerHandshake(namedConn{Conn: serverSide, remote: "plain"})
	require.NoError(t, err)
	_, ok = info.(PlaintextAuthInfo)
	assert.True(t, ok)

	_, info, err = creds.ServerHandshake(&PlaintextConn{hookedConn{Conn: serverSide, onClose: func() {}}})
	require.NoError(t, err)
	_, ok = info.(PlaintextAuthInfo)
	assert.True(t, ok, "a listener's plaintext marker is recognised by type")
}

// namedConn reports a chosen remote address.
type namedConn struct {
	net.Conn

	remote string
}

func (c namedConn) RemoteAddr() net.Addr {
	return &net.UnixAddr{Name: c.remote, Net: "unix"}
}

// serverSideTLS completes one TLS handshake over loopback TCP between a server
// built from serverCfg and a client presenting leaf, and returns the server's
// connection, handshake complete, the way a Listener hands one on.
func serverSideTLS(t *testing.T, serverCfg *tls.Config, leaf *mtlstest.Leaf) *tls.Conn {
	t.Helper()

	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	var dialer net.Dialer
	clientSide, err := dialer.DialContext(t.Context(), "tcp", ln.Addr().String())
	require.NoError(t, err)
	serverSide, err := ln.Accept()
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = serverSide.Close()
		_ = clientSide.Close()
	})

	client := tls.Client(clientSide, rawClientTLS(leaf, protoHTTP11))
	go func() { _ = client.HandshakeContext(t.Context()) }()
	server := tls.Server(serverSide, serverCfg)
	require.NoError(t, server.HandshakeContext(t.Context()))

	return server
}

// A listener's connection reaching the credentials without the listener's
// lookup carries no package verdict: only Go's own counts, and this package's
// handshakes leave it empty, so such a connection is unverified.
func TestServerCredentialsTakeOnlyGoVerdictsFromABareTLSConn(t *testing.T) {
	t.Parallel()

	fx := newFixture(t, serverDNS)
	leaf := fx.peerLeaf(t, clientID)
	creds := NewServerCredentials(newServerConfig(t, fx, StaticMode(ModeRequired), clientID))

	// The package's own handshake, as a Listener in permissive mode runs it.
	fromListener := serverSideTLS(t, newServerConfig(t, fx, StaticMode(ModePermissive), clientID).serverTLSConfig([]string{protoHTTP11}), leaf)
	_, info, err := creds.ServerHandshake(fromListener)
	require.NoError(t, err)
	tlsInfo, ok := info.(TLSAuthInfo)
	require.True(t, ok)
	assert.False(t, tlsInfo.ChainVerified, "no lookup, no Go verdict: not verified")
	assert.Nil(t, tlsInfo.SPIFFEID, "an unverified chain names nobody to a caller reading credentials.TLSInfo")

	// A server that let Go verify the chain against the loaded roots.
	goVerified := &tls.Config{
		MinVersion:   tls.VersionTLS13,
		NextProtos:   []string{protoHTTP11},
		Certificates: []tls.Certificate{fx.leaf.TLS},
		ClientAuth:   tls.VerifyClientCertIfGiven,
		ClientCAs:    fx.files.Bundle().Roots,
	}
	_, info, err = creds.ServerHandshake(serverSideTLS(t, goVerified, leaf))
	require.NoError(t, err)
	tlsInfo, ok = info.(TLSAuthInfo)
	require.True(t, ok)
	assert.True(t, tlsInfo.ChainVerified, "Go's own verdict counts")
}

func TestServerCredentialsCloseUnverifiedAndCloseByPeer(t *testing.T) {
	t.Parallel()

	fx := newFixture(t, serverDNS)
	creds := NewServerCredentials(newServerConfig(t, fx, StaticMode(ModePermissive), clientID, strangerID))
	addr, outcomes := serveCredentials(t, creds)

	var dialer net.Dialer
	plain, err := dialer.DialContext(t.Context(), "tcp", addr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = plain.Close() })
	_, err = plain.Write([]byte("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"))
	require.NoError(t, err)
	require.NoError(t, plain.SetReadDeadline(time.Now().Add(5*time.Second)))
	_, err = io.ReadFull(plain, make([]byte, 1))
	require.NoError(t, err)
	require.NoError(t, awaitOutcome(t, outcomes).err)

	client, err := dialTLS(t, addr, rawClientTLS(fx.peerLeaf(t, clientID), protoH2))
	require.NoError(t, err)
	require.NoError(t, awaitOutcome(t, outcomes).err)
	stranger, err := dialTLS(t, addr, rawClientTLS(fx.peerLeaf(t, strangerID), protoH2))
	require.NoError(t, err)
	require.NoError(t, awaitOutcome(t, outcomes).err)
	// Trusted certificate, name not on the list: permissive admits it.
	outsider, err := dialTLS(t, addr, rawClientTLS(fx.peerLeaf(t, serverID), protoH2))
	require.NoError(t, err)
	require.NoError(t, awaitOutcome(t, outcomes).err)

	// A flip to required: the plaintext connection and the one whose name
	// required would refuse are closed, the allowed TLS ones stay.
	closed, err := creds.CloseUnverified()
	require.NoError(t, err)
	assert.Equal(t, 2, closed)
	_, err = io.Copy(io.Discard, plain)
	assert.False(t, isTimeout(err), "the plaintext connection was closed: %v", err)
	_, err = io.Copy(io.Discard, outsider)
	assert.False(t, isTimeout(err), "the connection of a name required refuses was closed: %v", err)

	// A name removed from the list: its connection ends, the other name's stays.
	assert.Equal(t, 1, creds.CloseByPeer(clientID))
	_, err = io.Copy(io.Discard, client)
	assert.False(t, isTimeout(err), "the removed name's connection was closed: %v", err)
	require.NoError(t, stranger.SetReadDeadline(time.Now().Add(time.Second)))
	_, err = stranger.Read(make([]byte, 1))
	assert.True(t, isTimeout(err), "the other name's connection stays open")
	assert.Equal(t, 0, creds.CloseByPeer(clientID), "nothing left to close")
}

func TestClientCredentialsFollowTheHopMode(t *testing.T) {
	t.Parallel()

	fx := newFixture(t, serverDNS)
	clientFiles := fx.peerFiles(t, clientID)
	addr, outcomes := serveCredentials(t, NewServerCredentials(newServerConfig(t, fx, StaticMode(ModeRequired), clientID)))

	// Off: the connection is returned untouched, marked plaintext.
	off := NewClientCredentials(newClientConfig(t, clientFiles, fx.log, StaticClientMode(ClientOff), serverDNS, serverID))
	serverSide, clientSide := net.Pipe()
	t.Cleanup(func() {
		_ = serverSide.Close()
		_ = clientSide.Close()
	})
	conn, info, err := off.ClientHandshake(t.Context(), "ignored:443", clientSide)
	require.NoError(t, err)
	assert.Equal(t, clientSide, conn)
	_, ok := info.(PlaintextAuthInfo)
	assert.True(t, ok)

	// On: TLS with the client certificate; the server's name is verified.
	on := NewClientCredentials(newClientConfig(t, clientFiles, fx.log, StaticClientMode(ClientOn), serverDNS, serverID))
	var dialer net.Dialer
	raw, err := dialer.DialContext(t.Context(), "tcp", addr)
	require.NoError(t, err)
	conn, info, err = on.ClientHandshake(t.Context(), "ignored:443", raw)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	tlsInfo, ok := info.(TLSAuthInfo)
	require.True(t, ok)
	assert.True(t, tlsInfo.ChainVerified)
	assert.Equal(t, credentials.PrivacyAndIntegrity, tlsInfo.SecurityLevel)
	require.NotNil(t, tlsInfo.SPIFFEID)
	assert.Equal(t, serverID, tlsInfo.SPIFFEID.String())
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	_, err = io.ReadFull(conn, make([]byte, 1))
	require.NoError(t, err)
	require.NoError(t, awaitOutcome(t, outcomes).err)

	// On, but the server is not one of the expected names.
	wrong := NewClientCredentials(newClientConfig(t, clientFiles, fx.log, StaticClientMode(ClientOn), serverDNS, strangerID))
	raw, err = dialer.DialContext(t.Context(), "tcp", addr)
	require.NoError(t, err)
	_, _, err = wrong.ClientHandshake(t.Context(), "ignored:443", raw)
	var rejection *RejectionError
	require.ErrorAs(t, err, &rejection)
	assert.Equal(t, ReasonNotAllowed, rejection.Reason)

	assert.Equal(t, "mtls", on.Info().SecurityProtocol)
	assert.NotEqual(t, insecure.NewCredentials().Info().SecurityProtocol, on.Info().SecurityProtocol)
}

// startGRPC serves a health service with creds and returns its address.
func startGRPC(t *testing.T, creds credentials.TransportCredentials, opts ...grpc.ServerOption) string {
	t.Helper()

	server := grpc.NewServer(append([]grpc.ServerOption{grpc.Creds(creds)}, opts...)...)
	healthpb.RegisterHealthServer(server, health.NewServer())

	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = server.Serve(ln) }()
	t.Cleanup(server.Stop)

	return ln.Addr().String()
}

func TestServerCredentialsClosesASilentPeerAfterTheHandshake(t *testing.T) {
	t.Parallel()

	fx := newFixture(t, serverDNS)
	cfg := newServerConfig(t, fx, StaticMode(ModeRequired), clientID)
	cfg.HandshakeTimeout = 500 * time.Millisecond
	addr := startGRPC(t, NewServerCredentials(cfg))

	dialer := tls.Dialer{NetDialer: &net.Dialer{Timeout: 5 * time.Second}, Config: rawClientTLS(fx.peerLeaf(t, clientID), protoH2)}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	require.NoError(t, err, "the handshake itself succeeds")
	t.Cleanup(func() { _ = conn.Close() })

	// The peer sends no HTTP/2 preface. gRPC's read of it must still be
	// bounded. gRPC writes its SETTINGS frame before reading the preface, so
	// the socket is drained to EOF rather than read once.
	start := time.Now()
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	_, err = io.Copy(io.Discard, conn)
	assert.False(t, isTimeout(err), "the server closed the connection: %v", err)
	assert.Less(t, time.Since(start), 3*time.Second)
}
