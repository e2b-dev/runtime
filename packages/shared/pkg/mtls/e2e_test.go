package mtls

import (
	"context"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	e2bgrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc"
	proxygrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/proxy"
	"github.com/e2b-dev/infra/packages/shared/pkg/httpserver"
	"github.com/e2b-dev/infra/packages/shared/pkg/mtls/mtlstest"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// switchableMode is a ModeSource the test flips at runtime, standing in for
// a flag-backed source.
type switchableMode struct {
	mode atomic.Int32
}

func (s *switchableMode) Mode(context.Context) Mode {
	return Mode(s.mode.Load())
}

func (s *switchableMode) set(m Mode) {
	s.mode.Store(int32(m))
}

// recordingService is a business RPC that remembers who called it.
type recordingService struct {
	proxygrpc.UnimplementedSandboxServiceServer

	last atomic.Pointer[Peer]
}

func (s *recordingService) ResumeSandbox(ctx context.Context, _ *proxygrpc.SandboxResumeRequest) (*proxygrpc.SandboxResumeResponse, error) {
	p, _ := PeerFromContext(ctx)
	s.last.Store(&p)

	return &proxygrpc.SandboxResumeResponse{}, nil
}

// stack is one service as a later plan wires it: a gRPC server built with
// the shared constructor and an HTTP server behind the listener and guard,
// both on one ServerConfig.
type stack struct {
	cfg      ServerConfig
	mode     *switchableMode
	grpcAddr string
	httpAddr string
	service  *recordingService
	business atomic.Int32
	creds    *ServerCredentials
	listener *Listener
}

func noopTelemetry() *telemetry.Client {
	return &telemetry.Client{TracerProvider: tracenoop.NewTracerProvider(), MeterProvider: metricnoop.NewMeterProvider()}
}

func newStack(t *testing.T, fx *fixture, mode Mode) *stack {
	t.Helper()

	sw := &switchableMode{}
	sw.set(mode)
	cfg := newServerConfig(t, fx, sw, clientID)
	st := &stack{cfg: cfg, mode: sw, service: &recordingService{}, creds: NewServerCredentials(cfg)}

	// gRPC: credentials and both interceptors through the shared constructor.
	grpcServer := e2bgrpc.NewGRPCServer(noopTelemetry(),
		e2bgrpc.WithTransportCredentials(st.creds),
		e2bgrpc.WithUnaryInterceptors(UnaryServerInterceptor(cfg)),
		e2bgrpc.WithStreamInterceptors(StreamServerInterceptor(cfg)),
	)
	healthpb.RegisterHealthServer(grpcServer, health.NewServer())
	proxygrpc.RegisterSandboxServiceServer(grpcServer, st.service)

	var lc net.ListenConfig
	grpcListener, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = grpcServer.Serve(grpcListener) }()
	t.Cleanup(grpcServer.Stop)
	st.grpcAddr = grpcListener.Addr().String()

	// HTTP: the listener first, then the guard around the application with the
	// listener's lookup, then h2c, then Serve.
	app := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health" {
			st.business.Add(1)
		}
		p, _ := PeerFromContext(r.Context())
		w.Header().Set("X-Peer", p.ID)
		w.WriteHeader(http.StatusNoContent)
	})
	httpInner, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	st.listener = NewListener(httpInner, cfg)
	httpServer := &http.Server{
		Handler:           NewHTTPGuard(cfg, []string{"/health"}, app, WithGuardConnStateLookup(st.listener.ConnState)),
		ReadHeaderTimeout: 5 * time.Second,
	}
	httpserver.ConfigureH2C(httpServer)
	go func() { _ = httpServer.Serve(st.listener) }()
	t.Cleanup(func() { _ = httpServer.Close() })
	st.httpAddr = httpInner.Addr().String()

	return st
}

func (st *stack) dial(t *testing.T, creds credentials.TransportCredentials) *grpc.ClientConn {
	t.Helper()

	conn, err := grpc.NewClient(st.grpcAddr, grpc.WithTransportCredentials(creds))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	return conn
}

func rpcCall(t *testing.T, conn *grpc.ClientConn) error {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	_, err := proxygrpc.NewSandboxServiceClient(conn).ResumeSandbox(ctx, &proxygrpc.SandboxResumeRequest{})

	return err
}

func rpcHealth(t *testing.T, conn *grpc.ClientConn) error {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	_, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})

	return err
}

// httpStatus performs a GET and returns the status and the peer header, or
// the transport error. The body is drained so the client keeps the
// connection, which the flip tests rely on.
func httpStatus(t *testing.T, client *http.Client, url string) (int, string, error) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	require.NoError(t, err)
	resp, err := client.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	return resp.StatusCode, resp.Header.Get("X-Peer"), nil
}

func TestEndToEndEveryMode(t *testing.T) {
	t.Parallel()

	fx := newFixture(t, serverDNS)
	clientFiles := fx.peerFiles(t, clientID)
	strangerFiles := fx.peerFiles(t, strangerID)
	on := NewClientCredentials(newClientConfig(t, clientFiles, fx.log, StaticClientMode(ClientOn), serverDNS, serverID))
	off := NewClientCredentials(newClientConfig(t, clientFiles, fx.log, StaticClientMode(ClientOff), serverDNS, serverID))
	stranger := NewClientCredentials(newClientConfig(t, strangerFiles, fx.log, StaticClientMode(ClientOn), serverDNS, serverID))
	plainHTTP := httpClient(nil)
	tlsHTTP := httpClient(rawClientTLS(fx.peerLeaf(t, clientID), protoHTTP11))
	strangerHTTP := httpClient(rawClientTLS(fx.peerLeaf(t, strangerID), protoHTTP11))

	t.Run("off", func(t *testing.T) {
		t.Parallel()

		st := newStack(t, fx, ModeOff)
		require.NoError(t, rpcCall(t, st.dial(t, off)))
		require.NoError(t, rpcHealth(t, st.dial(t, off)))
		assert.Equal(t, Peer{}, *st.service.last.Load(), "plaintext callers have no identity")
		require.Error(t, rpcCall(t, st.dial(t, on)), "off speaks plaintext only")

		code, _, err := httpStatus(t, plainHTTP, "http://"+st.httpAddr+"/admin")
		require.NoError(t, err)
		assert.Equal(t, http.StatusNoContent, code)
		_, _, err = httpStatus(t, tlsHTTP, "https://"+st.httpAddr+"/admin")
		require.Error(t, err, "off speaks plaintext only")
	})

	t.Run("permissive", func(t *testing.T) {
		t.Parallel()

		st := newStack(t, fx, ModePermissive)
		require.NoError(t, rpcCall(t, st.dial(t, off)), "plaintext is admitted")
		assert.Equal(t, Peer{}, *st.service.last.Load())
		require.NoError(t, rpcCall(t, st.dial(t, on)), "mTLS is admitted")
		assert.Equal(t, Peer{ID: clientID, TLS: true, ChainVerified: true}, *st.service.last.Load())
		require.NoError(t, rpcCall(t, st.dial(t, stranger)), "a name off the list is admitted and counted")
		assert.Equal(t, Peer{ID: strangerID, TLS: true, ChainVerified: true}, *st.service.last.Load())
		require.NoError(t, rpcHealth(t, st.dial(t, insecure.NewCredentials())))

		code, _, err := httpStatus(t, plainHTTP, "http://"+st.httpAddr+"/admin")
		require.NoError(t, err)
		assert.Equal(t, http.StatusNoContent, code)
		code, peer, err := httpStatus(t, tlsHTTP, "https://"+st.httpAddr+"/admin")
		require.NoError(t, err)
		assert.Equal(t, http.StatusNoContent, code)
		assert.Equal(t, clientID, peer)
		code, _, err = httpStatus(t, strangerHTTP, "https://"+st.httpAddr+"/admin")
		require.NoError(t, err)
		assert.Equal(t, http.StatusNoContent, code)
	})

	t.Run("required", func(t *testing.T) {
		t.Parallel()

		st := newStack(t, fx, ModeRequired)
		require.NoError(t, rpcCall(t, st.dial(t, on)))
		assert.Equal(t, Peer{ID: clientID, TLS: true, ChainVerified: true}, *st.service.last.Load())
		require.NoError(t, rpcCall(t, st.dial(t, credentials.NewTLS(rawClientTLS(fx.peerLeaf(t, clientID), protoH2)))),
			"grpc-go's own TLS credentials interoperate with the server side: they advertise h2")
		require.Equal(t, codes.Unavailable, status.Code(rpcCall(t, st.dial(t, stranger))), "a name off the list never gets a transport")
		plaintext := st.dial(t, off)
		require.NoError(t, rpcHealth(t, plaintext), "the plaintext health probe passes")
		require.Equal(t, codes.Unauthenticated, status.Code(rpcCall(t, plaintext)), "a plaintext business RPC is refused")

		code, _, err := httpStatus(t, plainHTTP, "http://"+st.httpAddr+"/health")
		require.NoError(t, err)
		assert.Equal(t, http.StatusNoContent, code, "the plaintext health probe passes")
		code, _, err = httpStatus(t, plainHTTP, "http://"+st.httpAddr+"/admin")
		require.NoError(t, err)
		assert.Equal(t, http.StatusForbidden, code)
		code, peer, err := httpStatus(t, tlsHTTP, "https://"+st.httpAddr+"/admin")
		require.NoError(t, err)
		assert.Equal(t, http.StatusNoContent, code)
		assert.Equal(t, clientID, peer)
		_, _, err = httpStatus(t, strangerHTTP, "https://"+st.httpAddr+"/admin")
		require.Error(t, err, "refused at the handshake")
		assert.Equal(t, int32(1), st.business.Load(), "only the mTLS request reached the application")
	})
}

func TestEndToEndAFlipAndARemovalReachTheNextCall(t *testing.T) {
	t.Parallel()

	fx := newFixture(t, serverDNS)
	st := newStack(t, fx, ModePermissive)
	clientFiles := fx.peerFiles(t, clientID)
	on := NewClientCredentials(newClientConfig(t, clientFiles, fx.log, StaticClientMode(ClientOn), serverDNS, serverID))

	// Connections opened in permissive mode and kept open.
	plaintext := st.dial(t, insecure.NewCredentials())
	secure := st.dial(t, on)
	plainHTTP := httpClient(nil)
	tlsHTTP := httpClient(rawClientTLS(fx.peerLeaf(t, clientID), protoHTTP11))
	require.NoError(t, rpcCall(t, plaintext))
	require.NoError(t, rpcCall(t, secure))
	code, _, err := httpStatus(t, plainHTTP, "http://"+st.httpAddr+"/admin")
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, code)

	// Flip to required: the open plaintext connection is refused at its next
	// business RPC and request, health still passes, mTLS is untouched.
	st.mode.set(ModeRequired)
	require.Equal(t, codes.Unauthenticated, status.Code(rpcCall(t, plaintext)))
	require.NoError(t, rpcHealth(t, plaintext))
	require.NoError(t, rpcCall(t, secure))
	code, _, err = httpStatus(t, plainHTTP, "http://"+st.httpAddr+"/admin")
	require.NoError(t, err)
	assert.Equal(t, http.StatusForbidden, code)
	code, _, err = httpStatus(t, tlsHTTP, "https://"+st.httpAddr+"/admin")
	require.NoError(t, err)
	assert.Equal(t, http.StatusNoContent, code)

	// Remove the name: the open mTLS connection is refused at its next call.
	require.NoError(t, st.cfg.Allow.Replace([]string{strangerID}))
	require.Equal(t, codes.PermissionDenied, status.Code(rpcCall(t, secure)))
	code, _, err = httpStatus(t, tlsHTTP, "https://"+st.httpAddr+"/admin")
	require.NoError(t, err)
	assert.Equal(t, http.StatusForbidden, code)

	// Back to permissive: the mis-identified caller is admitted again, as the design says.
	st.mode.set(ModePermissive)
	require.NoError(t, rpcCall(t, secure))
	require.NoError(t, rpcCall(t, plaintext))
}

func TestRequiredRefusesAChainAdmittedUnverifiedUnderPermissive(t *testing.T) {
	t.Parallel()

	fx := newFixture(t, serverDNS)
	st := newStack(t, fx, ModePermissive)
	// A leaf under a root the server does not trust, carrying an allow-listed name.
	forger := mtlstest.NewRootCA(t, "forger")
	forgedLeaf := forger.Leaf(t, mtlstest.LeafSpec{SPIFFEID: clientID})
	forgedGRPC := st.dial(t, credentials.NewTLS(rawClientTLS(forgedLeaf, protoH2)))
	forgedHTTP := httpClient(rawClientTLS(forgedLeaf, protoHTTP11))
	genuine := st.dial(t, NewClientCredentials(newClientConfig(t, fx.peerFiles(t, clientID), fx.log, StaticClientMode(ClientOn), serverDNS, serverID)))

	require.NoError(t, rpcCall(t, forgedGRPC), "permissive admits the forged chain")
	assert.Equal(t, Peer{ID: clientID, TLS: true, ChainVerified: false}, *st.service.last.Load(),
		"the handler sees the name as presented, flagged as unverified, so it cannot mistake it for an authenticated one")
	code, _, err := httpStatus(t, forgedHTTP, "https://"+st.httpAddr+"/admin")
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, code, "permissive admits the forged chain")
	require.NoError(t, rpcCall(t, genuine))

	// Flip to required: the open forged connections are refused at their next
	// call, health excepted, while the genuine connection is untouched.
	st.mode.set(ModeRequired)
	require.Equal(t, codes.Unauthenticated, status.Code(rpcCall(t, forgedGRPC)), "an allow-listed name on an unverified chain is not an identity")
	require.NoError(t, rpcHealth(t, forgedGRPC), "health stays reachable, as for plaintext")
	code, _, err = httpStatus(t, forgedHTTP, "https://"+st.httpAddr+"/admin")
	require.NoError(t, err)
	assert.Equal(t, http.StatusForbidden, code)
	code, _, err = httpStatus(t, forgedHTTP, "https://"+st.httpAddr+"/health")
	require.NoError(t, err)
	assert.Equal(t, http.StatusNoContent, code)
	require.NoError(t, rpcCall(t, genuine))

	// The service closes what required would not have admitted; the forged
	// peer's reconnect is refused at the handshake.
	closedGRPC, err := st.creds.CloseUnverified()
	require.NoError(t, err)
	closedHTTP, err := st.listener.CloseUnverified()
	require.NoError(t, err)
	assert.Equal(t, 1, closedGRPC, "the forged gRPC connection, not the genuine one")
	assert.Equal(t, 1, closedHTTP, "the forged keep-alive HTTP connection")
	require.Equal(t, codes.Unavailable, status.Code(rpcCall(t, forgedGRPC)))
	_, _, err = httpStatus(t, forgedHTTP, "https://"+st.httpAddr+"/admin")
	require.Error(t, err, "refused at the handshake")
	require.NoError(t, rpcCall(t, genuine))
}

// chanListener hands a server the connections a multiplexer routed to it.
type chanListener struct {
	conns chan net.Conn
	addr  net.Addr
	done  chan struct{}
	once  sync.Once
}

func (l *chanListener) Accept() (net.Conn, error) {
	select {
	case conn := <-l.conns:
		return conn, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}

func (l *chanListener) Close() error {
	l.once.Do(func() { close(l.done) })

	return nil
}

func (l *chanListener) Addr() net.Addr {
	return l.addr
}

// hiddenConn is what a multiplexer hands on: the connection behind a type the
// server cannot inspect, so neither *tls.Conn nor *PlaintextConn shows.
type hiddenConn struct {
	net.Conn
}

func TestListenerCarriesGRPCThroughAMultiplexer(t *testing.T) {
	t.Parallel()

	fx := newFixture(t, serverDNS)
	cfg := newServerConfig(t, fx, StaticMode(ModeRequired), clientID)
	var lc net.ListenConfig
	inner, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	l := NewListener(inner, cfg, WithNextProtos(protoH2, protoHTTP11))
	t.Cleanup(func() { _ = l.Close() })

	// The multiplexer: accept from the listener, hide the type, hand on.
	mux := &chanListener{conns: make(chan net.Conn), addr: inner.Addr(), done: make(chan struct{})}
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				_ = mux.Close()

				return
			}
			select {
			case mux.conns <- hiddenConn{Conn: conn}:
			case <-mux.done:
				_ = conn.Close()

				return
			}
		}
	}()

	service := &recordingService{}
	server := e2bgrpc.NewGRPCServer(noopTelemetry(),
		e2bgrpc.WithTransportCredentials(NewServerCredentials(cfg, WithConnStateLookup(l.ConnState))),
		e2bgrpc.WithUnaryInterceptors(UnaryServerInterceptor(cfg)),
	)
	proxygrpc.RegisterSandboxServiceServer(server, service)
	go func() { _ = server.Serve(mux) }()
	t.Cleanup(server.Stop)

	st := &stack{grpcAddr: inner.Addr().String()}
	on := NewClientCredentials(newClientConfig(t, fx.peerFiles(t, clientID), fx.log, StaticClientMode(ClientOn), serverDNS, serverID))
	require.NoError(t, rpcCall(t, st.dial(t, on)), "a gRPC client reaches the server behind the listener")
	assert.Equal(t, Peer{ID: clientID, TLS: true, ChainVerified: true}, *service.last.Load(), "the credentials learn the peer from the listener's lookup")

	stranger := NewClientCredentials(newClientConfig(t, fx.peerFiles(t, strangerID), fx.log, StaticClientMode(ClientOn), serverDNS, serverID))
	require.Equal(t, codes.Unavailable, status.Code(rpcCall(t, st.dial(t, stranger))), "refused at the listener's handshake")
}
