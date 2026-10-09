package mtls

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/shared/pkg/mtls/mtlstest"
)

const (
	healthMethod   = "/grpc.health.v1.Health/Check"
	watchMethod    = "/grpc.health.v1.Health/Watch"
	businessMethod = "/example.Service/Do"
)

// peerContext attaches a gRPC peer carrying info to the test context.
func peerContext(t *testing.T, info credentials.AuthInfo) context.Context {
	t.Helper()

	return peer.NewContext(t.Context(), &peer.Peer{AuthInfo: info})
}

func tlsPeer(t *testing.T, cert *x509.Certificate) context.Context {
	t.Helper()

	return peerContext(t, tlsAuthInfo(tls.ConnectionState{HandshakeComplete: true, PeerCertificates: []*x509.Certificate{cert}}, true))
}

// unverifiedTLSPeer is a TLS caller whose chain did not verify at the
// handshake: admitted under permissive, it carries whatever name it likes.
func unverifiedTLSPeer(t *testing.T, cert *x509.Certificate) context.Context {
	t.Helper()

	return peerContext(t, tlsAuthInfo(tls.ConnectionState{HandshakeComplete: true, PeerCertificates: []*x509.Certificate{cert}}, false))
}

// Another credentials implementation hands the interceptors grpc-go's own
// TLSInfo: the verdict is then Go's, present only when that implementation
// let Go verify the chain.
func TestInterceptorsTakeOnlyGoVerdictsFromForeignTLSInfo(t *testing.T) {
	t.Parallel()

	fx := newFixture(t, serverDNS)
	cert := fx.peerLeaf(t, clientID).Cert
	required := newServerConfig(t, fx, StaticMode(ModeRequired), clientID)

	unrecorded := peerContext(t, credentials.TLSInfo{State: tls.ConnectionState{HandshakeComplete: true, PeerCertificates: []*x509.Certificate{cert}}})
	_, _, err := callUnary(t, required, unrecorded, businessMethod)
	require.Equal(t, codes.Unauthenticated, status.Code(err), "an unrecorded verdict is not a verified chain")
	_, _, err = callUnary(t, required, unrecorded, healthMethod)
	require.NoError(t, err)

	verified := peerContext(t, credentials.TLSInfo{State: tls.ConnectionState{HandshakeComplete: true, PeerCertificates: []*x509.Certificate{cert}, VerifiedChains: [][]*x509.Certificate{{cert}}}})
	seen, ok, err := callUnary(t, required, verified, businessMethod)
	require.NoError(t, err, "Go's own verdict counts")
	assert.True(t, ok)
	assert.Equal(t, Peer{ID: clientID, TLS: true, ChainVerified: true}, seen)
}

func TestInterceptorsTreatAnUnverifiedChainAsPlaintextWhenRequired(t *testing.T) {
	t.Parallel()

	fx := newFixture(t, serverDNS)
	unverified := unverifiedTLSPeer(t, fx.peerLeaf(t, clientID).Cert)

	required := newServerConfig(t, fx, StaticMode(ModeRequired), clientID)
	_, _, err := callUnary(t, required, unverified, healthMethod)
	require.NoError(t, err, "health stays reachable, as for plaintext")
	_, _, err = callUnary(t, required, unverified, businessMethod)
	require.Equal(t, codes.Unauthenticated, status.Code(err), "an allow-listed name on an unverified chain is not an identity")
	_, _, err = callStream(t, required, unverified, businessMethod)
	require.Equal(t, codes.Unauthenticated, status.Code(err))

	permissive := newServerConfig(t, fx, StaticMode(ModePermissive), clientID)
	seen, ok, err := callUnary(t, permissive, unverified, businessMethod)
	require.NoError(t, err, "permissive admits it, as it did at the handshake")
	assert.True(t, ok)
	assert.Equal(t, Peer{ID: clientID, TLS: true, ChainVerified: false}, seen, "the name is reported as presented, flagged as unverified")
	assert.Equal(t, int64(1), wouldRejectCount(t, fx, ReasonUnverifiedChain, KindGRPC), "and counts what required would refuse")
}

func TestInterceptorsRefuseAVerifiedChainWithoutAnIdentityAsUnauthenticated(t *testing.T) {
	t.Parallel()

	fx := newFixture(t, serverDNS)
	// A trusted certificate with no SPIFFE URI SAN authenticates as nobody.
	nameless := tlsPeer(t, fx.inter.Leaf(t, mtlstest.LeafSpec{URIs: []string{}}).Cert)
	required := newServerConfig(t, fx, StaticMode(ModeRequired), clientID)

	_, _, err := callUnary(t, required, nameless, businessMethod)
	require.Equal(t, codes.Unauthenticated, status.Code(err), "no identity is not a disallowed identity")
}

// fakeStream is the minimum of grpc.ServerStream the interceptor touches.
type fakeStream struct {
	grpc.ServerStream

	ctxFn func() context.Context
}

func (s fakeStream) Context() context.Context {
	return s.ctxFn()
}

// callUnary runs the interceptor and reports the handler's view of the peer.
func callUnary(t *testing.T, cfg ServerConfig, ctx context.Context, method string) (Peer, bool, error) {
	t.Helper()

	var seen Peer
	var ok bool
	handler := func(ctx context.Context, _ any) (any, error) {
		seen, ok = PeerFromContext(ctx)

		return "done", nil
	}
	_, err := UnaryServerInterceptor(cfg)(ctx, nil, &grpc.UnaryServerInfo{FullMethod: method}, handler)

	return seen, ok, err
}

func callStream(t *testing.T, cfg ServerConfig, ctx context.Context, method string) (Peer, bool, error) {
	t.Helper()

	var seen Peer
	var ok bool
	handler := func(_ any, ss grpc.ServerStream) error { //nolint:contextcheck // a stream handler reads the stream's context, as a real one does
		seen, ok = PeerFromContext(ss.Context())

		return nil
	}
	stream := fakeStream{ctxFn: func() context.Context { return ctx }}
	err := StreamServerInterceptor(cfg)(nil, stream, &grpc.StreamServerInfo{FullMethod: method}, handler)

	return seen, ok, err
}

func TestInterceptorsAdmitPlaintextOnlyToHealthWhenRequired(t *testing.T) {
	t.Parallel()

	fx := newFixture(t, serverDNS)
	cfg := newServerConfig(t, fx, StaticMode(ModeRequired), clientID)

	for _, ctx := range []context.Context{peerContext(t, plaintextAuthInfo()), peerContext(t, nil), t.Context()} {
		seen, ok, err := callUnary(t, cfg, ctx, healthMethod)
		require.NoError(t, err, "plaintext health check passes")
		assert.True(t, ok)
		assert.Equal(t, Peer{}, seen)

		_, _, err = callStream(t, cfg, ctx, watchMethod)
		require.NoError(t, err, "plaintext health watch passes")

		_, _, err = callUnary(t, cfg, ctx, businessMethod)
		require.Equal(t, codes.Unauthenticated, status.Code(err), "plaintext business RPC is refused")

		_, _, err = callStream(t, cfg, ctx, businessMethod)
		require.Equal(t, codes.Unauthenticated, status.Code(err))
	}

	for _, method := range []string{"/grpc.health.v1.Health", "/grpc.health.v1.HealthX/Check", "/x/grpc.health.v1.Health/Check"} {
		_, _, err := callUnary(t, cfg, peerContext(t, plaintextAuthInfo()), method)
		require.Equal(t, codes.Unauthenticated, status.Code(err), "look-alike %s is not the health service", method)
	}
}

func TestInterceptorsAdmitPlaintextInPermissiveAndOff(t *testing.T) {
	t.Parallel()

	fx := newFixture(t, serverDNS)
	for _, mode := range []Mode{ModePermissive, ModeOff} {
		cfg := newServerConfig(t, fx, StaticMode(mode), clientID)

		seen, ok, err := callUnary(t, cfg, peerContext(t, plaintextAuthInfo()), businessMethod)
		require.NoError(t, err, "%s admits plaintext", mode)
		assert.True(t, ok)
		assert.False(t, seen.TLS)

		_, _, err = callStream(t, cfg, peerContext(t, plaintextAuthInfo()), businessMethod)
		require.NoError(t, err)
	}
}

func TestInterceptorsReCheckTheNameOnEveryRPC(t *testing.T) {
	t.Parallel()

	fx := newFixture(t, serverDNS)
	cfg := newServerConfig(t, fx, StaticMode(ModeRequired), clientID)
	allowed := tlsPeer(t, fx.peerLeaf(t, clientID).Cert)
	stranger := tlsPeer(t, fx.peerLeaf(t, strangerID).Cert)

	seen, ok, err := callUnary(t, cfg, allowed, businessMethod)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, Peer{ID: clientID, TLS: true, ChainVerified: true}, seen)

	seen, ok, err = callStream(t, cfg, allowed, businessMethod)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, Peer{ID: clientID, TLS: true, ChainVerified: true}, seen)

	_, _, err = callUnary(t, cfg, stranger, businessMethod)
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	_, _, err = callStream(t, cfg, stranger, businessMethod)
	require.Equal(t, codes.PermissionDenied, status.Code(err))

	// The name is removed: its next RPC is refused, the connection notwithstanding.
	require.NoError(t, cfg.Allow.Replace([]string{strangerID}))
	_, _, err = callUnary(t, cfg, allowed, businessMethod)
	require.Equal(t, codes.PermissionDenied, status.Code(err), "a removed name is refused at its next RPC")
	_, _, err = callUnary(t, cfg, stranger, businessMethod)
	require.NoError(t, err, "an added name is admitted at its next RPC")
}

func TestInterceptorsPermissiveAdmitsATLSPeerOffTheList(t *testing.T) {
	t.Parallel()

	fx := newFixture(t, serverDNS)
	cfg := newServerConfig(t, fx, StaticMode(ModePermissive), clientID)

	seen, ok, err := callUnary(t, cfg, tlsPeer(t, fx.peerLeaf(t, strangerID).Cert), businessMethod)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, Peer{ID: strangerID, TLS: true, ChainVerified: true}, seen, "the handler still learns who called")

	noSAN := fx.inter.Leaf(t, mtlstestLeafSpecNoSAN()).Cert
	seen, ok, err = callUnary(t, cfg, tlsPeer(t, noSAN), businessMethod)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, Peer{TLS: true, ChainVerified: true}, seen, "a TLS peer without a usable name is admitted in permissive, anonymous")
}

func TestInterceptorsOffIgnoreTheListForTLSPeers(t *testing.T) {
	t.Parallel()

	fx := newFixture(t, serverDNS)
	cfg := newServerConfig(t, fx, StaticMode(ModeOff), clientID)

	seen, ok, err := callUnary(t, cfg, tlsPeer(t, fx.peerLeaf(t, strangerID).Cert), businessMethod)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, Peer{ID: strangerID, TLS: true, ChainVerified: true}, seen)
}

func TestPeerContextRoundTrip(t *testing.T) {
	t.Parallel()

	_, ok := PeerFromContext(t.Context())
	assert.False(t, ok)

	ctx := ContextWithPeer(t.Context(), Peer{ID: clientID, TLS: true, ChainVerified: true})
	seen, ok := PeerFromContext(ctx)
	assert.True(t, ok)
	assert.Equal(t, Peer{ID: clientID, TLS: true, ChainVerified: true}, seen)
}
