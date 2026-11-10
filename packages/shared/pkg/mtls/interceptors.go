package mtls

import (
	"context"
	"crypto/tls"
	"strings"

	middleware "github.com/grpc-ecosystem/go-grpc-middleware/v2"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// healthServicePrefix names the one gRPC service a plaintext caller may
// reach on a required listener: the kubelet's gRPC probe speaks plaintext.
const healthServicePrefix = "/grpc.health.v1.Health/"

// Peer is the caller as the package established it, available to handlers
// through PeerFromContext.
type Peer struct {
	// ID is the SPIFFE ID the caller's certificate carried, when it has one
	// valid SPIFFE SAN; empty otherwise. It is authenticated only when
	// ChainVerified is set: under off and permissive the package admits a
	// certificate whose chain did not verify, and reports its name as
	// presented, so a handler must not authorize on ID alone.
	ID string
	// TLS is false for a connection admitted without TLS.
	TLS bool
	// ChainVerified reports whether the certificate's chain reached a loaded
	// root at the handshake. True for every caller admitted to a business
	// path under required; false for a plaintext or unverified probe that
	// required admits to a health path, and for a self-signed or wrong-root
	// certificate admitted under permissive or off.
	ChainVerified bool
}

type peerKey struct{}

// ContextWithPeer returns ctx carrying p.
func ContextWithPeer(ctx context.Context, p Peer) context.Context {
	return context.WithValue(ctx, peerKey{}, p)
}

// PeerFromContext returns the caller the interceptors or the HTTP guard established.
func PeerFromContext(ctx context.Context) (Peer, bool) {
	p, ok := ctx.Value(peerKey{}).(Peer)

	return p, ok
}

// UnaryServerInterceptor re-checks the allow-list on every RPC and, on a
// required listener, lets a plaintext RPC through only to grpc.health.v1.
// Install it through the shared constructor's WithUnaryInterceptors so it
// runs after recovery, logging and the deadline.
func UnaryServerInterceptor(cfg ServerConfig) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		ctx, err := cfg.admitRPC(ctx, info.FullMethod)
		if err != nil {
			return nil, err
		}

		return handler(ctx, req)
	}
}

// StreamServerInterceptor is UnaryServerInterceptor for streams. Install it
// through the shared constructor's WithStreamInterceptors.
func StreamServerInterceptor(cfg ServerConfig) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx, err := cfg.admitRPC(ss.Context(), info.FullMethod)
		if err != nil {
			return err
		}

		wrapped := middleware.WrapServerStream(ss)
		wrapped.WrappedContext = ctx

		return handler(srv, wrapped)
	}
}

// admitRPC applies the policy in force to one RPC and returns a context
// carrying the peer, or the status the caller gets.
func (c ServerConfig) admitRPC(ctx context.Context, fullMethod string) (context.Context, error) {
	state, chainVerified, isTLS := peerTLSState(ctx)
	d := c.admit(ctx, KindGRPC, strings.HasPrefix(fullMethod, healthServicePrefix), isTLS, chainVerified, state, zap.String("method", fullMethod))
	switch {
	case d.refused == nil:
		return ContextWithPeer(ctx, d.peer), nil
	case d.unauthenticated():
		return ctx, status.Error(codes.Unauthenticated, "mtls: this listener requires a verified client certificate")
	default:
		return ctx, status.Error(codes.PermissionDenied, "mtls: caller is not allowed on this listener")
	}
}

// peerTLSState reads the connection's TLS state and chain verdict from the
// RPC's peer. A missing peer, a nil AuthInfo or a PlaintextAuthInfo all mean
// plaintext. Another implementation's TLSInfo carries only Go's own verdict,
// present when that implementation let Go verify the chain.
func peerTLSState(ctx context.Context) (state tls.ConnectionState, chainVerified, isTLS bool) {
	p, ok := peer.FromContext(ctx)
	if !ok || p.AuthInfo == nil {
		return tls.ConnectionState{}, false, false
	}

	switch info := p.AuthInfo.(type) {
	case TLSAuthInfo:
		return info.State, info.ChainVerified, true
	case credentials.TLSInfo:
		return info.State, len(info.State.VerifiedChains) > 0, true
	default:
		return tls.ConnectionState{}, false, false
	}
}
