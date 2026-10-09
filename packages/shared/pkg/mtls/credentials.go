package mtls

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/credentials"
)

// securityProtocol is what Info reports. gRPC treats only "insecure" specially.
const securityProtocol = "mtls"

var (
	errServerCredentialsDial   = errors.New("mtls: server credentials cannot dial")
	errClientCredentialsAccept = errors.New("mtls: client credentials cannot accept")
)

// PlaintextAuthInfo is the peer.AuthInfo of a connection admitted without
// TLS, so interceptors can tell it from a TLS connection.
type PlaintextAuthInfo struct {
	credentials.CommonAuthInfo
}

// AuthType implements credentials.AuthInfo.
func (PlaintextAuthInfo) AuthType() string {
	return "plaintext"
}

func plaintextAuthInfo() PlaintextAuthInfo {
	return PlaintextAuthInfo{CommonAuthInfo: credentials.CommonAuthInfo{SecurityLevel: credentials.NoSecurity}}
}

// TLSAuthInfo is the peer.AuthInfo of a TLS connection: grpc-go's TLSInfo,
// built the way its own credentials build it so gRPC tooling reads the usual
// shape, plus the verdict of the handshake checks, which the per-RPC check
// needs after a flip to required.
type TLSAuthInfo struct {
	credentials.TLSInfo

	// ChainVerified reports whether the peer's chain reached a loaded root
	// at the handshake. False for a connection admitted under permissive
	// with a chain that did not verify.
	ChainVerified bool
}

func tlsAuthInfo(state tls.ConnectionState, chainVerified bool) TLSAuthInfo {
	info := TLSAuthInfo{
		TLSInfo: credentials.TLSInfo{
			State:          state,
			CommonAuthInfo: credentials.CommonAuthInfo{SecurityLevel: credentials.PrivacyAndIntegrity},
		},
		ChainVerified: chainVerified,
	}
	// grpc-go's own credentials fill SPIFFEID only after the chain verified,
	// and code reading credentials.TLSInfo relies on that: a name on a chain
	// that did not verify is not an identity, so it is not reported as one.
	if chainVerified && len(state.PeerCertificates) > 0 {
		if _, err := PeerID(state.PeerCertificates[0]); err == nil {
			info.SPIFFEID = state.PeerCertificates[0].URIs[0]
		}
	}

	return info
}

// ServerCredentials implements credentials.TransportCredentials for a gRPC
// server. ServerHandshake peeks the first byte under the handshake deadline:
// a TLS record gets a TLS handshake with ALPN h2 and returns TLSAuthInfo,
// which embeds credentials.TLSInfo; anything else is returned as plaintext with
// PlaintextAuthInfo. In ModeOff the connection passes through unread,
// recorded as plaintext so that a later CloseUnverified reaches it.
type ServerCredentials struct {
	cfg       ServerConfig
	tlsConfig *tls.Config
	lookup    ConnStateLookup
	conns     *connRegistry
}

// CredentialsOption configures NewServerCredentials.
type CredentialsOption func(*ServerCredentials)

// WithConnStateLookup consults lookup before peeking, for a server that
// receives connections a Listener already classified, through a multiplexer
// that hides their type.
func WithConnStateLookup(lookup ConnStateLookup) CredentialsOption {
	return func(c *ServerCredentials) { c.lookup = lookup }
}

// NewServerCredentials builds the credentials for one listener. The policy
// is evaluated once here, so a required mode with an empty allow-list is
// reported at startup rather than at the first connection.
func NewServerCredentials(cfg ServerConfig, opts ...CredentialsOption) *ServerCredentials {
	c := &ServerCredentials{cfg: cfg, tlsConfig: cfg.serverTLSConfig([]string{protoH2}), conns: newConnRegistry()}
	for _, o := range opts {
		o(c)
	}
	c.cfg.policy(context.Background())

	return c
}

// ServerHandshake implements credentials.TransportCredentials.
func (c *ServerCredentials) ServerHandshake(raw net.Conn) (net.Conn, credentials.AuthInfo, error) {
	ctx := context.Background()
	metrics := c.cfg.metrics()

	if conn, info, ok := c.known(raw); ok {
		return conn, info, nil
	}

	key := remoteKey(raw)
	if c.cfg.policy(ctx).mode == ModeOff {
		return c.plaintext(ctx, key, raw), plaintextAuthInfo(), nil
	}

	// gRPC set its own, longer connection deadline before calling. This one
	// covers the peek and the handshake.
	timeout := c.cfg.handshakeTimeout()
	_ = raw.SetDeadline(time.Now().Add(timeout))

	isTLS, conn, err := peekTLS(raw)
	if err != nil {
		_ = raw.Close()
		metrics.handshake(ctx, c.cfg.Name, peekOutcome(err))

		return nil, nil, err
	}

	if !isTLS {
		// The deadline stays for gRPC's read of the HTTP/2 preface; gRPC
		// clears it once the transport is up.
		return c.plaintext(ctx, key, conn), plaintextAuthInfo(), nil
	}

	hooked := &hookedConn{Conn: conn, onClose: func() { c.conns.remove(key) }}
	tlsConn := tls.Server(hooked, c.tlsConfig)
	if err := tlsConn.HandshakeContext(ctx); err != nil {
		_ = tlsConn.Close()
		metrics.handshake(ctx, c.cfg.Name, handshakeOutcome(err))

		return nil, nil, err
	}

	state := tlsConn.ConnectionState()
	if state.NegotiatedProtocol != protoH2 {
		_ = tlsConn.Close()
		metrics.handshake(ctx, c.cfg.Name, OutcomeRefused)
		metrics.refused(ctx, c.cfg.Name, ReasonNoALPN, KindHandshake)

		return nil, nil, &RejectionError{Reason: ReasonNoALPN, Err: errors.New("client did not negotiate h2")}
	}

	// A fresh window for the HTTP/2 preface, so a peer that completes the
	// handshake and goes silent is closed; gRPC clears the deadline once the
	// transport is up.
	_ = tlsConn.SetDeadline(time.Now().Add(timeout))
	if !c.conns.add(key, hooked, ConnState{TLS: true, ChainVerified: hooked.chainVerified, State: state}) {
		warnSharedRemote(ctx, c.cfg, key)
	}
	metrics.handshake(ctx, c.cfg.Name, OutcomeTLS)

	return tlsConn, tlsAuthInfo(state, hooked.chainVerified), nil
}

// plaintext marks conn as admitted without TLS, records it and counts it:
// one handshake with the plaintext outcome, and one open plaintext
// connection until it closes.
func (c *ServerCredentials) plaintext(ctx context.Context, key string, conn net.Conn) *PlaintextConn {
	metrics := c.cfg.metrics()
	metrics.handshake(ctx, c.cfg.Name, OutcomePlaintext)
	metrics.plaintextConn(ctx, c.cfg.Name, 1)
	plain := &PlaintextConn{hookedConn{Conn: conn, onClose: func() {
		c.conns.remove(key)
		metrics.plaintextConn(ctx, c.cfg.Name, -1)
	}}}
	if !c.conns.add(key, plain, ConnState{}) {
		warnSharedRemote(ctx, c.cfg, key)
	}
	c.cfg.log().Debug(ctx, "mtls: plaintext connection admitted", zap.String("listener", c.cfg.Name), zap.String("remote", key))

	return plain
}

// CloseUnverified closes every open connection a required handshake would
// refuse now, the plaintext ones, the TLS ones whose chain did not verify and
// the TLS ones whose name is not on the allow-list, and returns how many:
// what a service calls when its mode flips to required. A connection a
// Listener classified in front of a multiplexer is the listener's to close.
func (c *ServerCredentials) CloseUnverified() (int, error) {
	return c.conns.closeUnverified(c.cfg.Allow.Allows)
}

// CloseByPeer closes every open TLS connection whose peer is id and returns
// how many it closed, for a name removed from the allow-list; the server's
// maximum connection age ends such connections as well.
func (c *ServerCredentials) CloseByPeer(id string) int {
	return c.conns.closeByPeer(id)
}

// known answers from the lookup, which carries the listener's chain verdict,
// or from a connection whose type already tells.
func (c *ServerCredentials) known(raw net.Conn) (net.Conn, credentials.AuthInfo, bool) {
	if c.lookup != nil && raw.RemoteAddr() != nil {
		if state, ok := c.lookup(raw.RemoteAddr().String()); ok {
			if state.TLS {
				return raw, tlsAuthInfo(state.State, state.ChainVerified), true
			}

			return raw, plaintextAuthInfo(), true
		}
	}

	switch conn := raw.(type) {
	case *tls.Conn:
		// A listener's connection without its lookup carries no package
		// verdict, so only Go's own verified chains count, and this
		// package's handshakes leave them empty: unverified until the
		// service wires the lookup.
		if state := conn.ConnectionState(); state.HandshakeComplete {
			return conn, tlsAuthInfo(state, len(state.VerifiedChains) > 0), true
		}
	case *PlaintextConn:
		return conn, plaintextAuthInfo(), true
	}

	return nil, nil, false
}

// ClientHandshake implements credentials.TransportCredentials; a server's
// credentials never dial.
func (c *ServerCredentials) ClientHandshake(context.Context, string, net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return nil, nil, errServerCredentialsDial
}

// Info implements credentials.TransportCredentials.
func (c *ServerCredentials) Info() credentials.ProtocolInfo {
	return credentials.ProtocolInfo{SecurityProtocol: securityProtocol}
}

// Clone implements credentials.TransportCredentials.
func (c *ServerCredentials) Clone() credentials.TransportCredentials {
	clone := *c

	return &clone
}

// OverrideServerName implements credentials.TransportCredentials; gRPC no
// longer calls it and the server name comes from the configuration.
func (c *ServerCredentials) OverrideServerName(string) error {
	return nil
}

// ClientCredentials implements credentials.TransportCredentials for one
// client hop. ClientHandshake reads the hop's mode on every handshake: on, it
// dials TLS with the client certificate and ALPN h2 and verifies the server;
// off, it returns the connection as plaintext.
type ClientCredentials struct {
	cfg       ClientConfig
	tlsConfig *tls.Config
}

// NewClientCredentials builds the credentials for one hop.
func NewClientCredentials(cfg ClientConfig) *ClientCredentials {
	return &ClientCredentials{cfg: cfg, tlsConfig: cfg.clientTLSConfig([]string{protoH2})}
}

// ClientHandshake implements credentials.TransportCredentials. The authority
// gRPC passes is not used for verification; ServerName and ExpectedServerIDs
// are, because several hops dial their server by IP.
func (c *ClientCredentials) ClientHandshake(ctx context.Context, _ string, raw net.Conn) (net.Conn, credentials.AuthInfo, error) {
	metrics := c.cfg.metrics()
	mode, source := c.cfg.clientMode(ctx)
	metrics.recordClientMode(ctx, c.cfg.Name, mode, source)

	if mode == ClientOff {
		metrics.clientHandshake(ctx, c.cfg.Name, OutcomePlaintext)

		return raw, plaintextAuthInfo(), nil
	}

	tlsConn := tls.Client(raw, c.tlsConfig)
	if err := tlsConn.HandshakeContext(ctx); err != nil {
		_ = tlsConn.Close()
		metrics.clientHandshake(ctx, c.cfg.Name, handshakeOutcome(err))
		c.cfg.log().Warn(ctx, "mtls: client handshake failed", zap.String("client_hop", c.cfg.Name), zap.Error(err))

		return nil, nil, err
	}

	state := tlsConn.ConnectionState()
	if state.NegotiatedProtocol != protoH2 {
		_ = tlsConn.Close()
		metrics.clientHandshake(ctx, c.cfg.Name, OutcomeRefused)

		return nil, nil, &RejectionError{Reason: ReasonNoALPN, Err: errors.New("server did not select h2")}
	}

	metrics.clientHandshake(ctx, c.cfg.Name, OutcomeTLS)

	// The client refuses the handshake on any failed check, so a completed
	// one is verified.
	return tlsConn, tlsAuthInfo(state, true), nil
}

// ServerHandshake implements credentials.TransportCredentials; a client's
// credentials never accept.
func (c *ClientCredentials) ServerHandshake(net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return nil, nil, errClientCredentialsAccept
}

// Info implements credentials.TransportCredentials.
func (c *ClientCredentials) Info() credentials.ProtocolInfo {
	return credentials.ProtocolInfo{SecurityProtocol: securityProtocol}
}

// Clone implements credentials.TransportCredentials.
func (c *ClientCredentials) Clone() credentials.TransportCredentials {
	clone := *c

	return &clone
}

// OverrideServerName implements credentials.TransportCredentials; the server
// name comes from the configuration.
func (c *ClientCredentials) OverrideServerName(string) error {
	return nil
}
