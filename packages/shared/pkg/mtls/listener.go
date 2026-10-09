package mtls

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"slices"
	"sync"
	"time"

	"go.uber.org/zap"
)

// ConnState is what the listener established for one accepted connection.
// A server that receives the connection through a multiplexer hiding its
// concrete type asks for it by remote address.
type ConnState struct {
	// TLS is false for a connection admitted as plaintext.
	TLS bool
	// ChainVerified reports whether the peer's chain reached a loaded root
	// at the handshake. It is false for a TLS connection admitted under
	// permissive with a chain that did not verify; required treats such a
	// connection as plaintext until the service closes it.
	ChainVerified bool
	// State is the completed handshake when TLS is true.
	State tls.ConnectionState
}

// ConnStateLookup answers whether the connection from remoteAddr completed
// TLS, or was admitted as plaintext, before it reached the server asking.
// (*Listener).ConnState is one.
type ConnStateLookup func(remoteAddr string) (ConnState, bool)

// hookedConn runs onClose once when the connection closes.
type hookedConn struct {
	net.Conn

	once    sync.Once
	onClose func()

	// chainVerified is set by VerifyConnection during the handshake and read
	// once the handshake has returned, in the goroutine that ran it.
	chainVerified bool
}

func (c *hookedConn) Close() error {
	c.once.Do(c.onClose)

	return c.Conn.Close()
}

// PlaintextConn marks a connection the listener admitted without TLS. It
// does not implement ConnectionState, so net/http leaves r.TLS nil.
type PlaintextConn struct {
	hookedConn
}

// Listener classifies each connection by its first byte, completes TLS
// itself and hands the server either the *tls.Conn, unwrapped so net/http
// populates r.TLS, or a *PlaintextConn. Classification and the handshake run
// in their own goroutine under the handshake deadline, so a silent, one-byte
// or stalled connection delays no other caller. In ModeOff connections pass
// through unread, recorded as plaintext so that a later CloseUnverified
// reaches them.
//
// The TLS branch offers http/1.1 unless WithNextProtos says otherwise. Put
// the listener in front of http.Server.Serve; for a port shared through a
// multiplexer, put it in front of the multiplexer, offer the protocols the
// servers behind it speak, and hand them ConnState.
type Listener struct {
	inner      net.Listener
	cfg        ServerConfig
	nextProtos []string
	tlsConfig  *tls.Config

	ready     chan net.Conn
	errs      chan error
	closed    chan struct{}
	closeOnce sync.Once

	conns *connRegistry
}

// ListenerOption configures NewListener.
type ListenerOption func(*Listener)

// WithNextProtos replaces the ALPN protocols the TLS branch offers, http/1.1
// alone by default. A port that also carries gRPC behind a multiplexer
// offers "h2" as well, since grpc-go clients require it.
func WithNextProtos(protos ...string) ListenerOption {
	return func(l *Listener) { l.nextProtos = slices.Clone(protos) }
}

// NewListener wraps inner and starts accepting. The policy is evaluated once
// here, so a required mode with an empty allow-list is reported at startup
// rather than at the first connection.
func NewListener(inner net.Listener, cfg ServerConfig, opts ...ListenerOption) *Listener {
	l := &Listener{
		inner:      inner,
		cfg:        cfg,
		nextProtos: []string{protoHTTP11},
		ready:      make(chan net.Conn),
		errs:       make(chan error, 1),
		closed:     make(chan struct{}),
		conns:      newConnRegistry(),
	}
	for _, o := range opts {
		o(l)
	}
	l.tlsConfig = cfg.serverTLSConfig(l.nextProtos)
	l.cfg.policy(context.Background())
	go l.acceptLoop()

	return l
}

// Accept implements net.Listener.
func (l *Listener) Accept() (net.Conn, error) {
	select {
	case conn := <-l.ready:
		return conn, nil
	case err := <-l.errs:
		return nil, err
	case <-l.closed:
		return nil, net.ErrClosed
	}
}

// Close implements net.Listener. A connection still being classified is
// closed. Closing twice is fine: http.Server.Close closes the listener it
// was given as well.
func (l *Listener) Close() error {
	l.markClosed()
	if err := l.inner.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		return err
	}

	return nil
}

// Addr implements net.Listener.
func (l *Listener) Addr() net.Addr {
	return l.inner.Addr()
}

// ConnState reports what the listener established for the open connection
// from remoteAddr. It is a ConnStateLookup. Remote addresses must be unique
// among open connections, as on TCP: two connections sharing one are both
// reported unverified.
func (l *Listener) ConnState(remoteAddr string) (ConnState, bool) {
	return l.conns.lookup(remoteAddr)
}

// CloseUnverified closes every open connection a required handshake would
// refuse now, the plaintext ones, the TLS ones whose chain did not verify and
// the TLS ones whose name is not on the allow-list, and returns how many:
// what a service calls when its mode flips to required, so callers reconnect
// and meet the new policy instead of riding a connection opened under the
// old one.
func (l *Listener) CloseUnverified() (int, error) {
	return l.conns.closeUnverified(l.cfg.Allow.Allows)
}

// CloseByPeer closes every open TLS connection whose peer is id and returns
// how many it closed: how a name removed from the allow-list loses its
// keep-alive connections, since http.Server has no connection age.
func (l *Listener) CloseByPeer(id string) int {
	return l.conns.closeByPeer(id)
}

func (l *Listener) markClosed() {
	l.closeOnce.Do(func() { close(l.closed) })
}

func (l *Listener) acceptLoop() {
	ctx := context.Background()
	for {
		conn, err := l.inner.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				l.markClosed()

				return
			}
			select {
			case l.errs <- err:
			case <-l.closed:
				return
			}
			// http.Server.Serve retries a temporary error after a backoff and
			// returns on any other, after which nobody would call Accept.
			if netErr, ok := errors.AsType[net.Error](err); !ok || !netErr.Temporary() {
				l.markClosed()

				return
			}

			continue
		}

		if l.cfg.policy(ctx).mode == ModeOff {
			l.deliver(l.plaintext(ctx, remoteKey(conn), conn))

			continue
		}

		go l.classify(ctx, conn)
	}
}

// classify runs in its own goroutine for one connection: peek, then either
// mark it plaintext or complete the TLS handshake, all under one deadline.
func (l *Listener) classify(ctx context.Context, raw net.Conn) {
	key := remoteKey(raw)
	metrics := l.cfg.metrics()
	_ = raw.SetDeadline(time.Now().Add(l.cfg.handshakeTimeout()))

	isTLS, conn, err := peekTLS(raw)
	if err != nil {
		_ = raw.Close()
		metrics.handshake(ctx, l.cfg.Name, peekOutcome(err))
		l.cfg.log().Debug(ctx, "mtls: connection closed before its first byte",
			zap.String("listener", l.cfg.Name), zap.String("remote", key), zap.Bool("timeout", isTimeout(err)), zap.Error(err))

		return
	}

	if !isTLS {
		_ = conn.SetDeadline(time.Time{})
		l.deliver(l.plaintext(ctx, key, conn))

		return
	}

	hooked := &hookedConn{Conn: conn, onClose: func() { l.conns.remove(key) }}
	tlsConn := tls.Server(hooked, l.tlsConfig)
	if err := tlsConn.HandshakeContext(ctx); err != nil {
		_ = tlsConn.Close()
		metrics.handshake(ctx, l.cfg.Name, handshakeOutcome(err))
		l.cfg.log().Debug(ctx, "mtls: TLS handshake failed",
			zap.String("listener", l.cfg.Name), zap.String("remote", key), zap.Bool("timeout", isTimeout(err)), zap.Error(err))

		return
	}

	// The server behind the listener sets its own deadlines from here on.
	_ = tlsConn.SetDeadline(time.Time{})
	if !l.conns.add(key, hooked, ConnState{TLS: true, ChainVerified: hooked.chainVerified, State: tlsConn.ConnectionState()}) {
		warnSharedRemote(ctx, l.cfg, key)
	}
	metrics.handshake(ctx, l.cfg.Name, OutcomeTLS)
	l.deliver(tlsConn)
}

// plaintext marks conn as admitted without TLS, records it and counts it:
// one handshake with the plaintext outcome, and one open plaintext connection
// until it closes.
func (l *Listener) plaintext(ctx context.Context, key string, conn net.Conn) *PlaintextConn {
	metrics := l.cfg.metrics()
	metrics.handshake(ctx, l.cfg.Name, OutcomePlaintext)
	metrics.plaintextConn(ctx, l.cfg.Name, 1)
	plain := &PlaintextConn{hookedConn{Conn: conn, onClose: func() {
		l.conns.remove(key)
		metrics.plaintextConn(ctx, l.cfg.Name, -1)
	}}}
	if !l.conns.add(key, plain, ConnState{}) {
		warnSharedRemote(ctx, l.cfg, key)
	}
	l.cfg.log().Debug(ctx, "mtls: plaintext connection admitted", zap.String("listener", l.cfg.Name), zap.String("remote", key))

	return plain
}

// peekOutcome labels a failed first-byte read.
func peekOutcome(err error) string {
	if isTimeout(err) {
		return OutcomeTimeout
	}

	return OutcomeFailed
}

// handshakeOutcome labels a failed TLS handshake: a deadline, a refusal by
// the policy, or a protocol failure.
func handshakeOutcome(err error) string {
	if isTimeout(err) {
		return OutcomeTimeout
	}

	if _, ok := errors.AsType[*RejectionError](err); ok {
		return OutcomeRefused
	}

	return OutcomeFailed
}

// deliver hands a classified connection to Accept, or closes it when the
// listener has closed meanwhile.
func (l *Listener) deliver(conn net.Conn) {
	select {
	case l.ready <- conn:
	case <-l.closed:
		_ = conn.Close()
	}
}
