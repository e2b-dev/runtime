package mtls

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"sync/atomic"

	"go.uber.org/zap"
)

// NewHTTPTransport returns a copy of base, or of http.DefaultTransport when
// base is nil, whose connections follow the hop's mode, read on every
// dial: off dials plaintext as before; on completes TLS 1.3 with the
// client certificate and ALPN http/1.1 within base's TLSHandshakeTimeout
// and verifies the server as ClientCredentials does. Request URLs keep the
// http scheme: the server's Listener tells TLS from plaintext by the first
// byte. The hop's mode is reported at once, as NewClientCredentials does.
// base's proxy is dropped: the hop dials its target directly. When a flag
// flips the hop, pooled connections dialled in the other mode are dropped;
// the hop's watcher keeps every transport built on it, so build one per hop.
func NewHTTPTransport(cfg ClientConfig, base *http.Transport) *http.Transport {
	if base == nil {
		base = http.DefaultTransport.(*http.Transport)
	}
	transport := base.Clone()
	transport.ForceAttemptHTTP2 = false
	// Through a proxy, such as HTTP_PROXY in the environment, the hop's
	// handshake would run with the proxy instead of the server it names.
	transport.Proxy = nil
	tlsConfig := cfg.clientTLSConfig([]string{protoHTTP11})
	dial := transport.DialContext
	if dial == nil {
		dial = (&net.Dialer{}).DialContext
	}

	initial := context.Background()
	mode, source := cfg.clientMode(initial)
	cfg.metrics().recordClientMode(initial, cfg.Name, mode, source)

	// A pooled keep-alive connection is never dialled again, so on every
	// tick the watcher reads the hop's mode, and while a connection dialled
	// in the other mode is open, idle ones are dropped. One busy at that
	// instant finishes its request and is dropped at the first tick it is idle.
	var plain, secure atomic.Int64
	cfg.watched.register(func(mode ClientMode) {
		if (mode == ClientOn && plain.Load() > 0) || (mode == ClientOff && secure.Load() > 0) {
			transport.CloseIdleConnections()
		}
	})

	transport.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		raw, err := dial(ctx, network, addr)
		if err != nil {
			return nil, err
		}

		metrics := cfg.metrics()
		mode, source := cfg.clientMode(ctx)
		metrics.recordClientMode(ctx, cfg.Name, mode, source)
		if mode == ClientOff {
			metrics.clientHandshake(ctx, cfg.Name, OutcomePlaintext)

			return countOpen(raw, &plain), nil
		}

		// net/http bounds only https handshakes, and the dial context outlives
		// the request, so a silent server would hold this dial forever.
		handshakeCtx := ctx
		if timeout := transport.TLSHandshakeTimeout; timeout > 0 {
			var cancel context.CancelFunc
			handshakeCtx, cancel = context.WithTimeout(ctx, timeout)
			defer cancel()
		}
		conn := tls.Client(countOpen(raw, &secure), tlsConfig)
		if err := conn.HandshakeContext(handshakeCtx); err != nil {
			_ = conn.Close()
			metrics.clientHandshake(ctx, cfg.Name, handshakeOutcome(err))
			cfg.log().Warn(ctx, "mtls: client handshake failed", zap.String("client_hop", cfg.Name), zap.Error(err))

			return nil, err
		}
		metrics.clientHandshake(ctx, cfg.Name, OutcomeTLS)

		return conn, nil
	}

	return transport
}

// countOpen counts conn in open until it closes.
func countOpen(conn net.Conn, open *atomic.Int64) net.Conn {
	open.Add(1)

	return &hookedConn{Conn: conn, onClose: func() { open.Add(-1) }}
}
