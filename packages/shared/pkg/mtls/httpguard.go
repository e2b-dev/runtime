package mtls

import (
	"crypto/tls"
	"net/http"
	"slices"

	"go.uber.org/zap"
)

// HTTPGuard applies the listener's policy to every request. On a required
// listener a plaintext request reaches only a configured health path, by
// GET or HEAD; everything else plaintext is answered 403. A TLS request's
// name is re-checked against the allow-list on every request, so a removed
// name is refused at its next call. net/http hands every HTTP/2 stream to
// the handler as its own request, so a plaintext connection upgraded on a
// health path carries nothing else.
//
// Wrap the application handler before httpserver.ConfigureH2C wraps the
// server's handler, so the guard sees each h2c stream.
type HTTPGuard struct {
	cfg         ServerConfig
	healthPaths []string
	lookup      ConnStateLookup
	next        http.Handler
}

// GuardOption configures NewHTTPGuard.
type GuardOption func(*HTTPGuard)

// WithGuardConnStateLookup hands the guard the listener's ConnState, which
// carries each connection's chain verdict; r.TLS does not, because the
// package verifies in VerifyConnection. A required listener needs it: without
// it every TLS request on a connection the package terminated is treated as
// unverified, plaintext in effect. It is also how a server behind a
// multiplexer that hides the connection type learns what the listener saw.
func WithGuardConnStateLookup(lookup ConnStateLookup) GuardOption {
	return func(g *HTTPGuard) { g.lookup = lookup }
}

// NewHTTPGuard wraps next. healthPaths are exact paths, such as "/health"
// and "/ready"; the query string is not part of the path.
func NewHTTPGuard(cfg ServerConfig, healthPaths []string, next http.Handler, opts ...GuardOption) *HTTPGuard {
	g := &HTTPGuard{cfg: cfg, healthPaths: slices.Clone(healthPaths), next: next}
	for _, o := range opts {
		o(g)
	}

	return g
}

// ServeHTTP implements http.Handler.
func (g *HTTPGuard) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	state, chainVerified, isTLS := g.tlsState(r)
	d := g.cfg.admit(ctx, KindHTTP, g.isHealth(r), isTLS, chainVerified, state,
		zap.String("method", r.Method), zap.String("path", r.URL.Path), zap.String("remote", r.RemoteAddr))
	switch {
	case d.refused == nil:
		g.next.ServeHTTP(w, r.WithContext(ContextWithPeer(ctx, d.peer)))
	case d.unauthenticated():
		http.Error(w, "mtls: this listener requires a verified client certificate", http.StatusForbidden)
	default:
		http.Error(w, "mtls: caller is not allowed on this listener", http.StatusForbidden)
	}
}

// isHealth reports whether the request is a GET or HEAD of exactly one of
// the health paths. The path is compared in its escaped form, as routers
// match it, so dot segments, doubled slashes, case differences and encoded
// aliases such as /%68ealth or /-%2Fhealth do not match: Go decodes those to
// the same URL.Path, but a router would serve them somewhere else.
func (g *HTTPGuard) isHealth(r *http.Request) bool {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		return false
	}

	return slices.Contains(g.healthPaths, r.URL.EscapedPath())
}

// tlsState reads the connection's TLS state and chain verdict. The verdict
// comes from the lookup when the guard has one, since this package verifies
// in VerifyConnection and Go records nothing of that in r.TLS. Without a
// lookup only Go's own verified chains count, which this package's handshakes
// leave empty, so a connection the package terminated is unverified until
// the service wires the listener's lookup.
func (g *HTTPGuard) tlsState(r *http.Request) (state tls.ConnectionState, chainVerified, isTLS bool) {
	if g.lookup != nil {
		if known, ok := g.lookup(r.RemoteAddr); ok {
			if known.TLS {
				return known.State, known.ChainVerified, true
			}

			return tls.ConnectionState{}, false, false
		}
	}
	if r.TLS != nil {
		return *r.TLS, len(r.TLS.VerifiedChains) > 0, true
	}

	return tls.ConnectionState{}, false, false
}
