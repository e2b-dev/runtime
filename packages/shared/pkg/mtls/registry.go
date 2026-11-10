package mtls

import (
	"context"
	"errors"
	"net"
	"sync"

	"go.uber.org/zap"
)

// connEntry is one open connection a listener or server credentials admitted.
type connEntry struct {
	// conn is what the server behind the registry holds (its hooked
	// underlying connection for TLS), so closing it ends the connection
	// for both sides and runs the close hook that removes the entry.
	conn  net.Conn
	state ConnState
	// peer is the SPIFFE ID of a TLS peer with one valid SPIFFE SAN; empty otherwise.
	peer string
	// underRequired marks a plaintext connection whose handshake read
	// required, which admits plaintext only for its health checks.
	underRequired bool
}

// connRegistry tracks open connections by remote address: what the
// multiplexer lookup answers from, and what a service closes on a flip to
// required (closeUnverified) or when a name leaves the allow-list
// (closeByPeer), since http.Server has no connection age.
type connRegistry struct {
	mu    sync.Mutex
	conns map[string]*connEntry
}

func newConnRegistry() *connRegistry {
	return &connRegistry{conns: map[string]*connEntry{}}
}

// add records conn under key with what the handshake established and
// reports whether the record is conn's alone. Lookups answer by remote
// address, which TCP keeps unique among open connections; on a transport
// that does not, or with an empty key, a second open connection under the
// key cannot be told from the first, so the record is kept unverified for
// both, underRequired unset, and the caller logs it.
func (r *connRegistry) add(key string, conn net.Conn, state ConnState, underRequired bool) bool {
	entry := &connEntry{conn: conn, state: state, underRequired: underRequired}
	if state.TLS && len(state.State.PeerCertificates) > 0 {
		if id, err := PeerID(state.State.PeerCertificates[0]); err == nil {
			entry.peer = id
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	existing, shared := r.conns[key]
	shared = key == "" || (shared && existing.conn != conn)
	if shared {
		entry = &connEntry{conn: conn}
	}
	r.conns[key] = entry

	return !shared
}

// warnSharedRemote logs that two open connections share key.
func warnSharedRemote(ctx context.Context, cfg ServerConfig, key string) {
	cfg.log().Warn(ctx, "mtls: another open connection shares this remote address; neither is treated as verified",
		zap.String("listener", cfg.Name), zap.String("remote", key))
}

// remove forgets key; the close hooks call it.
func (r *connRegistry) remove(key string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.conns, key)
}

// lookup is a ConnStateLookup over the registry.
func (r *connRegistry) lookup(key string) (ConnState, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	entry, ok := r.conns[key]
	if !ok {
		return ConnState{}, false
	}

	return entry.state, true
}

// closeUnverified closes every connection required would not admit now:
// plaintext ones a weaker mode admitted, TLS ones whose chain did not
// verify, and TLS ones whose peer has no identity or one that allowed does
// not admit. After a flip they are what must reconnect. A plaintext
// connection required itself admitted, for a health check, stays.
func (r *connRegistry) closeUnverified(allowed func(id string) bool) (int, error) {
	return r.closeWhere(func(e *connEntry) bool {
		if !e.state.TLS {
			return !e.underRequired
		}

		return !e.state.ChainVerified || e.peer == "" || !allowed(e.peer)
	})
}

// closeByPeer closes every TLS connection whose peer is id and returns how many.
func (r *connRegistry) closeByPeer(id string) int {
	closed, _ := r.closeWhere(func(e *connEntry) bool { return e.state.TLS && e.peer == id })

	return closed
}

// closeWhere closes the matching connections outside the lock, because each
// Close runs the hook that removes its entry.
func (r *connRegistry) closeWhere(match func(*connEntry) bool) (int, error) {
	r.mu.Lock()
	var victims []net.Conn
	for _, entry := range r.conns {
		if match(entry) {
			victims = append(victims, entry.conn)
		}
	}
	r.mu.Unlock()

	var errs []error
	for _, conn := range victims {
		if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			errs = append(errs, err)
		}
	}

	return len(victims), errors.Join(errs...)
}

// remoteKey is the registry key for conn: its remote address.
func remoteKey(conn net.Conn) string {
	if addr := conn.RemoteAddr(); addr != nil {
		return addr.String()
	}

	return ""
}
