package proxy

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/e2b-dev/infra/packages/shared/pkg/connlimit"
	"github.com/e2b-dev/infra/packages/shared/pkg/httpserver"
	"github.com/e2b-dev/infra/packages/shared/pkg/proxy/pool"
	"github.com/e2b-dev/infra/packages/shared/pkg/proxy/tracking"
	"github.com/e2b-dev/infra/packages/shared/pkg/smap"
)

// ConnectionLimitConfig bundles connection limiting and associated metric callbacks.
// When nil is passed, connection limiting is disabled.
type ConnectionLimitConfig struct {
	Limiter              *connlimit.ConnectionLimiter
	GetMaxLimit          func(ctx context.Context) int
	OnConnectionAcquired func(ctx context.Context, count int64)
	OnConnectionReleased func(ctx context.Context, durationMs int64)
	OnConnectionBlocked  func(ctx context.Context)
}

const (
	maxClientConns                      = 16384 // Reasonably big number that is lower than the number of available ports.
	idleTimeoutBufferUpstreamDownstream = 10 * time.Second
)

type Proxy struct {
	http.Server

	pool                      *pool.ProxyPool
	serverConnections         *smap.Map[*tracking.Connection]
	currentServerConnsCounter atomic.Int64
}

type MaxConnectionAttempts int

const (
	ClientProxyRetries  = 1
	SandboxProxyRetries = 5
)

func New(
	port uint16,
	maxConnectionAttempts MaxConnectionAttempts,
	idleTimeout time.Duration,
	getDestination func(r *http.Request) (*pool.Destination, error),
	connLimitConfig *ConnectionLimitConfig,
	disableKeepAlives bool,
) *Proxy {
	p := pool.New(
		maxClientConns,
		int(maxConnectionAttempts),
		idleTimeout,
		disableKeepAlives,
	)

	proxy := &Proxy{
		Server: http.Server{
			Addr:         fmt.Sprintf(":%d", port),
			ReadTimeout:  0,
			WriteTimeout: 0,
			// Downstream idle timeout (client facing) > upstream idle timeout (server facing)
			// otherwise there's a chance for a race condition when the server closes and the client tries to use the connection
			IdleTimeout:       idleTimeout + idleTimeoutBufferUpstreamDownstream,
			ReadHeaderTimeout: 0,
			Handler:           handler(p, getDestination, connLimitConfig),
		},
		pool:              p,
		serverConnections: smap.New[*tracking.Connection](),
	}
	httpserver.ConfigureH2C(&proxy.Server)

	return proxy
}

// TotalPoolConnections returns the total number of connections that have been established across whole pool.
func (p *Proxy) TotalPoolConnections() uint64 {
	return p.pool.TotalConnections()
}

// CurrentServerConnections returns the current number of connections that are alive across whole pool.
func (p *Proxy) CurrentServerConnections() int64 {
	return p.currentServerConnsCounter.Load()
}

func (p *Proxy) CurrentPoolSize() int {
	return p.pool.Size()
}

func (p *Proxy) CurrentPoolConnections() int64 {
	return p.pool.CurrentConnections()
}

func (p *Proxy) RemoveFromPool(connectionKey string) error {
	return p.pool.Close(connectionKey)
}

func (p *Proxy) ListenAndServe(ctx context.Context) error {
	var lisCfg net.ListenConfig
	l, err := lisCfg.Listen(ctx, "tcp", p.Addr)
	if err != nil {
		return err
	}

	return p.Serve(l)
}

func (p *Proxy) Serve(l net.Listener) error {
	return p.Server.Serve(tracking.NewListener(l, &p.currentServerConnsCounter, p.serverConnections))
}

func (p *Proxy) Shutdown(ctx context.Context) error {
	if err := p.Server.Shutdown(ctx); err != nil {
		return err
	}

	// net/http does not wait for hijacked H2C and upgraded connections.
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for p.CurrentServerConnections() != 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}

	return nil
}

func (p *Proxy) Close() error {
	err := p.Server.Close()
	for _, conn := range p.serverConnections.Items() {
		if closeErr := conn.Close(); closeErr != nil && !errors.Is(closeErr, net.ErrClosed) {
			err = errors.Join(err, closeErr)
		}
	}

	return err
}
