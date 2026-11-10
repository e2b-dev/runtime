package pool

import (
	"context"
	"net/http"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/smap"
)

// hostConnectionSplit is used for splitting the total number of connections between the hosts.
// This is used to limit the number of connections per host to avoid exhausting the number of available via one host.
// The total number of connection per host will be total connections / hostConnectionSplit.
// If the total connections is lower than hostConnectionSplit, the total connections will be used for each host.
const hostConnectionSplit = 4

// ClientProxyConnectionKey is a constant connection key for client proxy, because we don't have to separate connection pools
// as we need to do when connecting to sandboxes (from orchestrator proxy) to prevent reuse of pool connections
// by different sandboxes cause failed connections.
const ClientProxyConnectionKey = "client-proxy"

// TransportWrap returns the transport for the client the pool builds for a
// ConnectionKey or Owner, given the pool's own. Like
// Destination.InsecureSkipTLSVerify, it runs once, when that client is built,
// under the pool's lock, so it must not block or dial.
type TransportWrap func(connectionKey string, base *http.Transport) *http.Transport

// Option configures a ProxyPool.
type Option func(*ProxyPool)

// WithTransportWrap makes the pool build each client's transport through w.
func WithTransportWrap(w TransportWrap) Option {
	return func(p *ProxyPool) {
		p.transportWrap = w
	}
}

type ProxyPool struct {
	clients               atomic.Int64
	pool                  *smap.Map[*ProxyClient]
	maxClientConns        int
	maxConnectionAttempts int
	idleTimeout           time.Duration
	totalConnsCounter     atomic.Uint64
	currentConnsCounter   atomic.Int64
	disableKeepAlives     bool
	transportWrap         TransportWrap
}

func New(maxClientConns int, maxConnectionAttempts int, idleTimeout time.Duration, disableKeepAlives bool, opts ...Option) *ProxyPool {
	p := &ProxyPool{
		pool:                  smap.New[*ProxyClient](),
		maxClientConns:        maxClientConns,
		maxConnectionAttempts: maxConnectionAttempts,
		idleTimeout:           idleTimeout,
		disableKeepAlives:     disableKeepAlives,
	}
	for _, opt := range opts {
		opt(p)
	}

	return p
}

// NewOwner opens an admission lifetime.
func (p *ProxyPool) NewOwner() *Owner {
	ctx, cancel := context.WithCancel(context.Background())

	return &Owner{pool: p, ctx: ctx, cancel: cancel, done: make(chan struct{})}
}

func (p *ProxyPool) Get(ctx context.Context, d *Destination) *ProxyClient {
	if d.Owner != nil {
		return d.Owner.clientFor(ctx, d)
	}

	return p.pool.Upsert(d.ConnectionKey, nil, func(exist bool, client *ProxyClient, _ *ProxyClient) *ProxyClient {
		if exist {
			return client
		}

		return p.NewOwner().clientFor(ctx, d)
	})
}

func (p *ProxyPool) newClient(ctx context.Context, d *Destination, owner *Owner) *ProxyClient {
	withFields := make([]zap.Field, 0)

	if d.IncludeSandboxIdInProxyErrorLogger {
		withFields = append(withFields, logger.WithSandboxID(d.SandboxId), logger.WithExecutionID(d.ConnectionKey))
	}

	if d.MaskRequestHost != nil {
		withFields = append(withFields, zap.Stringp("mask_request_host", d.MaskRequestHost))
	}

	l := logger.L().With(withFields...)

	stdLogger, err := zap.NewStdLogAt(l.Detach(ctx), zap.WarnLevel)
	if err != nil {
		l.Warn(ctx, "failed to create logger", zap.Error(err))
	}

	l.Debug(ctx, "creating proxy client in pool")

	//nolint:contextcheck // Function `newProxyClient->newProxyClient$4` should pass the context parameter, but there is no reason to
	return newProxyClient(
		owner,
		p.maxClientConns,
		// We limit the max number of connections per host to avoid exhausting the number of available via one host.
		func() int {
			if p.maxClientConns <= hostConnectionSplit {
				return p.maxClientConns
			}

			return p.maxClientConns / hostConnectionSplit
		}(),
		p.maxConnectionAttempts,
		p.idleTimeout,
		&p.totalConnsCounter,
		&p.currentConnsCounter,
		stdLogger,
		p.disableKeepAlives,
		d.InsecureSkipTLSVerify,
		d.ConnectionKey,
		p.transportWrap,
	)
}

func (p *ProxyPool) TotalConnections() uint64 {
	return p.totalConnsCounter.Load()
}

func (p *ProxyPool) CurrentConnections() int64 {
	return p.currentConnsCounter.Load()
}

func (p *ProxyPool) Size() int {
	return int(p.clients.Load())
}
