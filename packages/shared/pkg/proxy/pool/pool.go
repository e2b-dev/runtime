package pool

import (
	"context"
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

// RequestOutcome names how a proxied request ended. It is reported through the
// OnOutcome callback so each service can count it on its own meter. The shared
// proxy stays telemetry-agnostic: it only classifies, the caller counts.
type RequestOutcome string

const (
	// OutcomeOK: the upstream answered and the response was forwarded.
	OutcomeOK RequestOutcome = "ok"
	// OutcomeRoutingFailed: the request could not be routed to a destination
	// (no destination resolved, or an input the proxy rejected). Answered 4xx/500.
	OutcomeRoutingFailed RequestOutcome = "routing_failed"
	// OutcomeUpstreamUnreachable: the destination resolved but the proxy could
	// not reach it (dial/transport failure, or a closed sandbox port). Answered 502.
	OutcomeUpstreamUnreachable RequestOutcome = "upstream_unreachable"
	// OutcomeClientClosed: the client went away before the proxy answered
	// (context cancelled, connection reset, EOF). Nobody is left to read a page.
	OutcomeClientClosed RequestOutcome = "client_closed"
	// OutcomeSandboxNotFound: no sandbox matched the request.
	OutcomeSandboxNotFound RequestOutcome = "sandbox_not_found"
	// OutcomeTooManyConnections: the per-sandbox connection limit shed the request.
	OutcomeTooManyConnections RequestOutcome = "too_many_connections"
)

// OutcomeRecorder is the callback the proxy calls once per request with its
// terminal outcome and the destination node it was routed to (empty when no
// destination was resolved or the embedder has no node identity). Nil is valid
// and means "do not record".
type OutcomeRecorder func(ctx context.Context, outcome RequestOutcome, node string)

type ProxyPool struct {
	clients               atomic.Int64
	pool                  *smap.Map[*ProxyClient]
	maxClientConns        int
	maxConnectionAttempts int
	idleTimeout           time.Duration
	totalConnsCounter     atomic.Uint64
	currentConnsCounter   atomic.Int64
	disableKeepAlives     bool
	onOutcome             OutcomeRecorder
}

func New(maxClientConns int, maxConnectionAttempts int, idleTimeout time.Duration, disableKeepAlives bool, onOutcome OutcomeRecorder) *ProxyPool {
	return &ProxyPool{
		pool:                  smap.New[*ProxyClient](),
		maxClientConns:        maxClientConns,
		maxConnectionAttempts: maxConnectionAttempts,
		idleTimeout:           idleTimeout,
		disableKeepAlives:     disableKeepAlives,
		onOutcome:             onOutcome,
	}
}

// RecordOutcome reports a request outcome (with the destination node, which may
// be empty) through the configured recorder, if any. Safe to call with a nil
// recorder.
func (p *ProxyPool) RecordOutcome(ctx context.Context, outcome RequestOutcome, node string) {
	if p.onOutcome != nil {
		p.onOutcome(ctx, outcome, node)
	}
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
		p.onOutcome,
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
