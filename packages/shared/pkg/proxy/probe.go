package proxy

import (
	"context"
	"errors"
	"net/http"

	"github.com/e2b-dev/infra/packages/shared/pkg/proxy/pool"
)

// HeaderProbe asks a proxy whether it can route the request's sandbox,
// without forwarding the request. A proxy built WithProbe answers a request
// carrying "true" with the same header set to "true" when the sandbox is
// here, "false" when it is not, and leaves it out when it cannot tell. A
// sandbox it routes is answered 204 instead of being forwarded, and one it
// does not have 404; every other answer is the page the request would get
// anyway.
const HeaderProbe = "X-E2b-Sandbox-Probe"

// ProbeAnswer is what a proxy told a HeaderProbe request.
type ProbeAnswer string

const (
	// ProbeFound: the sandbox was routed, answered 204 with "true".
	ProbeFound ProbeAnswer = "found"
	// ProbeClaimed: the sandbox is here but was not routed, answered with
	// its error page and "true".
	ProbeClaimed ProbeAnswer = "claimed"
	// ProbeNotFound: the sandbox is not here, answered "false".
	ProbeNotFound ProbeAnswer = "not_found"
	// ProbeUnknown: the proxy cannot tell, answered without the header.
	ProbeUnknown ProbeAnswer = "unknown"
	// ProbeCanceled: the request ended before it was routed.
	ProbeCanceled ProbeAnswer = "canceled"
)

// ProbeObserver is told the answer to every HeaderProbe request.
type ProbeObserver func(ctx context.Context, answer ProbeAnswer)

// Option configures a Proxy.
type Option func(*options)

type options struct {
	probe         bool
	observeProbe  ProbeObserver
	transportWrap pool.TransportWrap
}

// WithProbe makes the proxy answer HeaderProbe requests and, when observe is
// not nil, report each answer to it. Without it the header is forwarded like
// any other.
func WithProbe(observe ProbeObserver) Option {
	return func(o *options) {
		o.probe = true
		o.observeProbe = observe
	}
}

func (o options) observe(ctx context.Context, answer ProbeAnswer) {
	if o.observeProbe != nil {
		o.observeProbe(ctx, answer)
	}
}

// WithTransportWrap makes the proxy build each upstream transport through w.
func WithTransportWrap(w pool.TransportWrap) Option {
	return func(o *options) {
		o.transportWrap = w
	}
}

func isProbeRequest(r *http.Request) bool {
	return r.Header.Get(HeaderProbe) == "true"
}

// routePresence is what the routing outcome shows about whether this proxy
// has the sandbox: held when it was routed, the error's own Presence for a
// SandboxError, and unknown for any other error.
func routePresence(err error) Presence {
	if err == nil {
		return PresenceHeld
	}

	if sandboxErr, ok := errors.AsType[SandboxError](err); ok {
		return sandboxErr.Presence()
	}

	return PresenceUnknown
}
