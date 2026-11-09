package proxy

import (
	"errors"
	"net/http"

	"github.com/e2b-dev/infra/packages/shared/pkg/proxy/pool"
)

// HeaderProbe asks a proxy whether it can route the request's sandbox,
// without forwarding the request. A proxy built WithProbe answers a request
// carrying "true" with the same header set to "true" when the sandbox is
// here, "false" when it is not, and leaves it out when it cannot tell. A
// sandbox it routes is answered 204 instead of being forwarded; every other
// answer is the page the request would get anyway.
const HeaderProbe = "X-E2b-Sandbox-Probe"

// Option configures a Proxy.
type Option func(*options)

type options struct {
	probe     bool
	onOutcome pool.OutcomeRecorder
}

// WithProbe makes the proxy answer HeaderProbe requests. Without it the
// header is forwarded like any other.
func WithProbe() Option {
	return func(o *options) {
		o.probe = true
	}
}

// WithOutcomeRecorder records each request's terminal outcome through r.
// Without it outcomes are not recorded.
func WithOutcomeRecorder(r pool.OutcomeRecorder) Option {
	return func(o *options) {
		o.onOutcome = r
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
