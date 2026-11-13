package pool

import (
	"net/url"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

const MaskRequestHostPortPlaceholder = "${PORT}"

// ExpectedExecutionIDHeader carries the trusted execution selected by the
// routing lookup. The final proxy consumes it before forwarding to the guest.
const ExpectedExecutionIDHeader = "X-E2B-Expected-Execution"

// Destination contains information about where to route the request.
type Destination struct {
	Url         *url.URL
	SandboxId   string
	SandboxPort uint64
	// ExpectedExecutionID is set only when the next hop resolves executions.
	// An empty value strips any caller-supplied assertion.
	ExpectedExecutionID string
	// Should we return the error about closed port if there is a problem with a connection to upstream?
	DefaultToPortError bool
	RequestLogger      logger.Logger
	// ConnectionKey identifies the keepalive connection pool entry: connections
	// to a reused IP:port pair are not accidentally shared across sandboxes.
	// Embedders that isolate per lifecycle pick a value unique per lifecycle;
	// embedders that intentionally share one pool (client-proxy) use a constant.
	ConnectionKey string
	// LimitKey scopes the ingress connection limiter accounting. When empty the
	// limiter falls back to ConnectionKey (see LimiterKey), so a lifecycle-keyed
	// pool is limited per lifecycle with no extra field. An embedder that shares
	// one ConnectionKey across sandboxes (for pool reuse) sets LimitKey to the
	// per-sandbox value so limiting stays per sandbox without fragmenting the pool.
	LimitKey                           string
	IncludeSandboxIdInProxyErrorLogger bool
	// MaskRequestHost is used to mask the request host.
	MaskRequestHost *string
	// InsecureSkipTLSVerify is read once, when the pool builds the client for
	// ConnectionKey or Owner; later destinations sharing it inherit the first
	// one's value. Keep it constant per ConnectionKey.
	InsecureSkipTLSVerify bool
	// Owner, when set, holds the upstream connections instead of the pool's
	// ConnectionKey entry. Closing it ends them.
	Owner *Owner
}

// LimiterKey returns the key the ingress connection limiter accounts against:
// LimitKey when set, otherwise ConnectionKey. This lets an embedder share one
// ConnectionKey for pool reuse while still limiting per sandbox via LimitKey.
func (d Destination) LimiterKey() string {
	if d.LimitKey != "" {
		return d.LimitKey
	}

	return d.ConnectionKey
}
