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
	// ConnectionKey uniquely identifies a single sandbox lifecycle. It is
	// used for two purposes:
	//   1. keepalive connection pool isolation, so connections to a reused
	//      IP:port pair are not accidentally shared across sandboxes;
	//   2. per-sandbox ingress connection limiter accounting.
	// Embedders should pick a value that is unique per lifecycle.
	ConnectionKey                      string
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
