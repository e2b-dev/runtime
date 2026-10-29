package pool

import (
	"net/url"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

const MaskRequestHostPortPlaceholder = "${PORT}"

// Destination contains information about where to route the request.
type Destination struct {
	Url         *url.URL
	SandboxId   string
	SandboxPort uint64
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
	// Node identifies the destination node (e.g. the orchestrator instance
	// serving the sandbox), used only to label the request-outcome metric so an
	// unreachable-upstream spike can be attributed to a single node. Empty when
	// the embedder has no meaningful node identity (the orchestrator sandbox
	// proxy, whose node is itself); recorded as "unknown" then.
	Node string
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
