package cfg

import "github.com/willscott/go-nfs"

const (
	DefaultHandleCacheLimit = 16384
	DefaultDirVerifierLimit = 256
)

type Config struct {
	Logging           bool
	Tracing           bool
	Metrics           bool
	RecordStatCalls   bool
	RecordHandleCalls bool
	NFSLogLevel       nfs.LogLevel

	// HandleCacheLimit is how many file handles the proxy remembers for each
	// mounted volume. A handle that falls out is stale to the guest. Zero
	// means DefaultHandleCacheLimit.
	HandleCacheLimit int

	// DirVerifierLimit is how many directory listings are kept for paginated
	// READDIR and READDIRPLUS. Zero means DefaultDirVerifierLimit.
	DirVerifierLimit int
}
