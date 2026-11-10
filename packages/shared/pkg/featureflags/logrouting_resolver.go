package featureflags

import (
	"context"
	"sync"
	"time"

	"github.com/launchdarkly/go-sdk-common/v3/ldcontext"
)

// logWriteConfigCacheTTL bounds how often LogsWriteConfigFlag is resolved.
// LaunchDarkly already caches flag data locally; this cache is for the parsed
// and validated LogWriteConfig (type checks, URL validation, dedupe, defaults).
// Log writes happen on a very hot path (per line / per request), so resolving
// that JSON config on every write would add avoidable CPU/allocations. 1s keeps
// the flag responsive while making resolution cost negligible under high volume.
const logWriteConfigCacheTTL = 1 * time.Second

// LogWriteConfigResolver caches the resolved LogWriteConfig for a short TTL so
// callers on hot log-write paths avoid evaluating LaunchDarkly on every line.
// It is safe for concurrent use.
type LogWriteConfigResolver struct {
	ff       *Client
	defaults LogWriteDefaults
	ttl      time.Duration

	mu     sync.RWMutex
	cached LogWriteConfig
	expiry time.Time
	loaded bool
}

// NewLogWriteConfigResolver builds a resolver that caches LogsWriteConfigFlag
// evaluations, merged over defaults, for a short TTL. A nil ff is supported:
// Resolve then always returns the defaults alone.
func NewLogWriteConfigResolver(ff *Client, defaults LogWriteDefaults) *LogWriteConfigResolver {
	return newLogWriteConfigResolverWithTTL(ff, defaults, logWriteConfigCacheTTL)
}

// newLogWriteConfigResolverWithTTL is the test-friendly constructor allowing a
// custom TTL so tests can exercise cache expiry without real-time sleeps.
func newLogWriteConfigResolverWithTTL(ff *Client, defaults LogWriteDefaults, ttl time.Duration) *LogWriteConfigResolver {
	return &LogWriteConfigResolver{
		ff:       ff,
		defaults: defaults,
		ttl:      ttl,
	}
}

// Resolve returns the cached LogWriteConfig, refreshing it via
// ResolveLogWriteConfig when the cache is empty or expired. Behavior for
// null/malformed/unsafe flag values is identical to ResolveLogWriteConfig.
func (r *LogWriteConfigResolver) Resolve(ctx context.Context, contexts ...ldcontext.Context) LogWriteConfig {
	now := time.Now()

	r.mu.RLock()
	if r.loaded && now.Before(r.expiry) {
		cfg := r.cached
		r.mu.RUnlock()

		return cfg
	}
	r.mu.RUnlock()

	r.mu.Lock()
	defer r.mu.Unlock()

	// Another goroutine may have refreshed the cache while we were waiting for
	// the exclusive lock.
	if r.loaded && now.Before(r.expiry) {
		return r.cached
	}

	cfg := ResolveLogWriteConfig(ctx, r.ff, r.defaults, contexts...)
	r.cached = cfg
	r.expiry = now.Add(r.ttl)
	r.loaded = true

	return cfg
}
