package sandbox_catalog

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

// defaultCatalogCacheMaxStale bounds how long a cached routing record may be
// served after the backing catalog starts erroring. Routing records change
// only when a sandbox resumes or moves workers, so a short window trades a
// small staleness risk for surviving a brief Redis blip without 5xx-ing every
// request to a running sandbox.
const defaultCatalogCacheMaxStale = 60 * time.Second

// catalogCacheMaxEntries caps the fallback cache so it cannot grow without
// bound; the oldest entries are dropped once the cap is reached. The cache is a
// best-effort fallback, so evicting a live entry only costs a Redis round trip.
const catalogCacheMaxEntries = 50_000

type catalogCacheEntry struct {
	info *SandboxInfo
	at   time.Time
}

// CachingSandboxCatalog wraps a SandboxesCatalog with a small in-memory cache of
// the last successful GetSandbox result per sandbox. The cache is consulted ONLY
// when the inner catalog returns a transient error (e.g. Redis is briefly
// unavailable): a fresh success always refreshes it, and a definitive
// ErrSandboxNotFound is never served from cache (the sandbox is genuinely gone,
// and serving a stale route would send traffic to a dead worker). This narrows
// the blast radius of a Redis flap: in-flight routing of already-running
// sandboxes keeps working for up to maxStale instead of failing immediately.
type CachingSandboxCatalog struct {
	inner    SandboxesCatalog
	maxStale time.Duration

	mu      sync.Mutex
	entries map[string]catalogCacheEntry
}

var _ SandboxesCatalog = (*CachingSandboxCatalog)(nil)

// NewCachingSandboxCatalog wraps inner with an error-fallback cache. A maxStale
// of 0 uses defaultCatalogCacheMaxStale.
func NewCachingSandboxCatalog(inner SandboxesCatalog, maxStale time.Duration) *CachingSandboxCatalog {
	if maxStale <= 0 {
		maxStale = defaultCatalogCacheMaxStale
	}

	return &CachingSandboxCatalog{
		inner:    inner,
		maxStale: maxStale,
		entries:  make(map[string]catalogCacheEntry),
	}
}

func (c *CachingSandboxCatalog) GetSandbox(ctx context.Context, sandboxID string) (*SandboxInfo, error) {
	info, err := c.inner.GetSandbox(ctx, sandboxID)
	switch {
	case err == nil:
		c.store(sandboxID, info)

		return info, nil

	case errors.Is(err, ErrSandboxNotFound):
		// Definitive absence: drop any cached route and surface it. Serving a
		// stale route here would mis-route to a worker that no longer holds it.
		c.evict(sandboxID)

		return nil, err

	default:
		// Transient backing-store error (e.g. Redis blip). Serve the last known
		// route if it is still within the staleness window.
		if cached, ok := c.fresh(sandboxID); ok {
			logger.L().Warn(ctx, "sandbox catalog error; serving cached route",
				logger.WithSandboxID(sandboxID), zap.Error(err))

			return cached, nil
		}

		return nil, err
	}
}

func (c *CachingSandboxCatalog) StoreSandbox(ctx context.Context, sandboxID string, sandboxInfo *SandboxInfo, expiration time.Duration) error {
	err := c.inner.StoreSandbox(ctx, sandboxID, sandboxInfo, expiration)
	if err == nil {
		c.store(sandboxID, sandboxInfo)
	}

	return err
}

func (c *CachingSandboxCatalog) DeleteSandbox(ctx context.Context, sandboxID string, executionID string) error {
	// Evict regardless of the inner result: after a delete attempt the cached
	// route must not be served as a fallback.
	c.evict(sandboxID)

	return c.inner.DeleteSandbox(ctx, sandboxID, executionID)
}

func (c *CachingSandboxCatalog) Close(ctx context.Context) error {
	return c.inner.Close(ctx)
}

func (c *CachingSandboxCatalog) store(sandboxID string, info *SandboxInfo) {
	if info == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.entries[sandboxID]; !exists && len(c.entries) >= catalogCacheMaxEntries {
		c.evictOldestLocked()
	}

	c.entries[sandboxID] = catalogCacheEntry{info: info, at: time.Now()}
}

func (c *CachingSandboxCatalog) fresh(sandboxID string) (*SandboxInfo, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	e, ok := c.entries[sandboxID]
	if !ok {
		return nil, false
	}
	if time.Since(e.at) > c.maxStale {
		delete(c.entries, sandboxID)

		return nil, false
	}

	return e.info, true
}

func (c *CachingSandboxCatalog) evict(sandboxID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.entries, sandboxID)
}

// evictOldestLocked drops the single oldest entry; callers hold c.mu. The cache
// is a fallback, so approximate LRU by insertion time is enough.
func (c *CachingSandboxCatalog) evictOldestLocked() {
	var oldestID string
	var oldestAt time.Time
	for id, e := range c.entries {
		if oldestID == "" || e.at.Before(oldestAt) {
			oldestID, oldestAt = id, e.at
		}
	}
	if oldestID != "" {
		delete(c.entries, oldestID)
	}
}