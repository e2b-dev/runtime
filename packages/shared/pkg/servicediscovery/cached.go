package servicediscovery

import (
	"context"
	"sync"
	"time"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

// cachedDiscovery serves the last set a background loop read, so callers on a
// request path do not pay a source round trip. It wraps any Discoverer: the
// listers stay one-per-source, and caching is a property of the wiring rather
// than a second implementation of each one.
//
// A refresh failure is reported. The set is kept — a source that blinks should
// not empty the fleet — but ListInstances returns the error alongside it, so a
// caller can tell a broken source from an empty one and skip the cycle rather
// than reconcile against a set it cannot trust. That is what the query adapters
// do, and the two families disagreeing about it was its own recorded problem:
// a dead source used to read as an indefinitely stale one, and a source that
// had never been reached read as one with nothing on it.
type cachedDiscovery struct {
	lister   Discoverer
	logger   logger.Logger
	interval time.Duration

	mu      sync.RWMutex
	entries []Instance
	// err is the outcome of the most recent refresh, and starts non-nil: until
	// the first one lands there is no set to serve, which is not the same as an
	// empty one.
	err error

	cancel func()
}

// Cached wraps lister in a background loop that reads it every interval. The
// interval bounds one read too.
func Cached(lister Discoverer, logger logger.Logger, interval time.Duration) Discoverer {
	return &cachedDiscovery{
		lister:   lister,
		logger:   logger,
		interval: interval,
		err:      ErrNotYetSynced,
		cancel:   func() {},
	}
}

func (d *cachedDiscovery) Start(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)

	d.mu.Lock()
	d.cancel = cancel
	d.mu.Unlock()

	go d.keepInSync(ctx)
}

func (d *cachedDiscovery) Stop(context.Context) {
	d.mu.Lock()
	cancel := d.cancel
	d.mu.Unlock()

	cancel()
}

func (d *cachedDiscovery) ListInstances(context.Context) ([]Instance, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	return d.entries, d.err
}

func (d *cachedDiscovery) keepInSync(ctx context.Context) {
	d.refresh(ctx)

	ticker := time.NewTicker(d.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			d.logger.Info(ctx, "Stopping service discovery keep-in-sync")

			return
		case <-ticker.C:
			d.refresh(ctx)
		}
	}
}

func (d *cachedDiscovery) refresh(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, d.interval)
	defer cancel()

	instances, err := d.lister.ListInstances(ctx)

	d.mu.Lock()
	defer d.mu.Unlock()

	d.err = err
	if err != nil {
		// Not logged here: the error reaches the caller now, and every consumer
		// of a cached provider syncs through a loop that already reports it.
		return
	}

	d.entries = instances
}
