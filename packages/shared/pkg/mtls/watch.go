package mtls

import (
	"context"
	"slices"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

// DefaultWatchInterval is how often a mode that comes from a flag is
// re-read when no traffic reads it.
const DefaultWatchInterval = 10 * time.Second

// unverifiedCloser is a Listener or ServerCredentials.
type unverifiedCloser interface {
	CloseUnverified() (int, error)
}

// watcher re-reads every flag-backed mode of one process on a timer, so a
// mode gauge follows its flag on a port or hop that sees no traffic, and
// a listener in required closes the connections required would refuse.
type watcher struct {
	interval time.Duration
	log      logger.Logger

	mu        sync.Mutex
	listeners []*watchedListener
	hops      []*watchedHop
}

// newWatcher re-reads every interval, DefaultWatchInterval when zero,
// until ctx ends.
func newWatcher(ctx context.Context, interval time.Duration, log logger.Logger) *watcher {
	if interval <= 0 {
		interval = DefaultWatchInterval
	}
	w := &watcher{interval: interval, log: log}
	go w.run(ctx)

	return w
}

func (w *watcher) run(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.tick(ctx)
		}
	}
}

// listener watches a flag-backed listener's configuration and returns the
// handle the listeners and credentials built on it register with.
func (w *watcher) listener(cfg ServerConfig) *watchedListener {
	l := &watchedListener{cfg: cfg}
	w.mu.Lock()
	w.listeners = append(w.listeners, l)
	w.mu.Unlock()

	return l
}

// hop watches a flag-backed client hop's configuration and returns the
// handle the HTTP transports built on it register with.
func (w *watcher) hop(cfg ClientConfig) *watchedHop {
	h := &watchedHop{cfg: cfg}
	w.mu.Lock()
	w.hops = append(w.hops, h)
	w.mu.Unlock()

	return h
}

func (w *watcher) tick(ctx context.Context) {
	w.mu.Lock()
	listeners := slices.Clone(w.listeners)
	hops := slices.Clone(w.hops)
	w.mu.Unlock()

	for _, l := range listeners {
		l.observe(ctx, l.cfg.policy(ctx).mode, w.log)
	}
	for _, h := range hops {
		mode, source := h.cfg.clientMode(ctx)
		h.cfg.metrics().recordClientMode(ctx, h.cfg.Name, mode, source)
		h.observe(mode)
	}
}

// watchedHop is one flag-backed client hop and the transports built on it.
type watchedHop struct {
	cfg ClientConfig

	mu        sync.Mutex
	observers []func(ClientMode)
}

// register adds a function called with the hop's mode on every tick. A nil
// h, a mode not from a flag, does nothing.
func (h *watchedHop) register(observe func(ClientMode)) {
	if h == nil {
		return
	}
	h.mu.Lock()
	h.observers = append(h.observers, observe)
	h.mu.Unlock()
}

func (h *watchedHop) observe(mode ClientMode) {
	h.mu.Lock()
	observers := slices.Clone(h.observers)
	h.mu.Unlock()

	for _, observe := range observers {
		observe(mode)
	}
}

// watchedListener is one flag-backed listener and what was built on it.
type watchedListener struct {
	cfg ServerConfig

	mu      sync.Mutex
	closers []unverifiedCloser
}

// register adds a Listener or ServerCredentials built on this listener's
// configuration. A nil l, a mode not from a flag, does nothing.
func (l *watchedListener) register(c unverifiedCloser) {
	if l == nil {
		return
	}
	l.mu.Lock()
	l.closers = append(l.closers, c)
	l.mu.Unlock()
}

// observe, while mode is required, closes on every registered closer the
// connections required would refuse. It runs on every tick, not only on
// entering required: a handshake that read the earlier mode can register
// its connection after the tick that closed the others.
func (l *watchedListener) observe(ctx context.Context, mode Mode, log logger.Logger) {
	if mode != ModeRequired {
		return
	}
	l.mu.Lock()
	closers := slices.Clone(l.closers)
	l.mu.Unlock()

	closed := 0
	for _, c := range closers {
		n, err := c.CloseUnverified()
		closed += n
		if err != nil {
			log.Warn(ctx, "mtls: closing the connections required refuses failed", zap.String("listener", l.cfg.Name), zap.Error(err))
		}
	}
	if closed > 0 {
		log.Info(ctx, "mtls: required listener closed the connections it refuses",
			zap.String("listener", l.cfg.Name), zap.Int("closed", closed))
	}
}
