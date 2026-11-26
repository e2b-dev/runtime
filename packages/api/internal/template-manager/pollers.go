package template_manager

import (
	"context"
	"fmt"
	"sync"
)

// pollerGroup lets shutdown stop the build status pollers before it closes the
// builder connections and the database they use: a poll cut off by a closing
// connection records the build as failed, while a cancelled one leaves it in
// progress for another instance. The zero value is ready to use.
type pollerGroup struct {
	mu      sync.Mutex
	closed  bool
	stop    chan struct{}
	cancels map[uint64]context.CancelFunc
	nextKey uint64
	running sync.WaitGroup
}

// stopLocked returns the channel close closes. g.mu must be held.
func (g *pollerGroup) stopLocked() chan struct{} {
	if g.stop == nil {
		g.stop = make(chan struct{})
	}

	return g.stop
}

// stopping returns a channel that is closed once close has been called.
func (g *pollerGroup) stopping() <-chan struct{} {
	g.mu.Lock()
	defer g.mu.Unlock()

	return g.stopLocked()
}

// start registers a poller. The returned context ends with ctx or when the
// group closes, and done must be called once the poller has returned. ok is
// false once the group is closed, in which case no poller may start.
func (g *pollerGroup) start(ctx context.Context) (pollCtx context.Context, done func(), ok bool) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.closed {
		return nil, nil, false
	}

	pollCtx, cancel := context.WithCancel(ctx)
	if g.cancels == nil {
		g.cancels = make(map[uint64]context.CancelFunc)
	}
	key := g.nextKey
	g.nextKey++
	g.cancels[key] = cancel

	// Under g.mu, so every Add happens before close's Wait.
	g.running.Add(1)

	return pollCtx, func() {
		g.mu.Lock()
		delete(g.cancels, key)
		g.mu.Unlock()

		cancel()
		g.running.Done()
	}, true
}

// close cancels every running poller and waits until they have returned, or
// until ctx ends. Pollers cannot start afterwards.
func (g *pollerGroup) close(ctx context.Context) error {
	g.mu.Lock()
	if !g.closed {
		g.closed = true
		close(g.stopLocked())
		for _, cancel := range g.cancels {
			cancel()
		}
	}
	g.mu.Unlock()

	stopped := make(chan struct{})
	go func() {
		g.running.Wait()
		close(stopped)
	}()

	select {
	case <-stopped:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("build status pollers still running: %w", ctx.Err())
	}
}
