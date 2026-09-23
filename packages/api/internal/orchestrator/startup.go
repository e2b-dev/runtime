package orchestrator

import (
	"context"
	"sync"
)

// startupGate is a startup-only, monotonic readiness latch. It waits for the
// first clusters inventory to complete and for a local node before projecting
// the applied cluster snapshots into the node placement pool.
type startupGate struct {
	clustersReady <-chan struct{}
	project       func(context.Context)

	localReady     chan struct{}
	ready          chan struct{}
	localReadyOnce sync.Once
	readyOnce      sync.Once
}

func newStartupGate(ctx context.Context, clustersReady <-chan struct{}, project func(context.Context)) *startupGate {
	g := &startupGate{
		clustersReady: clustersReady,
		project:       project,
		localReady:    make(chan struct{}),
		ready:         make(chan struct{}),
	}

	go g.start(ctx)

	return g
}

func (g *startupGate) Ready() <-chan struct{} {
	return g.ready
}

func (g *startupGate) signalLocalNode() {
	g.localReadyOnce.Do(func() { close(g.localReady) })
}

func (g *startupGate) start(ctx context.Context) {
	select {
	case <-ctx.Done():
		return
	case <-g.clustersReady:
	}

	select {
	case <-ctx.Done():
		return
	case <-g.localReady:
	}

	if ctx.Err() != nil {
		return
	}
	g.project(ctx)
	if ctx.Err() == nil {
		g.readyOnce.Do(func() { close(g.ready) })
	}
}
