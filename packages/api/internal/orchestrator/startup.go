package orchestrator

import (
	"context"
	"sync"
)

// startupGate is a startup-only, monotonic readiness latch. It waits for the
// first clusters inventory to complete and for a local node before projecting
// the applied cluster snapshots into the node placement pool.
//
// When the local clusters registry owns the orchestrators, the projection is
// also what registers the local node, so every registry join observed while
// that node is still missing runs the projection. Left to the periodic sync,
// the node would register at its next tick.
type startupGate struct {
	clustersReady  <-chan struct{}
	localInstances <-chan struct{}
	project        func(context.Context)

	localReady     chan struct{}
	ready          chan struct{}
	localReadyOnce sync.Once
	readyOnce      sync.Once
}

func newStartupGate(clustersReady <-chan struct{}, localInstances <-chan struct{}, project func(context.Context)) *startupGate {
	return &startupGate{
		clustersReady:  clustersReady,
		localInstances: localInstances,
		project:        project,
		localReady:     make(chan struct{}),
		ready:          make(chan struct{}),
	}
}

// start begins the wait. registerNode signals the gate through the
// Orchestrator's startup field, so that field must hold the gate before
// start: a projection that runs earlier registers the local node without
// signaling, and nothing signals it again.
func (g *startupGate) start(ctx context.Context) {
	go g.run(ctx)
}

func (g *startupGate) Ready() <-chan struct{} {
	return g.ready
}

func (g *startupGate) signalLocalNode() {
	g.localReadyOnce.Do(func() { close(g.localReady) })
}

func (g *startupGate) run(ctx context.Context) {
	select {
	case <-ctx.Done():
		return
	case <-g.clustersReady:
	}

	if !g.awaitLocalNode(ctx) {
		return
	}

	g.project(ctx)
	if ctx.Err() == nil {
		g.readyOnce.Do(func() { close(g.ready) })
	}
}

// awaitLocalNode reports whether a local node registered before the context
// ended. A registry join seen meanwhile runs the projection, which registers a
// registry-owned local node. Both ctx checks matter: select picks among ready
// cases at random, so a canceled context does not win over a ready channel.
func (g *startupGate) awaitLocalNode(ctx context.Context) bool {
	for {
		select {
		case <-ctx.Done():
			return false
		case <-g.localReady:
			return ctx.Err() == nil
		case <-g.localInstances:
			if ctx.Err() != nil {
				return false
			}

			g.project(ctx)
		}
	}
}
