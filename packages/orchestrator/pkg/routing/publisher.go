// Package routing publishes the orchestrator-owned sandbox routing record
// (sandbox:routing:{sandboxID}) that client-proxy reads to reach the sandbox.
//
// The record exists exactly while the sandbox is in the live registry: it is
// written on MarkRunning and deleted on MarkStopping. Both are best-effort: a
// failed write is logged and counted, the sandbox keeps running. A background
// loop (Run) re-asserts the record on an interval so a sandbox whose initial
// write lost a Redis blip becomes routable again within one interval instead
// of staying dark for the whole TTL. See docs/ARCHITECTURE.md, "Sandbox
// routing records".
package routing

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	catalog "github.com/e2b-dev/infra/packages/shared/pkg/sandbox-catalog"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

var _ sandbox.MapSubscriber = (*Publisher)(nil)

// defaultRepublishInterval is how often Run re-asserts every live record. It
// must stay well below the record TTL (max sandbox length, hours) so a lost
// write is recovered long before the stale record would expire, while staying
// high enough that the per-tick write fan-out (one SET per live sandbox on the
// node) is negligible against Redis.
const defaultRepublishInterval = 30 * time.Second

// Store is the subset of the Redis catalog the publisher needs.
type Store interface {
	StoreSandbox(ctx context.Context, sandboxID string, info *catalog.SandboxInfo, expiration time.Duration) error
	DeleteSandbox(ctx context.Context, sandboxID string, executionID string) error
}

var _ Store = (*catalog.RedisSandboxCatalog)(nil)

// routeState serializes publish and stop for one lifecycle.
//
// MarkRunning inserts the sandbox into the live map before OnInsert runs, and
// the lifecycle cleanup goroutine can call MarkStopping at any moment. Without
// this state a stop that lands while the Redis SET is in flight, or even before
// OnInsert starts, would skip the delete and leave a route to a dead sandbox
// until its TTL.
type routeState struct {
	mu       sync.Mutex
	inserted bool // OnInsert ran; OnStopping owns the removal from routes
	written  bool // StoreSandbox was called (result unknown on timeout, so still delete)
	stopped  bool // OnStopping ran; a later OnInsert must not write

	// Payload captured on OnInsert so Run can re-assert the record without
	// another pass through the sandbox map. Valid once written is set.
	sandboxID string
	info      *catalog.SandboxInfo
	deadline  time.Time // StartedAt + MaxLength; republish refreshes the TTL up to here and stops past it
}

// Publisher writes the routing record for every sandbox that becomes live on
// this node and deletes it when the sandbox stops.
type Publisher struct {
	store Store

	orchestratorID string
	nodeIP         string

	republishInterval time.Duration

	mu     sync.Mutex
	routes map[string]*routeState // keyed by sandboxID/lifecycleID

	publishCounter   metric.Int64Counter
	republishCounter metric.Int64Counter
	deleteCounter    metric.Int64Counter
}

// New builds a Publisher. Call Subscribe on the sandbox map to activate the
// write/delete path, and run Run in a goroutine to activate periodic
// re-assertion.
func New(
	meterProvider metric.MeterProvider,
	store Store,
	orchestratorID string,
	nodeIP string,
) (*Publisher, error) {
	meter := meterProvider.Meter("github.com/e2b-dev/infra/packages/orchestrator/pkg/routing")

	publishCounter, err := telemetry.GetCounter(meter, telemetry.RoutingPublishTotal)
	if err != nil {
		return nil, err
	}

	republishCounter, err := telemetry.GetCounter(meter, telemetry.RoutingRepublishTotal)
	if err != nil {
		return nil, err
	}

	deleteCounter, err := telemetry.GetCounter(meter, telemetry.RoutingDeleteTotal)
	if err != nil {
		return nil, err
	}

	return &Publisher{
		store:             store,
		orchestratorID:    orchestratorID,
		nodeIP:            nodeIP,
		republishInterval: defaultRepublishInterval,
		routes:            map[string]*routeState{},
		publishCounter:    publishCounter,
		republishCounter:  republishCounter,
		deleteCounter:     deleteCounter,
	}, nil
}

// OnInsert writes the routing record. Build sandboxes are never routable and
// are skipped.
func (p *Publisher) OnInsert(ctx context.Context, sbx *sandbox.Sandbox) {
	if sbx.Runtime.SandboxType != sandboxtypes.SandboxTypeSandbox {
		return
	}

	key := lifecycleKey(sbx)
	state := p.getOrCreate(key)

	state.mu.Lock()
	defer state.mu.Unlock()

	if state.stopped {
		// MarkStopping already ran for this lifecycle; nothing to route to.
		p.remove(key)

		return
	}

	// From here OnStopping removes the entry, so the map holds at most one
	// entry per live sandbox.
	state.inserted = true

	info := &catalog.SandboxInfo{
		OrchestratorID:   p.orchestratorID,
		OrchestratorIP:   p.nodeIP,
		ExecutionID:      sbx.Runtime.ExecutionID,
		StartedAt:        sbx.GetStartedAt(),
		MaxLengthInHours: sbx.Config.MaxSandboxLengthHours,
		NetworkPlacement: catalog.NetworkPlacementOf(sbx.NetworkPlacement),
	}

	// Same TTL the API uses today: the full max length from now. MarkStopping
	// deletes the record earlier in every normal path.
	expiration := time.Duration(info.MaxLengthInHours) * time.Hour

	// Capture the payload so Run can re-assert this record. The deadline caps
	// how long a republished record may live: never past the sandbox's own max
	// length, measured from when it started.
	state.sandboxID = sbx.Runtime.SandboxID
	state.info = info
	state.deadline = info.StartedAt.Add(expiration)

	// Set before the call: a timeout after Redis applied the SET must still be
	// followed by a delete on stop.
	state.written = true

	err := telemetry.Observe0(ctx, tracer, "routing-publish", func(ctx context.Context) error {
		return p.store.StoreSandbox(ctx, sbx.Runtime.SandboxID, info, expiration)
	})
	if err != nil {
		p.publishCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("result", "error")))
		logger.L().Error(ctx, "failed to publish sandbox routing record",
			logger.WithSandboxID(sbx.Runtime.SandboxID),
			logger.WithLifecycleID(sbx.LifecycleID),
			zap.Error(err),
		)

		return
	}

	p.publishCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("result", "ok")))
}

// Run re-asserts the routing record of every live sandbox on this node every
// republishInterval until ctx is cancelled. The first write happens in
// OnInsert; if it lost a Redis blip the sandbox would otherwise stay
// unroutable for the whole TTL. Re-asserting also refreshes the TTL so a
// long-lived sandbox never loses its record to expiry. OnStopping still owns
// deletion; a stopped lifecycle is never republished, and no record is
// refreshed past its max-length deadline.
func (p *Publisher) Run(ctx context.Context) {
	ticker := time.NewTicker(p.republishInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.republishAll(ctx)
		}
	}
}

func (p *Publisher) republishAll(ctx context.Context) {
	p.mu.Lock()
	states := make([]*routeState, 0, len(p.routes))
	for _, state := range p.routes {
		states = append(states, state)
	}
	p.mu.Unlock()

	now := time.Now()
	for _, state := range states {
		p.republishOne(ctx, state, now)
	}
}

func (p *Publisher) republishOne(ctx context.Context, state *routeState, now time.Time) {
	state.mu.Lock()
	defer state.mu.Unlock()

	// Only a lifecycle that published and has not stopped owns a record here.
	// OnStopping removes the entry under p.mu before setting stopped is even
	// observable to a reader, but the snapshot in republishAll can still hold a
	// now-stopped state; this guard drops it.
	if !state.written || state.stopped {
		return
	}

	// Never refresh a record past the sandbox's max length. Once the deadline
	// passes, let the existing record expire on its own.
	ttl := state.deadline.Sub(now)
	if ttl <= 0 {
		return
	}

	err := telemetry.Observe0(ctx, tracer, "routing-republish", func(ctx context.Context) error {
		return p.store.StoreSandbox(ctx, state.sandboxID, state.info, ttl)
	})
	if err != nil {
		p.republishCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("result", "error")))
		logger.L().Warn(ctx, "failed to re-assert sandbox routing record; it will be retried next interval",
			logger.WithSandboxID(state.sandboxID),
			zap.Error(err),
		)

		return
	}

	p.republishCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("result", "ok")))
}

// OnStopping deletes the routing record this publisher wrote for the lifecycle.
// The delete is guarded by execution ID in Redis, so a stale lifecycle never
// removes the record of a newer execution.
func (p *Publisher) OnStopping(ctx context.Context, sbx *sandbox.Sandbox) {
	if sbx.Runtime.SandboxType != sandboxtypes.SandboxTypeSandbox {
		return
	}

	key := lifecycleKey(sbx)
	state := p.getOrCreate(key)

	state.mu.Lock()
	defer state.mu.Unlock()

	state.stopped = true

	if !state.inserted {
		// OnInsert has not run yet (MarkRunning exposes the sandbox before the
		// subscribers fire). Keep the tombstone; OnInsert removes it.
		return
	}

	p.remove(key)

	if !state.written {
		return
	}

	err := telemetry.Observe0(ctx, tracer, "routing-delete", func(ctx context.Context) error {
		return p.store.DeleteSandbox(ctx, sbx.Runtime.SandboxID, sbx.Runtime.ExecutionID)
	})
	if err != nil {
		p.deleteCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("result", "error")))
		logger.L().Error(ctx, "failed to delete sandbox routing record; it expires via TTL",
			logger.WithSandboxID(sbx.Runtime.SandboxID),
			logger.WithLifecycleID(sbx.LifecycleID),
			zap.Error(err),
		)

		return
	}

	p.deleteCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("result", "ok")))
}

// OnNetworkRelease is not used by the publisher.
func (p *Publisher) OnNetworkRelease(_ context.Context, _ *sandbox.Sandbox) error { return nil }

func (p *Publisher) getOrCreate(key string) *routeState {
	p.mu.Lock()
	defer p.mu.Unlock()

	state, ok := p.routes[key]
	if !ok {
		state = &routeState{}
		p.routes[key] = state
	}

	return state
}

func (p *Publisher) remove(key string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	delete(p.routes, key)
}

func lifecycleKey(sbx *sandbox.Sandbox) string {
	return sbx.Runtime.SandboxID + "/" + sbx.LifecycleID
}

var tracer = otel.Tracer("github.com/e2b-dev/infra/packages/orchestrator/pkg/routing")
