// Package routing publishes the orchestrator-owned sandbox routing record
// (sandbox:routing:{sandboxID}) that client-proxy can read to reach the node.
//
// The record exists exactly while the sandbox is in the live registry: it is
// written on MarkRunning and deleted on MarkStopping. Both are best-effort: a
// failed write is logged and counted, the sandbox keeps running. See
// docs/ARCHITECTURE.md, "Sandbox routing records".
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
}

// Publisher writes the routing record for every sandbox that becomes live on
// this node and deletes it when the sandbox stops.
type Publisher struct {
	store Store

	orchestratorID string
	nodeIP         string

	mu     sync.Mutex
	routes map[string]*routeState // keyed by sandboxID/lifecycleID

	publishCounter metric.Int64Counter
	deleteCounter  metric.Int64Counter
}

// New builds a Publisher. Call Subscribe on the sandbox map to activate it.
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

	deleteCounter, err := telemetry.GetCounter(meter, telemetry.RoutingDeleteTotal)
	if err != nil {
		return nil, err
	}

	return &Publisher{
		store:          store,
		orchestratorID: orchestratorID,
		nodeIP:         nodeIP,
		routes:         map[string]*routeState{},
		publishCounter: publishCounter,
		deleteCounter:  deleteCounter,
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
	}

	// Same TTL the API uses today: the full max length from now. MarkStopping
	// deletes the record earlier in every normal path.
	expiration := time.Duration(info.MaxLengthInHours) * time.Hour

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
