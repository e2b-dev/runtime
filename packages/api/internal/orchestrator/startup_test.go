package orchestrator

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	"github.com/e2b-dev/infra/packages/shared/pkg/smap"
)

// settle bounds how long a negative assertion waits for something that must
// not happen.
const settle = 100 * time.Millisecond

// joinLocalRegistry mirrors Pool.notifyLocalInstance: a coalescing,
// non-blocking token.
func joinLocalRegistry(registry chan struct{}) {
	select {
	case registry <- struct{}{}:
	default:
	}
}

func TestStartupGateRequiresLocalNodeAndProjectsAfterClusters(t *testing.T) {
	t.Parallel()

	clustersReady := make(chan struct{})
	projectionStarted := make(chan struct{})
	gate := newStartupGate(clustersReady, nil, func(context.Context) { close(projectionStarted) })
	o := &Orchestrator{nodes: smap.New[*nodemanager.Node](), startup: gate}
	gate.start(t.Context())

	o.registerNode(&nodemanager.Node{ID: "remote", ClusterID: uuid.New()})
	close(clustersReady)

	select {
	case <-gate.Ready():
		t.Fatal("remote node must not satisfy startup readiness")
	default:
	}
	select {
	case <-projectionStarted:
		t.Fatal("projection must wait for a local node")
	default:
	}

	o.registerNode(&nodemanager.Node{ID: "local", ClusterID: consts.LocalClusterID})

	select {
	case <-projectionStarted:
	case <-t.Context().Done():
		t.Fatal("startup projection did not run")
	}
	select {
	case <-gate.Ready():
	case <-t.Context().Done():
		t.Fatal("startup readiness was not released after projection")
	}

	o.deregisterNode(&nodemanager.Node{ID: "local", ClusterID: consts.LocalClusterID})
	require.NotNil(t, gate.Ready())
	select {
	case <-gate.Ready():
	default:
		t.Fatal("startup readiness must remain monotonic")
	}
}

func TestStartupGateCancellationPreventsReadiness(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	clustersReady := make(chan struct{})
	projectionStarted := make(chan struct{})
	gate := newStartupGate(clustersReady, nil, func(context.Context) { close(projectionStarted) })
	gate.start(ctx)

	cancel()
	close(clustersReady)
	gate.signalLocalNode()
	time.Sleep(settle)

	select {
	case <-projectionStarted:
		t.Fatal("canceled startup must not run projection")
	default:
	}
	select {
	case <-gate.Ready():
		t.Fatal("canceled startup must not become ready")
	default:
	}
}

func TestStartupGateProjectsOnLocalRegistryChange(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		// registersOnCall is the projection call, counted from one, that
		// registers the local node; zero never registers it.
		registersOnCall     int32
		joinBeforeInventory bool
		joinsAfterInventory int
		wantReady           bool
		wantProjections     int32
	}{
		{
			name:                "a join after the inventory runs the projection, which registers the node",
			registersOnCall:     1,
			joinsAfterInventory: 1,
			wantReady:           true,
			wantProjections:     2,
		},
		{
			name:                "a join before the inventory waits for it",
			registersOnCall:     1,
			joinBeforeInventory: true,
			wantReady:           true,
			wantProjections:     2,
		},
		{
			name:                "a projection that finds no orchestrator keeps the gate closed",
			registersOnCall:     0,
			joinsAfterInventory: 1,
			wantReady:           false,
			wantProjections:     1,
		},
		{
			name:                "a later join registers the node after an empty pass",
			registersOnCall:     2,
			joinsAfterInventory: 2,
			wantReady:           true,
			wantProjections:     3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			clustersReady := make(chan struct{})
			registry := make(chan struct{}, 1)
			o := &Orchestrator{nodes: smap.New[*nodemanager.Node]()}

			var projections atomic.Int32
			project := func(context.Context) {
				if projections.Add(1) == tt.registersOnCall {
					o.registerNode(&nodemanager.Node{ID: "local", ClusterID: consts.LocalClusterID})
				}
			}
			o.startup = newStartupGate(clustersReady, registry, project)
			o.startup.start(t.Context())

			if tt.joinBeforeInventory {
				joinLocalRegistry(registry)
				time.Sleep(settle)
				require.Zero(t, projections.Load(), "the projection must wait for the clusters inventory")
			}

			close(clustersReady)

			for i := range tt.joinsAfterInventory {
				if i > 0 {
					require.Eventually(t, func() bool { return projections.Load() == int32(i) }, time.Second, time.Millisecond)
				}

				joinLocalRegistry(registry)
			}

			select {
			case <-o.startup.Ready():
				require.True(t, tt.wantReady, "the gate opened without a local node")
			case <-time.After(time.Second):
				require.Falsef(t, tt.wantReady, "the gate did not open; projections=%d", projections.Load())
			}

			require.Eventually(t, func() bool { return projections.Load() == tt.wantProjections }, time.Second, time.Millisecond)
			time.Sleep(settle)
			require.Equal(t, tt.wantProjections, projections.Load())
		})
	}
}

// A join buffered before the gate exists must not run the projection until
// start, so the holder can store the gate first and the registration the
// projection performs reaches it.
func TestStartupGateIdlesUntilStarted(t *testing.T) {
	t.Parallel()

	clustersReady := make(chan struct{})
	close(clustersReady)
	registry := make(chan struct{}, 1)
	joinLocalRegistry(registry)
	o := &Orchestrator{nodes: smap.New[*nodemanager.Node]()}

	var projections atomic.Int32
	gate := newStartupGate(clustersReady, registry, func(context.Context) {
		projections.Add(1)
		o.registerNode(&nodemanager.Node{ID: "local", ClusterID: consts.LocalClusterID})
	})
	time.Sleep(settle)
	require.Zero(t, projections.Load(), "the projection must not run before start")

	o.startup = gate
	gate.start(t.Context())

	select {
	case <-gate.Ready():
	case <-time.After(time.Second):
		t.Fatalf("the gate did not open; projections=%d", projections.Load())
	}
	require.Equal(t, int32(2), projections.Load())
}

func TestStartupGateCancellationIgnoresRegistryChange(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	clustersReady := make(chan struct{})
	registry := make(chan struct{}, 1)

	var projections atomic.Int32
	gate := newStartupGate(clustersReady, registry, func(context.Context) { projections.Add(1) })
	gate.start(ctx)

	cancel()
	close(clustersReady)
	joinLocalRegistry(registry)
	time.Sleep(settle)

	require.Zero(t, projections.Load(), "canceled startup must not run the projection")
	select {
	case <-gate.Ready():
		t.Fatal("canceled startup must not become ready")
	default:
	}
}
