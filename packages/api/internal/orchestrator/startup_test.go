package orchestrator

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	"github.com/e2b-dev/infra/packages/shared/pkg/smap"
)

func TestStartupGateRequiresLocalNodeAndProjectsAfterClusters(t *testing.T) {
	t.Parallel()

	clustersReady := make(chan struct{})
	projectionStarted := make(chan struct{})
	gate := newStartupGate(t.Context(), clustersReady, func(context.Context) { close(projectionStarted) })
	o := &Orchestrator{nodes: smap.New[*nodemanager.Node](), startup: gate}

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
	gate := newStartupGate(ctx, clustersReady, func(context.Context) { close(projectionStarted) })

	cancel()
	close(clustersReady)
	gate.signalLocalNode()

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
