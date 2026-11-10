package clusters

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/connectivity"

	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	"github.com/e2b-dev/infra/packages/shared/pkg/synchronization"
)

type credentialRefreshSource struct {
	clustersSyncStore

	rows    []queries.Cluster
	removes atomic.Int32
}

// PoolRemove counts removals, so the tests can tell a cluster that was
// swapped in place from one that was dropped and re-added.
func (s *credentialRefreshSource) PoolRemove(ctx context.Context, cluster *Cluster) {
	s.removes.Add(1)
	s.clustersSyncStore.PoolRemove(ctx, cluster)
}

func (s *credentialRefreshSource) SourceList(context.Context) ([]queries.Cluster, error) {
	return s.rows, nil
}

func TestClusterSynchronizationRefreshesRemoteCredential(t *testing.T) {
	t.Parallel()
	var oldRequests, replacementRequests atomic.Int32
	edge := &edgeServer{instances: []string{"svc-a"}, grpcClients: map[string]struct{}{}, onDiscovery: func(r *http.Request) {
		switch r.Header.Get(consts.EdgeApiAuthHeader) {
		case "old-test-token":
			oldRequests.Add(1)
		case "replacement-test-token":
			replacementRequests.Add(1)
		}
	}}
	endpoint := edge.start(t)
	ctx, cancel := context.WithCancel(t.Context())
	pool := newLifecycleTestPool(ctx, cancel)
	t.Cleanup(func() { pool.Close(t.Context()) })
	pool.store.activateCluster = func(cluster *Cluster, _ initialSyncMode) error {
		return pool.activateCluster(ctx, cluster, syncOnNextTick)
	}
	source := &credentialRefreshSource{clustersSyncStore: pool.store, rows: []queries.Cluster{{
		ID: uuid.New(), Endpoint: endpoint, Token: "old-test-token",
	}}}
	syncer := synchronization.NewSynchronize("credential-refresh", "Credential refresh", source)
	require.NoError(t, syncer.Sync(ctx))
	old, ok := pool.clusters.Get(source.rows[0].ID.String())
	require.True(t, ok)
	require.NoError(t, old.SyncInstances(ctx))
	require.Positive(t, oldRequests.Load())
	instance, ok := old.instances.Get("svc-a")
	require.True(t, ok, "the discovered instance joins over the shared connections")
	require.NoError(t, syncer.Sync(ctx))
	unchanged, _ := pool.clusters.Get(old.ID.String())
	require.Same(t, old, unchanged)

	source.rows[0].Token = "replacement-test-token"
	// One round retires the old cluster, closing the connections its
	// instances shared, and adds its replacement.
	require.NoError(t, syncer.Sync(ctx))
	require.Equal(t, connectivity.Shutdown, instance.GetClient().Connection.GetState())
	replacement, ok := pool.clusters.Get(old.ID.String())
	require.True(t, ok, "the cluster is never missing between rounds")
	require.Zero(t, source.removes.Load(), "the changed cluster is swapped in place, never removed first")
	require.NotSame(t, old, replacement)
	require.NoError(t, replacement.SyncInstances(ctx))
	require.Positive(t, replacementRequests.Load())

	// Local discovery does not use the database edge credential.
	require.True(t, source.SourceExists(ctx, []queries.Cluster{{ID: consts.LocalClusterID, Token: "unused"}}, &Cluster{ID: consts.LocalClusterID}))
}

// A cluster that moves to a new domain keeps its credential but changes its
// endpoint and sandbox domain; the pool replaces its clients with ones for
// the new endpoint, and sandboxes get the new domain.
func TestClusterSynchronizationFollowsAMovedEndpoint(t *testing.T) {
	t.Parallel()
	var oldRequests, movedRequests atomic.Int32
	serve := func(requests *atomic.Int32) *httptest.Server {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			requests.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"orchestrators":[]}`))
		}))
		t.Cleanup(server.Close)

		return server
	}
	oldServer, movedServer := serve(&oldRequests), serve(&movedRequests)
	ctx, cancel := context.WithCancel(t.Context())
	pool := newLifecycleTestPool(ctx, cancel)
	t.Cleanup(func() { pool.Close(t.Context()) })
	pool.store.activateCluster = func(cluster *Cluster, _ initialSyncMode) error {
		return pool.activateCluster(ctx, cluster, syncOnNextTick)
	}
	oldDomain, movedDomain := "old.example.test", "moved.example.test"
	source := &credentialRefreshSource{clustersSyncStore: pool.store, rows: []queries.Cluster{{
		ID: uuid.New(), Endpoint: strings.TrimPrefix(oldServer.URL, "http://"), Token: "test-token", SandboxProxyDomain: &oldDomain,
	}}}
	syncer := synchronization.NewSynchronize("endpoint-move", "Endpoint move", source)
	require.NoError(t, syncer.Sync(ctx))
	old, ok := pool.clusters.Get(source.rows[0].ID.String())
	require.True(t, ok)
	require.NoError(t, old.SyncInstances(ctx))
	require.Positive(t, oldRequests.Load())

	source.rows[0].Endpoint = strings.TrimPrefix(movedServer.URL, "http://")
	source.rows[0].SandboxProxyDomain = &movedDomain
	require.NoError(t, syncer.Sync(ctx))
	moved, ok := pool.clusters.Get(old.ID.String())
	require.True(t, ok, "one round replaces the moved cluster, with no round where it is missing")
	require.Zero(t, source.removes.Load(), "the moved cluster is swapped in place, never removed first")
	require.NotSame(t, old, moved)
	require.Equal(t, movedDomain, *moved.SandboxDomain)
	require.NoError(t, moved.SyncInstances(ctx))
	require.Positive(t, movedRequests.Load())
}
