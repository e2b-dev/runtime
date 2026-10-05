package clusters

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/servicediscovery"
	"github.com/e2b-dev/infra/packages/shared/pkg/smap"
	"github.com/e2b-dev/infra/packages/shared/pkg/synchronization"
)

// storeWithInstances builds a sync store whose creation step is a stub, so the
// keying can be exercised without a gRPC server behind every instance.
func storeWithInstances(t *testing.T) (instancesSyncStore, *smap.Map[*Instance]) {
	t.Helper()

	instances := smap.New[*Instance]()

	return instancesSyncStore{
		clusterID: uuid.New(),
		instances: instances,
		instanceCreation: func(_ context.Context, item servicediscovery.Instance) (*Instance, error) {
			return &Instance{workloadID: item.WorkloadID, NodeID: item.NodeID, LocalIPAddress: item.IPAddress}, nil
		},
	}, instances
}

// The pool's map key and its source-existence check must be the same identity.
// While the map keyed on the machine and the check keyed on the process, a
// builder that restarted on a machine already in the pool was skipped by
// PoolExists and only re-added a cycle later, after PoolRemove had taken the
// dead one out.
func TestInstancesSyncStore_ReplacesAProcessThatRestartedOnTheSameMachine(t *testing.T) {
	t.Parallel()

	store, instances := storeWithInstances(t)

	before := servicediscovery.Instance{WorkloadID: "alloc-1", NodeID: "node-a", IPAddress: "10.0.0.1"}
	store.PoolInsert(t.Context(), before)
	require.Equal(t, 1, instances.Count())

	// Same machine, new run of the process.
	after := servicediscovery.Instance{WorkloadID: "alloc-2", NodeID: "node-a", IPAddress: "10.0.0.1"}

	assert.False(t, store.PoolExists(t.Context(), after),
		"the restarted process must not read as already present just because its machine is")

	store.PoolInsert(t.Context(), after)

	assert.False(t, store.SourceExists(t.Context(), []servicediscovery.Instance{after}, mustGet(t, instances, "alloc-1")),
		"the dead run must read as gone from the source")
	assert.True(t, store.SourceExists(t.Context(), []servicediscovery.Instance{after}, mustGet(t, instances, "alloc-2")))
}

// Two builders on one machine are two pool entries; keying the map on the
// machine silently kept only the first.
func TestInstancesSyncStore_KeepsBothInstancesSharingAMachine(t *testing.T) {
	t.Parallel()

	store, instances := storeWithInstances(t)

	for _, i := range []servicediscovery.Instance{
		{WorkloadID: "alloc-1", NodeID: "node-a", IPAddress: "10.0.0.1"},
		{WorkloadID: "alloc-2", NodeID: "node-a", IPAddress: "10.0.0.2"},
	} {
		require.False(t, store.PoolExists(t.Context(), i))
		store.PoolInsert(t.Context(), i)
	}

	assert.Equal(t, 2, instances.Count())
}

// The projection newInstance performs, exercised directly: the sync tests reach
// it through a stub, so a wrong facet here survives all of them.
func TestInstanceFrom_ProjectsBothFacetsOntoThePoolEntry(t *testing.T) {
	t.Parallel()

	clusterID := uuid.New()
	instance := instanceFrom(clusterID, servicediscovery.Instance{
		WorkloadID: "alloc-1", NodeID: "node-a", IPAddress: "10.0.0.1", Port: 5008,
	}, nil)

	assert.Equal(t, "alloc-1", instance.workloadID, "the pool entry is identified by the process")
	assert.Equal(t, "node-a", instance.NodeID, "and still records the machine, which builds persist")
	assert.Equal(t, "10.0.0.1", instance.LocalIPAddress)
	assert.Equal(t, clusterID, instance.ClusterID)
}

// The remote proxy routes on the service id, which is the discovered ID. Given
// the machine instead, a call lands on whatever is running there now.
func TestRemoteInstanceAuthorization_RoutesOnTheProcess(t *testing.T) {
	t.Parallel()

	auth := remoteInstanceAuthorization("shh", true, servicediscovery.Instance{WorkloadID: "svc-aaa", NodeID: "remote-node-1"})

	assert.Equal(t, "svc-aaa", auth.serviceInstanceID)
	assert.Equal(t, "shh", auth.secret)
	assert.True(t, auth.tls)
}

// Cluster.Start's mode decides whether discovery runs immediately or waits for
// the first scheduled tick. A startup remote already applied its snapshot, so
// running one immediately would be a second, overlapping discovery round.
func TestClusterStartRunsInitialDiscoveryOnlyForSyncImmediately(t *testing.T) {
	t.Parallel()

	// The scheduled interval is far longer than this window, so any discovery
	// observed here is the initial round rather than a periodic one.
	const initialRoundWindow = 250 * time.Millisecond

	for _, tc := range []struct {
		name            string
		mode            initialSyncMode
		expectDiscovery bool
	}{
		{name: "syncImmediately runs the initial round", mode: syncImmediately, expectDiscovery: true},
		{name: "syncOnNextTick skips the initial round", mode: syncOnNextTick, expectDiscovery: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			discovered := make(chan struct{}, 1)
			instances := smap.New[*Instance]()
			cluster := NewCluster(
				uuid.New(),
				nil,
				"",
				instances,
				synchronization.NewSynchronize("test-cluster-instances", "Test cluster instances", instancesSyncStore{
					clusterID: uuid.New(),
					instances: instances,
					discovery: listInstancesFunc{list: func(context.Context) ([]servicediscovery.Instance, error) {
						select {
						case discovered <- struct{}{}:
						default:
						}

						return nil, nil
					}},
					instanceCreation: func(context.Context, servicediscovery.Instance) (*Instance, error) {
						return &Instance{}, nil
					},
				}),
				nil,
			)
			t.Cleanup(func() { _ = cluster.Close(t.Context()) })

			cluster.Start(t.Context(), tc.mode)

			select {
			case <-discovered:
				require.True(t, tc.expectDiscovery, "discovery must not run before the first scheduled tick")
			case <-time.After(initialRoundWindow):
				require.False(t, tc.expectDiscovery, "the initial discovery round did not run")
			}
		})
	}
}

// listInstancesFunc is a query-style discoverer: it answers every call
// directly, so it takes the shared no-op lifecycle half of the interface.
type listInstancesFunc struct {
	servicediscovery.NoSync

	list func(ctx context.Context) ([]servicediscovery.Instance, error)
}

func (f listInstancesFunc) ListInstances(ctx context.Context) ([]servicediscovery.Instance, error) {
	return f.list(ctx)
}

func TestPoolCloseAndParentCancellationPreventLateStartupMutation(t *testing.T) {
	t.Parallel()

	t.Run("Pool Close cancels an in-flight startup probe", func(t *testing.T) {
		t.Parallel()

		lifecycleCtx, cancel := context.WithCancel(t.Context())
		pool := newLifecycleTestPool(lifecycleCtx, cancel)
		t.Cleanup(func() { pool.Close(t.Context()) })
		probeStarted := make(chan struct{})
		probeCanceled := make(chan error, 1)
		server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
			close(probeStarted)
			<-request.Context().Done()
			probeCanceled <- request.Context().Err()
		}))
		t.Cleanup(server.Close)
		target := queries.Cluster{ID: uuid.New(), Endpoint: strings.TrimPrefix(server.URL, "http://")}
		startResult := make(chan error, 1)
		go func() { startResult <- pool.startInitialRemoteCluster(lifecycleCtx, lifecycleCtx, target) }()

		completionCtx, completionCancel := context.WithTimeout(t.Context(), time.Second)
		defer completionCancel()
		select {
		case <-probeStarted:
		case <-completionCtx.Done():
			t.Fatal("startup probe did not reach its cancellation barrier")
		}

		closeFinished := make(chan struct{})
		go func() {
			pool.Close(completionCtx)
			close(closeFinished)
		}()
		select {
		case <-pool.lifecycleDone:
		case <-completionCtx.Done():
			t.Fatal("Pool.Close did not cancel the lifecycle context")
		}
		select {
		case err := <-probeCanceled:
			require.ErrorIs(t, err, context.Canceled)
		case <-completionCtx.Done():
			t.Fatal("in-flight startup probe did not observe Pool.Close cancellation")
		}
		select {
		case err := <-startResult:
			require.ErrorIs(t, err, context.Canceled)
		case <-completionCtx.Done():
			t.Fatal("in-flight startup probe did not return after Pool.Close")
		}
		select {
		case <-closeFinished:
		case <-completionCtx.Done():
			t.Fatal("Pool.Close did not finish after canceling the startup probe")
		}

		pool.finishStartup(lifecycleCtx)

		assert.Equal(t, 0, pool.clusters.Count(), "Pool.Close must prevent late insertion and background startup")
		select {
		case <-pool.StartupReady():
			t.Fatal("Pool.Close must prevent late readiness")
		default:
		}
	})

	t.Run("parent cancellation rejects a staged synchronization insertion", func(t *testing.T) {
		t.Parallel()

		lifecycleCtx, cancel := context.WithCancel(t.Context())
		pool := newLifecycleTestPool(lifecycleCtx, cancel)
		t.Cleanup(func() { pool.Close(t.Context()) })
		startEntered := make(chan struct{})
		releaseStart := make(chan struct{})
		startResult := make(chan error, 1)
		pool.store.activateCluster = func(cluster *Cluster, mode initialSyncMode) error {
			close(startEntered)
			<-releaseStart
			err := pool.activateCluster(lifecycleCtx, cluster, mode)
			startResult <- err

			return err
		}
		insertionDone := make(chan struct{})
		go func() {
			pool.store.PoolInsert(lifecycleCtx, queries.Cluster{ID: uuid.New(), Endpoint: "127.0.0.1:1"})
			close(insertionDone)
		}()

		completionCtx, completionCancel := context.WithTimeout(t.Context(), time.Second)
		defer completionCancel()
		select {
		case <-startEntered:
		case <-completionCtx.Done():
			t.Fatal("synchronization insertion did not reach its start barrier")
		}
		cancel()
		select {
		case <-pool.lifecycleDone:
		case <-completionCtx.Done():
			t.Fatal("parent cancellation did not reach the staged insertion")
		}
		close(releaseStart)
		select {
		case err := <-startResult:
			require.ErrorIs(t, err, context.Canceled)
		case <-completionCtx.Done():
			t.Fatal("staged insertion did not observe parent cancellation")
		}
		select {
		case <-insertionDone:
		case <-completionCtx.Done():
			t.Fatal("staged insertion did not complete after cancellation")
		}
		pool.finishStartup(lifecycleCtx)

		assert.Equal(t, 0, pool.clusters.Count(), "parent cancellation must prevent late insertion and background startup")
		select {
		case <-pool.StartupReady():
			t.Fatal("parent cancellation must prevent readiness")
		default:
		}
	})

	t.Run("open pool positive control", func(t *testing.T) {
		t.Parallel()

		lifecycleCtx, cancel := context.WithCancel(t.Context())
		pool := newLifecycleTestPool(lifecycleCtx, cancel)
		remote, err := newRemoteCluster(nil, "127.0.0.1:1", false, "", uuid.New(), nil, "")
		require.NoError(t, err)
		require.NoError(t, pool.activateCluster(lifecycleCtx, remote, syncOnNextTick))
		require.Equal(t, 1, pool.clusters.Count(), "an open pool must accept startup insertion")
		pool.finishStartup(lifecycleCtx)

		select {
		case <-pool.StartupReady():
		case <-t.Context().Done():
			t.Fatal("open pool did not become ready")
		}
		pool.Close(t.Context())
	})
}

func newLifecycleTestPool(lifecycleCtx context.Context, cancel context.CancelFunc) *Pool {
	clusters := smap.New[*Cluster]()
	p := &Pool{
		clusters:      clusters,
		cancel:        cancel,
		lifecycleDone: lifecycleCtx.Done(),
		startupReady:  make(chan struct{}),
		lifecycleMu:   &sync.Mutex{},
	}
	p.store = clustersSyncStore{
		clusters: clusters,
		activateCluster: func(cluster *Cluster, mode initialSyncMode) error {
			return p.activateCluster(lifecycleCtx, cluster, mode)
		},
	}
	p.synchronization = synchronization.NewSynchronize("test-clusters-pool", "Test clusters pool", p.store)

	return p
}

func mustGet(t *testing.T, instances *smap.Map[*Instance], key string) *Instance {
	t.Helper()

	instance, found := instances.Get(key)
	require.True(t, found, "expected an instance keyed %q", key)

	return instance
}
