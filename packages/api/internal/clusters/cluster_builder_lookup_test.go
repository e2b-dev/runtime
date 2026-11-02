package clusters

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	infogrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator-info"
	"github.com/e2b-dev/infra/packages/shared/pkg/machineinfo"
	"github.com/e2b-dev/infra/packages/shared/pkg/smap"
)

// clusterWithInstances builds only the fields the builder lookup reads.
func clusterWithInstances(t *testing.T, instances ...*Instance) *Cluster {
	t.Helper()

	pool := smap.New[*Instance]()
	for _, i := range instances {
		pool.Insert(i.workloadID, i)
	}

	return &Cluster{ID: uuid.New(), instances: pool}
}

func builderOn(nodeID, processID string) *Instance {
	return &Instance{
		workloadID: processID,
		NodeID:     nodeID,
		isBuilder:  true,
		status:     infogrpc.ServiceInfoStatus_Healthy,
	}
}

// A build persists the machine it was placed on (env_builds.cluster_node_id)
// and re-resolves it later. The pool is keyed by process now, so this lookup
// has to find the machine some other way or every in-flight build breaks.
func TestGetTemplateBuilderByNodeID_ResolvesAPersistedMachine(t *testing.T) {
	t.Parallel()

	wanted := builderOn("node-b", "alloc-2")
	c := clusterWithInstances(t, builderOn("node-a", "alloc-1"), wanted, builderOn("node-c", "alloc-3"))

	got, err := c.GetTemplateBuilderByNodeID("node-b")
	require.NoError(t, err)
	assert.Same(t, wanted, got)
}

func TestTemplateBuilder_DrainingStatusesOnlyServeExistingWork(t *testing.T) {
	t.Parallel()

	for _, status := range []infogrpc.ServiceInfoStatus{
		infogrpc.ServiceInfoStatus_Draining,
		infogrpc.ServiceInfoStatus_ShuttingDown,
		infogrpc.ServiceInfoStatus_NotReady,
	} {
		t.Run(status.String(), func(t *testing.T) {
			t.Parallel()

			builder := builderOn("node-a", "alloc-1")
			builder.status = status
			c := clusterWithInstances(t, builder)

			got, err := c.GetTemplateBuilderByNodeID("node-a")
			require.NoError(t, err)
			assert.Same(t, builder, got)

			got, found := instanceOnNode(c.instances, "node-a")
			require.True(t, found)
			assert.Same(t, builder, got)

			_, err = c.GetAvailableTemplateBuilder(t.Context(), machineinfo.MachineInfo{})
			require.ErrorIs(t, err, ErrAvailableTemplateBuilderNotFound)
		})
	}
}

// After a restart the machine is the same and the process is not, which is the
// case the re-keying exists for: the persisted node id must resolve to whatever
// is running there now.
func TestGetTemplateBuilderByNodeID_ResolvesToTheCurrentProcessAfterARestart(t *testing.T) {
	t.Parallel()

	replacement := builderOn("node-a", "alloc-2")
	c := clusterWithInstances(t, replacement)

	got, err := c.GetTemplateBuilderByNodeID("node-a")
	require.NoError(t, err)
	assert.Equal(t, "alloc-2", got.workloadID)
}

func TestGetTemplateBuilderByNodeID_RejectsAMachineItDoesNotHave(t *testing.T) {
	t.Parallel()

	c := clusterWithInstances(t, builderOn("node-a", "alloc-1"))

	_, err := c.GetTemplateBuilderByNodeID("node-z")
	require.ErrorIs(t, err, ErrTemplateBuilderNotFound)
}

func TestGetTemplateBuilderByNodeID_RejectsANonBuilderAndAnUnhealthyOne(t *testing.T) {
	t.Parallel()

	notABuilder := builderOn("node-a", "alloc-1")
	notABuilder.isBuilder = false

	unhealthy := builderOn("node-b", "alloc-2")
	unhealthy.status = infogrpc.ServiceInfoStatus_Unhealthy

	c := clusterWithInstances(t, notABuilder, unhealthy)

	_, err := c.GetTemplateBuilderByNodeID("node-a")
	require.ErrorIs(t, err, ErrTemplateBuilderNotFound)

	_, err = c.GetTemplateBuilderByNodeID("node-b")
	require.ErrorIs(t, err, ErrTemplateBuilderNotFound)
}

// A restart leaves the dead run and the live one on the same machine for a sync
// round — the state the pool re-keying deliberately allows. Answering with
// whichever entry the map yielded first sent builds to the dead one, or
// reported the machine as having no builder at all.
//
// Repeated because the pool is a map: one pass only exercises whichever order
// Go happened to randomise into, and the wrong answer is the order-dependent
// one.
func TestGetTemplateBuilderByNodeID_SkipsADeadRunSharingTheMachine(t *testing.T) {
	t.Parallel()

	dead := builderOn("node-a", "alloc-1")
	dead.status = infogrpc.ServiceInfoStatus_Unhealthy
	notABuilder := builderOn("node-a", "alloc-3")
	notABuilder.isBuilder = false
	live := builderOn("node-a", "alloc-2")

	c := clusterWithInstances(t, dead, notABuilder, live)

	for range 200 {
		got, err := c.GetTemplateBuilderByNodeID("node-a")
		require.NoError(t, err, "the live builder must be found whatever order the pool yields")
		require.Equal(t, "alloc-2", got.workloadID)
	}
}

// Build-log streaming resolves the builder from the machine the build was
// placed on, which is what the database persists. The pool is keyed by process
// now, so a map lookup on the machine finds nothing on Nomad and Kubernetes,
// where an allocation ID or pod name is never a node name — and the failure is
// silent, falling back to persistent logs instead of erroring.
func TestBuildLogs_ResolveTheBuilderByMachineNotByPoolKey(t *testing.T) {
	t.Parallel()

	builder := builderOn("node-a", "alloc-1")
	pool := smap.New[*Instance]()
	pool.Insert(builder.workloadID, builder)

	_, foundByPoolKey := pool.Get("node-a")
	require.False(t, foundByPoolKey, "the machine is not the pool key; this is the lookup that broke")

	got, found := instanceOnNode(pool, "node-a")
	require.True(t, found, "the builder on the machine must still be reachable")
	assert.Equal(t, "alloc-1", got.workloadID)
}
