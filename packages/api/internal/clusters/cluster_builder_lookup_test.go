package clusters

import (
	"testing"
	"time"

	"github.com/Masterminds/semver/v3"
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

			_, err = c.GetAvailableTemplateBuilder(t.Context(), machineinfo.MachineInfo{}, nil, 0)
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

// A builder older than the resizable-template gate drops maxVCpuCount and
// builds a fixed-size template, which the caller cannot tell apart from
// success. Below the gate it must never be chosen for a resizable build; the
// suffix a distribution stamps onto the release must not sort it below.
func TestGetAvailableTemplateBuilder_RequiresTheMinimumVersion(t *testing.T) {
	t.Parallel()

	minVersion := semver.New(0, 16, 202610140000, "", "")

	tests := []struct {
		name    string
		version string
		picked  bool
	}{
		{name: "older patch", version: "0.16.202610131200", picked: false},
		{name: "older minor", version: "0.15.202610150000", picked: false},
		{name: "equal", version: "0.16.202610140000", picked: true},
		{name: "newer patch", version: "0.16.202610150000", picked: true},
		{name: "newer minor", version: "0.17.0", picked: true},
		{name: "equal with prerelease suffix", version: "0.16.202610140000-nomad.3", picked: true},
		{name: "equal with build metadata", version: "0.16.202610140000+abc123", picked: true},
		{name: "empty", version: "", picked: false},
		{name: "unparsable", version: "dev", picked: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			builder := builderOn("node-a", "alloc-1")
			builder.serviceVersion = tt.version
			c := clusterWithInstances(t, builder)

			got, err := c.GetAvailableTemplateBuilder(t.Context(), machineinfo.MachineInfo{}, minVersion, 0)
			if tt.picked {
				require.NoError(t, err)
				assert.Same(t, builder, got)

				return
			}

			require.ErrorIs(t, err, ErrTemplateBuilderVersionTooOld)
			require.NotErrorIs(t, err, ErrAvailableTemplateBuilderNotFound)

			// Without a requirement the same builder is still picked.
			got, err = c.GetAvailableTemplateBuilder(t.Context(), machineinfo.MachineInfo{}, nil, 0)
			require.NoError(t, err)
			assert.Same(t, builder, got)
		})
	}
}

// Among builders of mixed versions only one at the gate may be chosen, whatever
// order the pool yields. Repeated because the lookup shuffles.
func TestGetAvailableTemplateBuilder_SkipsOldBuildersForANewerOne(t *testing.T) {
	t.Parallel()

	old := builderOn("node-a", "alloc-1")
	old.serviceVersion = "0.16.202610010000"
	current := builderOn("node-b", "alloc-2")
	current.serviceVersion = "0.16.202610191109-nomad.1"
	c := clusterWithInstances(t, old, current)

	for range 200 {
		got, err := c.GetAvailableTemplateBuilder(t.Context(), machineinfo.MachineInfo{}, ResizableTemplateBuildsMinVersion, 0)
		require.NoError(t, err)
		require.Same(t, current, got)
	}
}

// The version error is only for a miss caused by the version: a pool with no
// usable builder at all still reports the plain not-found error, so callers
// keep their existing fallback.
func TestGetAvailableTemplateBuilder_VersionErrorOnlyWhenTheVersionIsTheCause(t *testing.T) {
	t.Parallel()

	unhealthy := builderOn("node-a", "alloc-1")
	unhealthy.status = infogrpc.ServiceInfoStatus_Unhealthy
	unhealthy.serviceVersion = "0.16.202610010000"
	c := clusterWithInstances(t, unhealthy)

	_, err := c.GetAvailableTemplateBuilder(t.Context(), machineinfo.MachineInfo{}, ResizableTemplateBuildsMinVersion, 0)
	require.ErrorIs(t, err, ErrAvailableTemplateBuilderNotFound)
	assert.NotErrorIs(t, err, ErrTemplateBuilderVersionTooOld)
}

// A builder at the version that is draining or unhealthy means the fleet is
// upgraded but busy, not too old: callers get the plain not-found error.
func TestGetAvailableTemplateBuilder_NotAVersionErrorWhenANewBuilderIsOnlyUnavailable(t *testing.T) {
	t.Parallel()

	old := builderOn("node-a", "alloc-1")
	old.serviceVersion = "0.16.202610010000"
	draining := builderOn("node-b", "alloc-2")
	draining.serviceVersion = "0.16.202610191109"
	draining.status = infogrpc.ServiceInfoStatus_Draining
	c := clusterWithInstances(t, old, draining)

	_, err := c.GetAvailableTemplateBuilder(t.Context(), machineinfo.MachineInfo{}, ResizableTemplateBuildsMinVersion, 0)
	require.ErrorIs(t, err, ErrAvailableTemplateBuilderNotFound)
	assert.NotErrorIs(t, err, ErrTemplateBuilderVersionTooOld)
}

func TestInstanceAtLeast(t *testing.T) {
	t.Parallel()

	minVersion := semver.New(0, 16, 202610140000, "", "")

	assert.True(t, InstanceAtLeast("0.16.202610140000", minVersion))
	assert.True(t, InstanceAtLeast("0.16.202610140000-k8s.2", minVersion))
	assert.True(t, InstanceAtLeast("1.0.0", minVersion))
	assert.False(t, InstanceAtLeast("0.16.202610139999", minVersion))
	assert.False(t, InstanceAtLeast("", minVersion))
	assert.False(t, InstanceAtLeast("not-a-version", minVersion))
}

func builderWithLoad(nodeID string, cpuPercent, cpuAllocated uint32) *Instance {
	b := builderOn(nodeID, "alloc-"+nodeID)
	b.cpuCount = 80
	b.cpuPercent = cpuPercent
	b.cpuAllocated = cpuAllocated

	return b
}

// Builds used to go to a uniformly random builder, so a builder already at
// 100% CPU kept receiving its share of new builds, and all of them timed out
// waiting for their VMs. With no more matching builders than are sampled, the
// least loaded one must win every time; one that reports no load loses to any
// that does.
func TestGetAvailableTemplateBuilder_PicksTheLeastLoadedCandidate(t *testing.T) {
	t.Parallel()

	idle := builderWithLoad("idle", 5, 10)
	c := clusterWithInstances(t,
		builderWithLoad("hot", 100, 80),
		idle,
		builderOn("unreported", "alloc-unreported"),
	)

	for range 100 {
		got, err := c.GetAvailableTemplateBuilder(t.Context(), machineinfo.MachineInfo{}, nil, 0)
		require.NoError(t, err)
		require.Same(t, idle, got)
	}
}

// With more builders than are sampled, the busiest one is never picked while
// another candidate exists, and equally idle builders still share the builds
// rather than one taking them all between load reports.
func TestGetAvailableTemplateBuilder_AvoidsTheBusiestAndSpreadsTheRest(t *testing.T) {
	t.Parallel()

	hot := builderWithLoad("hot", 100, 80)
	instances := []*Instance{hot}
	for _, node := range []string{"a", "b", "c", "d", "e", "f", "g", "h", "i"} {
		instances = append(instances, builderWithLoad(node, 10, 10))
	}
	c := clusterWithInstances(t, instances...)

	picked := map[string]int{}
	for range 500 {
		got, err := c.GetAvailableTemplateBuilder(t.Context(), machineinfo.MachineInfo{}, nil, 0)
		require.NoError(t, err)
		require.NotSame(t, hot, got)
		picked[got.NodeID]++
	}
	assert.Len(t, picked, 9, "every idle builder should receive builds")
}

// Load reports arrive once per instance sync, so every build in a burst sees
// the same reports. With three or fewer matching builders the sample is the
// whole pool, and each build would go to whichever reported the lowest load
// unless the builds already placed count against their builders.
func TestGetAvailableTemplateBuilder_SpreadsABurstBetweenLoadReports(t *testing.T) {
	t.Parallel()

	c := clusterWithInstances(t,
		builderWithLoad("a", 5, 10),
		builderWithLoad("b", 6, 10),
		builderWithLoad("c", 7, 10),
	)

	picked := map[string]int{}
	for range 30 {
		got, err := c.GetAvailableTemplateBuilder(t.Context(), machineinfo.MachineInfo{}, nil, 8)
		require.NoError(t, err)
		picked[got.NodeID]++
	}
	assert.Equal(t, map[string]int{"a": 10, "b": 10, "c": 10}, picked)
}

// A reservation only bridges the gap until the builder reports the build
// itself; one that never expired would make a builder look busier with every
// build ever sent to it.
func TestBuildReservations_Expire(t *testing.T) {
	t.Parallel()

	b := builderWithLoad("a", 0, 0)
	b.reservations = []buildReservation{{vcpus: 80, expires: time.Now().Add(-time.Second)}}
	assert.Zero(t, b.cpuLoad())

	b.reserveBuild(8)
	assert.InDelta(t, 0.1, b.cpuLoad(), 1e-9)
}
