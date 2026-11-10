package orchestrator

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/placement"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	redisreservations "github.com/e2b-dev/infra/packages/api/internal/sandbox/reservations/redis"
	sandboxredis "github.com/e2b-dev/infra/packages/api/internal/sandbox/storage/redis"
	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
	"github.com/e2b-dev/infra/packages/shared/pkg/servicediscovery"
	"github.com/e2b-dev/infra/packages/shared/pkg/smap"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// The platform tag is only useful if it reaches an observer.
func TestSetupMetrics_LabelsTheOrchestratorGaugeWithItsDiscoveryBackend(t *testing.T) {
	t.Parallel()

	reader := sdkmetric.NewManualReader()

	o := &Orchestrator{nodes: smap.New[*nodemanager.Node](), sandboxStore: emptySandboxStore(t)}
	o.nodes.Insert("nomad-node", &nodemanager.Node{ID: "nomad-node", Backend: servicediscovery.BackendNomad})
	o.nodes.Insert("k8s-node", &nodemanager.Node{ID: "k8s-node", Backend: servicediscovery.BackendKubernetes})

	require.NoError(t, o.setupMetrics(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))))

	var collected metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &collected))

	assert.Equal(t, map[string]string{
		"nomad-node": string(servicediscovery.BackendNomad),
		"k8s-node":   string(servicediscovery.BackendKubernetes),
	}, backendsByNode(t, collected))
}

// setupMetrics registers every api metric, including the sandbox-count gauge
// that reads the store on collection, so the store has to be real.
func emptySandboxStore(t *testing.T) *sandbox.Store {
	t.Helper()

	client := redis_utils.SetupInstance(t)
	storage, err := sandboxredis.NewStorage(client, noop.NewMeterProvider())
	require.NoError(t, err)
	go storage.Start(t.Context())
	t.Cleanup(func() { storage.Close(context.WithoutCancel(t.Context())) })

	return sandbox.NewStore(
		storage,
		redisreservations.NewReservationStorage(client, storage.Notifier()),
		sandbox.Callbacks{
			AsyncNewlyCreatedSandbox: func(context.Context, sandbox.Sandbox, sandbox.CreationMetadata) {},
		},
	)
}

func backendsByNode(t *testing.T, collected metricdata.ResourceMetrics) map[string]string {
	t.Helper()

	got := map[string]string{}
	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != string(telemetry.ApiOrchestratorCountMeterName) {
				continue
			}

			gauge, ok := m.Data.(metricdata.Gauge[int64])
			require.True(t, ok, "%s is not an int64 gauge", m.Name)

			for _, point := range gauge.DataPoints {
				nodeID, found := point.Attributes.Value("node.id")
				require.True(t, found, "datapoint carries no node.id")
				platform, found := point.Attributes.Value("backend")
				require.True(t, found, "datapoint carries no source")

				got[nodeID.AsString()] = platform.AsString()
			}
		}
	}

	return got
}

// hugepageCeilingTestNodes are one ready node of each kind the hugepage limit
// meets, bounded and exempt for each reason, plus one that is not ready.
func hugepageCeilingTestNodes() []*nodemanager.Node {
	const pageSize = 2 << 20

	return []*nodemanager.Node{
		nodemanager.NewTestNode("bounded", api.NodeStatusReady, 0, 8, nodemanager.WithHugePages(1000, 0, 0, pageSize), nodemanager.WithHugePagesCapacity(1000)),
		nodemanager.NewTestNode("no-capacity", api.NodeStatusReady, 0, 8, nodemanager.WithHugePages(1000, 0, 0, pageSize)),
		nodemanager.NewTestNode("nothing-reported", api.NodeStatusReady, 0, 8),
		nodemanager.NewTestNode("no-page-size", api.NodeStatusReady, 0, 8, nodemanager.WithHugePagesCapacity(1000)),
		nodemanager.NewTestNode("draining-no-capacity", api.NodeStatusDraining, 0, 8),
	}
}

func TestNodesWithoutHugepageCeiling_CountsReadyNodesByReason(t *testing.T) {
	t.Parallel()

	nodes := smap.New[*nodemanager.Node]()
	for _, node := range hugepageCeilingTestNodes() {
		nodes.Insert(node.ID, node)
	}

	assert.Equal(t, map[string]int64{
		placement.NoHugepageCapacity: 2,
		placement.NoHugepagePageSize: 1,
	}, nodesWithoutHugepageCeiling(nodes.Items()))

	// Both series stay present on a fleet with every ceiling reported.
	assert.Equal(t, map[string]int64{
		placement.NoHugepageCapacity: 0,
		placement.NoHugepagePageSize: 0,
	}, nodesWithoutHugepageCeiling(nil))
}

// The count is only useful if it reaches an observer with its reason.
func TestSetupMetrics_ObservesNodesWithoutHugepageCeilingByReason(t *testing.T) {
	t.Parallel()

	reader := sdkmetric.NewManualReader()

	o := &Orchestrator{nodes: smap.New[*nodemanager.Node](), sandboxStore: emptySandboxStore(t)}
	for _, node := range hugepageCeilingTestNodes() {
		o.nodes.Insert(node.ID, node)
	}

	require.NoError(t, o.setupMetrics(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))))

	var collected metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &collected))

	assert.Equal(t, map[string]int64{
		placement.NoHugepageCapacity: 2,
		placement.NoHugepagePageSize: 1,
	}, nodesWithoutCeilingByReason(t, collected))
}

func nodesWithoutCeilingByReason(t *testing.T, collected metricdata.ResourceMetrics) map[string]int64 {
	t.Helper()

	got := map[string]int64{}
	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != string(telemetry.ApiOrchestratorNodesWithoutHugepageCeiling) {
				continue
			}

			gauge, ok := m.Data.(metricdata.Gauge[int64])
			require.True(t, ok, "%s is not an int64 gauge", m.Name)

			for _, point := range gauge.DataPoints {
				reason, found := point.Attributes.Value("reason")
				require.True(t, found, "datapoint carries no reason")

				got[reason.AsString()] = point.Value
			}
		}
	}

	return got
}
