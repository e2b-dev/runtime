package orchestrator

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
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
	storage, err := sandboxredis.NewStorage(client, noop.NewMeterProvider(), nil)
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
