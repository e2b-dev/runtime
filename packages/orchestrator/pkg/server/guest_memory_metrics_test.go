//go:build linux

package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

type fakePageStates struct {
	states  [block.NumStates]int64
	tracked bool
}

func (f fakePageStates) MemoryPageStates() ([block.NumStates]int64, bool) {
	return f.states, f.tracked
}

// The gauge sums the sandboxes into one series per page state, every state
// observed even when no sandbox is in it.
func TestRegisterGuestMemoryGauge(t *testing.T) {
	t.Parallel()

	var first, second [block.NumStates]int64
	first[block.NotPresent] = 600
	first[block.Clean] = 300
	first[block.Dirty] = 100
	second[block.NotPresent] = 50
	second[block.Dirty] = 40
	second[block.Installing] = 10

	sandboxes := map[string]fakePageStates{
		"first":  {states: first, tracked: true},
		"second": {states: second, tracked: true},
		// Nonzero, so a gauge that ignored tracked=false would be off by a GiB.
		"untracked": {states: [block.NumStates]int64{block.NotPresent: 1 << 30}},
	}

	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("github.com/e2b-dev/infra/packages/orchestrator/pkg/server")
	require.NoError(t, registerGuestMemoryGauge(meter, func() map[string]fakePageStates { return sandboxes }))

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))

	guest := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != string(telemetry.OrchestratorMemoryGuestGaugeName) {
				continue
			}

			gauge, ok := m.Data.(metricdata.Gauge[int64])
			require.True(t, ok)

			for _, dp := range gauge.DataPoints {
				state, ok := dp.Attributes.Value(attribute.Key("page_state"))
				require.True(t, ok)
				guest[state.AsString()] = dp.Value
			}
		}
	}

	assert.Equal(t, map[string]int64{
		"not_present": 650,
		"dirty":       140,
		"zero":        0,
		"removed":     0,
		"clean":       300,
		"installing":  10,
	}, guest)
}
