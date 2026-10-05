package telemetry

import (
	"maps"
	"runtime"
	rtmetrics "runtime/metrics"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	contribruntime "go.opentelemetry.io/contrib/instrumentation/runtime"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestStartRuntimeInstrumentationExportsGCMetrics(t *testing.T) {
	t.Parallel()

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(t.Context()) })

	require.NoError(t, (&Client{MeterProvider: provider}).StartRuntimeInstrumentation())

	// Guarantees a completed cycle, so the live heap and the cycle count
	// are both non-zero.
	runtime.GC() //nolint:revive // intentional: a completed cycle is what the counters report

	// Both counters are monotonic, so the exported values must fall between
	// a read before and a read after the collection.
	before := readCounters()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	after := readCounters()

	found := map[string]metricdata.Metrics{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			_, dup := found[m.Name]
			require.False(t, dup, "%s is exported by two meters", m.Name)
			found[m.Name] = m
		}
	}

	live, ok := found["go.memory.live"]
	require.True(t, ok, "go.memory.live not exported")
	assert.Equal(t, "By", live.Unit)
	liveData, ok := live.Data.(metricdata.Gauge[int64])
	require.True(t, ok, "go.memory.live is %T, want an int64 gauge", live.Data)
	require.Len(t, liveData.DataPoints, 1)
	assert.Positive(t, liveData.DataPoints[0].Value)
	assert.Zero(t, liveData.DataPoints[0].Attributes.Len())

	cpuTime, ok := found["go.cpu.time"]
	require.True(t, ok, "go.cpu.time not exported")
	assert.Equal(t, "s", cpuTime.Unit)
	cpuTimeData, ok := cpuTime.Data.(metricdata.Sum[float64])
	require.True(t, ok, "go.cpu.time is %T, want a float64 sum", cpuTime.Data)
	assert.True(t, cpuTimeData.IsMonotonic)
	byState := map[string]float64{}
	for _, dp := range cpuTimeData.DataPoints {
		require.Equal(t, 1, dp.Attributes.Len(), "attribute set must be go.cpu.state alone")
		state, ok := dp.Attributes.Value("go.cpu.state")
		require.True(t, ok)
		byState[state.AsString()] = dp.Value
	}
	assert.ElementsMatch(t, []string{"user", "gc", "scavenge", "idle"}, slices.Collect(maps.Keys(byState)))
	assert.Positive(t, byState["gc"])
	assert.GreaterOrEqual(t, byState["gc"], before.gcCPU)
	assert.LessOrEqual(t, byState["gc"], after.gcCPU)

	cycles, ok := found["go.memory.gc.cycles"]
	require.True(t, ok, "go.memory.gc.cycles not exported")
	assert.Equal(t, "{gc_cycle}", cycles.Unit)
	cyclesData, ok := cycles.Data.(metricdata.Sum[int64])
	require.True(t, ok, "go.memory.gc.cycles is %T, want an int64 sum", cycles.Data)
	assert.True(t, cyclesData.IsMonotonic)
	require.Len(t, cyclesData.DataPoints, 1)
	assert.Positive(t, cyclesData.DataPoints[0].Value)
	assert.GreaterOrEqual(t, cyclesData.DataPoints[0].Value, before.cycles)
	assert.LessOrEqual(t, cyclesData.DataPoints[0].Value, after.cycles)
	assert.Zero(t, cyclesData.DataPoints[0].Attributes.Len())

	// The contrib instruments are still registered beside them.
	_, ok = found["go.memory.gc.goal"]
	assert.True(t, ok, "go.memory.gc.goal not exported")
}

// Fails when the contrib instrumentation starts emitting a series this
// package exports itself; the fix is to delete ours from startGCMetrics.
func TestContribRuntimeDoesNotEmitTheGCMetrics(t *testing.T) {
	t.Parallel()

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(t.Context()) })

	require.NoError(t, contribruntime.Start(contribruntime.WithMeterProvider(provider)))

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))

	var names []string
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			names = append(names, m.Name)
		}
	}
	require.NotEmpty(t, names, "the contrib instrumentation exported nothing")
	for _, ours := range []string{"go.memory.live", "go.memory.gc.cycles", "go.cpu.time"} {
		assert.NotContains(t, names, ours, "contrib now emits %s; delete it from startGCMetrics", ours)
	}
}

type gcCounters struct {
	gcCPU  float64
	cycles int64
}

// readCounters names its samples itself rather than through the package's
// constants, so a constant pointing at the wrong sample cannot move the
// bracket along with the value it checks.
func readCounters() gcCounters {
	samples := []rtmetrics.Sample{{Name: "/cpu/classes/gc/total:cpu-seconds"}, {Name: "/gc/cycles/total:gc-cycles"}}
	rtmetrics.Read(samples)

	return gcCounters{gcCPU: samples[0].Value.Float64(), cycles: int64(samples[1].Value.Uint64())}
}
