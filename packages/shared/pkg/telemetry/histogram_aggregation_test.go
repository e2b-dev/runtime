package telemetry

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestExplicitHistogramViewsExport(t *testing.T) {
	t.Parallel()

	reader := sdkmetric.NewManualReader(sdkmetric.WithAggregationSelector(histogramAggregation))
	provider, err := NewMeterProvider(noopMetricExporter{}, time.Hour, nil, sdkmetric.WithReader(reader))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.WithoutCancel(t.Context()))) })
	meter := provider.Meter("github.com/e2b-dev/infra/packages/shared/pkg/telemetry")
	for _, name := range []HistogramType{NetworkSlotReturnDurationName, NetworkEgressRetirementDurationName} {
		histogram, err := GetHistogram(meter, name)
		require.NoError(t, err)
		histogram.Record(t.Context(), 60_001)
	}
	punch, err := GetFloatHistogram(meter, OrchestratorBlockCachePunchDuration)
	require.NoError(t, err)
	punch.Record(t.Context(), 60_001)

	var collected metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &collected))
	require.Len(t, collected.ScopeMetrics, 1)
	require.Len(t, collected.ScopeMetrics[0].Metrics, 3)
	for _, m := range collected.ScopeMetrics[0].Metrics {
		var sum float64
		var bounds []float64
		switch data := m.Data.(type) {
		case metricdata.Histogram[int64]:
			require.Len(t, data.DataPoints, 1)
			sum, bounds = float64(data.DataPoints[0].Sum), data.DataPoints[0].Bounds
		case metricdata.Histogram[float64]:
			require.Len(t, data.DataPoints, 1)
			sum, bounds = data.DataPoints[0].Sum, data.DataPoints[0].Bounds
		default:
			require.Failf(t, "not explicit", "%s must export explicit buckets, got %T", m.Name, m.Data)
		}
		assert.InDelta(t, 60_001, sum, 0)
		assert.Contains(t, bounds, float64(60_000))
		assert.Contains(t, bounds, float64(120_000))
		require.GreaterOrEqual(t, len(bounds), 4)
		if m.Name == string(OrchestratorBlockCachePunchDuration) {
			assert.Equal(t, []float64{0.01, 0.05, 0.25, 1}, bounds[:4], "punch duration keeps its sub-millisecond edges")
		} else {
			assert.InDelta(t, 0, bounds[0], 0, "%s keeps the shared view", m.Name)
		}
	}
}

// Histograms here measure everything from sub-millisecond cache hits to
// multi-day sandbox lifetimes, which no fixed boundary set covers. Exponential
// buckets adapt to whatever a metric records, so nothing has to be tuned per
// instrument.
func TestHistogramAggregationIsExponential(t *testing.T) {
	t.Parallel()

	agg := histogramAggregation(sdkmetric.InstrumentKindHistogram)

	exponential, ok := agg.(sdkmetric.AggregationBase2ExponentialHistogram)
	require.Truef(t, ok, "histograms must export as base-2 exponential, got %T", agg)
	assert.Equal(t, int32(160), exponential.MaxSize)
	assert.Equal(t, int32(20), exponential.MaxScale)

	assert.Equal(t,
		sdkmetric.DefaultAggregationSelector(sdkmetric.InstrumentKindCounter),
		histogramAggregation(sdkmetric.InstrumentKindCounter),
		"only histograms are overridden")
}

// WithExplicitBucketBoundaries is advisory and the SDK keeps it only when the
// reader default is an explicit-bucket aggregation, which ours is not. Setting
// boundaries on an instrument therefore compiles, passes a ManualReader test,
// and does nothing in production. Pin that so nobody spends an afternoon
// tuning buckets that are thrown away.
func TestExplicitBucketBoundariesAreDiscarded(t *testing.T) {
	t.Parallel()

	reader := sdkmetric.NewManualReader(sdkmetric.WithAggregationSelector(histogramAggregation))
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("github.com/e2b-dev/infra/packages/shared/pkg/telemetry")

	histogram, err := meter.Int64Histogram("test.duration",
		metric.WithUnit("ms"),
		metric.WithExplicitBucketBoundaries(1, 2, 3),
	)
	require.NoError(t, err)

	histogram.Record(t.Context(), 86_400_000)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))

	require.Len(t, rm.ScopeMetrics, 1)
	require.Len(t, rm.ScopeMetrics[0].Metrics, 1)

	data := rm.ScopeMetrics[0].Metrics[0].Data
	_, ok := data.(metricdata.ExponentialHistogram[int64])
	assert.Truef(t, ok, "boundaries should have been discarded, got %T", data)
}
