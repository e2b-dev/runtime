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

func TestNetworkReleaseHistogramExport(t *testing.T) {
	t.Parallel()

	reader := sdkmetric.NewManualReader(sdkmetric.WithAggregationSelector(histogramAggregation))
	provider, err := NewMeterProvider(noopMetricExporter{}, time.Hour, nil, sdkmetric.WithReader(reader))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.WithoutCancel(t.Context()))) })
	for _, name := range []HistogramType{NetworkSlotReturnDurationName, NetworkEgressRetirementDurationName} {
		histogram, err := GetHistogram(provider.Meter("github.com/e2b-dev/infra/packages/shared/pkg/telemetry"), name)
		require.NoError(t, err)
		histogram.Record(t.Context(), 60_001)
	}
	var collected metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &collected))
	require.Len(t, collected.ScopeMetrics, 1)
	require.Len(t, collected.ScopeMetrics[0].Metrics, 2)
	for _, m := range collected.ScopeMetrics[0].Metrics {
		data, ok := m.Data.(metricdata.Histogram[int64])
		require.Truef(t, ok, "%s must export explicit buckets, got %T", m.Name, m.Data)
		require.Len(t, data.DataPoints, 1)
		assert.Equal(t, int64(60_001), data.DataPoints[0].Sum)
		assert.Contains(t, data.DataPoints[0].Bounds, float64(60_000))
		assert.Contains(t, data.DataPoints[0].Bounds, float64(120_000))
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
