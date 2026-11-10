package telemetry

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// TestSandboxListMetricsRegistered guards the two signals the sandbox list
// flag is tuned against: each needs a description and its unit (the age is
// compared with the flag's milliseconds, the counter is a rate of requests),
// must construct on the SDK meter, which rejects an invalid name or unit
// where the noop meter would not, and the age must export with explicit
// buckets so collectors can forward it to Prometheus.
func TestSandboxListMetricsRegistered(t *testing.T) {
	t.Parallel()

	assert.NotEmpty(t, counterDesc[ApiSandboxListReads])
	assert.Equal(t, "{request}", counterUnits[ApiSandboxListReads])
	assert.NotEmpty(t, histogramDesc[ApiSandboxListAge])
	assert.Equal(t, "ms", histogramUnits[ApiSandboxListAge])

	reader := sdkmetric.NewManualReader(sdkmetric.WithAggregationSelector(histogramAggregation))
	provider, err := NewMeterProvider(noopMetricExporter{}, time.Hour, nil, sdkmetric.WithReader(reader))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.WithoutCancel(t.Context()))) })
	meter := provider.Meter("github.com/e2b-dev/infra/packages/shared/pkg/telemetry")

	counter, err := GetCounter(meter, ApiSandboxListReads)
	require.NoError(t, err)
	counter.Add(t.Context(), 1)
	age, err := GetHistogram(meter, ApiSandboxListAge)
	require.NoError(t, err)
	age.Record(t.Context(), 2_000)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	require.Len(t, rm.ScopeMetrics, 1)
	exported := map[string]metricdata.Metrics{}
	for _, m := range rm.ScopeMetrics[0].Metrics {
		exported[m.Name] = m
	}
	assert.Equal(t, "{request}", exported[string(ApiSandboxListReads)].Unit)
	ageMetric := exported[string(ApiSandboxListAge)]
	assert.Equal(t, "ms", ageMetric.Unit)
	data, ok := ageMetric.Data.(metricdata.Histogram[int64])
	require.Truef(t, ok, "%s must export explicit buckets, got %T", ApiSandboxListAge, ageMetric.Data)
	require.Len(t, data.DataPoints, 1)
	assert.Equal(t, int64(2_000), data.DataPoints[0].Sum)
	// A hit's age is bounded by the lag flag and a read's by the request
	// timeout, so the buckets must resolve the second or two a flag is set to:
	// one bucket from 1 s to 3 s cannot tell a 1.2 s hit from a 2.9 s one.
	for _, bound := range []float64{500, 750, 1_000, 1_250, 1_500, 1_750, 2_000, 2_250, 2_500, 2_750, 3_000, 5_000, 70_000} {
		assert.Containsf(t, data.DataPoints[0].Bounds, bound, "bound %v ms", bound)
	}
	assert.NotContains(t, data.DataPoints[0].Bounds, float64(0), "a 0 bound only catches sub-millisecond sweeps of empty teams")
}
