package pool

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

var (
	testSpanRecorder  = tracetest.NewSpanRecorder()
	testTraceProvider = sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(testSpanRecorder))
	testMetricReader  = sdkmetric.NewManualReader()
)

func TestMain(m *testing.M) {
	otel.SetTracerProvider(testTraceProvider)
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(testMetricReader)))
	m.Run()
}

func TestConnectTelemetryOmitsCoordinatesAndNamesThePool(t *testing.T) {
	t.Parallel()

	client, err := Connect(t.Context(), testDatabaseURL(t), "telemetry-test")
	require.NoError(t, err)
	t.Cleanup(client.Close)

	ctx, parent := testTraceProvider.Tracer("github.com/e2b-dev/infra/packages/shared/pkg/postgres/pool").Start(t.Context(), "query")
	var one int
	require.NoError(t, client.Pool().QueryRow(ctx, "SELECT 1").Scan(&one))
	parent.End()
	require.Equal(t, 1, one)

	databaseSpans := 0
	for _, span := range testSpanRecorder.Ended() {
		if span.SpanKind() != trace.SpanKindClient ||
			span.InstrumentationScope().Name != "github.com/exaring/otelpgx" {
			continue
		}
		databaseSpans++

		keys := make([]string, 0, len(span.Attributes()))
		for _, attribute := range span.Attributes() {
			keys = append(keys, string(attribute.Key))
		}
		for _, forbidden := range []string{"server.address", "server.port", "user.name", "db.namespace"} {
			assert.Falsef(t, slices.Contains(keys, forbidden),
				"database span %q contains connection coordinate %q", span.Name(), forbidden)
		}
	}
	require.Positive(t, databaseSpans)

	var collected metricdata.ResourceMetrics
	require.NoError(t, testMetricReader.Collect(t.Context(), &collected))
	namedPoints := 0
	for _, scope := range collected.ScopeMetrics {
		for _, instrument := range scope.Metrics {
			for _, attributes := range metricAttributes(instrument.Data) {
				poolName, ok := attributes.Value(attribute.Key("pool.name"))
				if ok && poolName.AsString() == "telemetry-test" {
					namedPoints++
				}
			}
		}
	}
	require.Positive(t, namedPoints)
}

func metricAttributes(data metricdata.Aggregation) []attribute.Set {
	sets := make([]attribute.Set, 0)
	switch points := data.(type) {
	case metricdata.Sum[int64]:
		for _, point := range points.DataPoints {
			sets = append(sets, point.Attributes)
		}
	case metricdata.Gauge[int64]:
		for _, point := range points.DataPoints {
			sets = append(sets, point.Attributes)
		}
	}

	return sets
}
