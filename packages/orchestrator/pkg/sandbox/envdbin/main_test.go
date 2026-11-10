package envdbin

import (
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// testMetricReader captures the package's own counters. They are created from
// the global meter at package init, so the reader has to be installed
// process-wide and once -- otel's global instruments delegate on the first
// SetMeterProvider and ignore later ones.
var testMetricReader = sdkmetric.NewManualReader()

func TestMain(m *testing.M) {
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(testMetricReader)))

	m.Run()
}

// warmsByResult reads the warm counter, summed per result.
//
// The reader is process-wide and the suite is parallel, so an absolute count
// belongs to no single test. What every caller here asserts instead is that its
// own result GREW across the action under test, which a cumulative counter
// supports whatever else is running.
func warmsByResult(t *testing.T) map[string]int64 {
	t.Helper()

	var rm metricdata.ResourceMetrics
	if err := testMetricReader.Collect(t.Context(), &rm); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}

	out := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != string(telemetry.OrchestratorEnvdBinaryCacheWarms) {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("%s is not an int64 sum", m.Name)
			}
			for _, dp := range sum.DataPoints {
				v, ok := dp.Attributes.Value(attribute.Key("result"))
				if !ok {
					t.Fatalf("%s point carries no result attribute", m.Name)
				}
				out[v.AsString()] += dp.Value
			}
		}
	}

	return out
}

// readsByCause reads the reads counter for one op, summed per miss cause ("" for a
// hit). A test that needs an absolute count gives itself an op no other test uses.
func readsByCause(t *testing.T, op Op) map[string]int64 {
	t.Helper()

	var rm metricdata.ResourceMetrics
	if err := testMetricReader.Collect(t.Context(), &rm); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}

	out := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != string(telemetry.OrchestratorEnvdBinaryCacheReads) {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("%s is not an int64 sum", m.Name)
			}
			for _, dp := range sum.DataPoints {
				if v, _ := dp.Attributes.Value(attribute.Key("op")); v.AsString() != string(op) {
					continue
				}
				cause, _ := dp.Attributes.Value(attribute.Key("cause"))
				out[cause.AsString()] += dp.Value
			}
		}
	}

	return out
}
