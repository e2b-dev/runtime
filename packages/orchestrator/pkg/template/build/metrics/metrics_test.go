package metrics

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

func TestRecordBuildResultLabels(t *testing.T) {
	t.Parallel()

	reader := sdkmetric.NewManualReader()
	m, err := NewBuildMetrics(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	require.NoError(t, err)

	m.RecordBuildResult(t.Context(), "team-1", BuildResultSuccess, "resizable", "not_resizable")

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &rm))
	for _, sm := range rm.ScopeMetrics {
		for _, md := range sm.Metrics {
			if md.Name != string(telemetry.BuildResultCounterName) {
				continue
			}
			sum, ok := md.Data.(metricdata.Sum[int64])
			require.True(t, ok)
			require.Len(t, sum.DataPoints, 1)
			want := attribute.NewSet(
				attribute.String("result", "success"),
				attribute.String("vcpu_sizing", "resizable"),
				attribute.String("built", "not_resizable"),
				telemetry.WithTeamID("team-1"),
			)
			assert.True(t, want.Equals(&sum.DataPoints[0].Attributes), sum.DataPoints[0].Attributes.Encoded(attribute.DefaultEncoder()))

			return
		}
	}
	t.Fatalf("%s not recorded", telemetry.BuildResultCounterName)
}
