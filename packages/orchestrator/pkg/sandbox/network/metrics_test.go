//go:build linux

package network

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

//nolint:paralleltest // Replaces the package histogram while parallel tests are paused.
func TestRecordSlotReturnDuration(t *testing.T) {
	original := slotReturnDuration
	t.Cleanup(func() { slotReturnDuration = original })
	for _, version := range []int{1, 2} {
		for _, tc := range []struct {
			err    error
			result string
		}{
			{nil, "success"},
			{ErrClosed, "shutdown"},
			{context.Canceled, "shutdown"},
			{context.DeadlineExceeded, "shutdown"},
			{errors.Join(ErrClosed, fmt.Errorf("release: %w", ErrSlotRetained)), "retained"},
			{errors.New("cleanup failed"), "error"},
			{errors.Join(ErrClosed, errors.New("cleanup failed")), "error"},
		} {
			reader := sdkmetric.NewManualReader()
			provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			var err error
			slotReturnDuration, err = telemetry.GetHistogram(provider.Meter("github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/network"), telemetry.NetworkSlotReturnDurationName)
			require.NoError(t, err)
			finished := time.Now()
			RecordSlotReturnDuration(t.Context(), finished.Add(-time.Second), finished.Add(-200*time.Millisecond), version, tc.err)
			var collected metricdata.ResourceMetrics
			require.NoError(t, reader.Collect(t.Context(), &collected))
			require.Len(t, collected.ScopeMetrics, 1)
			require.Len(t, collected.ScopeMetrics[0].Metrics, 1)
			data, ok := collected.ScopeMetrics[0].Metrics[0].Data.(metricdata.Histogram[int64])
			require.True(t, ok)
			require.Len(t, data.DataPoints, 2)
			phases := make(map[string]int64)
			for _, point := range data.DataPoints {
				phase, _ := point.Attributes.Value("phase")
				want := attribute.NewSet(attribute.Int("network_version", version), attribute.String("result", tc.result), attribute.String("phase", phase.AsString()))
				require.Equal(t, want, point.Attributes, "version=%d, err=%v", version, tc.err)
				require.Equal(t, uint64(1), point.Count)
				phases[phase.AsString()] = point.Sum
			}
			require.Contains(t, phases, "total")
			require.Contains(t, phases, "cleanup")
			require.Equal(t, int64(800), phases["total"]-phases["cleanup"])
			require.NoError(t, provider.Shutdown(t.Context()))
		}
	}
}

func TestRegisterDatapathMetric(t *testing.T) {
	t.Parallel()

	for _, version := range []int{1, 2} {
		t.Run(string(rune('0'+version)), func(t *testing.T) {
			t.Parallel()

			reader := sdkmetric.NewManualReader()
			provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
			registration, err := RegisterDatapathMetric(provider, version)
			require.NoError(t, err)

			var collected metricdata.ResourceMetrics
			require.NoError(t, reader.Collect(t.Context(), &collected))
			points := datapathPoints(t, collected)
			require.Len(t, points, 1)
			require.Equal(t, int64(1), points[0].Value)
			value, ok := points[0].Attributes.Value(attribute.Key("network_version"))
			require.True(t, ok)
			require.Equal(t, string(rune('0'+version)), value.AsString())
			require.Equal(t, 1, points[0].Attributes.Len())

			require.NoError(t, registration.Unregister())
			collected = metricdata.ResourceMetrics{}
			require.NoError(t, reader.Collect(t.Context(), &collected))
			require.Empty(t, datapathPoints(t, collected))
		})
	}
}

func datapathPoints(t *testing.T, collected metricdata.ResourceMetrics) []metricdata.DataPoint[int64] {
	t.Helper()

	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name == datapathMetricName {
				gauge, ok := m.Data.(metricdata.Gauge[int64])
				require.True(t, ok)

				return gauge.DataPoints
			}
		}
	}

	return nil
}
