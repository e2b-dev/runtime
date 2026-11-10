package telemetry

import (
	"testing"

	"github.com/stretchr/testify/assert"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// Only the synchronous kinds that accumulate attribute sets switch to delta;
// up-down counters must stay cumulative, since a delta of a level is not a
// level, and the collector converts deltas back for these kinds only.
func TestLowMemoryTemporality(t *testing.T) {
	t.Parallel()

	for kind, want := range map[sdkmetric.InstrumentKind]metricdata.Temporality{
		sdkmetric.InstrumentKindCounter:                 metricdata.DeltaTemporality,
		sdkmetric.InstrumentKindHistogram:               metricdata.DeltaTemporality,
		sdkmetric.InstrumentKindUpDownCounter:           metricdata.CumulativeTemporality,
		sdkmetric.InstrumentKindObservableCounter:       metricdata.CumulativeTemporality,
		sdkmetric.InstrumentKindObservableUpDownCounter: metricdata.CumulativeTemporality,
		sdkmetric.InstrumentKindObservableGauge:         metricdata.CumulativeTemporality,
		sdkmetric.InstrumentKindGauge:                   metricdata.CumulativeTemporality,
	} {
		assert.Equal(t, want, lowMemoryTemporality(kind), kind.String())
	}
}

// The cap is lifted only where delta makes it pointless; a cumulative kind
// defers to the provider's limit, which stays the overflow guard.
func TestLowMemoryCardinalityLimit(t *testing.T) {
	t.Parallel()

	for kind, wantFallback := range map[sdkmetric.InstrumentKind]bool{
		sdkmetric.InstrumentKindCounter:                 false,
		sdkmetric.InstrumentKindHistogram:               false,
		sdkmetric.InstrumentKindUpDownCounter:           true,
		sdkmetric.InstrumentKindGauge:                   true,
		sdkmetric.InstrumentKindObservableCounter:       true,
		sdkmetric.InstrumentKindObservableUpDownCounter: true,
		sdkmetric.InstrumentKindObservableGauge:         true,
	} {
		limit, fallback := lowMemoryCardinalityLimit(kind)
		assert.Equal(t, wantFallback, fallback, kind.String())
		assert.Equal(t, 0, limit, kind.String())
	}
}
