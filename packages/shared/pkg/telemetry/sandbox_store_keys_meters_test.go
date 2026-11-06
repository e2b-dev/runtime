package telemetry

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// Guard the signals that make the sandbox store's key-form drain observable:
// each needs its description and unit, and must construct on the SDK meter,
// which rejects an invalid name or unit where the noop meter would not.
func TestSandboxStoreKeysMeterMapsPopulated(t *testing.T) {
	t.Parallel()

	meter := sdkmetric.NewMeterProvider().Meter("github.com/e2b-dev/infra/packages/shared/pkg/telemetry")

	for _, name := range []CounterType{ApiRedisStorageReadsFallback, ApiRedisStorageIndexHeal, ApiRedisStorageWriteAnomalies, ApiRedisStorageErrors} {
		assert.NotEmptyf(t, counterDesc[name], "missing description for counter %s", name)
		assert.NotEmptyf(t, counterUnits[name], "missing unit for counter %s", name)
		_, err := GetCounter(meter, name)
		require.NoError(t, err)
	}

	for _, name := range []GaugeIntType{ApiRedisStorageRecordsForm, ApiRedisStorageCensusComplete, ApiRedisStorageIndexHealLastSweep, ApiRedisStorageIndexHealPreviousSweep} {
		assert.NotEmptyf(t, gaugeIntDesc[name], "missing description for gauge %s", name)
		assert.NotEmptyf(t, gaugeIntUnits[name], "missing unit for gauge %s", name)
		_, err := GetGaugeInt(meter, name)
		require.NoError(t, err)
	}

	assert.NotEmptyf(t, histogramDesc[ApiRedisStorageListGets], "missing description for histogram %s", ApiRedisStorageListGets)
	assert.NotEmptyf(t, histogramUnits[ApiRedisStorageListGets], "missing unit for histogram %s", ApiRedisStorageListGets)
	_, err := GetHistogram(meter, ApiRedisStorageListGets)
	require.NoError(t, err)
}
