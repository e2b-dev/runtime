package telemetry

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
)

// GetCounter substitutes an empty string for a name missing from either map, so a
// counter declared without both entries emits a live but undescribed series that
// looks correct on a dashboard. Nothing else catches the omission — hence the
// per-feature guard files the siblings here follow, one test each covering that
// feature's counters.
func TestLifecycleUnstoppedCounterMeterMapsPopulated(t *testing.T) {
	t.Parallel()

	// The name is asserted literally: it is a one-way door for dashboards, alert
	// rules and recording rules, so a rename has to be a test change too.
	assert.Equal(t, SandboxLifecycleUnstoppedCounterName, CounterType("orchestrator.sandbox.lifecycle.unstopped"))

	assert.NotEmptyf(t, counterDesc[SandboxLifecycleUnstoppedCounterName], "missing description for counter %s", SandboxLifecycleUnstoppedCounterName)
	assert.Equalf(t, "{sandbox}", counterUnits[SandboxLifecycleUnstoppedCounterName], "wrong unit for counter %s", SandboxLifecycleUnstoppedCounterName)

	meter := noop.NewMeterProvider().Meter("github.com/e2b-dev/infra/packages/shared/pkg/telemetry")
	_, err := GetCounter(meter, SandboxLifecycleUnstoppedCounterName)
	require.NoError(t, err)
}
