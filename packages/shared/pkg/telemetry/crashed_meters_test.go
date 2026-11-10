package telemetry

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
)

// A metric shipped without a description/unit map entry is an easy omission that
// silently emits an undocumented series. Guard the sandbox crash counter.
func TestSandboxCrashedCounterMeterMapsPopulated(t *testing.T) {
	t.Parallel()

	assert.NotEmptyf(t, counterDesc[OrchestratorSandboxCrashedCounterName], "missing description for counter %s", OrchestratorSandboxCrashedCounterName)
	assert.NotEmptyf(t, counterUnits[OrchestratorSandboxCrashedCounterName], "missing unit for counter %s", OrchestratorSandboxCrashedCounterName)

	meter := noop.NewMeterProvider().Meter("github.com/e2b-dev/infra/packages/shared/pkg/telemetry")
	_, err := GetCounter(meter, OrchestratorSandboxCrashedCounterName)
	require.NoError(t, err)
}
