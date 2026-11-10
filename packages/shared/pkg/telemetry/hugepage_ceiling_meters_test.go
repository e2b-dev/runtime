package telemetry

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// Guard the two signals that show where the hugepage placement limit is a
// no-op: each needs its description and unit, and must construct on the SDK
// meter, which rejects an invalid name or unit where the noop meter would not.
func TestHugepageCeilingMeterMapsPopulated(t *testing.T) {
	t.Parallel()

	assert.NotEmptyf(t, gaugeIntDesc[ApiOrchestratorNodesWithoutHugepageCeiling], "missing description for gauge %s", ApiOrchestratorNodesWithoutHugepageCeiling)
	assert.NotEmptyf(t, gaugeIntUnits[ApiOrchestratorNodesWithoutHugepageCeiling], "missing unit for gauge %s", ApiOrchestratorNodesWithoutHugepageCeiling)
	assert.NotEmptyf(t, counterDesc[ApiOrchestratorPlacementsWithoutHugepageCeiling], "missing description for counter %s", ApiOrchestratorPlacementsWithoutHugepageCeiling)
	assert.NotEmptyf(t, counterUnits[ApiOrchestratorPlacementsWithoutHugepageCeiling], "missing unit for counter %s", ApiOrchestratorPlacementsWithoutHugepageCeiling)

	meter := sdkmetric.NewMeterProvider().Meter("github.com/e2b-dev/infra/packages/shared/pkg/telemetry")
	_, err := GetGaugeInt(meter, ApiOrchestratorNodesWithoutHugepageCeiling)
	require.NoError(t, err)
	_, err = GetCounter(meter, ApiOrchestratorPlacementsWithoutHugepageCeiling)
	require.NoError(t, err)
}
