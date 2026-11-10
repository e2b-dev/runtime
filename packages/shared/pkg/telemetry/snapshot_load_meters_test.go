package telemetry

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
)

// A metric shipped without a description/unit map entry is an easy omission that
// silently emits an undocumented series. Guard the snapshot-load failure counter.
func TestSnapshotLoadFailureCounterMeterMapsPopulated(t *testing.T) {
	t.Parallel()

	assert.NotEmptyf(t, counterDesc[SandboxFCSnapshotLoadFailures], "missing description for counter %s", SandboxFCSnapshotLoadFailures)
	assert.NotEmptyf(t, counterUnits[SandboxFCSnapshotLoadFailures], "missing unit for counter %s", SandboxFCSnapshotLoadFailures)

	meter := noop.NewMeterProvider().Meter("github.com/e2b-dev/infra/packages/shared/pkg/telemetry")
	_, err := GetCounter(meter, SandboxFCSnapshotLoadFailures)
	require.NoError(t, err)
}
