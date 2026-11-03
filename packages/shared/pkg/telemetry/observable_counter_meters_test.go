package telemetry

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEveryObservableCounterHasADescriptionAndAUnit(t *testing.T) {
	t.Parallel()

	for name := range observableCounterDesc {
		assert.NotEmptyf(t, observableCounterUnits[name], "missing unit for counter %s", name)
	}
	for name := range observableCounterUnits {
		assert.NotEmptyf(t, observableCounterDesc[name], "missing description for counter %s", name)
	}
}
