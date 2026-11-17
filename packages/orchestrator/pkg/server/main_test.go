package server

import (
	"testing"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// testMetricReader captures the package's own instruments, the ones created
// from the global meter at package init (the harvest metrics). The reader has
// to be installed process-wide and once -- otel's global instruments delegate
// on the first SetMeterProvider and ignore later ones -- and it is cumulative
// across every test in the binary, so each test filters what it reads by
// something unique to its own run.
var testMetricReader = sdkmetric.NewManualReader()

func TestMain(m *testing.M) {
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(testMetricReader)))

	m.Run()
}
