//go:build linux

package sandbox

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

// testSpanRecorder, testMetricReader and testLogObserver capture the package's spans,
// instruments and logs. All three resolve against process-wide globals (the
// package-level tracer and meter, and the global logger), so the test doubles are
// installed once here and every test filters what it reads by something unique to its
// own run -- otel's global instruments delegate on the first Set*Provider and ignore
// later ones, and the reader is cumulative across every test in the binary.
var (
	testSpanRecorder = tracetest.NewSpanRecorder()
	testMetricReader = sdkmetric.NewManualReader()
	testLogObserver  *observer.ObservedLogs
)

func TestMain(m *testing.M) {
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(testSpanRecorder)))
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(testMetricReader)))

	observerCore, logs := observer.New(zap.InfoLevel)
	testLogObserver = logs
	logger.ReplaceGlobals(context.Background(), logger.NewTracedLoggerFromCore(observerCore))

	m.Run()
}
