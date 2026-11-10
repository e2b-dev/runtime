//go:build linux

package block

import (
	"testing"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// testSpanRecorder collects the package's chunk.fetch spans. The chunker builds
// its spans from the package-level tracer, which resolves against the global
// provider, so the provider has to be installed globally rather than passed in
// -- and only once, because otel's global tracers delegate on the first
// SetTracerProvider. Tests filter the recorded spans by an attribute unique to
// the test.
var testSpanRecorder = tracetest.NewSpanRecorder()

// fetchSpanSampler records only chunk.fetch spans. The package's other spans
// stay non-recording, so they neither pile up in the recorder nor change what
// the benchmarks measure.
type fetchSpanSampler struct{}

func (fetchSpanSampler) ShouldSample(p sdktrace.SamplingParameters) sdktrace.SamplingResult {
	decision := sdktrace.Drop
	if p.Name == "chunk.fetch" {
		decision = sdktrace.RecordAndSample
	}

	return sdktrace.SamplingResult{
		Decision:   decision,
		Tracestate: trace.SpanContextFromContext(p.ParentContext).TraceState(),
	}
}

func (fetchSpanSampler) Description() string { return "chunk.fetch only" }

func TestMain(m *testing.M) {
	otel.SetTracerProvider(sdktrace.NewTracerProvider(
		sdktrace.WithSampler(fetchSpanSampler{}),
		sdktrace.WithSpanProcessor(testSpanRecorder),
	))

	m.Run()
}
