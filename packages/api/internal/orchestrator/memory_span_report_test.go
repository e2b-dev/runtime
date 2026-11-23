package orchestrator

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func spanAttrs(kvs []attribute.KeyValue) map[string]attribute.Value {
	m := map[string]attribute.Value{}
	for _, kv := range kvs {
		m[string(kv.Key)] = kv.Value
	}

	return m
}

// Both memory reports land on the start's span with the sizes as attributes, not only in the log.
func TestMemoryReportsLandOnTheSpan(t *testing.T) {
	t.Parallel()

	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	ctx, span := provider.Tracer("github.com/e2b-dev/infra/packages/api/internal/orchestrator").Start(t.Context(), "create-sandbox")

	build := growableBuild(4096, 2048)
	reportMemoryRecordMismatch(ctx, errors.New("memory hotplug refused (shrink)"), "sbx-1",
		SandboxMetadata{TemplateID: "tpl", Build: build}, errCodeSandboxMemoryShrinkUnsupported, 2048)
	reportMemorySizeUnknown(ctx, "start", 1024, 4096)
	span.End()

	spans := recorder.Ended()
	require.Len(t, spans, 1)
	events := map[string]map[string]attribute.Value{}
	for _, e := range spans[0].Events() {
		events[e.Name] = spanAttrs(e.Attributes)
	}

	mismatch, ok := events["exception"]
	require.True(t, ok, "the record mismatch is recorded as an error on the span")
	assert.Equal(t, int64(2048), mismatch["sent_memory_mb"].AsInt64())
	assert.Equal(t, int64(2048), mismatch["start_memory_mb"].AsInt64())
	assert.Equal(t, int64(1024), mismatch["boot_memory_mb"].AsInt64())
	assert.Equal(t, int64(4096), mismatch["max_memory_mb"].AsInt64())
	assert.Equal(t, errCodeSandboxMemoryShrinkUnsupported, mismatch["refusal"].AsString())

	unknown, ok := events["sandbox memory size unknown"]
	require.True(t, ok)
	assert.Equal(t, "start", unknown["path"].AsString())
	assert.Equal(t, int64(1024), unknown["boot_memory_mb"].AsInt64())
	assert.Equal(t, int64(4096), unknown["max_memory_mb"].AsInt64())
}
