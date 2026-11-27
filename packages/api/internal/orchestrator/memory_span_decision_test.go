package orchestrator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// A start's memory decision is on its span: the sizes as attributes and the named-size
// outcome as an event, and a resume preflight refusal also as an error with its code.
func TestMemoryDecisionsLandOnTheSpan(t *testing.T) {
	t.Parallel()

	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	flags := sandboxCPUFlags(t, true)
	team := memoryTeam()
	team.Limits.MaxRamMb = 2048

	var o *Orchestrator
	ctx, span := provider.Tracer("github.com/e2b-dev/infra/packages/api/internal/orchestrator").Start(t.Context(), "refused")
	_, _, apiErr := o.resolveSandboxMemory(ctx, flags, team, "sbx", "tpl", growableBuild(4096, 1024), new(int32(3072)), true)
	require.NotNil(t, apiErr)
	span.End()

	ctx, span = provider.Tracer("github.com/e2b-dev/infra/packages/api/internal/orchestrator").Start(t.Context(), "accepted")
	_, _, apiErr = o.resolveSandboxMemory(ctx, flags, memoryTeam(), "sbx", "tpl", growableBuild(4096, 1024), new(int32(2048)), true)
	require.Nil(t, apiErr)
	span.End()

	ctx, span = provider.Tracer("github.com/e2b-dev/infra/packages/api/internal/orchestrator").Start(t.Context(), "preflight")
	require.NotNil(t, o.CheckSandboxMemory(ctx, flags, memoryTeam(), growableBuild(4096, 2048), new(int32(1024))))
	span.End()

	spans := map[string]sdktrace.ReadOnlySpan{}
	for _, s := range recorder.Ended() {
		spans[s.Name()] = s
	}

	refused := spans["refused"]
	attrs := spanAttrs(refused.Attributes())
	assert.Equal(t, int64(3072), attrs["memory.requested_mb"].AsInt64())
	assert.Equal(t, int64(1024), attrs["memory.boot_mb"].AsInt64())
	assert.Equal(t, int64(4096), attrs["memory.max_mb"].AsInt64())
	require.Len(t, refused.Events(), 1)
	event := spanAttrs(refused.Events()[0].Attributes)
	assert.Equal(t, memoryOverrideRejected, event["outcome"].AsString())
	assert.Equal(t, errCodeSandboxMemoryTeamLimit, event["error_code"].AsString())

	accepted := spanAttrs(spans["accepted"].Attributes())
	assert.Equal(t, int64(2048), accepted["memory.resolved_mb"].AsInt64())

	events := map[string]map[string]string{}
	for _, e := range spans["preflight"].Events() {
		m := map[string]string{}
		for k, v := range spanAttrs(e.Attributes) {
			m[k] = v.Emit()
		}
		events[e.Name] = m
	}
	assert.Equal(t, errCodeSandboxMemoryShrinkUnsupported, events["exception"]["error_code"])
	assert.Equal(t, memoryOverrideRejected, events["sandbox memory override"]["outcome"])
}
