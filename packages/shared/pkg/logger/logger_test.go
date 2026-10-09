package logger

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestContextFieldsReachEveryLine(t *testing.T) {
	t.Parallel()

	core, logs := observer.New(zap.InfoLevel)
	l := NewTracedLoggerFromCore(core)
	start, end := time.Unix(1, 0).UTC(), time.Unix(2, 0).UTC()

	outer := ContextWithFields(t.Context(), zap.String("tenant.id", "outer"), zap.String("region", "eu"), Time("start", start))
	inner := ContextWithFields(outer, zap.String("tenant.id", "inner"), Time("end", end))

	l.Info(outer, "outer line", zap.String("explicit", "yes"))
	l.Info(inner, "inner line")
	l.Info(t.Context(), "bare line")

	entries := logs.AllUntimed()
	require.Len(t, entries, 3)
	assert.Equal(t, map[string]any{
		"tenant.id":  "outer",
		"region":     "eu",
		"start":      start.Format(time.RFC3339Nano),
		"start_unix": start.UnixNano(),
		"explicit":   "yes",
	}, entries[0].ContextMap())
	assert.Equal(t, map[string]any{
		"tenant.id":  "inner",
		"region":     "eu",
		"start":      start.Format(time.RFC3339Nano),
		"start_unix": start.UnixNano(),
		"end":        end.Format(time.RFC3339Nano),
		"end_unix":   end.UnixNano(),
	}, entries[1].ContextMap())
	assert.Empty(t, entries[2].Context)
}

func TestALineCarriesEachKeyOnce(t *testing.T) {
	t.Parallel()

	core, logs := observer.New(zap.InfoLevel)
	l := NewTracedLoggerFromCore(core)
	span := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{1},
		SpanID:  trace.SpanID{2},
	})
	ctx := ContextWithFields(trace.ContextWithSpanContext(t.Context(), span),
		zap.String("tenant.id", "first"),
		zap.String("tenant.id", "last"),
		zap.String("trace_id", "forged"),
		zap.String("request.id", "context"),
	)

	l.Info(ctx, "line", zap.String("request.id", "call"))

	require.Len(t, logs.All(), 1)
	values := map[string][]string{}
	for _, field := range logs.All()[0].Context {
		values[field.Key] = append(values[field.Key], field.String)
	}
	assert.Equal(t, map[string][]string{
		"trace_id":   {span.TraceID().String()},
		"span_id":    {span.SpanID().String()},
		"tenant.id":  {"last"},
		"request.id": {"call"},
	}, values)
}
