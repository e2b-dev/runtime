package sharedriver

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

func TestNewOTelMiddlewareLinksWorkSpanToInsert(t *testing.T) {
	t.Parallel()

	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithSpanProcessor(recorder),
	)
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.WithoutCancel(t.Context()))) })

	tel := telemetry.NewNoopClient()
	tel.TracerProvider = provider
	middleware := NewOTelMiddleware(tel)

	params := &rivertype.JobInsertParams{Kind: "provision_project", Queue: "default"}
	_, err := middleware.InsertMany(t.Context(), []*rivertype.JobInsertParams{params}, func(context.Context) ([]*rivertype.JobInsertResult, error) {
		return nil, nil
	})
	require.NoError(t, err)
	require.NoError(t, middleware.Work(t.Context(), &rivertype.JobRow{
		Kind:     params.Kind,
		Queue:    params.Queue,
		Metadata: params.Metadata,
	}, func(context.Context) error { return nil }))

	insert := findSpan(t, recorder.Ended(), "river.insert_many")
	work := findSpan(t, recorder.Ended(), "river.work/provision_project")
	require.Len(t, work.Links(), 1)
	require.Equal(t, insert.SpanContext().SpanID(), work.Links()[0].SpanContext.SpanID())
}

func TestNewOTelMiddlewareEmitsRiverMetricsWithoutSemanticDuplicates(t *testing.T) {
	t.Parallel()

	reader := sdkmetric.NewManualReader()
	tel := telemetry.NewNoopClient()
	tel.MeterProvider = sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	middleware := NewOTelMiddleware(tel)

	require.NoError(t, middleware.Work(t.Context(), &rivertype.JobRow{Kind: "provision_project", Queue: "default"}, func(context.Context) error {
		return nil
	}))

	var collected metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &collected))
	names := []string{}
	for _, scope := range collected.ScopeMetrics {
		for _, recorded := range scope.Metrics {
			names = append(names, recorded.Name)
		}
	}
	require.Contains(t, names, "river.work_count")
	for _, name := range names {
		require.NotContains(t, name, "messaging.", "semantic-convention metrics duplicate river.* and must stay off")
	}
}

func TestNewOTelMiddlewareWithoutTelemetryIsNoop(t *testing.T) {
	t.Parallel()

	middleware := NewOTelMiddleware(nil)
	worked := false
	require.NoError(t, middleware.Work(t.Context(), &rivertype.JobRow{Kind: "noop", Queue: "default"}, func(context.Context) error {
		worked = true

		return nil
	}))
	require.True(t, worked)
}

func findSpan(t *testing.T, spans []sdktrace.ReadOnlySpan, name string) sdktrace.ReadOnlySpan {
	t.Helper()
	for _, span := range spans {
		if span.Name() == name {
			return span
		}
	}

	require.Failf(t, "span not found", "name: %s", name)

	return nil
}

func TestOutboxJobTelemetryClassifiesTerminalAndRetryableJobs(t *testing.T) {
	t.Parallel()

	reader := sdkmetric.NewManualReader()
	tel := telemetry.NewNoopClient()
	tel.MeterProvider = sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	jobs := NewOutboxJobTelemetry(tel, nil)

	failed := errors.New("control plane unreachable")
	want := map[jobOutcomeKey]int64{}
	for _, tc := range []struct {
		kind    string
		attempt int
		err     error
		outcome string
	}{
		{kind: "provision_project", attempt: 1, outcome: "completed"},
		{kind: "apply_project_member", attempt: 1, err: failed, outcome: "retrying"},
		{kind: "apply_project_limits", attempt: 3, err: failed, outcome: "discarded"},
		{kind: "purge_user", attempt: 1, err: river.JobCancel(failed), outcome: "cancelled"},
		{kind: "announce_signup", attempt: 1, err: river.JobSnooze(time.Minute), outcome: "snoozed"},
	} {
		job := &rivertype.JobRow{Kind: tc.kind, Attempt: tc.attempt, MaxAttempts: 3}
		err := jobs.Work(t.Context(), job, func(context.Context) error { return tc.err })
		require.Equal(t, tc.err, err, tc.outcome)
		want[jobOutcomeKey{kind: tc.kind, outcome: tc.outcome}] = 1
	}

	require.Equal(t, want, collectedJobOutcomes(t, reader))
}

func TestOutboxJobTelemetryLogsEveryAttemptThatDidNotComplete(t *testing.T) {
	t.Parallel()

	failed := errors.New("control plane unreachable")
	for outcome, tc := range map[string]struct {
		attempt int
		err     error
		level   zapcore.Level
		logged  bool
	}{
		"completed": {attempt: 1},
		"snoozed":   {attempt: 1, err: river.JobSnooze(time.Minute)},
		"cancelled": {attempt: 1, err: river.JobCancel(failed), level: zapcore.InfoLevel, logged: true},
		"retrying":  {attempt: 1, err: failed, level: zapcore.WarnLevel, logged: true},
		"discarded": {attempt: 3, err: failed, level: zapcore.ErrorLevel, logged: true},
	} {
		t.Run(outcome, func(t *testing.T) {
			t.Parallel()

			core, logged := observer.New(zapcore.DebugLevel)
			jobs := NewOutboxJobTelemetry(nil, logger.NewTracedLoggerFromCore(core))
			job := &rivertype.JobRow{ID: 42, Kind: "apply_project_limits", Queue: "us1", Attempt: tc.attempt, MaxAttempts: 3}
			require.Equal(t, tc.err, jobs.Work(t.Context(), job, func(context.Context) error { return tc.err }))

			if !tc.logged {
				assert.Empty(t, logged.All())

				return
			}
			require.Len(t, logged.All(), 1)
			entry := logged.All()[0]
			assert.Equal(t, tc.level, entry.Level)
			assert.Equal(t, "outbox job apply_project_limits: "+outcome, entry.Message)
			fields := entry.ContextMap()
			assert.Equal(t, "outbox.job."+outcome, fields["event.name"])
			assert.Equal(t, int64(42), fields["outbox.job.id"])
			assert.Equal(t, "us1", fields["outbox.job.queue"])
			assert.Equal(t, int64(tc.attempt), fields["outbox.job.attempt"])
			assert.Equal(t, int64(3), fields["outbox.job.max_attempts"])
			assert.Contains(t, fields["error"], failed.Error())
		})
	}
}

func TestEveryLineAnAttemptLogsNamesTheJobAndTheAttempt(t *testing.T) {
	t.Parallel()

	core, logged := observer.New(zapcore.DebugLevel)
	l := logger.NewTracedLoggerFromCore(core)
	job := &rivertype.JobRow{ID: 42, Kind: "apply_project_limits", Attempt: 2, MaxAttempts: 3}
	failed := errors.New("control plane unreachable")
	require.ErrorIs(t, NewOutboxJobTelemetry(nil, l).Work(t.Context(), job, func(ctx context.Context) error {
		l.Info(ctx, "worker line")

		return failed
	}), failed)

	require.Len(t, logged.All(), 2)
	for _, entry := range logged.All() {
		ids := 0
		for _, field := range entry.Context {
			if field.Key == "outbox.job.id" {
				ids++
			}
		}
		assert.Equal(t, 1, ids, entry.Message)
		fields := entry.ContextMap()
		assert.Equal(t, int64(42), fields["outbox.job.id"], entry.Message)
		assert.Equal(t, int64(2), fields["outbox.job.attempt"], entry.Message)
	}
}

func TestOutboxJobTelemetryCountsAPanicAndLetsItPropagate(t *testing.T) {
	t.Parallel()

	reader := sdkmetric.NewManualReader()
	tel := telemetry.NewNoopClient()
	tel.MeterProvider = sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	core, logged := observer.New(zapcore.DebugLevel)
	jobs := NewOutboxJobTelemetry(tel, logger.NewTracedLoggerFromCore(core))

	require.PanicsWithValue(t, "worker bug", func() {
		_ = jobs.Work(t.Context(), &rivertype.JobRow{Kind: "provision_project", Attempt: 1, MaxAttempts: 3},
			func(context.Context) error { panic("worker bug") })
	})

	require.Equal(t, map[jobOutcomeKey]int64{{kind: "provision_project", outcome: "panicked"}: 1},
		collectedJobOutcomes(t, reader))
	assert.Empty(t, logged.All(), "River logs the panic with its value; a second line would repeat it")
}

func TestNewOutboxJobTelemetryWithoutTelemetryIsNoop(t *testing.T) {
	t.Parallel()

	worked := false
	require.NoError(t, NewOutboxJobTelemetry(nil, nil).Work(t.Context(), &rivertype.JobRow{Kind: "noop", Attempt: 1, MaxAttempts: 1},
		func(context.Context) error {
			worked = true

			return nil
		}))
	require.True(t, worked)
}

type jobOutcomeKey struct {
	kind    string
	outcome string
}

func collectedJobOutcomes(t *testing.T, reader sdkmetric.Reader) map[jobOutcomeKey]int64 {
	t.Helper()

	var collected metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &collected))

	outcomes := map[jobOutcomeKey]int64{}
	for _, scope := range collected.ScopeMetrics {
		if scope.Scope.Name != instrumentationScope {
			continue
		}
		for _, recorded := range scope.Metrics {
			if recorded.Name != "outbox.jobs.finished" {
				continue
			}
			sum, ok := recorded.Data.(metricdata.Sum[int64])
			require.True(t, ok)
			for _, point := range sum.DataPoints {
				kind, _ := point.Attributes.Value(attribute.Key("job.kind"))
				outcome, _ := point.Attributes.Value(attribute.Key("outcome"))
				outcomes[jobOutcomeKey{kind: kind.AsString(), outcome: outcome.AsString()}] = point.Value
			}
		}
	}

	return outcomes
}
