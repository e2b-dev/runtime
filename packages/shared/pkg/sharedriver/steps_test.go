package sharedriver

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

const testMetricPrefix = "test_service"

var errRefusedForGood = errors.New("refused for good")

func testSteps(tel *telemetry.Client, l logger.Logger) *Steps {
	return NewSteps(tel, l, StepsConfig{
		MetricPrefix: testMetricPrefix,
		ErrorClass: func(err error) string {
			if errors.Is(err, errRefusedForGood) {
				return "validation"
			}

			return ""
		},
	})
}

func TestAStepIsAChildSpanOfTheWork(t *testing.T) {
	t.Parallel()

	recorder, provider := recordingTracer(t)
	steps := testSteps(telemetryWith(provider, nil), nil).Job("provision_project")
	workerCtx, workerSpan := provider.Tracer("github.com/e2b-dev/infra/packages/shared/pkg/sharedriver").Start(t.Context(), "river.work/provision_project")
	require.NoError(t, steps.Step(workerCtx, "upsert_control_plane_project", func(context.Context) error {
		return nil
	}))
	workerSpan.End()

	step := findSpan(t, recorder.Ended(), "outbox step upsert_control_plane_project")
	assert.Equal(t, instrumentationScope, step.InstrumentationScope().Name)
	assert.Equal(t, workerSpan.SpanContext().SpanID(), step.Parent().SpanID())
	assert.Equal(t, trace.SpanKindInternal, step.SpanKind())
	assert.Equal(t, codes.Ok, step.Status().Code)
	assert.Equal(t, "provision_project", spanAttribute(t, step, "outbox.job.kind"))
	assert.Equal(t, "upsert_control_plane_project", spanAttribute(t, step, "outbox.step.name"))
	assert.Equal(t, []string{"outbox.step.succeeded"}, eventNames(step))
}

func TestAStepNestsUnderTheRiverWorkSpan(t *testing.T) {
	t.Parallel()

	recorder, provider := recordingTracer(t)
	tel := telemetryWith(provider, nil)
	middleware := NewOTelMiddleware(tel)
	steps := testSteps(tel, nil).Job("reconcile_purchase")
	params := &rivertype.JobInsertParams{Kind: "reconcile_purchase", Queue: river.QueueDefault}
	_, err := middleware.InsertMany(t.Context(), []*rivertype.JobInsertParams{params}, func(context.Context) ([]*rivertype.JobInsertResult, error) {
		return nil, nil
	})
	require.NoError(t, err)

	require.NoError(t, middleware.Work(t.Context(), &rivertype.JobRow{
		Kind:     params.Kind,
		Queue:    params.Queue,
		Metadata: params.Metadata,
	}, func(ctx context.Context) error {
		return steps.Step(ctx, "reconcile_purchase", func(context.Context) error { return nil })
	}))

	insert := findSpan(t, recorder.Ended(), "river.insert_many")
	work := findSpan(t, recorder.Ended(), "river.work/reconcile_purchase")
	step := findSpan(t, recorder.Ended(), "outbox step reconcile_purchase")
	require.Len(t, work.Links(), 1)
	assert.Equal(t, insert.SpanContext().SpanID(), work.Links()[0].SpanContext.SpanID())
	assert.Equal(t, work.SpanContext().SpanID(), step.Parent().SpanID())
}

func TestZeroJobStepsRunEveryOperation(t *testing.T) {
	t.Parallel()

	var steps JobSteps
	ran := 0
	require.NoError(t, steps.Step(t.Context(), "delete_identity", func(context.Context) error {
		ran++

		return nil
	}))
	refused := errors.New("refused")
	require.ErrorIs(t, steps.StepWith(t.Context(), "apply_paid_purchase", StepOptions{SkipReason: SkipWhen("pending", refused)},
		func(context.Context) error {
			ran++

			return refused
		}), refused)
	skipped, err := steps.StepWithSkip(t.Context(), "activate_project", func(context.Context) (bool, error) {
		ran++

		return true, nil
	})
	require.NoError(t, err)
	assert.True(t, skipped)
	steps.Skipped(t.Context(), "delete_identity", "superseded")
	assert.Equal(t, 3, ran)
}

func TestAFailedStepLogsItsErrorAndSubjectButKeepsThemOffTheSpan(t *testing.T) {
	t.Parallel()

	recorder, provider := recordingTracer(t)
	core, logs := observer.New(zap.InfoLevel)
	steps := testSteps(telemetryWith(provider, nil), logger.NewTracedLoggerFromCore(core)).Job("reconcile_purchase")
	secret := errors.New("card_last4=4242 customer_email=ada@example.test")
	require.ErrorIs(t, steps.StepWith(t.Context(), "reconcile_purchase", StepOptions{
		Subject: []zap.Field{zap.String("purchase_id", "p-1")},
	}, func(context.Context) error {
		return secret
	}), secret)

	span := findSpan(t, recorder.Ended(), "outbox step reconcile_purchase")
	assert.Equal(t, codes.Error, span.Status().Code)
	assert.Equal(t, "failed", span.Status().Description)
	for _, kv := range span.Attributes() {
		assert.NotContains(t, kv.Value.AsString(), "ada@example.test")
	}
	assert.Equal(t, []string{"outbox.step.failed"}, eventNames(span))
	for _, event := range span.Events() {
		for _, kv := range event.Attributes {
			assert.NotContains(t, kv.Value.AsString(), "ada@example.test")
		}
	}

	entries := logs.FilterMessage("outbox step reconcile_purchase: failed").All()
	require.Len(t, entries, 1)
	assert.Equal(t, zap.ErrorLevel, entries[0].Level)
	assert.Equal(t, map[string]any{
		"event.name":          "outbox.step.failed",
		"outbox.job.kind":     "reconcile_purchase",
		"outbox.step.name":    "reconcile_purchase",
		"outbox.step.outcome": "failed",
		"purchase_id":         "p-1",
		"error.class":         "unknown",
		"error":               secret.Error(),
		"trace_id":            span.SpanContext().TraceID().String(),
		"span_id":             span.SpanContext().SpanID().String(),
	}, entries[0].ContextMap(), "the line links to the step's span")
}

func TestASkipNamesItsReasonAndSubjectWithoutSwallowingTheError(t *testing.T) {
	t.Parallel()

	recorder, provider := recordingTracer(t)
	reader := sdkmetric.NewManualReader()
	core, logs := observer.New(zap.InfoLevel)
	steps := testSteps(
		telemetryWith(provider, sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))),
		logger.NewTracedLoggerFromCore(core),
	).Job("push_project_limits")

	project := zap.String("project_id", "p-1")
	superseded := errors.New("superseded")
	err := steps.StepWith(t.Context(), "mark_project_limits_propagated", StepOptions{
		SkipReason: SkipWhen("superseded", superseded),
		Subject:    []zap.Field{project},
	}, func(context.Context) error {
		return fmt.Errorf("mark: %w", superseded)
	})
	require.ErrorIs(t, err, superseded, "the worker still decides what a skip means for the job")
	steps.Skipped(t.Context(), "publish_project_limits", "propagation_gone", project)
	steps.Skipped(t.Context(), "publish_project_limits", "")

	marked := findSpan(t, recorder.Ended(), "outbox step mark_project_limits_propagated")
	assert.Equal(t, codes.Ok, marked.Status().Code)
	assert.Equal(t, []string{"outbox.step.skipped"}, eventNames(marked))

	reasons := []string{}
	for _, entry := range logs.All() {
		assert.Equal(t, zap.InfoLevel, entry.Level)
		assert.Equal(t, "skipped", entry.ContextMap()["outbox.step.outcome"])
		reason, _ := entry.ContextMap()["outbox.step.skip_reason"].(string)
		reasons = append(reasons, reason)
	}
	assert.Equal(t, []string{"superseded", "propagation_gone", ""}, reasons)
	assert.Equal(t, "p-1", logs.All()[0].ContextMap()["project_id"], "a skip names its subject")

	recorded := []string{}
	for _, point := range stepCounter(t, reader) {
		recorded = append(recorded, metricAttribute(point.Attributes, "skip.reason"))
		assert.Empty(t, metricAttribute(point.Attributes, "error.class"))
	}
	assert.ElementsMatch(t, []string{"superseded", "propagation_gone", ""}, recorded)
}

func TestAStepThatReportsItsOwnSkipIsASkippedSpan(t *testing.T) {
	t.Parallel()

	recorder, provider := recordingTracer(t)
	steps := testSteps(telemetryWith(provider, nil), nil).Job("provision_project")
	skipped, err := steps.StepWithSkip(t.Context(), "activate_project", func(context.Context) (bool, error) {
		return true, nil
	})
	require.NoError(t, err)
	assert.True(t, skipped)

	span := findSpan(t, recorder.Ended(), "outbox step activate_project")
	assert.Equal(t, codes.Ok, span.Status().Code)
	assert.Equal(t, []string{"outbox.step.skipped"}, eventNames(span))
}

func TestStepMetricsCarryOnlyBoundedAttributes(t *testing.T) {
	t.Parallel()

	reader := sdkmetric.NewManualReader()
	steps := testSteps(telemetryWith(nil, sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))), nil).Job("reconcile_purchase")
	for _, err := range []error{
		nil,
		fmt.Errorf("reconcile purchase p-1: %w", errRefusedForGood),
		fmt.Errorf("reconcile purchase p-2: %w", context.DeadlineExceeded),
		errors.New("connection reset"),
	} {
		_ = steps.Step(t.Context(), "reconcile_purchase", func(context.Context) error { return err })
	}

	outcomes := []string{}
	for _, point := range stepCounter(t, reader) {
		assert.Equal(t, int64(1), point.Value)
		assert.Equal(t, "reconcile_purchase", metricAttribute(point.Attributes, "job.kind"))
		assert.Equal(t, "reconcile_purchase", metricAttribute(point.Attributes, "step.name"))
		outcomes = append(outcomes, metricAttribute(point.Attributes, "outcome")+"/"+metricAttribute(point.Attributes, "error.class"))
		for _, kv := range point.Attributes.ToSlice() {
			assert.Contains(t, []attribute.Key{"job.kind", "step.name", "outcome", "error.class"}, kv.Key)
		}
	}
	assert.ElementsMatch(t, []string{"succeeded/", "failed/validation", "failed/timeout", "failed/unknown"}, outcomes)
}

func recordingTracer(t *testing.T) (*tracetest.SpanRecorder, *sdktrace.TracerProvider) {
	t.Helper()

	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithSpanProcessor(recorder),
	)
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.WithoutCancel(t.Context()))) })

	return recorder, provider
}

func telemetryWith(tracerProvider trace.TracerProvider, meterProvider *sdkmetric.MeterProvider) *telemetry.Client {
	tel := telemetry.NewNoopClient()
	if tracerProvider != nil {
		tel.TracerProvider = tracerProvider
	}
	if meterProvider != nil {
		tel.MeterProvider = meterProvider
	}

	return tel
}

func spanAttribute(t *testing.T, span sdktrace.ReadOnlySpan, key string) string {
	t.Helper()
	for _, kv := range span.Attributes() {
		if string(kv.Key) == key {
			return kv.Value.AsString()
		}
	}

	require.Failf(t, "span attribute not found", "key: %s", key)

	return ""
}

func eventNames(span sdktrace.ReadOnlySpan) []string {
	names := []string{}
	for _, event := range span.Events() {
		names = append(names, event.Name)
	}

	return names
}

func stepCounter(t *testing.T, reader *sdkmetric.ManualReader) []metricdata.DataPoint[int64] {
	t.Helper()

	var collected metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &collected))
	for _, scope := range collected.ScopeMetrics {
		if scope.Scope.Name != instrumentationScope {
			continue
		}
		for _, recorded := range scope.Metrics {
			if recorded.Name != testMetricPrefix+".outbox.steps.finished" {
				continue
			}
			counter, ok := recorded.Data.(metricdata.Sum[int64])
			require.True(t, ok)

			return counter.DataPoints
		}
	}

	require.Fail(t, "outbox step metric not found")

	return nil
}

func metricAttribute(attributes attribute.Set, key string) string {
	value, ok := attributes.Value(attribute.Key(key))
	if !ok {
		return ""
	}

	return value.AsString()
}
