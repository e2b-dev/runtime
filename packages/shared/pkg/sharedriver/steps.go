package sharedriver

import (
	"cmp"
	"context"
	"errors"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// Steps reports each effect an outbox job performs — a publish, a remote
// call, the write that records it — as a child span of the job's work span,
// a count and a duration, and a log line. Errors stay off the span: their
// text can carry customer data, so only the log line holds it. Inside a job
// run by OutboxJobTelemetry, a failed or skipped step leaves its line to the
// middleware, which logs it once with the attempt's outcome.
type Steps struct {
	tracer     trace.Tracer
	log        logger.Logger
	finished   metric.Int64Counter
	duration   metric.Float64Histogram
	errorClass func(error) string
}

type StepsConfig struct {
	// MetricPrefix names the service's step metrics,
	// <prefix>.outbox.steps.finished and <prefix>.outbox.step.duration.
	MetricPrefix string
	// ErrorClass names the class of a failure the service recognises, or
	// returns "" to leave it unknown. A deadline is always a timeout.
	ErrorClass func(error) string
}

func NewSteps(tel *telemetry.Client, l logger.Logger, config StepsConfig) *Steps {
	if tel == nil {
		tel = telemetry.NewNoopClient()
	}
	if l == nil {
		l = logger.NewNopLogger()
	}
	meter := tel.MeterProvider.Meter(instrumentationScope)

	return &Steps{
		tracer:     tel.TracerProvider.Tracer(instrumentationScope),
		log:        l,
		finished:   utils.Must(meter.Int64Counter(config.MetricPrefix + ".outbox.steps.finished")),
		duration:   utils.Must(meter.Float64Histogram(config.MetricPrefix+".outbox.step.duration", metric.WithUnit("s"))),
		errorClass: config.ErrorClass,
	}
}

// Job returns the steps of one job kind. On a nil Steps they report nothing.
func (s *Steps) Job(kind string) JobSteps {
	return JobSteps{steps: s, kind: kind}
}

// JobSteps reports the steps of one job kind. Its zero value runs every
// operation and reports nothing.
type JobSteps struct {
	steps *Steps
	kind  string
}

type StepOptions struct {
	// SkipReason turns an error into a skip with that reason when it returns
	// one. The step still returns the error: what a skip means for the job is
	// the worker's call.
	SkipReason func(error) string
	// Subject names what the step acted on, on every line it logs.
	Subject []zap.Field
}

func (j JobSteps) Step(ctx context.Context, name string, operation func(context.Context) error) error {
	return j.StepWith(ctx, name, StepOptions{}, operation)
}

func (j JobSteps) StepWith(ctx context.Context, name string, options StepOptions, operation func(context.Context) error) error {
	if j.steps == nil {
		return operation(ctx)
	}

	var err error
	j.steps.observe(ctx, j.kind, name, options.Subject, func(ctx context.Context) stepResult {
		err = operation(ctx)
		if options.SkipReason != nil {
			if reason := options.SkipReason(err); reason != "" {
				return stepResult{skipped: true, reason: reason, skipErr: err}
			}
		}

		return stepResult{err: err}
	})

	return err
}

// StepWithSkip runs a step whose operation reports for itself that it had
// nothing to do.
func (j JobSteps) StepWithSkip(ctx context.Context, name string, operation func(context.Context) (bool, error)) (bool, error) {
	if j.steps == nil {
		return operation(ctx)
	}

	var (
		skipped bool
		err     error
	)
	j.steps.observe(ctx, j.kind, name, nil, func(ctx context.Context) stepResult {
		skipped, err = operation(ctx)

		return stepResult{skipped: skipped, err: err}
	})

	return skipped, err
}

// Skipped reports a step the job decided not to run. An empty reason is
// reported without one.
func (j JobSteps) Skipped(ctx context.Context, name, reason string, subject ...zap.Field) {
	if j.steps == nil {
		return
	}

	j.steps.observe(ctx, j.kind, name, subject, func(context.Context) stepResult {
		return stepResult{skipped: true, reason: reason}
	})
}

// SkipWhen reports a step as skipped for reason when its error is any of the
// targets.
func SkipWhen(reason string, targets ...error) func(error) string {
	return func(err error) string {
		for _, target := range targets {
			if errors.Is(err, target) {
				return reason
			}
		}

		return ""
	}
}

type stepResult struct {
	skipped bool
	reason  string
	err     error
	skipErr error
}

func (s *Steps) observe(ctx context.Context, jobKind, name string, subject []zap.Field, operation func(context.Context) stepResult) {
	ctx, span := s.tracer.Start(ctx, "outbox step "+name, trace.WithSpanKind(trace.SpanKindInternal))
	span.SetAttributes(
		attribute.String("outbox.job.kind", jobKind),
		attribute.String("outbox.step.name", name),
	)

	startedAt := time.Now()
	result := operation(ctx)
	outcome := stepOutcome(result)
	errorClass := ""
	if result.err != nil {
		errorClass = s.classify(result.err)
	}
	if result.err == nil {
		span.SetStatus(codes.Ok, "")
	} else {
		span.SetStatus(codes.Error, outcome)
	}
	span.AddEvent("outbox.step." + outcome)
	span.End()

	attributes := metric.WithAttributes(stepMetricAttributes(jobKind, name, outcome, result.reason, errorClass)...)
	s.finished.Add(ctx, 1, attributes)
	s.duration.Record(ctx, time.Since(startedAt).Seconds(), attributes)

	if outcome != "succeeded" && recordStep(ctx, stepRecord{
		jobKind:    jobKind,
		name:       name,
		outcome:    outcome,
		reason:     result.reason,
		errorClass: errorClass,
		err:        cmp.Or(result.err, result.skipErr),
		subject:    subject,
		span:       span.SpanContext(),
	}) {
		return
	}

	fields := append(stepLogFields(jobKind, name, outcome, result.reason, errorClass, result.err), subject...)
	message := "outbox step " + name + ": " + outcome
	if result.err != nil {
		s.log.Error(ctx, message, fields...)
	} else {
		s.log.Info(ctx, message, fields...)
	}
}

func (s *Steps) classify(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	if s.errorClass != nil {
		if class := s.errorClass(err); class != "" {
			return class
		}
	}

	return "unknown"
}

func stepOutcome(result stepResult) string {
	switch {
	case result.err != nil:
		return "failed"
	case result.skipped:
		return "skipped"
	default:
		return "succeeded"
	}
}

func stepMetricAttributes(jobKind, name, outcome, reason, errorClass string) []attribute.KeyValue {
	attributes := []attribute.KeyValue{
		attribute.String("job.kind", jobKind),
		attribute.String("step.name", name),
		attribute.String("outcome", outcome),
	}
	if reason != "" {
		attributes = append(attributes, attribute.String("skip.reason", reason))
	}
	if errorClass != "" {
		attributes = append(attributes, attribute.String("error.class", errorClass))
	}

	return attributes
}

func stepLogFields(jobKind, name, outcome, reason, errorClass string, err error) []zap.Field {
	fields := []zap.Field{
		zap.String("event.name", "outbox.step."+outcome),
		zap.String("outbox.job.kind", jobKind),
		zap.String("outbox.step.name", name),
		zap.String("outbox.step.outcome", outcome),
	}
	if reason != "" {
		fields = append(fields, zap.String("outbox.step.skip_reason", reason))
	}
	if err != nil {
		fields = append(fields,
			zap.String("error.class", errorClass),
			zap.Error(err),
		)
	}

	return fields
}
