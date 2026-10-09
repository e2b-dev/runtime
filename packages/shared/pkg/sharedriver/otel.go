package sharedriver

import (
	"cmp"
	"context"
	"errors"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
	"github.com/riverqueue/rivercontrib/otelriver"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// NewOTelMiddleware returns the River plugin every River client installs:
// insert and work spans suffixed with the job kind, and trace context carried
// from the insert into the job's metadata so the work span links back to the
// transaction that enqueued it.
//
// The OpenTelemetry messaging semantic-convention metrics stay off: they
// duplicate every river.* measurement under messaging.* with only the
// operation name as a label, so nothing can read them per kind or queue.
func NewOTelMiddleware(tel *telemetry.Client) *otelriver.Middleware {
	if tel == nil {
		tel = telemetry.NewNoopClient()
	}

	return otelriver.NewMiddleware(&otelriver.MiddlewareConfig{
		EnableTracePropagation:      true,
		EnableWorkSpanJobKindSuffix: true,
		MeterProvider:               tel.MeterProvider,
		TracerProvider:              tel.TracerProvider,
	})
}

// instrumentationScope is the meter name of the metrics this package owns,
// as distinct from the river.* ones otelriver emits under its own scope.
const instrumentationScope = "github.com/e2b-dev/infra/packages/shared/pkg/sharedriver"

// OutboxJobTelemetry counts every job a River outbox finishes, by kind and by
// the outcome River gave it, and logs once each attempt that did not
// complete. River's own river.work_count carries the attempt but not the
// maximum, so nothing in it separates a job that will be retried from one
// that has failed for the last time — which is the outcome worth alerting on.
type OutboxJobTelemetry struct {
	river.MiddlewareDefaults

	finished metric.Int64Counter
	log      logger.Logger
}

// NewOutboxJobTelemetry returns the worker middleware to install in
// river.Config.Middleware. The counter is named for the outbox rather than
// for a service, so one query covers every service that installs it; the
// resource attributes each exports say whose outbox reported the point.
func NewOutboxJobTelemetry(tel *telemetry.Client, l logger.Logger) *OutboxJobTelemetry {
	if tel == nil {
		tel = telemetry.NewNoopClient()
	}
	if l == nil {
		l = logger.NewNopLogger()
	}
	meter := tel.MeterProvider.Meter(instrumentationScope)

	return &OutboxJobTelemetry{
		finished: utils.Must(meter.Int64Counter("outbox.jobs.finished")),
		log:      l,
	}
}

// Work records the outcome from a defer, the way otelriver does: a panicking
// worker is counted and the panic still reaches River, which is what turns
// the job into an error River can retry or discard. Every line logged during
// the attempt names the job and the attempt, so one job's retries can be read
// together.
func (t *OutboxJobTelemetry) Work(ctx context.Context, job *rivertype.JobRow, doInner func(context.Context) error) error {
	var (
		err      error
		panicked = true
	)
	ctx = logger.ContextWithFields(ctx,
		zap.Int64("outbox.job.id", job.ID),
		zap.Int("outbox.job.attempt", job.Attempt),
	)
	ctx, current := withAttempt(ctx)
	defer func() {
		outcome := jobOutcome(job, panicked, err)
		t.finished.Add(ctx, 1, metric.WithAttributes(
			attribute.String("job.kind", job.Kind),
			attribute.String("outcome", outcome),
		))
		t.logOutcome(ctx, job, outcome, err, current.recorded())
	}()

	err = doInner(ctx)
	panicked = false

	return err
}

// When a step caused the outcome, the line carries the step's own error, not
// the worker's wrapping of it: a worker may wrap it with a subject, such as an
// identity, that the step keeps out of its log fields. A panic gets no line
// here: River logs it, with the panic value, through the logger NewLogger
// returns.
func (t *OutboxJobTelemetry) logOutcome(ctx context.Context, job *rivertype.JobRow, outcome string, err error, steps []stepRecord) {
	causeAt := cause(outcome, err, steps)
	for i, skipped := range steps {
		if skipped.outcome != "skipped" || i == causeAt {
			continue
		}
		t.log.Info(trace.ContextWithSpanContext(ctx, skipped.span), "outbox step "+skipped.name+": skipped", skipped.logFields()...)
	}

	level, logged := jobLogLevel(outcome, causeAt >= 0)
	if !logged {
		return
	}

	fields := []zap.Field{
		zap.String("event.name", "outbox.job."+outcome),
		zap.String("outbox.job.kind", job.Kind),
		zap.String("outbox.job.queue", job.Queue),
		zap.Int("outbox.job.max_attempts", job.MaxAttempts),
		zap.String("outbox.job.outcome", outcome),
	}
	if causeAt >= 0 {
		fields = append(fields, steps[causeAt].causeFields()...)
		err = cmp.Or(steps[causeAt].err, err)
	}
	if err != nil {
		fields = append(fields, zap.Error(err))
	}
	t.log.Log(ctx, level, "outbox job "+job.Kind+": "+outcome, fields...)
}

// jobOutcome is the state River will put the job in. A cancelled job is
// cancelled however many attempts it has left, and Attempt is already this
// attempt's number, so an error on the last one is the discard.
func jobOutcome(job *rivertype.JobRow, panicked bool, err error) string {
	var (
		cancelErr *rivertype.JobCancelError
		snoozeErr *rivertype.JobSnoozeError
	)

	switch {
	case panicked:
		return "panicked"
	case err == nil:
		return "completed"
	case errors.As(err, &snoozeErr):
		return "snoozed"
	case errors.As(err, &cancelErr):
		return "cancelled"
	case job.Attempt >= job.MaxAttempts:
		return "discarded"
	default:
		return "retrying"
	}
}
