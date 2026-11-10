package sharedriver

import (
	"context"
	"errors"
	"slices"
	"sync"

	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type attemptKey struct{}

// attempt holds the steps of one job attempt that failed or were skipped, so
// OutboxJobTelemetry can log the step behind the attempt's outcome on the
// job's own line rather than on a second one. Steps may run concurrently.
type attempt struct {
	mu    sync.Mutex
	steps []stepRecord
}

type stepRecord struct {
	jobKind    string
	name       string
	outcome    string
	reason     string
	errorClass string
	err        error
	subject    []zap.Field
	span       trace.SpanContext
}

func withAttempt(ctx context.Context) (context.Context, *attempt) {
	current := &attempt{}

	return context.WithValue(ctx, attemptKey{}, current), current
}

// recordStep reports false outside a job attempt, where the step logs for
// itself.
func recordStep(ctx context.Context, record stepRecord) bool {
	current, ok := ctx.Value(attemptKey{}).(*attempt)
	if !ok {
		return false
	}

	current.mu.Lock()
	defer current.mu.Unlock()
	current.steps = append(current.steps, record)

	return true
}

func (a *attempt) recorded() []stepRecord {
	a.mu.Lock()
	defer a.mu.Unlock()

	return slices.Clone(a.steps)
}

// cause picks the step behind the attempt's outcome: the one whose error the
// worker returned, else for a cancel the last skip, and for a snooze the last
// failure, since a snooze carries no error of its own. A failure no outcome
// points to was handled by the worker and is not logged.
func cause(outcome string, err error, steps []stepRecord) int {
	last := func(match func(stepRecord) bool) int {
		for i, step := range slices.Backward(steps) {
			if match(step) {
				return i
			}
		}

		return -1
	}

	switch outcome {
	case "retrying", "discarded", "cancelled":
		if i := last(func(step stepRecord) bool { return step.err != nil && errors.Is(err, step.err) }); i >= 0 || outcome != "cancelled" {
			return i
		}

		return last(func(step stepRecord) bool { return step.outcome == "skipped" })
	case "snoozed":
		return last(func(step stepRecord) bool { return step.outcome == "failed" })
	default:
		return -1
	}
}

func jobLogLevel(outcome string, hasCause bool) (zapcore.Level, bool) {
	switch outcome {
	case "cancelled":
		return zapcore.InfoLevel, true
	case "retrying":
		return zapcore.WarnLevel, true
	case "discarded":
		return zapcore.ErrorLevel, true
	case "snoozed":
		return zapcore.WarnLevel, hasCause
	default:
		return zapcore.InfoLevel, false
	}
}

func (r stepRecord) causeFields() []zap.Field {
	fields := []zap.Field{
		zap.String("outbox.step.name", r.name),
		zap.String("outbox.step.outcome", r.outcome),
	}
	if r.reason != "" {
		fields = append(fields, zap.String("outbox.step.skip_reason", r.reason))
	}
	if r.errorClass != "" {
		fields = append(fields, zap.String("error.class", r.errorClass))
	}

	return append(fields, r.subject...)
}

func (r stepRecord) logFields() []zap.Field {
	return append(stepLogFields(r.jobKind, r.name, r.outcome, r.reason, r.errorClass, nil), r.subject...)
}
