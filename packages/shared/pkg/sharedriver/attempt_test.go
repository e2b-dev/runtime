package sharedriver

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

const attemptJobKind = "push_project_limits"

var subject = zap.String("project_id", "p-1")

func runAttempt(t *testing.T, tel *telemetry.Client, attempt int, work func(context.Context, JobSteps) error) ([]observer.LoggedEntry, error) {
	t.Helper()

	core, logs := observer.New(zapcore.DebugLevel)
	log := logger.NewTracedLoggerFromCore(core)
	steps := testSteps(tel, log).Job(attemptJobKind)
	job := &rivertype.JobRow{ID: 7, Kind: attemptJobKind, Queue: river.QueueDefault, Attempt: attempt, MaxAttempts: 3}
	err := NewOutboxJobTelemetry(tel, log).Work(t.Context(), job, func(ctx context.Context) error {
		return work(ctx, steps)
	})

	return logs.All(), err
}

func TestAFailedStepIsLoggedOnceOnItsJobsLine(t *testing.T) {
	t.Parallel()

	for outcome, tc := range map[string]struct {
		attempt int
		level   zapcore.Level
	}{
		"retrying":  {attempt: 1, level: zapcore.WarnLevel},
		"discarded": {attempt: 3, level: zapcore.ErrorLevel},
	} {
		t.Run(outcome, func(t *testing.T) {
			t.Parallel()

			unreachable := errors.New("stream unavailable")
			entries, err := runAttempt(t, nil, tc.attempt, func(ctx context.Context, steps JobSteps) error {
				if err := steps.StepWith(ctx, "publish_project_limits", StepOptions{Subject: []zap.Field{subject}}, func(context.Context) error {
					return unreachable
				}); err != nil {
					return fmt.Errorf("deliver limits for project p-1: %w", err)
				}

				return nil
			})
			require.ErrorIs(t, err, unreachable)

			require.Len(t, entries, 1)
			assert.Equal(t, tc.level, entries[0].Level)
			assert.Equal(t, "outbox job "+attemptJobKind+": "+outcome, entries[0].Message)
			fields := entries[0].ContextMap()
			assert.Equal(t, int64(tc.attempt), fields["outbox.job.attempt"])
			assert.Equal(t, int64(3), fields["outbox.job.max_attempts"])
			assert.Equal(t, "publish_project_limits", fields["outbox.step.name"])
			assert.Equal(t, "failed", fields["outbox.step.outcome"])
			assert.Equal(t, "unknown", fields["error.class"])
			assert.Equal(t, "p-1", fields["project_id"])
			assert.Equal(t, "stream unavailable", fields["error"],
				"the step's own error; the worker's wrapping can name a subject the step keeps out of its fields")
		})
	}
}

func TestAFailureOutsideAnyStepIsLoggedWithoutStepFields(t *testing.T) {
	t.Parallel()

	entries, err := runAttempt(t, nil, 1, func(context.Context, JobSteps) error {
		return errors.New("read the projection: connection reset")
	})
	require.Error(t, err)

	require.Len(t, entries, 1)
	assert.Equal(t, zapcore.WarnLevel, entries[0].Level)
	assert.Equal(t, "read the projection: connection reset", entries[0].ContextMap()["error"])
	assert.NotContains(t, entries[0].ContextMap(), "outbox.step.name")
}

func TestASkipThatCancelsTheJobIsOneLine(t *testing.T) {
	t.Parallel()

	superseded := errors.New("superseded")
	for name, work := range map[string]func(context.Context, JobSteps) error{
		"skip reported by the worker": func(ctx context.Context, steps JobSteps) error {
			steps.Skipped(ctx, "publish_project_limits", "superseded", subject)

			return river.JobCancel(superseded)
		},
		"skip read from the step's error": func(ctx context.Context, steps JobSteps) error {
			return river.JobCancel(steps.StepWith(ctx, "publish_project_limits", StepOptions{
				SkipReason: SkipWhen("superseded", superseded),
				Subject:    []zap.Field{subject},
			}, func(context.Context) error {
				return fmt.Errorf("revision 3: %w", superseded)
			}))
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			entries, err := runAttempt(t, nil, 1, work)
			require.ErrorIs(t, err, superseded)

			require.Len(t, entries, 1)
			assert.Equal(t, zapcore.InfoLevel, entries[0].Level)
			assert.Equal(t, "outbox job "+attemptJobKind+": cancelled", entries[0].Message)
			fields := entries[0].ContextMap()
			assert.Equal(t, "publish_project_limits", fields["outbox.step.name"])
			assert.Equal(t, "superseded", fields["outbox.step.skip_reason"])
			assert.Equal(t, "p-1", fields["project_id"])
		})
	}
}

func TestASkipOnAJobThatCompletesKeepsItsOwnLineAndSpan(t *testing.T) {
	t.Parallel()

	recorder, provider := recordingTracer(t)
	entries, err := runAttempt(t, telemetryWith(provider, nil), 1, func(ctx context.Context, steps JobSteps) error {
		steps.Skipped(ctx, "mark_project_limits_propagated", "superseded", subject)

		return nil
	})
	require.NoError(t, err)

	require.Len(t, entries, 1)
	assert.Equal(t, zapcore.InfoLevel, entries[0].Level)
	assert.Equal(t, "outbox step mark_project_limits_propagated: skipped", entries[0].Message)
	fields := entries[0].ContextMap()
	assert.Equal(t, "superseded", fields["outbox.step.skip_reason"])
	assert.Equal(t, "p-1", fields["project_id"])
	step := findSpan(t, recorder.Ended(), "outbox step mark_project_limits_propagated")
	assert.Equal(t, step.SpanContext().SpanID().String(), fields["span_id"], "the line links to the step's span")
}

func TestAStepFailureTheWorkerTurnsIntoASnoozeIsLoggedOnce(t *testing.T) {
	t.Parallel()

	refused := errors.New("upstream: rate limited")
	entries, err := runAttempt(t, nil, 1, func(ctx context.Context, steps JobSteps) error {
		if err := steps.StepWith(ctx, "retire_legacy_subscriptions", StepOptions{Subject: []zap.Field{subject}}, func(context.Context) error {
			return refused
		}); err != nil {
			return river.JobSnooze(time.Minute)
		}

		return nil
	})
	var snoozed *river.JobSnoozeError
	require.ErrorAs(t, err, &snoozed)

	require.Len(t, entries, 1)
	assert.Equal(t, zapcore.WarnLevel, entries[0].Level)
	assert.Equal(t, "outbox job "+attemptJobKind+": snoozed", entries[0].Message)
	fields := entries[0].ContextMap()
	assert.Equal(t, "retire_legacy_subscriptions", fields["outbox.step.name"])
	assert.Equal(t, refused.Error(), fields["error"], "a snooze carries no error, so the line names the step's")
}

func TestAStepFailureTheWorkerHandlesIsCountedButNotLogged(t *testing.T) {
	t.Parallel()

	reader := sdkmetric.NewManualReader()
	entries, err := runAttempt(t, telemetryWith(nil, sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))), 1,
		func(ctx context.Context, steps JobSteps) error {
			_ = steps.Step(ctx, "fetch_email_metrics", func(context.Context) error { return errors.New("timeout") })

			return nil
		})
	require.NoError(t, err)

	assert.Empty(t, entries)
	points := stepCounter(t, reader)
	require.Len(t, points, 1)
	assert.Equal(t, "failed", metricAttribute(points[0].Attributes, "outcome"))
}

func TestStepsOfOneAttemptMayRunConcurrently(t *testing.T) {
	t.Parallel()

	entries, err := runAttempt(t, nil, 1, func(ctx context.Context, steps JobSteps) error {
		var group sync.WaitGroup
		for range 20 {
			group.Go(func() { steps.Skipped(ctx, "fetch_email_metrics", "empty") })
		}
		group.Wait()

		return nil
	})
	require.NoError(t, err)
	assert.Len(t, entries, 20)
}
