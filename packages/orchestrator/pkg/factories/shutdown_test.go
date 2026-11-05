//go:build linux

package factories

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

// A closer that never returns must not hold the exit past the shared
// deadline: it is cut and logged as forced, and the closer whose turn comes
// after the deadline is skipped and logged the same way. Runs on synctest's
// clock, so nothing waits in wall time.
func TestRunClosers_DeadlineCutsAHungCloser(t *testing.T) { //nolint:paralleltest // ReplaceGlobals mutates process state
	core, logs := observer.New(zapcore.InfoLevel)
	t.Cleanup(logger.ReplaceGlobals(t.Context(), logger.NewTracedLoggerFromCore(core)))

	synctest.Test(t, func(t *testing.T) {
		ran := make(chan string, 2)
		record := func(name string) closer {
			return closer{name, func(context.Context) error {
				ran <- name

				return nil
			}}
		}
		release := make(chan struct{})
		defer close(release)
		closers := []closer{
			record("first"),
			{"hung", func(context.Context) error {
				<-release

				return nil
			}},
			record("third"),
		}

		require.False(t, runClosers(t.Context(), closers, false, 200*time.Millisecond))
		synctest.Wait()

		require.Equal(t, "third", <-ran)
		select {
		case got := <-ran:
			t.Fatalf("closer %q ran after the deadline", got)
		default:
		}

		forced := func(name string) []observer.LoggedEntry {
			return logs.FilterField(zap.String("service", name)).FilterField(zap.Bool("forced", true)).All()
		}
		cut := forced("hung")
		require.NotEmpty(t, cut, "the hung closer is logged as forced")
		require.Equal(t, zapcore.ErrorLevel, cut[0].Level)
		skipped := forced("first")
		require.NotEmpty(t, skipped, "the closer after the deadline is logged as forced")
		require.Equal(t, zapcore.ErrorLevel, skipped[0].Level)
	})
}

// Closers run in reverse registration order with a context that carries
// the shared deadline; a failing closer does not stop the others and makes
// the run not clean. Waits on nothing.
func TestRunClosers_ReverseOrderAndErrorsReported(t *testing.T) {
	t.Parallel()

	var ran []string
	var hasDeadline bool
	record := func(name string, err error) closer {
		return closer{name, func(ctx context.Context) error {
			_, hasDeadline = ctx.Deadline()
			ran = append(ran, name)

			return err
		}}
	}
	closers := []closer{record("first", nil), record("second", errors.New("boom")), record("third", nil)}

	require.False(t, runClosers(t.Context(), closers, false, time.Minute))
	require.Equal(t, []string{"third", "second", "first"}, ran)
	require.True(t, hasDeadline)

	require.True(t, runClosers(t.Context(), closers[2:], false, time.Minute))
}

// The wait returns the group's error when it finishes in time and reports
// completed=false once the deadline passes. The timed-out half waits on an
// already-expired timer, not a sleep.
func TestWaitBounded_CompletesAndTimesOut(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	completed, err := waitBounded(func() error { return errBoom }, time.Minute)
	require.True(t, completed)
	require.ErrorIs(t, err, errBoom)

	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	completed, err = waitBounded(func() error {
		<-release

		return nil
	}, 0)
	require.False(t, completed)
	require.NoError(t, err)
}
