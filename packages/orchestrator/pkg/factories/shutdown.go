//go:build linux

package factories

import (
	"context"
	"slices"
	"time"

	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

// closersDeadline bounds the closers as a whole after the sandbox drain, and
// the wait for the service goroutines after them.
const closersDeadline = 30 * time.Second

// runClosers runs closers in reverse registration order, each with a context
// bounded by the shared deadline. A closer that returns an error, or is still
// running at the deadline, is logged with its name and forced=true, and a
// closer whose turn comes after the deadline is skipped and logged the same
// way, so the ones that ran kept their order. Returns false if any closer
// failed, was cut or was skipped.
func runClosers(ctx context.Context, closers []closer, forceStop bool, deadline time.Duration) bool {
	deadlineAt := time.Now().Add(deadline)
	ctx, cancel := context.WithDeadline(ctx, deadlineAt)
	defer cancel()

	clean := true
	for _, c := range slices.Backward(closers) {
		clog := logger.L().With(zap.String("service", c.name))
		if !time.Now().Before(deadlineAt) {
			// Not started: it would run unordered and unwaited.
			clog.Error(ctx, "closer skipped: the shutdown deadline has passed", zap.Bool("forced", true))
			clean = false

			continue
		}
		clog.Info(ctx, "closing", zap.Bool("forced", forceStop || ctx.Err() != nil))

		// A forced stop cancels ctx before any closer runs, so the cut is the
		// deadline alone, not ctx.Done().
		completed, err := waitBounded(func() error { return c.close(ctx) }, time.Until(deadlineAt))
		switch {
		case !completed:
			// The closer keeps running until the process exits.
			clog.Error(ctx, "closer did not finish within the shutdown deadline", zap.Bool("forced", true))
			clean = false
		case err != nil:
			clog.Error(ctx, "error during shutdown", zap.Error(err), zap.Bool("forced", forceStop || ctx.Err() != nil))
			clean = false
		}
	}

	return clean
}

// waitBounded waits for wait() up to d. Returns completed=false when d
// elapses first.
func waitBounded(wait func() error, d time.Duration) (completed bool, err error) {
	done := make(chan error, 1)
	go func() { done <- wait() }()

	select {
	case err := <-done:
		return true, err
	case <-time.After(d):
		return false, nil
	}
}
