package utils

import (
	"context"
	"sync"
	"time"
)

// ContextExtendedTo returns a context with ctx's values that ends once ctx
// has ended and until has passed, whichever is later, so work a caller
// started completes for a caller that leaves early while a caller that stays
// keeps its own context's end. The context carries no deadline: Err is
// context.Canceled, and context.Cause is ctx's cause when ctx ended after
// until or context.DeadlineExceeded when the end came at until. Call the
// returned function when the work ends.
func ContextExtendedTo(ctx context.Context, until time.Time) (context.Context, context.CancelFunc) {
	extended, cancel := context.WithCancelCause(context.WithoutCancel(ctx))

	var mu sync.Mutex
	var timer *time.Timer
	released := false
	stop := context.AfterFunc(ctx, func() {
		mu.Lock()
		defer mu.Unlock()

		// Released before the callback took the lock: nothing to arm.
		if released {
			return
		}
		delay := time.Until(until)
		if delay <= 0 {
			cancel(context.Cause(ctx))

			return
		}
		timer = time.AfterFunc(delay, func() { cancel(context.DeadlineExceeded) })
	})

	return extended, func() {
		stop()
		mu.Lock()
		released = true
		if timer != nil {
			timer.Stop()
		}
		mu.Unlock()
		cancel(context.Canceled)
	}
}
