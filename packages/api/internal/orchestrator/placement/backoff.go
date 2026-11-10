package placement

import (
	"context"
	"math/rand/v2"
	"time"
)

// refusalBackoff paces placement after a node refuses a create with
// ResourceExhausted. A refusal neither excludes the node nor spends a retry, so
// without a wait a placement that finds every node busy re-sends creates as fast
// as the refusals come back, until the request deadline.
type refusalBackoff struct {
	// base is the window after the first refusal; it doubles with each refusal.
	base time.Duration
	// max caps the window.
	max time.Duration
}

var defaultRefusalBackoff = refusalBackoff{base: refusalBackoffBase, max: refusalBackoffMax}

// window is the upper bound on the wait after the n-th refusal of a placement.
func (b refusalBackoff) window(n int) time.Duration {
	if n < 1 || b.base <= 0 || b.max <= 0 {
		return 0
	}

	w := b.base
	for i := 1; i < n && w < b.max; i++ {
		w *= 2
	}

	return min(w, b.max)
}

// delay draws the wait after the n-th refusal uniformly from [0, window(n)).
// Full jitter, so concurrent placements refused together (the creates of one
// fork request) spread their retries out instead of arriving back in step.
func (b refusalBackoff) delay(n int) time.Duration {
	w := b.window(n)
	if w <= 0 {
		return 0
	}

	return rand.N(w)
}

// sleep waits for d, reporting false if ctx ends first.
func sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}

	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
