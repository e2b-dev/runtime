package retry

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"time"
)

// ErrBudgetExhausted is returned (wrapped) when fn never succeeded within
// Policy.TotalBudget.
var ErrBudgetExhausted = errors.New("retry budget exhausted")

// Policy configures Do.
type Policy struct {
	// TotalBudget bounds the wall-clock time across all attempts. Required
	// (a non-positive value makes Do fail immediately with ErrBudgetExhausted).
	TotalBudget time.Duration
	// AttemptTimeout bounds a single attempt. 0 means no per-attempt timeout —
	// each attempt is bounded only by the remaining budget.
	AttemptTimeout time.Duration
	// InitialBackoff is the wait before the first retry.
	InitialBackoff time.Duration
	// MaxBackoff caps the backoff between attempts. 0 means uncapped.
	MaxBackoff time.Duration
	// Multiplier is the exponential growth factor between attempts (< 1 is
	// treated as 1, i.e. constant backoff).
	Multiplier int
}

// Backoff is the wait schedule between attempts: exponential from Initial
// up to Max, with optional jitter. The zero value waits nothing; Reset
// restarts the schedule after a success.
type Backoff struct {
	// Initial is the first nominal wait. Initial <= 0 waits zero.
	Initial time.Duration
	// Max caps every nominal wait, including the first. Max <= 0 is uncapped.
	Max time.Duration
	// Multiplier grows the nominal wait after each call (< 1 is treated as 1).
	// Growth saturates at Max or the largest Duration instead of overflowing.
	Multiplier int
	// Jitter spreads each wait uniformly over [wait×(1−Jitter), wait×(1+Jitter)],
	// limited to nonnegative Durations. It applies after the Max cap, so a
	// jittered wait can exceed Max. Values above 1 are treated as 1, and
	// negative values or NaN disable jitter.
	Jitter float64

	next time.Duration
}

// Next returns the wait before the next attempt and advances the schedule.
func (b *Backoff) Next() time.Duration {
	if b.next == 0 {
		b.next = b.Initial
	}
	limit := time.Duration(math.MaxInt64)
	if b.Max > 0 {
		limit = b.Max
	}
	wait := min(max(b.next, 0), limit)
	growth := max(b.Multiplier, 1)
	if wait > limit/time.Duration(growth) {
		b.next = limit
	} else {
		b.next = wait * time.Duration(growth)
	}

	if b.Jitter > 0 && wait > 0 {
		// Keeping spread at most wait treats Jitter above 1 as 1. Comparing as
		// float also avoids converting float64(wait) rounded up to 2^63.
		spread := wait
		if f := float64(wait) * b.Jitter; f < float64(wait) {
			spread = time.Duration(f)
		}
		low := wait - spread
		high := wait + min(spread, math.MaxInt64-wait)
		wait = low + time.Duration(rand.N(uint64(high-low)+1))
	}

	return wait
}

// Reset restarts the schedule from Initial.
func (b *Backoff) Reset() {
	b.next = 0
}

// Do runs fn with retries until it returns nil, retryable reports the error as
// non-retryable, the budget is exhausted, or ctx is cancelled.
//
// The whole call runs under a single budget context derived from ctx, so each
// attempt's context is capped to the remaining budget and the loop never runs
// past TotalBudget. fn must respect the context it is given.
//
// retryable classifies an error as worth retrying; a nil retryable treats every
// error as retryable. onRetry, if non-nil, is called before each backoff sleep
// with the 1-based attempt number, the upcoming backoff, and the error that
// triggered the retry.
func Do(
	ctx context.Context,
	policy Policy,
	retryable func(error) bool,
	fn func(context.Context) error,
	onRetry func(attempt int, backoff time.Duration, err error),
) error {
	budgetCtx, cancel := context.WithTimeoutCause(ctx, policy.TotalBudget, ErrBudgetExhausted)
	defer cancel()

	schedule := Backoff{Initial: policy.InitialBackoff, Max: policy.MaxBackoff, Multiplier: policy.Multiplier}

	for attempt := 1; ; attempt++ {
		err := runAttempt(budgetCtx, policy.AttemptTimeout, fn)
		if err == nil {
			return nil
		}

		// Budget exhausted or parent cancelled: stop. Checking the budget
		// context (not err) distinguishes these from a per-attempt timeout,
		// which leaves budgetCtx alive and is retryable.
		if budgetCtx.Err() != nil {
			return stopError(budgetCtx, attempt, err)
		}

		if retryable != nil && !retryable(err) {
			return fmt.Errorf("non-retryable error after %d attempts: %w", attempt, err)
		}

		wait := schedule.Next()
		if onRetry != nil {
			onRetry(attempt, wait, err)
		}

		select {
		case <-budgetCtx.Done():
			return stopError(budgetCtx, attempt, err)
		case <-time.After(wait):
		}
	}
}

// runAttempt runs a single attempt under a fresh per-attempt timeout derived
// from ctx. Because the attempt context derives from the budget context, its
// effective deadline is min(attemptTimeout, remaining budget).
func runAttempt(ctx context.Context, attemptTimeout time.Duration, fn func(context.Context) error) error {
	if attemptTimeout <= 0 {
		return fn(ctx)
	}

	attemptCtx, cancel := context.WithTimeout(ctx, attemptTimeout)
	defer cancel()

	return fn(attemptCtx)
}

// stopError maps a stopped budget context to a terminal error: budget
// exhaustion vs. parent cancellation (e.g. caller shutdown).
func stopError(budgetCtx context.Context, attempt int, lastErr error) error {
	cause := context.Cause(budgetCtx)
	if errors.Is(cause, ErrBudgetExhausted) {
		return fmt.Errorf("%w after %d attempts: %w", ErrBudgetExhausted, attempt, lastErr)
	}

	return errors.Join(lastErr, cause)
}
