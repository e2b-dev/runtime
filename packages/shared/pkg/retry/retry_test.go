package retry

import (
	"context"
	"errors"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func fastPolicy() Policy {
	return Policy{
		TotalBudget:    2 * time.Second,
		AttemptTimeout: 50 * time.Millisecond,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     5 * time.Millisecond,
		Multiplier:     2,
	}
}

func TestDo_RetriesThenSucceeds(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32
	fn := func(context.Context) error {
		if attempts.Add(1) < 3 {
			return errors.New("transient")
		}

		return nil
	}

	require.NoError(t, Do(t.Context(), fastPolicy(), nil, fn, nil))
	assert.EqualValues(t, 3, attempts.Load())
}

func TestDo_BudgetExhausted(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32
	fn := func(context.Context) error {
		attempts.Add(1)

		return errors.New("persistent")
	}

	err := Do(t.Context(), fastPolicy(), nil, fn, nil)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrBudgetExhausted)
	assert.Greater(t, attempts.Load(), int32(1))
}

func TestDo_PerAttemptTimeoutDoesNotAbortLoop(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32
	fn := func(ctx context.Context) error {
		if attempts.Add(1) == 1 {
			<-ctx.Done() // first attempt blows its per-attempt deadline

			return ctx.Err()
		}

		return nil
	}

	require.NoError(t, Do(t.Context(), fastPolicy(), nil, fn, nil))
	assert.EqualValues(t, 2, attempts.Load())
}

func TestDo_CapsAttemptToRemainingBudget(t *testing.T) {
	t.Parallel()

	// AttemptTimeout (10s) far exceeds the budget (100ms): a blocking attempt
	// must be cut off at the budget, not run for the full per-attempt timeout.
	policy := Policy{
		TotalBudget:    100 * time.Millisecond,
		AttemptTimeout: 10 * time.Second,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     5 * time.Millisecond,
		Multiplier:     2,
	}

	fn := func(ctx context.Context) error {
		<-ctx.Done()

		return ctx.Err()
	}

	start := time.Now()
	err := Do(t.Context(), policy, nil, fn, nil)
	elapsed := time.Since(start)

	require.ErrorIs(t, err, ErrBudgetExhausted)
	assert.Less(t, elapsed, 2*time.Second)
}

func TestDo_NonRetryableStops(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("permanent")
	var attempts atomic.Int32
	fn := func(context.Context) error {
		attempts.Add(1)

		return sentinel
	}
	retryable := func(err error) bool { return !errors.Is(err, sentinel) }

	err := Do(t.Context(), fastPolicy(), retryable, fn, nil)
	require.ErrorIs(t, err, sentinel)
	assert.EqualValues(t, 1, attempts.Load())
}

func TestDo_ParentCancelAborts(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	fn := func(context.Context) error {
		cancel()

		return errors.New("failed before cancel observed")
	}

	err := Do(ctx, fastPolicy(), nil, fn, nil)
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
	assert.NotErrorIs(t, err, ErrBudgetExhausted)
}

func TestDo_NoAttemptTimeoutUsesBudget(t *testing.T) {
	t.Parallel()

	// AttemptTimeout == 0: the attempt is bounded only by the remaining budget.
	policy := Policy{
		TotalBudget:    80 * time.Millisecond,
		AttemptTimeout: 0,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     5 * time.Millisecond,
		Multiplier:     2,
	}

	fn := func(ctx context.Context) error {
		<-ctx.Done()

		return ctx.Err()
	}

	start := time.Now()
	err := Do(t.Context(), policy, nil, fn, nil)

	require.ErrorIs(t, err, ErrBudgetExhausted)
	assert.Less(t, time.Since(start), time.Second)
}

func TestDo_OnRetryInvoked(t *testing.T) {
	t.Parallel()

	var retries atomic.Int32
	var attempts atomic.Int32
	fn := func(context.Context) error {
		if attempts.Add(1) < 3 {
			return errors.New("transient")
		}

		return nil
	}
	onRetry := func(int, time.Duration, error) { retries.Add(1) }

	require.NoError(t, Do(t.Context(), fastPolicy(), nil, fn, onRetry))
	assert.EqualValues(t, 2, retries.Load(), "onRetry fires once per retry (not the final success)")
}

// The schedule grows from Initial by Multiplier up to Max, spreads each wait
// within the jitter band, and restarts after Reset.
func TestBackoffScheduleAndJitter(t *testing.T) {
	t.Parallel()
	exact := Backoff{Initial: time.Second, Max: 5 * time.Second, Multiplier: 2}
	var waits []time.Duration
	for range 4 {
		waits = append(waits, exact.Next())
	}
	require.Equal(t, []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 5 * time.Second}, waits)
	exact.Reset()
	require.Equal(t, time.Second, exact.Next())

	jittered := Backoff{Initial: 10 * time.Second, Max: 10 * time.Second, Multiplier: 2, Jitter: 0.5}
	distinct := map[time.Duration]bool{}
	var longest time.Duration
	for range 50 {
		wait := jittered.Next()
		require.GreaterOrEqual(t, wait, 5*time.Second)
		require.LessOrEqual(t, wait, 15*time.Second)
		distinct[wait] = true
		longest = max(longest, wait)
	}
	require.Greater(t, len(distinct), 1, "jitter spreads the waits")
	require.Greater(t, longest, 10*time.Second, "jitter applies after the Max cap")
	require.Zero(t, (&Backoff{}).Next(), "the zero value waits nothing")
}

// Each case lists the inclusive range of consecutive waits, checked repeatedly
// and again after Reset.
func TestBackoffArithmetic(t *testing.T) {
	t.Parallel()
	const maxWait = time.Duration(math.MaxInt64)
	type span struct{ low, high time.Duration }
	tests := []struct {
		name    string
		backoff Backoff
		want    []span
	}{
		{
			name:    "max caps the first wait",
			backoff: Backoff{Initial: 10 * time.Second, Max: 2 * time.Second, Multiplier: 2},
			want:    []span{{2 * time.Second, 2 * time.Second}, {2 * time.Second, 2 * time.Second}},
		},
		{
			name:    "negative initial waits zero",
			backoff: Backoff{Initial: -time.Second, Multiplier: 2, Jitter: 0.5},
			want:    []span{{0, 0}, {0, 0}},
		},
		{
			name:    "jitter above one is clamped to one",
			backoff: Backoff{Initial: time.Second, Jitter: 5},
			want:    []span{{0, 2 * time.Second}, {0, 2 * time.Second}},
		},
		{
			name:    "infinite jitter is clamped to one",
			backoff: Backoff{Initial: time.Second, Jitter: math.Inf(1)},
			want:    []span{{0, 2 * time.Second}},
		},
		{
			name:    "NaN jitter is disabled",
			backoff: Backoff{Initial: time.Second, Jitter: math.NaN()},
			want:    []span{{time.Second, time.Second}},
		},
		{
			name:    "growth saturates instead of overflowing",
			backoff: Backoff{Initial: maxWait/2 + 1, Multiplier: 2},
			want:    []span{{maxWait/2 + 1, maxWait/2 + 1}, {maxWait, maxWait}, {maxWait, maxWait}},
		},
		{
			name:    "jitter band at the largest wait",
			backoff: Backoff{Initial: maxWait, Jitter: 1},
			want:    []span{{0, maxWait}},
		},
		{
			name:    "jitter band above the largest wait is clipped",
			backoff: Backoff{Initial: maxWait - 1, Jitter: 0.5},
			want:    []span{{maxWait/2 - 1, maxWait}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := tc.backoff
			for range 50 {
				for _, want := range tc.want {
					wait := b.Next()
					require.GreaterOrEqual(t, wait, want.low)
					require.LessOrEqual(t, wait, want.high)
				}
				b.Reset()
			}
		})
	}
}
