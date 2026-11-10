package placement

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRefusalBackoff_WindowDoublesUpToCap(t *testing.T) {
	t.Parallel()

	b := refusalBackoff{base: 25 * time.Millisecond, max: time.Second}

	tests := []struct {
		refusals int
		want     time.Duration
	}{
		{0, 0},
		{1, 25 * time.Millisecond},
		{2, 50 * time.Millisecond},
		{3, 100 * time.Millisecond},
		{6, 800 * time.Millisecond},
		{7, time.Second},
		{1 << 20, time.Second},
	}

	for _, tt := range tests {
		assert.Equal(t, tt.want, b.window(tt.refusals), "refusals=%d", tt.refusals)
	}
}

func TestRefusalBackoff_ZeroValueNeverWaits(t *testing.T) {
	t.Parallel()

	var b refusalBackoff
	for n := range 10 {
		assert.Zero(t, b.delay(n))
	}
}

func TestRefusalBackoff_DelayIsJitteredWithinWindow(t *testing.T) {
	t.Parallel()

	b := refusalBackoff{base: 25 * time.Millisecond, max: time.Second}

	for n := 1; n <= 8; n++ {
		window := b.window(n)
		seen := make(map[time.Duration]struct{})

		for range 200 {
			d := b.delay(n)
			assert.GreaterOrEqual(t, d, time.Duration(0))
			assert.Less(t, d, window)
			seen[d] = struct{}{}
		}

		assert.Greater(t, len(seen), 1, "delays after %d refusals should be jittered", n)
	}
}

func TestSleep_StopsWhenContextEnds(t *testing.T) {
	t.Parallel()

	assert.True(t, sleep(t.Context(), time.Millisecond))

	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	assert.False(t, sleep(cancelled, 0), "a zero wait still reports an ended context")

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()

	start := time.Now()
	assert.False(t, sleep(ctx, time.Hour))
	assert.Less(t, time.Since(start), 5*time.Second, "the wait must end with the context, not the timer")
}
