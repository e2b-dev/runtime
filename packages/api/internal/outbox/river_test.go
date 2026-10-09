package outbox

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/db/pkg/outbox"
)

// unjitteredJobID is a job ID whose jitter is zero: 20 % 41 - 20.
const unjitteredJobID = 20

func TestRetryPolicyGrowsFromTheFloorToTheCap(t *testing.T) {
	t.Parallel()

	policy := retryPolicy{floor: retryFloor, cap: retryCap}
	for attempt, want := range map[int]time.Duration{
		1:   retryFloor,
		3:   81 * time.Second,
		5:   625 * time.Second,
		6:   retryCap,
		100: retryCap,
	} {
		require.Equal(t, want, policy.delay(unjitteredJobID, attempt), "attempt %d", attempt)
	}
}

func TestTeardownRetriesForAboutADayBeforeItIsDiscarded(t *testing.T) {
	t.Parallel()

	policy := retryPolicy{floor: retryFloor, cap: retryCap}
	maxAttempts := outbox.TeardownTeamResources{}.InsertOpts().MaxAttempts
	var total time.Duration
	for attempt := 1; attempt < maxAttempts; attempt++ {
		total += policy.delay(unjitteredJobID, attempt)
	}

	require.InDelta(t, 24*time.Hour, total, float64(2*time.Hour))
}
