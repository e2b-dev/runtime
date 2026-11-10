package pauselimit

import (
	"strconv"
	"testing"
	"time"

	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
)

func TestParseLimit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value ldvalue.Value
		want  Limit
		ok    bool
	}{
		{name: "unset", value: ldvalue.Null()},
		{name: "valid", value: ldvalue.Parse([]byte(`{"limit": 3, "period_s": 60}`)), want: Limit{Pauses: 3, Period: time.Minute}, ok: true},
		{name: "longest period", value: ldvalue.Parse([]byte(`{"limit": 1, "period_s": 600}`)), want: Limit{Pauses: 1, Period: MaxPeriod}, ok: true},
		{name: "period over ten minutes", value: ldvalue.Parse([]byte(`{"limit": 1, "period_s": 601}`))},
		{name: "zero period", value: ldvalue.Parse([]byte(`{"limit": 1, "period_s": 0}`))},
		{name: "zero limit", value: ldvalue.Parse([]byte(`{"limit": 0, "period_s": 60}`))},
		{name: "fractional limit", value: ldvalue.Parse([]byte(`{"limit": 1.5, "period_s": 60}`))},
		{name: "missing period", value: ldvalue.Parse([]byte(`{"limit": 3}`))},
		{name: "string values", value: ldvalue.Parse([]byte(`{"limit": "3", "period_s": "60"}`))},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, ok := parseLimit(tc.value)

			assert.Equal(t, tc.ok, ok)
			assert.Equal(t, tc.want, got)
		})
	}
}

// The key exists only until the budget is full again, so a sandbox costs Redis
// memory for at most one period after its last counted pause.
func TestAllowKeyLivesUntilBudgetRefills(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupContainerInstance(t)
	limiter, err := New(client, nil, noop.NewMeterProvider())
	require.NoError(t, err)
	limit := Limit{Pauses: 3, Period: time.Minute}

	keyTTL := func() time.Duration {
		t.Helper()

		keys, err := client.Keys(t.Context(), "*").Result()
		require.NoError(t, err)
		require.Len(t, keys, 1)
		ttl, err := client.PTTL(t.Context(), keys[0]).Result()
		require.NoError(t, err)

		return ttl
	}
	allow := func(executionID string) Decision {
		t.Helper()

		result, err := limiter.Allow(t.Context(), limit, "team", "sandbox", executionID)
		require.NoError(t, err)

		return result.Decision
	}

	require.Equal(t, DecisionAllowed, allow("0"))
	assert.InDelta(t, 20*time.Second, keyTTL(), float64(time.Second), "one pause refills in period/limit")

	for i := 1; i < limit.Pauses; i++ {
		require.Equal(t, DecisionAllowed, allow(strconv.Itoa(i)))
	}
	full := keyTTL()
	assert.LessOrEqual(t, full, limit.Period)
	assert.InDelta(t, limit.Period, full, float64(time.Second))

	require.Equal(t, DecisionLimited, allow("refused"))
	assert.LessOrEqual(t, keyTTL(), full, "a refused pause does not extend the key")
}
