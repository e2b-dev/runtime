package featureflags

import (
	"context"
	"testing"
	"time"

	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetPeriodicHintingConfig(t *testing.T) {
	t.Parallel()

	obj := func(kv ...any) ldvalue.Value {
		b := ldvalue.ObjectBuild()
		for i := 0; i+1 < len(kv); i += 2 {
			b.Set(kv[i].(string), kv[i+1].(ldvalue.Value))
		}

		return b.Build()
	}
	n := ldvalue.Int
	defaults := PeriodicHintingConfig{Interval: 0, Timeout: 500 * time.Millisecond, QuietAfterStart: 10 * time.Second, SilentRuns: 3, UnresponsiveRetry: 5 * time.Minute, HostSlots: 16, Stop: HintStopConfig{Timeout: 2 * time.Second, Grace: 100 * time.Millisecond}}
	with := func(f func(c *PeriodicHintingConfig)) PeriodicHintingConfig {
		c := defaults
		f(&c)

		return c
	}

	cases := []struct {
		name string
		flag ldvalue.Value
		want PeriodicHintingConfig
	}{
		{"no periodic block disables", obj("enabled", ldvalue.Bool(true), "pause", n(4000)), defaults},
		{
			"full block", obj("enabled", ldvalue.Bool(true), "stop", n(750), "stop_grace", n(20), "periodic", obj("interval", n(10000), "timeout", n(300), "quiet_after_start", n(5000), "observe_only", ldvalue.Bool(true), "silent_runs", n(5), "unresponsive_retry", n(60000), "host_slots", n(4))),
			PeriodicHintingConfig{Interval: 10 * time.Second, Timeout: 300 * time.Millisecond, QuietAfterStart: 5 * time.Second, ObserveOnly: true, SilentRuns: 5, UnresponsiveRetry: time.Minute, HostSlots: 4, Stop: HintStopConfig{Timeout: 750 * time.Millisecond, Grace: 20 * time.Millisecond}},
		},
		{
			"zero silent_runs and host_slots turn those bounds off", obj("periodic", obj("interval", n(15000), "silent_runs", n(0), "host_slots", n(-1))),
			with(func(c *PeriodicHintingConfig) { c.Interval = 15 * time.Second; c.SilentRuns = 0; c.HostSlots = 0 }),
		},
		{"partial block takes defaults", obj("periodic", obj("interval", n(15000))), with(func(c *PeriodicHintingConfig) { c.Interval = 15 * time.Second })},
		{"zero timeout falls back to the default", obj("periodic", obj("interval", n(15000), "timeout", n(0))), with(func(c *PeriodicHintingConfig) { c.Interval = 15 * time.Second })},
		{
			"negative values clamp", obj("periodic", obj("interval", n(15000), "timeout", n(-1), "quiet_after_start", n(-5))),
			with(func(c *PeriodicHintingConfig) { c.Interval = 15 * time.Second; c.QuietAfterStart = 0 }),
		},
		{"non-number fields are ignored", obj("periodic", obj("interval", ldvalue.String("10s"), "timeout", ldvalue.Bool(true))), defaults},
		{"negative interval disables", obj("periodic", obj("interval", n(-1000))), defaults},
		{"sub-second interval disables rather than running at the floor", obj("periodic", obj("interval", n(15))), defaults},
		{"non-object periodic disables", obj("periodic", n(15000)), defaults},
		{"non-boolean observe_only takes the default", obj("periodic", obj("interval", n(15000), "observe_only", n(1))), with(func(c *PeriodicHintingConfig) { c.Interval = 15 * time.Second })},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			source := ldtestdata.DataSource()
			client, err := NewClientWithDatasource(source)
			require.NoError(t, err)
			t.Cleanup(func() { assert.NoError(t, client.Close(context.WithoutCancel(t.Context()))) })
			source.Update(source.Flag(FreePageHintingConfig.Key()).ValueForAll(tc.flag))

			got := GetPeriodicHintingConfig(t.Context(), client)
			assert.Equal(t, tc.want, got)
			assert.Equal(t, tc.want.Interval > 0, got.Enabled())
		})
	}
}

// A client built without an API key serves the offline store for the life of
// the process; a test data source is live.
func TestClientLive(t *testing.T) {
	t.Parallel()
	live, err := NewClientWithDatasource(ldtestdata.DataSource())
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, live.Close(context.WithoutCancel(t.Context()))) })
	assert.True(t, live.Live())

	var nilClient *Client
	assert.False(t, nilClient.Live())
	assert.False(t, (&Client{}).Live())
	assert.False(t, (&Client{ld: live.ld, static: true}).Live())
}
