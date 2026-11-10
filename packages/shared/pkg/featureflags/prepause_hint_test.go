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

func TestGetPrePauseHintConfig(t *testing.T) {
	t.Parallel()

	obj := func(kv ...any) ldvalue.Value {
		b := ldvalue.ObjectBuild()
		for i := 0; i+1 < len(kv); i += 2 {
			b.Set(kv[i].(string), kv[i+1].(ldvalue.Value))
		}

		return b.Build()
	}
	n := ldvalue.Int

	cases := []struct {
		name    string
		flag    ldvalue.Value
		useCase string
		want    PrePauseHintConfig
	}{
		{
			"no flag disables the drain and keeps the stop defaults", ldvalue.Null(), "pause",
			PrePauseHintConfig{Timeout: 0, Stop: HintStopConfig{Timeout: 2 * time.Second, Grace: 100 * time.Millisecond}},
		},
		{
			"budget for the use case", obj("enabled", ldvalue.Bool(true), "pause", n(500), "build", n(0)), "pause",
			PrePauseHintConfig{Timeout: 500 * time.Millisecond, Stop: HintStopConfig{Timeout: 2 * time.Second, Grace: 100 * time.Millisecond}},
		},
		{
			"a use case set to zero is disabled", obj("pause", n(500), "build", n(0)), "build",
			PrePauseHintConfig{Timeout: 0, Stop: HintStopConfig{Timeout: 2 * time.Second, Grace: 100 * time.Millisecond}},
		},
		{
			"stop settings override the defaults", obj("pause", n(500), "stop", n(750), "stop_grace", n(20)), "pause",
			PrePauseHintConfig{Timeout: 500 * time.Millisecond, Stop: HintStopConfig{Timeout: 750 * time.Millisecond, Grace: 20 * time.Millisecond}},
		},
		{
			"stop zero leaves an abandoned cycle running", obj("pause", n(500), "stop", n(0)), "pause",
			PrePauseHintConfig{Timeout: 500 * time.Millisecond, Stop: HintStopConfig{Timeout: 0, Grace: 100 * time.Millisecond}},
		},
		{
			"negative values clamp to zero", obj("pause", n(-1), "stop", n(-5), "stop_grace", n(-5)), "pause",
			PrePauseHintConfig{Timeout: 0, Stop: HintStopConfig{Timeout: 0, Grace: 0}},
		},
		{
			"non-number fields take the defaults", obj("pause", ldvalue.String("500"), "stop", ldvalue.Bool(true)), "pause",
			PrePauseHintConfig{Timeout: 0, Stop: HintStopConfig{Timeout: 2 * time.Second, Grace: 100 * time.Millisecond}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			source := ldtestdata.DataSource()
			client, err := NewClientWithDatasource(source)
			require.NoError(t, err)
			t.Cleanup(func() { assert.NoError(t, client.Close(context.WithoutCancel(t.Context()))) })
			source.Update(source.Flag(FreePageHintingConfig.Key()).ValueForAll(tc.flag))

			assert.Equal(t, tc.want, GetPrePauseHintConfig(t.Context(), client, tc.useCase))
		})
	}
}
