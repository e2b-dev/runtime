package featureflags

import (
	"context"
	"testing"

	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetRootfsReadAheadConfig(t *testing.T) {
	t.Parallel()

	defaults := RootfsReadAheadConfig{
		Chunks:          RootfsReadAheadDefaultChunks,
		Streams:         RootfsReadAheadDefaultStreams,
		NodeMaxInflight: RootfsReadAheadDefaultNodeMaxInflight,
	}

	tests := []struct {
		name  string
		value *ldvalue.Value // nil: flag not set at all
		want  RootfsReadAheadConfig
	}{
		{name: "unset", want: defaults},
		{name: "null", value: new(ldvalue.Null()), want: defaults},
		{
			name:  "every field",
			value: new(ldvalue.Parse([]byte(`{"chunks": 8, "streams": 16, "nodeMaxInflight": 64}`))),
			want:  RootfsReadAheadConfig{Chunks: 8, Streams: 16, NodeMaxInflight: 64},
		},
		{
			name:  "missing fields take their defaults",
			value: new(ldvalue.Parse([]byte(`{"chunks": 4}`))),
			want:  RootfsReadAheadConfig{Chunks: 4, Streams: 4, NodeMaxInflight: 32},
		},
		{
			name:  "a non-numeric field takes its default",
			value: new(ldvalue.Parse([]byte(`{"chunks": "8", "streams": 16}`))),
			want:  RootfsReadAheadConfig{Chunks: 0, Streams: 16, NodeMaxInflight: 32},
		},
		{
			name:  "a non-numeric streams takes its default, not zero",
			value: new(ldvalue.Parse([]byte(`{"chunks": 8, "streams": "16", "nodeMaxInflight": null}`))),
			want:  RootfsReadAheadConfig{Chunks: 8, Streams: 4, NodeMaxInflight: 32},
		},
		{
			name:  "zero and negative values are passed through for the reader to clamp",
			value: new(ldvalue.Parse([]byte(`{"chunks": 0, "streams": -1, "nodeMaxInflight": 0}`))),
			want:  RootfsReadAheadConfig{Chunks: 0, Streams: -1, NodeMaxInflight: 0},
		},
		{name: "not an object leaves read-ahead off", value: new(ldvalue.Parse([]byte(`[8, 16, 32]`))), want: defaults},
		{name: "a bare number leaves read-ahead off", value: new(ldvalue.Int(8)), want: defaults},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			td := ldtestdata.DataSource()
			if tc.value != nil {
				td.Update(td.Flag(RootfsReadAheadFlag.Key()).ValueForAll(*tc.value))
			}
			ff, err := NewClientWithDatasource(td)
			require.NoError(t, err)
			t.Cleanup(func() { _ = ff.Close(context.WithoutCancel(t.Context())) })

			assert.Equal(t, tc.want, GetRootfsReadAheadConfig(t.Context(), ff))
		})
	}
}

func TestGetRootfsReadAheadConfigWithoutLaunchDarkly(t *testing.T) {
	t.Parallel()

	got := GetRootfsReadAheadConfig(t.Context(), &Client{})
	assert.Equal(t, RootfsReadAheadDefaultChunks, got.Chunks, "no LaunchDarkly client: read-ahead stays off")
	assert.Equal(t, RootfsReadAheadDefaultStreams, got.Streams)
	assert.Equal(t, RootfsReadAheadDefaultNodeMaxInflight, got.NodeMaxInflight)
}
