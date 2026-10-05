package featureflags

import (
	"context"
	"testing"

	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/e2b-dev/infra/packages/shared/pkg/fcversion"
)

func newResolverTestClient(t *testing.T, versions map[string]string) *Client {
	t.Helper()

	td := ldtestdata.DataSource()
	td.Update(td.Flag(FirecrackerVersions.Key()).ValueForAll(ldvalue.FromJSONMarshal(versions)))

	client, err := NewClientWithDatasource(td)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, client.Close(context.WithoutCancel(t.Context())))
	})

	return client
}

func TestResolveFirecrackerVersion_LegacyGoldens(t *testing.T) {
	t.Parallel()

	// Pins key derivation and fallback semantics for the legacy versions stored in production.
	client := newResolverTestClient(t, map[string]string{
		"v1.10": "mapped-v1.10",
		"v1.12": "mapped-v1.12",
		"v1.14": "mapped-v1.14",
	})

	cases := []struct {
		stored string
		want   string
	}{
		{"v1.10.1_30cbb07", "mapped-v1.10"},
		{"v1.12.1_210cbac", "mapped-v1.12"},
		{"v1.14.1_431f1fc", "mapped-v1.14"},
		{"v1.14.1", "v1.14.1"},
		{"v9.9.9_abcdefg", "v9.9.9_abcdefg"},
		{"not-a-version", "not-a-version"},
	}

	for _, tc := range cases {
		t.Run(tc.stored, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.want, ResolveFirecrackerVersion(t.Context(), client, tc.stored))
		})
	}
}

func TestDisableLegacyTeamMutationsFlagFallback(t *testing.T) {
	t.Parallel()

	client, err := NewClientWithDatasource(ldtestdata.DataSource())
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, client.Close(context.WithoutCancel(t.Context())))
	})

	assert.False(t, client.BoolFlag(t.Context(), DisableLegacyTeamMutationsFlag))
}

// Guards the default map shipped in this package: every entry's key must be
// the LD key its value derives to, or the lookup silently never hits.
func TestFirecrackerVersionMap_KeysMatchLDKeys(t *testing.T) {
	t.Parallel()

	for key, value := range FirecrackerVersionMap {
		info, err := fcversion.New(value)
		require.NoError(t, err)

		derived, ok := info.LDKey()
		require.True(t, ok)
		assert.Equal(t, key, derived)
	}
}

func TestResolveFirecrackerVersion_MixedFormatMap(t *testing.T) {
	t.Parallel()

	// Legacy and e2b-format lines coexist in the map: each stored format
	// resolves through its own key, so the map can never remap across the
	// compatibility contract.
	client := newResolverTestClient(t, map[string]string{
		"v1.14":   "v1.14.1_431f1fc",
		"v1.14-0": "v1.14-0.2.0",
	})

	cases := []struct {
		stored string
		want   string
	}{
		{"v1.14.1_431f1fc", "v1.14.1_431f1fc"},
		{"v1.14-0.1.0", "v1.14-0.2.0"},
		{"v1.14-1.0.0", "v1.14-1.0.0"},
	}

	for _, tc := range cases {
		t.Run(tc.stored, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.want, ResolveFirecrackerVersion(t.Context(), client, tc.stored))
		})
	}
}

// testMetricReader collects the package's OTel metrics; the provider is
// installed once in TestMain because otel's global instruments delegate on
// the first SetMeterProvider only.
var testMetricReader = sdkmetric.NewManualReader()

func TestMain(m *testing.M) {
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(testMetricReader)))
	m.Run()
}

// TestResolveFirecrackerVersion_MetricAttributes pins the exact attribute set
// of every resolution datapoint — the served-version dashboard groups by
// these names, so a renamed, dropped, or added attribute breaks it silently.
func TestResolveFirecrackerVersion_MetricAttributes(t *testing.T) {
	t.Parallel()

	client := newResolverTestClient(t, map[string]string{
		"v9.9-9": "v9.9-9.9.9",
		"v7.7-7": "",
	})

	require.Equal(t, "v9.9-9.9.9", ResolveFirecrackerVersion(t.Context(), client, "v9.9-9.0.0"))
	require.Equal(t, "v8.8-8.8.8", ResolveFirecrackerVersion(t.Context(), client, "v8.8-8.8.8"))
	require.Equal(t, "v7.7-7.0.0", ResolveFirecrackerVersion(t.Context(), client, "v7.7-7.0.0"))
	require.Equal(t, "surely-not-a-version-xyz", ResolveFirecrackerVersion(t.Context(), client, "surely-not-a-version-xyz"))

	attrs := func(outcome, reason, key, version string) attribute.Set {
		return attribute.NewSet(
			attribute.String("outcome", outcome),
			attribute.String("reason", reason),
			attribute.String("key", key),
			attribute.String("version", version),
		)
	}
	want := []attribute.Set{
		attrs("resolved", "", "v9.9-9", "v9.9-9.9.9"),
		attrs("fallback", "key_absent", "v8.8-8", ""),
		attrs("fallback", "empty_value", "v7.7-7", ""),
		attrs("fallback", "parse_error", "", ""),
	}

	var rm metricdata.ResourceMetrics
	require.NoError(t, testMetricReader.Collect(t.Context(), &rm))

	found := make([]bool, len(want))
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != firecrackerVersionResolutionMetricName {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok)
			for _, dp := range sum.DataPoints {
				for i, w := range want {
					// Full-set equality: an added attribute fails the match.
					if dp.Attributes.Equals(&w) {
						found[i] = true
					}
				}
			}
		}
	}
	for i, w := range want {
		assert.True(t, found[i], "no datapoint with exactly %v", w.Encoded(attribute.DefaultEncoder()))
	}
}

func TestParseLogsReadConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		raw  string
		want bool
	}{
		{raw: "", want: false},
		{raw: "true", want: true},
		{raw: "1", want: true},
		{raw: " TRUE ", want: true},
		{raw: "false", want: false},
		{raw: "yes", want: false},
		{raw: "clickhouse", want: false},
	}

	for _, tt := range tests {
		t.Run("LOGS_READ_CONFIG="+tt.raw, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, parseLogsReadConfig(tt.raw))
		})
	}
}

func TestGetIsolatedSchedulingHosts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		value    ldvalue.Value
		expected map[string]struct{}
	}{
		{
			name:     "unset leaves scheduling unpartitioned",
			value:    ldvalue.Null(),
			expected: map[string]struct{}{},
		},
		{
			name:     "array of host ids",
			value:    ldvalue.FromJSONMarshal([]string{"node-a", "node-b"}),
			expected: map[string]struct{}{"node-a": {}, "node-b": {}},
		},
		{
			name:     "empty entries are dropped",
			value:    ldvalue.FromJSONMarshal([]string{"node-a", ""}),
			expected: map[string]struct{}{"node-a": {}},
		},
		{
			name:     "a non-array value reads as empty rather than stranding every sandbox",
			value:    ldvalue.FromJSONMarshal(map[string]bool{"node-a": true}),
			expected: map[string]struct{}{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			td := ldtestdata.DataSource()
			td.Update(td.Flag(IsolatedSchedulingHostsFlag.Key()).ValueForAll(tt.value))

			client, err := NewClientWithDatasource(td)
			require.NoError(t, err)
			t.Cleanup(func() {
				require.NoError(t, client.Close(context.WithoutCancel(t.Context())))
			})

			assert.Equal(t, tt.expected, GetIsolatedSchedulingHosts(t.Context(), client))
		})
	}
}
