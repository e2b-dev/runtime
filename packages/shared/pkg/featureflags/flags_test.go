package featureflags

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"sync/atomic"
	"testing"

	"github.com/launchdarkly/go-sdk-common/v3/ldcontext"
	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/launchdarkly/go-server-sdk/v7/testhelpers/ldtestdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

var malformedTestRun atomic.Int64

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

	_, err := FirecrackerVersionsFallback(FirecrackerVersionMap, "")
	require.NoError(t, err)
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

func TestGetExtraCompatibleNodeModels(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		value    ldvalue.Value
		expected map[string][]string
	}{
		{"unset adds nothing", ldvalue.Null(), nil},
		{
			"build model to node models",
			ldvalue.FromJSONMarshal(map[string][]string{"106": {"173"}}),
			map[string][]string{"106": {"173"}},
		},
		{"a non-object value adds nothing", ldvalue.FromJSONMarshal([]string{"173"}), nil},
		{"non-array entries add nothing", ldvalue.FromJSONMarshal(map[string]string{"106": "173"}), nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			td := ldtestdata.DataSource()
			td.Update(td.Flag(ExtraCompatibleNodeModelsFlag.Key()).ValueForAll(tt.value))

			client, err := NewClientWithDatasource(td)
			require.NoError(t, err)
			t.Cleanup(func() {
				require.NoError(t, client.Close(context.WithoutCancel(t.Context())))
			})

			assert.Equal(t, tt.expected, GetExtraCompatibleNodeModels(t.Context(), client))
		})
	}
}

// A malformed value is logged once per distinct value, not on every
// create, and reads as empty.
//
//nolint:paralleltest // replaces the global logger
func TestGetExtraCompatibleNodeModels_LogsMalformedValueOnce(t *testing.T) {
	core, logs := observer.New(zap.DebugLevel)
	t.Cleanup(logger.ReplaceGlobals(t.Context(), logger.NewTracedLoggerFromCore(core)))

	// The dedupe set is process-wide, so a per-run suffix keeps values unique
	// across -count runs.
	suffix := fmt.Sprintf("%s-%d", t.Name(), malformedTestRun.Add(1))
	read := func(value map[string]string) map[string][]string {
		td := ldtestdata.DataSource()
		td.Update(td.Flag(ExtraCompatibleNodeModelsFlag.Key()).ValueForAll(ldvalue.FromJSONMarshal(value)))

		client, err := NewClientWithDatasource(td)
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, client.Close(context.WithoutCancel(t.Context())))
		})

		return GetExtraCompatibleNodeModels(t.Context(), client, ldcontext.NewWithKind("cluster", "c1"))
	}
	logged := func() []observer.LoggedEntry {
		return logs.FilterMessage("malformed feature flag value, ignoring").All()
	}

	single := map[string]string{"106": "single-" + suffix}
	assert.Nil(t, read(single))
	assert.Nil(t, read(single))

	entries := logged()
	require.Len(t, entries, 1)
	assert.Equal(t, zap.WarnLevel, entries[0].Level)
	assert.Equal(t, ExtraCompatibleNodeModelsFlag.Key(), entries[0].ContextMap()["flag"])
	assert.JSONEq(t, `{"106":"single-`+suffix+`"}`, entries[0].ContextMap()["value"].(string))
	assert.Equal(t, []any{"cluster:c1"}, entries[0].ContextMap()["contexts"])

	// A different malformed value logs again.
	assert.Nil(t, read(map[string]string{"106": "other-" + suffix}))
	assert.Len(t, logged(), 2)

	// Object key order is random in the serialized value, so a multi-key
	// object must still log once.
	multi := map[string]string{}
	for i := range 8 {
		multi[fmt.Sprintf("k%d", i)] = "multi-" + suffix
	}
	for range 50 {
		assert.Nil(t, read(multi))
	}
	assert.Len(t, logged(), 3)
}

func TestFirecrackerVersionsFallback_MergesOverridesOverBase(t *testing.T) {
	t.Parallel()

	base := map[string]string{
		"v1.14":   "v1.14.1_431f1fc",
		"v1.14-0": "v1.14-0.2.0",
	}

	merged, err := FirecrackerVersionsFallback(base, "")
	require.NoError(t, err)
	assert.Equal(t, base, merged)

	merged, err = FirecrackerVersionsFallback(base, `{"v1.14-0":"v1.14-0.3.1","v1.14-1":"v1.14-1.0.0"}`)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{
		"v1.14":   "v1.14.1_431f1fc",
		"v1.14-0": "v1.14-0.3.1",
		"v1.14-1": "v1.14-1.0.0",
	}, merged)
	// The baked map is never mutated by an override.
	assert.Equal(t, "v1.14-0.2.0", base["v1.14-0"])
}

func TestFirecrackerVersionsFallback_RejectsInvalidOverrides(t *testing.T) {
	t.Parallel()

	cases := map[string]string{
		"not json":            `v1.14-0.3.1`,
		"not an object":       `["v1.14-0.3.1"]`,
		"non-string value":    `{"v1.14-0": 1}`,
		"empty value":         `{"v1.14-0": ""}`,
		"unparseable version": `{"v1.14-0": "v1.14-0.3"}`,
	}

	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			merged, err := FirecrackerVersionsFallback(FirecrackerVersionMap, raw)
			require.Error(t, err)
			assert.Contains(t, err.Error(), FirecrackerVersionsEnv)
			// The baked map survives a rejected override.
			assert.Equal(t, FirecrackerVersionMap, merged)
		})
	}

	t.Run("bad base entry", func(t *testing.T) {
		t.Parallel()

		_, err := FirecrackerVersionsFallback(map[string]string{"v1.14-0": "v1.14-1.0.0"}, "")
		require.ErrorContains(t, err, `does not belong to release line "v1.14-0"`)
	})
}

// The env is read at package init, so the wiring is exercised in a child test process.
func TestFirecrackerVersionsEnv_Wiring(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name, raw, helper string
	}{
		{"valid override reaches the fallback and the resolver", `{"v1.14":"v1.14-0.3.1","v1.14-0":"v1.14-0.3.1"}`, "TestFirecrackerVersionsEnvHelper_Valid"},
		{"invalid override keeps the baked map and fails NewClient", `{"v1.14-0":"v1.14-0.3"}`, "TestFirecrackerVersionsEnvHelper_Invalid"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^"+tc.helper+"$", "-test.v")
			cmd.Env = append(os.Environ(), "FIRECRACKER_VERSIONS_ENV_HELPER=1", "LAUNCH_DARKLY_API_KEY=", FirecrackerVersionsEnv+"="+tc.raw)
			out, err := cmd.CombinedOutput()
			require.NoError(t, err, string(out))
			assert.Contains(t, string(out), "--- PASS: "+tc.helper, string(out))
		})
	}
}

func TestFirecrackerVersionsEnvHelper_Valid(t *testing.T) {
	t.Parallel()

	if os.Getenv("FIRECRACKER_VERSIONS_ENV_HELPER") == "" {
		t.Skip("child process of TestFirecrackerVersionsEnv_Wiring")
	}

	require.NoError(t, firecrackerVersionsEnvErr)
	assert.Equal(t, "v1.14-0.3.1", FirecrackerVersions.Fallback().GetByKey("v1.14-0").StringValue())
	assert.Equal(t, "v1.14-0.3.1", FirecrackerVersions.Fallback().GetByKey("v1.14").StringValue())
	assert.Equal(t, DefaultFirecrackerV1_12Version, FirecrackerVersions.Fallback().GetByKey("v1.12").StringValue())

	client, err := NewClient("test", "test", "test")
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, client.Close(context.WithoutCancel(t.Context())))
	})

	assert.Equal(t, "v1.14-0.3.1", ResolveFirecrackerVersion(t.Context(), client, "v1.14-0.2.0"))
	assert.Equal(t, "v1.14-0.3.1", ResolveFirecrackerVersion(t.Context(), client, "v1.14.1_431f1fc"))
	assert.Equal(t, DefaultFirecrackerV1_12Version, ResolveFirecrackerVersion(t.Context(), client, "v1.12.1_210cbac"))
}

func TestFirecrackerVersionsEnvHelper_Invalid(t *testing.T) {
	t.Parallel()

	if os.Getenv("FIRECRACKER_VERSIONS_ENV_HELPER") == "" {
		t.Skip("child process of TestFirecrackerVersionsEnv_Wiring")
	}

	require.ErrorContains(t, firecrackerVersionsEnvErr, `DEFAULT_FIRECRACKER_VERSIONS["v1.14-0"]`)
	assert.Equal(t, DefaultFirecrackerV1_14_0Version, FirecrackerVersions.Fallback().GetByKey("v1.14-0").StringValue())

	_, err := NewClient("test", "test", "test")
	require.ErrorContains(t, err, `DEFAULT_FIRECRACKER_VERSIONS["v1.14-0"]`)
}

// One override can move every line, legacy ones included, onto a single release, as a security fix needs.
func TestResolveFirecrackerVersion_OverrideMovesEveryLineOntoOneRelease(t *testing.T) {
	t.Parallel()

	versions, err := FirecrackerVersionsFallback(FirecrackerVersionMap, `{"v1.10":"v1.14-0.3.1","v1.12":"v1.14-0.3.1","v1.14":"v1.14-0.3.1","v1.14-0":"v1.14-0.3.1"}`)
	require.NoError(t, err)

	client := newResolverTestClient(t, versions)

	for _, stored := range []string{"v1.10.1_30cbb07", "v1.12.1_210cbac", "v1.14.1_431f1fc", "v1.14-0.1.0", "v1.14-0.2.0", "v1.14-0.3.1"} {
		assert.Equal(t, "v1.14-0.3.1", ResolveFirecrackerVersion(t.Context(), client, stored), stored)
	}
	// A line the override does not name keeps its baked default.
	assert.Equal(t, "v1.14-1.0.0", ResolveFirecrackerVersion(t.Context(), client, "v1.14-1.0.0"))
}

func TestResolveFirecrackerVersion_OverriddenLineServesNewRelease(t *testing.T) {
	t.Parallel()

	versions, err := FirecrackerVersionsFallback(map[string]string{
		"v1.14":   "v1.14.1_431f1fc",
		"v1.14-0": "v1.14-0.2.0",
	}, `{"v1.14-0":"v1.14-0.3.1"}`)
	require.NoError(t, err)

	client := newResolverTestClient(t, versions)

	// Templates built before and after the override both serve the override; the legacy line is untouched.
	assert.Equal(t, "v1.14-0.3.1", ResolveFirecrackerVersion(t.Context(), client, "v1.14-0.2.0"))
	assert.Equal(t, "v1.14-0.3.1", ResolveFirecrackerVersion(t.Context(), client, "v1.14-0.3.1"))
	assert.Equal(t, "v1.14.1_431f1fc", ResolveFirecrackerVersion(t.Context(), client, "v1.14.1_431f1fc"))
}

func TestParseExtraCompatibleNodeModels(t *testing.T) {
	t.Parallel()

	t.Run("empty is null without error", func(t *testing.T) {
		t.Parallel()
		v, err := parseExtraCompatibleNodeModels("")
		require.NoError(t, err)
		assert.True(t, v.IsNull())
	})

	t.Run("whitespace is null without error", func(t *testing.T) {
		t.Parallel()
		v, err := parseExtraCompatibleNodeModels("  \n ")
		require.NoError(t, err)
		assert.True(t, v.IsNull())
	})

	t.Run("object parses to the flag value", func(t *testing.T) {
		t.Parallel()
		v, err := parseExtraCompatibleNodeModels(`{"106":["173"]}`)
		require.NoError(t, err)

		td := ldtestdata.DataSource()
		td.Update(td.Flag(ExtraCompatibleNodeModelsFlag.Key()).ValueForAll(v))
		client, err := NewClientWithDatasource(td)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, client.Close(context.WithoutCancel(t.Context()))) })

		assert.Equal(t, map[string][]string{"106": {"173"}}, GetExtraCompatibleNodeModels(t.Context(), client))
	})

	t.Run("malformed JSON errors and is null", func(t *testing.T) {
		t.Parallel()
		v, err := parseExtraCompatibleNodeModels(`{"106":`)
		require.Error(t, err)
		assert.Contains(t, err.Error(), ExtraCompatibleNodeModelsEnv)
		assert.True(t, v.IsNull())
	})
}