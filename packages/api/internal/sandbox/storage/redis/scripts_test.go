package redis

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	redis_utils "github.com/e2b-dev/infra/packages/shared/pkg/redis"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// Every script run is counted under its name and outcome and timed under its
// name. A nil reply, the remove of a key that is already gone, is a result
// and counts as ok; only a run the server refused counts as an error.
func TestRunScript_RecordsCallsAndDurationByScript(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.WithoutCancel(t.Context()))) })
	metrics, err := newScriptMetrics(provider.Meter(meterScope))
	require.NoError(t, err)

	key := getSandboxKey("team", "sbx")
	teamKey := GetSandboxStorageTeamIndexKey("team")
	goneKey := getSandboxGoneKey("team", "sbx")

	require.NoError(t, metrics.run(t.Context(), client, "add_sandbox", addSandboxScript, []string{key, goneKey, teamKey}, "{}", "sbx", "exec", "0", 60).Err())

	deleted, raw, _, err := parseRemoveReply(metrics.run(t.Context(), client, "remove_sandbox", removeSandboxScript, []string{key, goneKey, teamKey}, "sbx", "", 60))
	require.NoError(t, err)
	require.True(t, deleted)
	require.Equal(t, "{}", raw)

	err = metrics.run(t.Context(), client, "remove_sandbox", removeSandboxScript, []string{key, goneKey, teamKey}, "sbx", "", 60).Err()
	require.ErrorIs(t, err, redis.Nil)

	failing := redis.NewScript(`return redis.call('NOSUCHCOMMAND')`)
	err = metrics.run(t.Context(), client, "failing", failing, nil).Err()
	require.Error(t, err)
	require.NotErrorIs(t, err, redis.Nil)

	var collected metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &collected))

	calls := counterPoints(t, collected, string(telemetry.ApiRedisScriptCalls))
	require.Len(t, calls, 3)
	require.Equal(t, int64(1), calls[scriptAttrs("add_sandbox", scriptOutcomeOK)])
	require.Equal(t, int64(2), calls[scriptAttrs("remove_sandbox", scriptOutcomeOK)])
	require.Equal(t, int64(1), calls[scriptAttrs("failing", scriptOutcomeError)])

	durations := histogramCounts(t, collected, string(telemetry.ApiRedisScriptDuration))
	require.Len(t, durations, 3)
	require.Equal(t, uint64(1), durations[durationAttrs("add_sandbox")])
	require.Equal(t, uint64(2), durations[durationAttrs("remove_sandbox")])
	require.Equal(t, uint64(1), durations[durationAttrs("failing")])
}

// With slot assertions enabled, RunScript panics on keys that span hash
// slots, naming the script and the keys, and runs keys that share a tag.
func TestRunScriptAssertsSameSlot(t *testing.T) {
	t.Parallel()

	client := redis_utils.SetupInstance(t)
	script := redis.NewScript(`return 1`)

	recovered := func() (value any) {
		defer func() { value = recover() }()
		RunScript(t.Context(), client, "spans_slots", script, []string{"{a}:1", "{b}:2"})

		return nil
	}()
	require.NotNil(t, recovered, "RunScript accepted keys in two hash slots")
	assert.Contains(t, recovered, "spans_slots")
	assert.Contains(t, recovered, "{b}:2")

	var cmd *redis.Cmd
	require.NotPanics(t, func() {
		cmd = RunScript(t.Context(), client, "one_slot", script, []string{"{a}:1", "{a}:2"})
	})
	require.NoError(t, cmd.Err())
	assert.Equal(t, int64(1), cmd.Val())
}

// An update's fenced record write is counted and timed like every other
// script run.
//
//nolint:paralleltest // replaces the package's script metrics
func TestUpdate_RecordsTheFencedWriteAsAScriptRun(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.WithoutCancel(t.Context()))) })
	metrics, err := newScriptMetrics(provider.Meter(meterScope))
	require.NoError(t, err)

	previous := defaultScriptMetrics
	defaultScriptMetrics = metrics
	t.Cleanup(func() { defaultScriptMetrics = previous })

	// Not started: the update needs no notifications, and a started storage here made the parallel pub/sub tests flakier.
	storage := newTestStorage(t, redis_utils.SetupInstance(t))
	sbx := createTestSandbox("sbx-fenced-metric")
	require.NoError(t, storage.Add(t.Context(), sbx))

	updated, err := storage.Update(t.Context(), sbx.TeamID, sbx.SandboxID, updateTemplateID("after"))
	require.NoError(t, err)
	require.Equal(t, "after", updated.TemplateID)

	var collected metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &collected))

	calls := counterPoints(t, collected, string(telemetry.ApiRedisScriptCalls))
	require.Equal(t, int64(1), calls[scriptAttrs("add_sandbox", scriptOutcomeOK)])
	require.Equal(t, int64(1), calls[scriptAttrs("fenced_set", scriptOutcomeOK)])

	durations := histogramCounts(t, collected, string(telemetry.ApiRedisScriptDuration))
	require.Equal(t, uint64(1), durations[durationAttrs("fenced_set")])
}

func scriptAttrs(script, outcome string) attribute.Distinct {
	set := attribute.NewSet(attribute.String(scriptAttr, script), attribute.String(scriptOutcomeAttr, outcome))

	return set.Equivalent()
}

func durationAttrs(script string) attribute.Distinct {
	set := attribute.NewSet(attribute.String(scriptAttr, script))

	return set.Equivalent()
}

func counterPoints(t *testing.T, collected metricdata.ResourceMetrics, name string) map[attribute.Distinct]int64 {
	t.Helper()

	points := map[attribute.Distinct]int64{}
	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			require.True(t, ok)
			for _, point := range sum.DataPoints {
				points[point.Attributes.Equivalent()] = point.Value
			}
		}
	}

	return points
}

func histogramCounts(t *testing.T, collected metricdata.ResourceMetrics, name string) map[attribute.Distinct]uint64 {
	t.Helper()

	counts := map[attribute.Distinct]uint64{}
	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != name {
				continue
			}
			hist, ok := m.Data.(metricdata.Histogram[float64])
			require.True(t, ok)
			for _, point := range hist.DataPoints {
				counts[point.Attributes.Equivalent()] = point.Count
			}
		}
	}

	return counts
}
