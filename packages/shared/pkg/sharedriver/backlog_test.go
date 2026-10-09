package sharedriver

import (
	"maps"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

type observedBacklog map[string][]metricdata.DataPoint[float64]

func observeBacklog(t *testing.T, config BacklogConfig, snapshot *backlogSnapshot, startedAgo time.Duration) observedBacklog {
	t.Helper()

	reader := sdkmetric.NewManualReader()
	backlog, err := NewBacklog(nil, telemetryWith(nil, sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))), nil, config)
	require.NoError(t, err)
	backlog.startedAt = time.Now().Add(-startedAgo)
	if snapshot != nil {
		backlog.snapshot = *snapshot
	}

	var collected metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &collected))
	observed := observedBacklog{}
	for _, scope := range collected.ScopeMetrics {
		if scope.Scope.Name != instrumentationScope {
			continue
		}
		for _, recorded := range scope.Metrics {
			switch gauge := recorded.Data.(type) {
			case metricdata.Gauge[int64]:
				for _, point := range gauge.DataPoints {
					observed[recorded.Name] = append(observed[recorded.Name], metricdata.DataPoint[float64]{
						Attributes: point.Attributes, Value: float64(point.Value),
					})
				}
			case metricdata.Gauge[float64]:
				observed[recorded.Name] = append(observed[recorded.Name], gauge.DataPoints...)
			}
		}
	}

	return observed
}

func (o observedBacklog) value(t *testing.T, name string, attributes map[string]string) float64 {
	t.Helper()

	for _, point := range o[name] {
		matched := true
		for key, want := range attributes {
			if metricAttribute(point.Attributes, key) != want {
				matched = false
			}
		}
		if matched && point.Attributes.Len() == len(attributes) {
			return point.Value
		}
	}

	require.Failf(t, "series not found", "%s %v", name, attributes)

	return 0
}

func TestEveryWorkedKindReportsAtZeroInEveryQueueClassAndState(t *testing.T) {
	t.Parallel()

	observed := observeBacklog(t, BacklogConfig{
		Kinds:        []string{"apply_project_limits", "purge_user"},
		QueueClasses: []string{"default", "control_plane"},
	}, nil, 0)

	assert.Len(t, observed["outbox.jobs"], 2*2*len(ActiveJobStates))
	assert.Len(t, observed["outbox.oldest_available_age"], 2*2)
	assert.Len(t, observed["outbox.oldest_running_age"], 2*2)
	assert.Len(t, observed["outbox.discarded"], 2)
	for _, series := range []string{"outbox.jobs", "outbox.oldest_available_age", "outbox.oldest_running_age", "outbox.discarded"} {
		for _, point := range observed[series] {
			assert.Zerof(t, point.Value, "%s %v", series, point.Attributes)
		}
	}
}

func TestTheBacklogReportsItsSnapshot(t *testing.T) {
	t.Parallel()

	readyAt, runningAt := time.Now().Add(-time.Minute), time.Now().Add(-2*time.Minute)
	observed := observeBacklog(t, BacklogConfig{
		Kinds:        []string{"apply_project_limits", "purge_user"},
		QueueClasses: []string{"default", "control_plane"},
	}, &backlogSnapshot{
		collectedAt: time.Now(),
		series: map[backlogKey]backlogSeries{
			{kind: "apply_project_limits", queueClass: "control_plane", state: "available"}: {jobs: 2, oldestReady: &readyAt},
			{kind: "apply_project_limits", queueClass: "control_plane", state: "running"}:   {jobs: 1, oldestRunning: &runningAt},
			{kind: "purge_user", queueClass: "default", state: "retryable"}:                 {jobs: 3},
		},
		discarded: map[string]int64{"purge_user": 4, "retired_kind": 1},
	}, 0)

	limits := map[string]string{"job.kind": "apply_project_limits", "queue.class": "control_plane"}
	assert.InDelta(t, 2, observed.value(t, "outbox.jobs", with(limits, "state", "available")), 0)
	assert.InDelta(t, 1, observed.value(t, "outbox.jobs", with(limits, "state", "running")), 0)
	assert.InDelta(t, 3, observed.value(t, "outbox.jobs", map[string]string{"job.kind": "purge_user", "queue.class": "default", "state": "retryable"}), 0)
	assert.InDelta(t, 60, observed.value(t, "outbox.oldest_available_age", limits), 5)
	assert.InDelta(t, 120, observed.value(t, "outbox.oldest_running_age", limits), 5)
	assert.InDelta(t, 4, observed.value(t, "outbox.discarded", map[string]string{"job.kind": "purge_user"}), 0)
	assert.InDelta(t, 1, observed.value(t, "outbox.discarded", map[string]string{"job.kind": "retired_kind"}), 0,
		"a kind no longer worked is reported while it still has jobs")
	assert.InDelta(t, 0, observed.value(t, "outbox.snapshot_age", map[string]string{}), 5)
}

func TestTheSnapshotAgeCountsFromStartBeforeTheFirstRead(t *testing.T) {
	t.Parallel()

	observed := observeBacklog(t, BacklogConfig{Kinds: []string{"meter_usage"}}, nil, time.Minute)

	assert.GreaterOrEqual(t, observed.value(t, "outbox.snapshot_age", map[string]string{}), 59.0)
	assert.InDelta(t, 0, observed.value(t, "outbox.jobs", map[string]string{"job.kind": "meter_usage", "queue.class": "default", "state": "available"}), 0,
		"a service with no queue classes reports its queues as default")
}

func with(attributes map[string]string, key, value string) map[string]string {
	extended := map[string]string{key: value}
	maps.Copy(extended, attributes)

	return extended
}
