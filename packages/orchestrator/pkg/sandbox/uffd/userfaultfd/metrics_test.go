//go:build linux

package userfaultfd

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

func BenchmarkBuildServeNewAttrs(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		attrs := buildServeNewAttrs()
		if attrs[0][0][0][0] == nil {
			b.Fatal("missing source attributes")
		}
	}
}

// TestServeMetric exercises the serve-stats recording exactly as the Serve
// worker does — serveTimer.Begin() + RecordRaw(ctx, bytes, serveAttrs[mode][bucket][class][result])
// — and asserts the resulting orchestrator.sandbox.uffd.serve datapoints carry
// the right page_class / result attributes, fault counts and faulted bytes.
//
// The real serve loop runs in a forked child process (TestHelperServingProcess),
// so its metrics can't be collected from the parent; this test pins the metric
// definition and the page_class/result attribute table in-process instead. It
// is intentionally NOT parallel: it swaps the package-level serveTimer.
//
//nolint:paralleltest // swaps the package-level serveTimer
func TestServeMetric(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	prev := serveTimer
	serveTimer = utils.Must(telemetry.NewTimerFactory(
		mp.Meter("github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd/userfaultfd"),
		serveMetricName,
		"Time to serve a UFFD page fault",
		"Bytes faulted into the guest",
		"UFFD faults served",
	))
	t.Cleanup(func() { serveTimer = prev })

	ctx := t.Context()
	pageSize := int64(header.PageSize)

	// Record a mix of faults the way the worker would. All under one
	// generation bucket — the per-bucket split is pinned separately by
	// TestServeMetric_GenerationBucket.
	var u Userfaultfd
	u.genBucket = genBucket1To10
	record := func(class pageClass, result faultResult, bytes int64, n int) {
		for range n {
			sw := serveTimer.Begin()
			src := storage.UnknownSource
			if class == pageClassNew && result == faultResultInstalled {
				src = storage.SourceGCS
			}
			sw.RecordRaw(ctx, bytes, u.serveOption(class, result, src))
		}
	}
	record(pageClassNew, faultResultInstalled, pageSize, 3)
	record(pageClassZero, faultResultInstalled, pageSize, 1)
	record(pageClassResident, faultResultPresent, 0, 2)
	record(pageClassNew, faultResultPresent, 0, 1)
	record(pageClassNew, faultResultDeferred, 0, 1)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))

	counts, bytesSum := collectTriplet(t, rm, serveMetricName)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			data, ok := m.Data.(metricdata.Histogram[int64])
			if m.Name != serveMetricName || !ok {
				continue
			}
			for _, dp := range data.DataPoints {
				class, _ := dp.Attributes.Value("page_class")
				source, hasSource := dp.Attributes.Value("source")
				if class.AsString() == "new" {
					require.True(t, hasSource, "new-page faults need a source label")
					if result, _ := dp.Attributes.Value("result"); result.AsString() == "installed" {
						assert.Equal(t, "gcs", source.AsString())
					}
				} else {
					assert.False(t, hasSource, "zero/resident pages read no backend")
				}
			}
		}
	}

	// new+installed: 3 faults, each one page of faulted bytes.
	assert.Equal(t, int64(3), counts[key("new", "installed")], "new+installed fault count")
	assert.Equal(t, 3*pageSize, bytesSum[key("new", "installed")], "new+installed faulted bytes")

	// zero+installed: 1 fault, one page installed (zeros) → counted as faulted.
	assert.Equal(t, int64(1), counts[key("zero", "installed")])
	assert.Equal(t, pageSize, bytesSum[key("zero", "installed")])

	// resident short-circuit: counted as "present", nothing copied.
	assert.Equal(t, int64(2), counts[key("resident", "present")])
	assert.Equal(t, int64(0), bytesSum[key("resident", "present")])

	// lost install race (EEXIST): counted as "present", no bytes attributed
	// to this serve — keeps sum(bytes) == pages actually copied by serves.
	assert.Equal(t, int64(1), counts[key("new", "present")])
	assert.Equal(t, int64(0), bytesSum[key("new", "present")])

	// deferred: counted, no bytes faulted.
	assert.Equal(t, int64(1), counts[key("new", "deferred")])
	assert.Equal(t, int64(0), bytesSum[key("new", "deferred")])
}

//nolint:paralleltest // swaps the package-level serve instruments
func TestDroppedFaultsOnlyCount(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	m := mp.Meter("github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd/userfaultfd")

	prevTimer, prevCount := serveTimer, serveCount
	serveTimer = utils.Must(telemetry.NewTimerFactory(
		m,
		serveMetricName,
		"Time to serve a UFFD demand page fault",
		"Bytes installed into the guest by demand-fault serves",
		"UFFD demand-fault serve attempts",
	))
	serveCount = utils.Must(m.Int64Counter(serveMetricName, metric.WithDescription("UFFD demand-fault serve attempts")))
	t.Cleanup(func() {
		serveTimer, serveCount = prevTimer, prevCount
	})

	ctx := t.Context()
	pageSize := int64(header.PageSize)
	serveTimer.Begin().RecordRaw(ctx, pageSize, serveAttrs[BalloonModeUnknown][genBucket0][pageClassNew][faultResultInstalled])

	var u Userfaultfd
	for range 63 {
		u.recordDropped(ctx, pageClassNew, faultResultDeduped)
	}
	u.recordDropped(ctx, pageClassResident, faultResultPresent)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))

	histogramCounts, bytesSum := collectTriplet(t, rm, serveMetricName)
	assert.Equal(t, map[string]int64{key("new", "installed"): 1}, histogramCounts,
		"dropped faults must not dilute latency histograms")
	assert.Equal(t, map[string]int64{key("new", "installed"): pageSize}, bytesSum,
		"dropped faults install no bytes")

	counts := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			data, ok := m.Data.(metricdata.Sum[int64])
			if m.Name != serveMetricName || m.Unit != "" || !ok {
				continue
			}
			for _, dp := range data.DataPoints {
				counts[attrKey(t, dp.Attributes)] += dp.Value
			}
		}
	}
	assert.Equal(t, map[string]int64{
		key("new", "installed"):    1,
		key("new", "deduped"):      63,
		key("resident", "present"): 1,
	}, counts, "served and dropped faults share the attempt counter")
	assert.Equal(t, int64(63), u.ServeStats().Deduped)
	assert.Equal(t, int64(1), u.ServeStats().Pages)
}

// TestPrefaultMetric exercises the prefault recording exactly as Prefault does
// — prefaultTimer.Begin() + RecordRaw(ctx, bytes, prefaultAttrs[result]) — and
// asserts the orchestrator.sandbox.uffd.prefault datapoints carry the right
// result attribute, attempt counts and installed bytes. Like TestServeMetric,
// the full Prefault path needs a real userfaultfd (root-only, forked harness),
// so this pins the metric definition and attrs table in-process instead. NOT
// parallel: it swaps the package-level prefaultTimer.
//
//nolint:paralleltest // swaps the package-level prefaultTimer
func TestPrefaultMetric(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	prev := prefaultTimer
	prefaultTimer = utils.Must(telemetry.NewTimerFactory(
		mp.Meter("github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd/userfaultfd"),
		prefaultMetricName,
		"Time to prefault a page into the guest",
		"Bytes installed into the guest by prefaults",
		"UFFD prefault attempts",
	))
	t.Cleanup(func() { prefaultTimer = prev })

	ctx := t.Context()
	pageSize := int64(header.PageSize)

	record := func(result faultResult, bytes int64, n int) {
		for range n {
			sw := prefaultTimer.Begin()
			sw.RecordRaw(ctx, bytes, prefaultAttrs[genBucket0][result])
		}
	}
	record(faultResultInstalled, pageSize, 3)
	record(faultResultPresent, 0, 1)
	record(faultResultSkipped, 0, 2)
	record(faultResultDeferred, 0, 1)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))

	counts, bytesSum := collectTriplet(t, rm, prefaultMetricName)

	// installed: 3 prefaults, one page of installed bytes each.
	assert.Equal(t, int64(3), counts["installed"])
	assert.Equal(t, 3*pageSize, bytesSum["installed"])

	// lost install race (EEXIST): counted, bytes belong to the winning serve.
	assert.Equal(t, int64(1), counts["present"])
	assert.Equal(t, int64(0), bytesSum["present"])

	// tracker said Dirty/Zero: prefetch arrived too late, nothing copied.
	assert.Equal(t, int64(2), counts["skipped"])
	assert.Equal(t, int64(0), bytesSum["skipped"])

	// EAGAIN: the prefetcher does not retry.
	assert.Equal(t, int64(1), counts["deferred"])
	assert.Equal(t, int64(0), bytesSum["deferred"])
}

// TestServeStats folds the same mix of finished serve attempts that
// TestServeMetric records and asserts the cumulative ServeStats() snapshot —
// the point-in-time count the startup metric samples at the envd-init boundary.
// Only resolved faults (installed or already-present) count as a needed page;
// deferred and errored attempts do not, so a deferred fault is not
// double-counted when it is later re-served and resolves.
func TestServeStats(t *testing.T) {
	t.Parallel()

	pageSize := int64(header.PageSize)

	var u Userfaultfd
	// Each attempt takes 1ms more than the class index, so the serve-time sums
	// show which attempts were counted.
	fold := func(class pageClass, result faultResult, bytes int64, n int) {
		for range n {
			u.recordServeStats(class, result, bytes, time.Duration(class+1)*time.Millisecond, storage.SourceMmap)
		}
	}
	fold(pageClassNew, faultResultInstalled, pageSize, 3)  // +3 pages, +3 source, +3 pages of bytes
	fold(pageClassZero, faultResultInstalled, pageSize, 1) // +1 page, +1 page of bytes (not source)
	fold(pageClassResident, faultResultPresent, 0, 2)      // +2 pages, no bytes
	fold(pageClassNew, faultResultPresent, 0, 1)           // +1 page (lost race), no bytes/source
	fold(pageClassNew, faultResultDeferred, 0, 1)          // not counted (re-served later)
	fold(pageClassNew, faultResultError, 0, 1)             // not counted (never resolved)

	stats := u.ServeStats()
	assert.Equal(t, int64(7), stats.Pages, "resolved demand faults (installed+present)")
	assert.Equal(t, int64(3), stats.SourcePages, "subset installed from the source")
	assert.Equal(t, 4*pageSize, stats.Bytes, "bytes installed (new+zero), present/deferred/error contribute none")
	// new 1ms x4 (3 installed + 1 present), zero 2ms x1, resident 3ms x2; deferred and error add none.
	assert.Equal(t, 12*time.Millisecond, stats.ServeTime, "serve time of resolved faults only")
	assert.Equal(t, 3*time.Millisecond, stats.SourceServeTime, "serve time of the faults installed from the source")
	// resident faults took 3ms each; deferred and error attempts never count.
	assert.Equal(t, 3*time.Millisecond, stats.MaxServe, "longest resolved serve")
	assert.Equal(t, int64(3), stats.BySource[storage.SourceMmap], "the source pages, by backend")
	assert.Equal(t, 3*time.Millisecond, stats.ServeTimeBySource[storage.SourceMmap], "their serve time, by backend")
}

// Sub gives what was served between two snapshots, field by field.
func TestServeSnapshotSub(t *testing.T) {
	t.Parallel()

	var u Userfaultfd
	u.recordServeStats(pageClassNew, faultResultInstalled, 4096, 5*time.Millisecond, storage.SourceMmap)
	u.recordServeStats(pageClassNew, faultResultDeduped, 0, 0, storage.UnknownSource)
	before := u.ServeStats()
	u.recordServeStats(pageClassNew, faultResultInstalled, 4096, 7*time.Millisecond, storage.SourceGCS)
	u.recordServeStats(pageClassZero, faultResultInstalled, 4096, 2*time.Millisecond, storage.UnknownSource)
	u.recordServeStats(pageClassNew, faultResultDeferred, 0, time.Millisecond, storage.UnknownSource)
	u.recordServeStats(pageClassNew, faultResultDeduped, 0, 0, storage.UnknownSource)
	u.recordServeStats(pageClassNew, faultResultDeduped, 0, 0, storage.UnknownSource)

	d := u.ServeStats().Sub(before)
	assert.Equal(t, ServeSnapshot{
		Pages:           2,
		SourcePages:     1,
		Bytes:           8192,
		Deferred:        1,
		Deduped:         2,
		ServeTime:       9 * time.Millisecond,
		SourceServeTime: 7 * time.Millisecond,
		BySource:        bySource(storage.SourceGCS, 1),
		ServeTimeBySource: func() (t [storage.NumSources]time.Duration) {
			t[storage.SourceGCS] = 7 * time.Millisecond

			return t
		}(),
	}, d)
}

func bySource(src storage.Source, n int64) (t [storage.NumSources]int64) {
	t[src] = n

	return t
}

// bucketForGeneration pins the log-scale bucket edges: 0 is its own bucket
// (fresh template), then 1-10 / 11-100 / 101-1000 / >1000.
func TestBucketForGeneration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		generation uint64
		want       generationBucket
	}{
		{0, genBucket0},
		{1, genBucket1To10},
		{10, genBucket1To10},
		{11, genBucket11To100},
		{100, genBucket11To100},
		{101, genBucket101To1000},
		{1000, genBucket101To1000},
		{1001, genBucketOver1000},
		{2314, genBucketOver1000}, // prod p99 at time of writing
	}

	for _, tt := range tests {
		require.Equal(t, tt.want, bucketForGeneration(tt.generation), "generation %d", tt.generation)
	}
}

// Serve datapoints recorded under different generation buckets land in
// distinct series, tagged with the right generation_bucket label value.
//
//nolint:paralleltest // swaps the package-level serveTimer
func TestServeMetric_GenerationBucket(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	prev := serveTimer
	serveTimer = utils.Must(telemetry.NewTimerFactory(
		mp.Meter("github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd/userfaultfd"),
		serveMetricName,
		"Time to serve a UFFD page fault",
		"Bytes faulted into the guest",
		"UFFD faults served",
	))
	t.Cleanup(func() { serveTimer = prev })

	ctx := t.Context()

	record := func(bucket generationBucket, n int) {
		for range n {
			sw := serveTimer.Begin()
			sw.RecordRaw(ctx, int64(header.PageSize), serveAttrs[BalloonModeUnknown][bucket][pageClassNew][faultResultInstalled])
		}
	}
	record(genBucket0, 1)
	record(genBucket11To100, 2)
	record(genBucketOver1000, 3)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))

	// Count datapoints by generation_bucket label value.
	counts := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			hist, ok := m.Data.(metricdata.Histogram[int64])
			if m.Name != serveMetricName || !ok {
				continue
			}
			for _, dp := range hist.DataPoints {
				g, ok := dp.Attributes.Value("generation_bucket")
				require.True(t, ok, "datapoint missing generation_bucket attribute")
				counts[g.AsString()] += int64(dp.Count)
			}
		}
	}

	assert.Equal(t, map[string]int64{"0": 1, "11-100": 2, ">1000": 3}, counts)
}

func key(pageClass, result string) string { return pageClass + "/" + result }

// attrKey returns "page_class/result" for serve datapoints and just "result"
// for prefault datapoints (which carry no page_class). Both kinds must carry
// a generation_bucket attribute; its value is asserted separately by
// TestServeMetric_GenerationBucket, not folded into the key.
func attrKey(t *testing.T, attrs attribute.Set) string {
	t.Helper()
	_, ok := attrs.Value("generation_bucket")
	require.True(t, ok, "datapoint missing generation_bucket attribute")
	r, ok := attrs.Value("result")
	require.True(t, ok, "datapoint missing result attribute")
	pc, ok := attrs.Value("page_class")
	if !ok {
		return r.AsString()
	}

	return key(pc.AsString(), r.AsString())
}

// collectTriplet returns, keyed by attrKey, the attempt count (from the
// duration histogram) and the bytes sum (the "By"-unit counter) for the named
// TimerFactory metric.
func collectTriplet(t *testing.T, rm metricdata.ResourceMetrics, name string) (counts map[string]int64, bytesSum map[string]int64) {
	t.Helper()

	counts = map[string]int64{}
	bytesSum = map[string]int64{}
	sawMetric := false

	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			sawMetric = true

			switch data := m.Data.(type) {
			case metricdata.Histogram[int64]:
				for _, dp := range data.DataPoints {
					counts[attrKey(t, dp.Attributes)] += int64(dp.Count)
				}
			case metricdata.Sum[int64]:
				// The TimerFactory emits two same-named counters; the
				// bytes one carries the "By" unit.
				if m.Unit != "By" {
					continue
				}
				for _, dp := range data.DataPoints {
					bytesSum[attrKey(t, dp.Attributes)] += dp.Value
				}
			}
		}
	}

	require.True(t, sawMetric, "metric %q not found in collected metrics", name)

	return counts, bytesSum
}
