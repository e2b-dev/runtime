package cleaner

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// meterName is the instrumentation scope for the cleaner's instruments.
const meterName = "github.com/e2b-dev/infra/packages/orchestrator/cmd/clean-nfs-cache/cleaner"

// Metric attribute keys. op is the error axis.
const (
	AttrOp    = "op"
	AttrPhase = "phase"

	// error ops (AttrOp)
	ValOpRootReaddir  = "root_readdir"
	ValOpBuildReaddir = "build_readdir"
	ValOpStat         = "stat"
	ValOpDelete       = "delete"

	// run phases (AttrPhase) — stack last.phase.duration by these
	ValPhaseList   = "list"
	ValPhaseScan   = "scan"
	ValPhaseDelete = "delete"
)

// Metrics holds the OTEL instruments emitted by the cleaner. Field names mirror
// the metric names.
type Metrics struct {
	// scan/delete are the two real operations — a hand-rolled triple each (like a
	// storage timer, but the bytes counter is named <op>.bytes, not <op>.size):
	// duration histogram (ms) + bytes counter + event counter. Query with increase()
	// for the counters and histogram_quantile for the duration — NOT rate() (this is
	// an hourly burst, not a steady stream, so rate() averages it to ~0).
	ScanMs      metric.Float64Histogram // nfsclean.scan        (ms) per-build scan wall time
	ScanBytes   metric.Int64Counter     // nfsclean.scan.bytes  on-disk bytes scanned
	ScanCount   metric.Int64Counter     // nfsclean.scan        builds scanned
	DeleteMs    metric.Float64Histogram // nfsclean.delete       (ms) per-build RemoveAll wall
	DeleteBytes metric.Int64Counter     // nfsclean.delete.bytes bytes freed
	DeleteCount metric.Int64Counter     // nfsclean.delete       builds deleted

	// I/O syscall counters, all under scan.* even though opens/reads also happen in
	// the list and (FF-gated) verify phases — one place to read the NFS op load.
	ScanOpen       metric.Int64Counter // open() syscalls — root + per-build data dirs (incl. failed compression probes)
	ScanRead       metric.Int64Counter // readdir page reads — root (list) + per-build (scan)
	ScanStat       metric.Int64Counter // statx syscalls (atime samples + create-time grace)
	ScanGraced     metric.Int64Counter // builds skipped by the create-time (btime) grace filter
	RunUnderTarget metric.Int64Counter // emitted once when a run can't free its target (a bump = investigate)
	Errors         metric.Int64Counter // {op}; logged-and-continued errors, surfaced for dashboards

	// Gauges (one value per run — raw number, no _total, no rate). Phase wall times
	// and the population/throughput numbers that have no counter/histogram source;
	// the per-run scalar totals are read from the triples/counters via increase().
	LastListBuilds  metric.Int64Gauge // build dirs found by the most recent root listing (the population; the scan triple's count is the sampled subset)
	LastCacheBytes  metric.Int64Gauge // By; estimated whole-cache size after the most recent scan (the value the byte budget is judged against)
	PhaseDuration   metric.Int64Gauge // ms; {phase}=list/scan/delete wall time this run (stack by phase)
	LastRunDuration metric.Int64Gauge // s; most recent whole-run wall time

	// Per-build distributions (heatmaps): scan / delete / verify each record the
	// build's size and age, so you get the size + age distribution of scanned vs
	// deleted builds, with verify as the actual-minus-estimate deltas on deletions.
	ScanSize         metric.Int64Histogram // By; on-disk size of each scanned build
	ScanAge          metric.Int64Histogram // s; coldness (now - warmest sampled atime) of each scanned build
	ScanChunks       metric.Int64Histogram // {chunk}; chunk count per scanned build
	ScanReadEntries  metric.Int64Histogram // {file}; entries returned per readdir page
	DeleteSize       metric.Int64Histogram // By; on-disk size of each deleted build
	DeleteAge        metric.Int64Histogram // s; warmest-sample age of each deleted build
	DeleteVerifySize metric.Int64Histogram // By; verify (FF-gated): actual non-chunk bytes minus estimate
	DeleteVerifyAge  metric.Int64Histogram // s; verify (FF-gated): sampled minus true warmest age
}

func NewMetrics(meter metric.Meter) (*Metrics, error) {
	m := &Metrics{}
	var err error

	counters := []struct {
		dst  *metric.Int64Counter
		name string
		desc string
		unit string
	}{
		{&m.ScanCount, "nfsclean.scan", "Builds scanned (count side of the scan triple)", ""},
		{&m.ScanBytes, "nfsclean.scan.bytes", "On-disk bytes of scanned builds, summed (bytes side of the scan triple)", ""},
		{&m.DeleteCount, "nfsclean.delete", "Builds deleted (count side of the delete triple)", ""},
		{&m.DeleteBytes, "nfsclean.delete.bytes", "Bytes freed by deleted builds, summed (bytes side of the delete triple)", ""},
		{&m.ScanOpen, "nfsclean.scan.open", "Total open() syscalls — root + per-build data dirs, including failed compression-variant probes (each is an NFS LOOKUP)", ""},
		{&m.ScanRead, "nfsclean.scan.read", "Total readdir page reads — root listing + per-build data dirs", ""},
		{&m.ScanStat, "nfsclean.scan.stat", "Total statx syscalls during scan (atime samples + create-time grace checks)", ""},
		{&m.ScanGraced, "nfsclean.scan.graced", "Builds skipped by the create-time (btime) grace filter — too new to touch", ""},
		{&m.RunUnderTarget, "nfsclean.run.under_target", "Incremented once when a run deletes everything it's allowed to and still can't free its target (grace floor or out of builds — either way, couldn't free enough). Any non-zero value = investigate.", ""},
		{&m.Errors, "nfsclean.errors", "Errors that were logged and skipped (best-effort), by op — so they're visible without grepping Loki", ""},
	}
	for _, c := range counters {
		opts := []metric.Int64CounterOption{metric.WithDescription(c.desc)}
		if c.unit != "" {
			opts = append(opts, metric.WithUnit(c.unit))
		}
		if *c.dst, err = meter.Int64Counter(c.name, opts...); err != nil {
			return nil, fmt.Errorf("%s: %w", c.name, err)
		}
	}

	gauges := []struct {
		dst  *metric.Int64Gauge
		name string
		desc string
		unit string
	}{
		{&m.LastListBuilds, "nfsclean.last.list.builds", "Build dirs found by the most recent root listing — the population the sample is drawn from (sticky last-value gauge)", ""},
		{&m.LastCacheBytes, "nfsclean.last.cache.bytes", "Estimated on-disk size of the whole cache after the most recent scan, summed from chunk filenames and scaled up when build sampling is on — what --max-cache-bytes is judged against; the only size signal on elastic filesystems (EFS) where df is meaningless (sticky last-value gauge)", "By"},
		{&m.PhaseDuration, "nfsclean.last.phase.duration", "Wall time of each phase this run, labeled {phase} (list/scan/delete) — stack by phase for a per-run breakdown (sticky last-value gauge)", "ms"},
		{&m.LastRunDuration, "nfsclean.last.run.duration", "Most recent whole-run wall time (sticky last-value gauge)", "s"},
	}
	for _, g := range gauges {
		if *g.dst, err = meter.Int64Gauge(g.name,
			metric.WithDescription(g.desc), metric.WithUnit(g.unit)); err != nil {
			return nil, fmt.Errorf("%s: %w", g.name, err)
		}
	}

	histograms := []struct {
		dst  *metric.Int64Histogram
		name string
		desc string
		unit string
	}{
		{&m.ScanSize, "nfsclean.scan.size", "On-disk size of each scanned build — the size distribution of the scanned population", "By"},
		{&m.ScanAge, "nfsclean.scan.age", "Age (now - warmest sampled atime) of each scanned build — the population coldness distribution", "s"},
		{&m.ScanChunks, "nfsclean.scan.chunks", "Chunk count of each live build (drives sample size and size estimate)", "{chunk}"},
		{&m.ScanReadEntries, "nfsclean.scan.read.entries", "Entries returned per readdir page — the size of the directory as read (root build dirs, or a data dir's chunks); sum is total entries enumerated, count is read pages", "{file}"},
		{&m.DeleteSize, "nfsclean.delete.size", "On-disk size of each deleted build — the size distribution of deletions", "By"},
		{&m.DeleteAge, "nfsclean.delete.age", "Warmest-sample age of each cold-deleted build with chunks — high percentiles are the oldest/coldest; query histogram_quantile(0.01,…) for the youngest (worst) deletion. Chunkless zombies are excluded (no sampled atime), so they don't pin this at the epoch-0 sentinel.", "s"},
		{&m.DeleteVerifySize, "nfsclean.delete.verify.size", "FF-gated size-estimate error: a build's actual statted non-chunk bytes (headers/snapfile/metadata) minus the flat otherFilesBytes estimate. Chunk sizes are exact from filenames, so they're not statted; this is the only uncertain part. ≈0 means the estimate is good; large negative means we over-charge.", "By"},
		{&m.DeleteVerifyAge, "nfsclean.delete.verify.age", "FF-gated coldness-sample error: the scan's sampled warmest age minus the true warmest age from a full stat of every chunk. ≥0 means the sample overestimated coldness (missed a warmer chunk); large values flag churn risk (we may delete builds that were recently touched).", "s"},
	}
	for _, h := range histograms {
		if *h.dst, err = meter.Int64Histogram(h.name,
			metric.WithDescription(h.desc), metric.WithUnit(h.unit)); err != nil {
			return nil, fmt.Errorf("%s: %w", h.name, err)
		}
	}

	// Duration histograms share their counter's name (nfsclean.scan / .delete);
	// Prometheus disambiguates by suffix (_milliseconds vs _total). Float so sub-ms
	// scans aren't truncated to 0.
	if m.ScanMs, err = meter.Float64Histogram("nfsclean.scan",
		metric.WithDescription("Per-build scan wall time"), metric.WithUnit("ms")); err != nil {
		return nil, fmt.Errorf("nfsclean.scan ms: %w", err)
	}
	if m.DeleteMs, err = meter.Float64Histogram("nfsclean.delete",
		metric.WithDescription("Per-build RemoveAll wall time"), metric.WithUnit("ms")); err != nil {
		return nil, fmt.Errorf("nfsclean.delete ms: %w", err)
	}

	return m, nil
}

// NoopMetrics builds Metrics from a no-op meter — instruments record nothing.
// It's the fallback NewCleaner uses when given nil (tests, local dev). A no-op
// meter never errors on instrument creation, so the error is safe to ignore.
func NoopMetrics() *Metrics {
	m, _ := NewMetrics(noop.NewMeterProvider().Meter(meterName))

	return m
}

// --- recording helpers: keep the attribute boilerplate out of the call sites ---

func oneAttr(key, value string) metric.MeasurementOption {
	return metric.WithAttributes(attribute.String(key, value))
}

// ageSeconds returns now - atime in seconds (clamped at 0).
func ageSeconds(atimeUnix int64) int64 {
	age := time.Since(time.Unix(atimeUnix, 0)).Seconds()
	if age < 0 {
		return 0
	}

	return int64(age)
}

// recordRead records one readdir page read and how many entries it returned.
func (m *Metrics) recordRead(ctx context.Context, entries int) {
	m.ScanRead.Add(ctx, 1)
	m.ScanReadEntries.Record(ctx, int64(entries))
}

func (m *Metrics) recordOpen(ctx context.Context)   { m.ScanOpen.Add(ctx, 1) }
func (m *Metrics) recordStatx(ctx context.Context)  { m.ScanStat.Add(ctx, 1) }
func (m *Metrics) recordGraced(ctx context.Context) { m.ScanGraced.Add(ctx, 1) }

// recordError counts a logged-and-continued error so it shows on a dashboard.
func (m *Metrics) recordError(ctx context.Context, op string) {
	m.Errors.Add(ctx, 1, oneAttr(AttrOp, op))
}

// recordPhase records the wall time of a run phase (list/scan/delete), labeled by
// phase so a stacked-by-phase panel shows the per-run breakdown.
func (m *Metrics) recordPhase(ctx context.Context, phase string, d time.Duration) {
	m.PhaseDuration.Record(ctx, d.Milliseconds(), oneAttr(AttrPhase, phase))
}

// recordLastListBuilds records the build-dir count found by the root listing this run.
func (m *Metrics) recordLastListBuilds(ctx context.Context, n int) {
	m.LastListBuilds.Record(ctx, int64(n))
}

// recordLastCacheBytes records the estimated whole-cache size after this run's scan.
func (m *Metrics) recordLastCacheBytes(ctx context.Context, n uint64) {
	m.LastCacheBytes.Record(ctx, int64(n))
}

// recordScanBuild records one scanned build's triple (nfsclean.scan): wall time,
// on-disk size, and a +1 count; plus the size into the scan size distribution.
func (m *Metrics) recordScanBuild(ctx context.Context, d time.Duration, size uint64) {
	m.ScanMs.Record(ctx, float64(d)/float64(time.Millisecond))
	m.ScanBytes.Add(ctx, int64(size))
	m.ScanCount.Add(ctx, 1)
	m.ScanSize.Record(ctx, int64(size))
}

// recordSample records the chunk-count and coldness (age) distributions of a live
// build (called only for builds with chunks, which have a real warmest atime).
func (m *Metrics) recordSample(ctx context.Context, chunks int, warmest int64) {
	m.ScanChunks.Record(ctx, int64(chunks))
	m.ScanAge.Record(ctx, ageSeconds(warmest))
}

// recordDelete records one deleted build's triple (nfsclean.delete): RemoveAll
// wall time, bytes freed, and a +1 count; plus its size and age distributions.
func (m *Metrics) recordDelete(ctx context.Context, d time.Duration, size uint64, warmest int64) {
	m.DeleteMs.Record(ctx, float64(d)/float64(time.Millisecond))
	m.DeleteBytes.Add(ctx, int64(size))
	m.DeleteCount.Add(ctx, 1)
	m.DeleteSize.Record(ctx, int64(size))
	// A chunkless "zombie" build never had a sampled atime — warmest stays at the
	// epoch-0 sentinel, so ageSeconds(0) would pin delete_age at a bogus ~56-year
	// value. Skip the age for it (it still counts in delete/size/bytes). Real builds
	// always have a positive warmest atime.
	if warmest > 0 {
		m.DeleteAge.Record(ctx, ageSeconds(warmest))
	}
}

// recordVerified records the verify deltas: the non-chunk size estimate error
// (otherDelta) and the coldness-sample error (ageDelta = sampled warmest age −
// true warmest age from a full chunk stat).
func (m *Metrics) recordVerified(ctx context.Context, otherDelta, ageDelta int64) {
	m.DeleteVerifySize.Record(ctx, otherDelta)
	m.DeleteVerifyAge.Record(ctx, ageDelta)
}

func (m *Metrics) recordUnderTarget(ctx context.Context) {
	m.RunUnderTarget.Add(ctx, 1)
}

// recordLastRunDuration records the most recent whole-run wall time once at the end.
func (m *Metrics) recordLastRunDuration(ctx context.Context, d time.Duration) {
	m.LastRunDuration.Record(ctx, int64(d.Seconds()))
}
