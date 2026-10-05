//go:build linux

package userfaultfd

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

var meter = otel.Meter("github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd/userfaultfd")

// serveMetricName is the metric under which per-fault serve latency / faulted
// bytes / fault count are reported (a TimerFactory triplet).
const serveMetricName = "orchestrator.sandbox.uffd.serve"

// The UFFD handler serves a template's memfile leg; a data fetch that fails
// after its retries is a fault on that leg.
var (
	memfileLegFaults     = utils.Must(telemetry.GetCounter(meter, telemetry.SandboxTemplateLegFaults))
	memfileLegFaultAttrs = metric.WithAttributeSet(attribute.NewSet(attribute.String("leg", "memfile")))
)

// serveTimer records, per served UFFD demand fault, the serve latency (ms
// histogram), bytes installed into the guest by this serve (counter) and the
// serve-attempt count (counter) under serveMetricName. It is always on: the
// fault-in window right after a resume is the prime suspect for slow resumes,
// and this is the only fleet-wide signal for it.
//
// Scope: demand faults only. Pages installed by the prefetcher (Prefault)
// bypass the serve loop and are not counted here; a demand fault that loses
// the install race to a concurrent worker or prefault is recorded as
// result="present" with zero bytes, so the bytes counter only ever counts
// pages this serve actually copied.

var serveTimer = utils.Must(telemetry.NewTimerFactory(
	meter,
	serveMetricName,
	"Time to serve a UFFD demand page fault",
	"Bytes installed into the guest by demand-fault serves",
	"UFFD demand-fault serve attempts",
))

// pageClass classifies a fault by the page-tracker state at the time it was
// served — the distinction that matters for diagnosing slow resumes, since only
// pageClassNew faults reach the source (and may hit GCS).
type pageClass uint8

const (
	pageClassNew      pageClass = iota // block.NotPresent: pulled from the source chunker
	pageClassZero                      // block.Zero / block.Removed: zero-filled
	pageClassResident                  // block.Dirty / block.Clean: already present, short-circuited
	pageClassUnknown                   // classification failed (unexpected tracker state)
	numPageClass
)

// faultResult is the terminal outcome of serving a fault.
type faultResult uint8

const (
	faultResultInstalled faultResult = iota // page installed by this serve
	faultResultPresent                      // page already present: resident short-circuit or lost install race (EEXIST)
	faultResultDeferred                     // EAGAIN: must be retried later
	faultResultDiscarded                    // ESRCH: faulting thread gone, retry pointless
	faultResultError                        // serving failed
	faultResultSkipped                      // prefault only: tracker not NotPresent — prefetch arrived too late (or page was removed)
	numFaultResult
)

// resultNames maps faultResult values to their metric label strings.
var resultNames = [numFaultResult]string{
	faultResultInstalled: "installed",
	faultResultPresent:   "present",
	faultResultDeferred:  "deferred",
	faultResultDiscarded: "discarded",
	faultResultError:     "error",
	faultResultSkipped:   "skipped",
}

// generationBucket coarsens the snapshot's Metadata.Generation (its
// pause/resume cycle count) into log-scale ranges, so fault latency can be
// cut by snapshot chain depth — the fragmentation axis — without
// per-generation label cardinality. Generations reach thousands in prod
// (p99 ≈ 2.3k), hence the split tail.
type generationBucket uint8

const (
	genBucket0 generationBucket = iota // generation 0: fresh template build
	genBucket1To10
	genBucket11To100
	genBucket101To1000
	genBucketOver1000
	numGenerationBucket
)

// generationBucketNames maps generationBucket values to their metric label
// strings.
var generationBucketNames = [numGenerationBucket]string{
	genBucket0:         "0",
	genBucket1To10:     "1-10",
	genBucket11To100:   "11-100",
	genBucket101To1000: "101-1000",
	genBucketOver1000:  ">1000",
}

func bucketForGeneration(generation uint64) generationBucket {
	switch {
	case generation == 0:
		return genBucket0
	case generation <= 10:
		return genBucket1To10
	case generation <= 100:
		return genBucket11To100
	case generation <= 1000:
		return genBucket101To1000
	default:
		return genBucketOver1000
	}
}

// BalloonMode is the free-page mechanism the sandbox's balloon runs. It is
// baked into the template at build time and travels with the snapshot, so it
// is read from Firecracker after start and is unknown until then. Serve and
// write-protect metrics carry it so a REMOVE stall can be read per cohort.
type BalloonMode uint8

const (
	BalloonModeUnknown BalloonMode = iota
	BalloonModeNone
	BalloonModeReporting
	BalloonModeHinting
	numBalloonMode
)

var balloonModeNames = [numBalloonMode]string{
	BalloonModeUnknown:   "unknown",
	BalloonModeNone:      "none",
	BalloonModeReporting: "reporting",
	BalloonModeHinting:   "hinting",
}

func (m BalloonMode) String() string {
	if m >= numBalloonMode {
		return balloonModeNames[BalloonModeUnknown]
	}

	return balloonModeNames[m]
}

// SetBalloonMode labels this handler's serve and write-protect metrics from
// here on; faults served before the mode was read carry unknown.
func (u *Userfaultfd) SetBalloonMode(m BalloonMode) {
	u.mode.Store(uint32(m))
}

func (u *Userfaultfd) balloonMode() BalloonMode {
	return BalloonMode(u.mode.Load())
}

// serveAttrs holds a precomputed metric.MeasurementOption per
// (balloonMode, generationBucket, pageClass, faultResult) combination so the
// per-fault hot path allocates no attributes (mirrors the precomputed attrs
// in block/streaming_chunk.go).
var serveAttrs = buildServeAttrs()

func buildServeAttrs() [numBalloonMode][numGenerationBucket][numPageClass][numFaultResult]metric.MeasurementOption {
	classNames := [numPageClass]string{
		pageClassNew:      "new",
		pageClassZero:     "zero",
		pageClassResident: "resident",
		pageClassUnknown:  "unknown",
	}

	var t [numBalloonMode][numGenerationBucket][numPageClass][numFaultResult]metric.MeasurementOption
	for m := range balloonModeNames {
		for g := range generationBucketNames {
			for c := range classNames {
				for r := range resultNames {
					t[m][g][c][r] = telemetry.PrecomputeAttrs(
						attribute.String("balloon_mode", balloonModeNames[m]),
						attribute.String("generation_bucket", generationBucketNames[g]),
						attribute.String("page_class", classNames[c]),
						attribute.String("result", resultNames[r]),
					)
				}
			}
		}
	}

	return t
}

// prefaultMetricName is the metric under which per-page prefault latency /
// installed bytes / attempt count are reported (a TimerFactory triplet).
const prefaultMetricName = "orchestrator.sandbox.uffd.prefault"

// prefaultTimer records, per Prefault call, the install latency (ms
// histogram), bytes installed into the guest by this prefault (counter) and
// the attempt count (counter) under prefaultMetricName. Prefault data is
// already in memory, so the latency is lock wait + UFFDIO_COPY — a host
// contention proxy rather than a storage signal. Together with serveTimer it
// makes memory materialization race-proof: whichever side loses the EEXIST
// install race records result="present" with zero bytes, so
// serve.bytes + prefault.bytes never double-counts a page.
var prefaultTimer = utils.Must(telemetry.NewTimerFactory(
	meter,
	prefaultMetricName,
	"Time to prefault a page into the guest",
	"Bytes installed into the guest by prefaults",
	"UFFD prefault attempts",
))

// wpResolveMetricName is the metric under which synchronous write-protect
// fault resolution latency (us histogram) and count (counter) are reported.
// Only sync-WP guests (use_sync_wp on snapshot load) emit it: under WP_ASYNC
// no WP events are ever delivered.
const wpResolveMetricName = "orchestrator.sandbox.uffd.wp_resolve"

// wpResolveDuration is the guest-observed write stall: measured from the
// serve loop reading the WP event to unprotect+wake completing, so it
// INCLUDES the worker-pool dispatch wait (a WP resolve can queue behind
// ms-scale MISSING serves — exactly the tail this metric exists to expose).
// Microseconds, not ms: the whole distribution sits at tens of us.
//
// Known undercount: kernel-queue time before the serve loop reads the event
// is not observable (uffd events carry no timestamps); the largest such
// window is ExportPageStates holding readSerial during pause.
var wpResolveDuration = utils.Must(meter.Int64Histogram(wpResolveMetricName,
	metric.WithDescription("Time to resolve a synchronous userfault write-protect fault, from serve-loop event read to unprotect+wake"),
	metric.WithUnit("us"),
))

// wpResolveCount counts WP fault resolutions under wpResolveMetricName,
// tagged like wpResolveDuration.
var wpResolveCount = utils.Must(meter.Int64Counter(wpResolveMetricName,
	metric.WithDescription("Synchronous userfault write-protect faults resolved"),
))

// wpResolveOutcome is the terminal classification of a resolveWriteProtect call.
type wpResolveOutcome uint8

const (
	wpResolveOK wpResolveOutcome = iota
	// wpResolveOKCoW: resolved after a CoW-window pre-image capture (copied
	// by this resolve or waited on / fast-pathed a concurrent capture).
	wpResolveOKCoW
	// wpResolveDeferred: the unprotect hit EAGAIN (mmap_changing) and the
	// fault was re-queued; the re-serve records the resolution separately.
	wpResolveDeferred
	// wpResolveClosed: the handler was already closed; nothing was resolved.
	wpResolveClosed
	// wpResolveError: the unprotect ioctl failed.
	wpResolveError
	// wpResolveStale: a REMOVE raced ahead of the resolve and the presence
	// probe confirmed the page absent; it was zero-installed armed and the
	// writer woken — the retried write WP-faults and promotes normally.
	wpResolveStale
	numWPResolveOutcome
)

// wpResolveAttrs holds a precomputed metric.MeasurementOption per
// (balloonMode, generationBucket, wpResolveOutcome) so the per-fault hot path
// allocates no attributes.
var wpResolveAttrs = buildWPResolveAttrs()

func buildWPResolveAttrs() [numBalloonMode][numGenerationBucket][numWPResolveOutcome]metric.MeasurementOption {
	outcomeNames := [numWPResolveOutcome]string{
		wpResolveOK:       "resolved",
		wpResolveOKCoW:    "resolved_cow",
		wpResolveDeferred: "deferred",
		wpResolveClosed:   "closed",
		wpResolveError:    "error",
		wpResolveStale:    "stale_removed",
	}

	var t [numBalloonMode][numGenerationBucket][numWPResolveOutcome]metric.MeasurementOption
	for m := range balloonModeNames {
		for g := range generationBucketNames {
			for o := range outcomeNames {
				t[m][g][o] = telemetry.PrecomputeAttrs(
					attribute.String("balloon_mode", balloonModeNames[m]),
					attribute.String("generation_bucket", generationBucketNames[g]),
					attribute.String("result", outcomeNames[o]),
				)
			}
		}
	}

	return t
}

// cowCaptureMetricName counts pages captured into a CoW window, split by
// which path did the copy: the background sweep or a guest write's WP-fault
// resolve (the racing captures that are the window's whole reason to exist).
const cowCaptureMetricName = "orchestrator.sandbox.uffd.cow_capture"

var cowCaptureCount = utils.Must(meter.Int64Counter(cowCaptureMetricName,
	metric.WithDescription("Pages captured into a CoW memory-export window, by capture path"),
	metric.WithUnit("{page}"),
))

var (
	cowCaptureSweepAttrs = telemetry.PrecomputeAttrs(attribute.String("source", "sweep"))
	cowCaptureFaultAttrs = telemetry.PrecomputeAttrs(attribute.String("source", "fault"))
)

// cowCancelMetricName counts CoW-window cancellations by reason. The reason
// label is what separates the systemic case the REMOVE tripwire exists to
// catch (a hole in the FPR pause) from ordinary I/O failures — without it a
// cancelled window collapses into memory_seal.duration's success=false and
// the two are indistinguishable short of reading logs.
const cowCancelMetricName = "orchestrator.sandbox.uffd.cow_window_cancel"

var cowCancelCount = utils.Must(meter.Int64Counter(cowCancelMetricName,
	metric.WithDescription("CoW memory-export windows canceled, by reason"),
))

// recordCoWCancel records a window cancellation under the given reason
// (remove_tripwire, sweep_error, sink_error, abort, teardown).
func recordCoWCancel(ctx context.Context, reason string) {
	cowCancelCount.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", reason)))
}

// wpPromoteMetricName counts tracker dirty-promotions performed by WP fault
// resolutions, tagged by the state the page was promoted from. This is the
// write signal the tracker-based dirty source relies on. The "removed"
// bucket stays at zero by construction: a stale WP fault that raced a
// REMOVE returns from the resolve (wp_resolve result="stale_removed")
// before the promotion, under the same lock that excludes the REMOVE
// batch — a non-zero reading here means that guard was bypassed.
const wpPromoteMetricName = "orchestrator.sandbox.uffd.wp_promote"

var wpPromoteCount = utils.Must(meter.Int64Counter(wpPromoteMetricName,
	metric.WithDescription("Tracker dirty-promotions from synchronous write-protect faults, by prior page state"),
))

// wpPromoteAttrs holds a precomputed metric.MeasurementOption per
// (generationBucket, prior block.State). Sized by block.NumStates and named
// by State.String(), both owned by the block package next to the enum, so a
// new state cannot leave this table short or unnamed.
var wpPromoteAttrs = buildWPPromoteAttrs()

func buildWPPromoteAttrs() [numGenerationBucket][block.NumStates]metric.MeasurementOption {
	var t [numGenerationBucket][block.NumStates]metric.MeasurementOption
	for g := range generationBucketNames {
		for s := range block.NumStates {
			t[g][s] = telemetry.PrecomputeAttrs(
				attribute.String("generation_bucket", generationBucketNames[g]),
				attribute.String("from_state", block.State(s).String()),
			)
		}
	}

	return t
}

// ServeSnapshot is a cumulative count of demand faults a handler has served,
// read at a point in time via Userfaultfd.ServeStats. Prefaults bypass the
// serve loop and are not counted. Sampling it at the envd-init boundary yields
// the pages/bytes a guest needed to start.
type ServeSnapshot struct {
	// Pages is the number of demand faults resolved (installed or already
	// present) — the count of distinct pages the guest needed.
	Pages int64
	// SourcePages is the subset of Pages installed from the source
	// (page_class=new); the rest were zero-filled or already resident (a
	// prefetch hit or a concurrent install).
	SourcePages int64
	// Bytes is the number of bytes installed into the guest by this handler
	// (page_class new + zero). On a fixed page size this is Pages-minus-present
	// times the page size, but it is tracked directly so it stays correct
	// across mixed page sizes.
	Bytes int64
	// WPFaults is the number of synchronous write-protect faults resolved
	// (guest writes to protected pages). Zero under WP_ASYNC guests. Sampled
	// per interval it approximates the dirty-set growth at 2 MiB granularity.
	WPFaults int64
	// Deferred is the number of serve attempts that hit EAGAIN and were
	// re-served later; each is a REMOVE or a mapping change racing the
	// install. WPDeferred is the same for write-protect resolves.
	Deferred   int64
	WPDeferred int64
}

// ServeStats returns a cumulative snapshot of the demand faults served so far.
func (u *Userfaultfd) ServeStats() ServeSnapshot {
	return ServeSnapshot{
		Pages:       u.servedPages.Load(),
		SourcePages: u.servedSourcePages.Load(),
		Bytes:       u.servedBytes.Load(),
		WPFaults:    u.wpFaultsResolved.Load(),
		Deferred:    u.servedDeferred.Load(),
		WPDeferred:  u.wpDeferred.Load(),
	}
}

// recordServeStats folds one finished serve attempt into the cumulative
// counters read by ServeStats. Only resolved faults (installed or already
// present) count as a needed page; deferred faults are re-served and counted
// when they finally resolve, so they are not double counted here.
func (u *Userfaultfd) recordServeStats(pclass pageClass, result faultResult, servedBytes int64) {
	switch result {
	case faultResultInstalled, faultResultPresent:
		u.servedPages.Add(1)
		if servedBytes > 0 {
			u.servedBytes.Add(servedBytes)
			if pclass == pageClassNew {
				u.servedSourcePages.Add(1)
			}
		}
	case faultResultDeferred:
		u.servedDeferred.Add(1)
	}
}

// prefaultAttrs holds a precomputed metric.MeasurementOption per
// (generationBucket, faultResult). Prefaults have no page_class: they only
// ever target not-present pages (the Dirty/Zero pre-check records
// result="skipped" instead).
var prefaultAttrs = buildPrefaultAttrs()

func buildPrefaultAttrs() [numGenerationBucket][numFaultResult]metric.MeasurementOption {
	var t [numGenerationBucket][numFaultResult]metric.MeasurementOption
	for g := range generationBucketNames {
		for r := range resultNames {
			t[g][r] = telemetry.PrecomputeAttrs(
				attribute.String("generation_bucket", generationBucketNames[g]),
				attribute.String("result", resultNames[r]),
			)
		}
	}

	return t
}
