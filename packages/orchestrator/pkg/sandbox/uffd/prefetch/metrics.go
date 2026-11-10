//go:build linux

package prefetch

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

var meter = otel.Meter("github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd/prefetch")

// pagesCounter counts pages processed by prefetch runs, by stage:
// fetched / fetch_skipped (fetch phase: cache population from the source) and
// copied / copy_skipped (copy phase: Prefault into the guest). copied counts
// only pages this run actually installed — equal to
// prefault{result="installed"} by construction; prefaults that found the page
// already resident, lost the install race, or hit EAGAIN land in
// copy_skipped. Recorded once per run at completion, matching the
// "prefetch: completed" log line. Per-page copy detail (latency, install
// outcome) lives in orchestrator.sandbox.uffd.prefault.
var pagesCounter = utils.Must(meter.Int64Counter(
	"orchestrator.sandbox.uffd.prefetch.pages",
	metric.WithDescription("Pages processed by prefetch runs, by stage"),
))

// durationHistogram records prefetch run phase durations. The fetch and copy
// phases overlap (copy starts once uffd is ready); "copy_start_delay" is the
// time from run start until copy could begin, and "total" spans the whole run.
// Copy/total durations far exceeding the resume duration mean the prefetch
// ran too late to help.
var durationHistogram = utils.Must(meter.Int64Histogram(
	"orchestrator.sandbox.uffd.prefetch.duration",
	metric.WithDescription("Prefetch run phase durations"),
	metric.WithUnit("ms"),
))

// mappingBlocksHistogram records the size (in blocks) of each prefetch mapping
// replayed on a resume, tagged by mode ("prefault" for the init trace, "fetch"
// for the fetch-only last-cycle diff). It gives the fleet distribution of the
// recorded working-set size; the "fetch" series sizes the last-cycle-prefetch
// rollout (how much a resume prefetches) and flags idle-at-pause sandboxes in
// its bottom bucket.
var mappingBlocksHistogram = utils.Must(meter.Int64Histogram(
	"orchestrator.sandbox.uffd.prefetch.mapping_blocks",
	metric.WithDescription("Prefetch mapping size in blocks, by mode"),
	metric.WithUnit("{block}"),
))

var (
	modePrefaultAttr = telemetry.PrecomputeAttrs(attribute.String("mode", "prefault"))
	modeFetchAttr    = telemetry.PrecomputeAttrs(attribute.String("mode", "fetch"))

	stageFetchedAttr      = telemetry.PrecomputeAttrs(attribute.String("stage", "fetched"))
	stageFetchSkippedAttr = telemetry.PrecomputeAttrs(attribute.String("stage", "fetch_skipped"))
	stageCopiedAttr       = telemetry.PrecomputeAttrs(attribute.String("stage", "copied"))
	stageCopySkippedAttr  = telemetry.PrecomputeAttrs(attribute.String("stage", "copy_skipped"))

	phaseFetchAttr          = telemetry.PrecomputeAttrs(attribute.String("phase", "fetch"))
	phaseCopyAttr           = telemetry.PrecomputeAttrs(attribute.String("phase", "copy"))
	phaseCopyStartDelayAttr = telemetry.PrecomputeAttrs(attribute.String("phase", "copy_start_delay"))
	phaseTotalAttr          = telemetry.PrecomputeAttrs(attribute.String("phase", "total"))
)
