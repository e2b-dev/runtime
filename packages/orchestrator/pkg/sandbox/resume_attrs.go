package sandbox

import (
	"context"
	"math/rand/v2"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd/prefetch"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd/userfaultfd"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

// resumesInFlight counts the resumes this orchestrator is running right now.
var resumesInFlight atomic.Int64

// startupSpanAttrs describes the demand faults a guest raised before its /init
// answered: the count, the bytes, the time they cost in sum and at worst, and
// the backends the fetched pages came from. Concurrent serves make the sum
// larger than wall time, so it is a work measure rather than a critical path.
func startupSpanAttrs(stats userfaultfd.ServeSnapshot) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.Int64("startup.pages", stats.Pages),
		attribute.Int64("startup.source_pages", stats.SourcePages),
		attribute.Int64("startup.bytes", stats.Bytes),
		attribute.Int64("startup.deferred", stats.Deferred),
		attribute.Int64("startup.deduped", stats.Deduped),
		attribute.Float64("startup.serve_sum_ms", ms(stats.ServeTime)),
		attribute.Float64("startup.source_serve_sum_ms", ms(stats.SourceServeTime)),
		attribute.Float64("startup.max_serve_ms", ms(stats.MaxServe)),
	}

	return append(attrs, sourceAttrs("startup", stats)...)
}

// sourceAttrs gives pages and serve time per backend, for backends that served any.
func sourceAttrs(prefix string, stats userfaultfd.ServeSnapshot) []attribute.KeyValue {
	var attrs []attribute.KeyValue
	for src, n := range stats.BySource {
		if n > 0 {
			name := storage.Source(src).String()
			attrs = append(attrs,
				attribute.Int64(prefix+".source_pages."+name, n),
				attribute.Float64(prefix+".source_serve_ms."+name, ms(stats.ServeTimeBySource[src])),
			)
		}
	}

	return attrs
}

// resumeNodeAttrs is the node's concurrent work when a resume began.
func resumeNodeAttrs(otherResumes int64, prefetching prefetch.Activity) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.Int64("node.resumes_in_flight", otherResumes),
		attribute.Int64("node.prefetch.runs", prefetching.Runs),
		attribute.Int64("node.prefetch.fetch_workers", prefetching.FetchWorkers),
		attribute.Int64("node.prefetch.copy_workers", prefetching.CopyWorkers),
	}
}

// resumeHostAttrs is how busy the host's CPUs were over the resume, from the
// sample taken when it began. Unreadable values are left off rather than zeroed.
func resumeHostAttrs(start cpuSample, startOK bool) []attribute.KeyValue {
	if !startOK {
		return nil
	}
	end, ok := sampleHostCPU()
	if !ok {
		return nil
	}
	share, ok := end.since(start)
	if !ok {
		return nil
	}

	return []attribute.KeyValue{
		attribute.Float64("host.cpu.busy_pct", share.busyPct),
		attribute.Float64("host.cpu.steal_pct", share.stealPct),
	}
}

type pageResidencyProbe interface {
	PageResident(ctx context.Context, off int64) (resident, known bool)
}

// cachedPercent estimates local 4 KiB pages across the prefetch list with at
// most 64 single-mapping probes. Unknown residency is -1, never a cold result.
func cachedPercent(ctx context.Context, memfile any, mapping *metadata.MemoryPrefetchMapping) float64 {
	probe, ok := memfile.(pageResidencyProbe)
	if !ok || mapping.Count() == 0 || mapping.BlockSize <= 0 || mapping.BlockSize%header.PageSize != 0 {
		return -1
	}

	pagesPerBlock := mapping.BlockSize / header.PageSize
	pages := int64(len(mapping.Indices)) * pagesPerBlock
	samples := min(pages, 64)
	var cached int
	for i := range samples {
		// One random page per equal-sized part of the list avoids repeatedly
		// sampling the same position within large blocks.
		start, end := i*pages/samples, (i+1)*pages/samples
		page := start + rand.Int64N(end-start)
		idx := mapping.Indices[page/pagesPerBlock]
		off := int64(idx)*mapping.BlockSize + page%pagesPerBlock*header.PageSize
		resident, known := probe.PageResident(ctx, off)
		if !known {
			return -1
		}
		if resident {
			cached++
		}
	}

	return 100 * float64(cached) / float64(samples)
}

func ms(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}
