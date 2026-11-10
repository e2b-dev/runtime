package sandbox

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/attribute"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd/userfaultfd"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

func attrMap(attrs []attribute.KeyValue) map[string]any {
	m := make(map[string]any, len(attrs))
	for _, kv := range attrs {
		m[string(kv.Key)] = kv.Value.AsInterface()
	}

	return m
}

func TestStartupSpanAttrs(t *testing.T) {
	t.Parallel()

	var bySource [storage.NumSources]int64
	var timeBySource [storage.NumSources]time.Duration
	bySource[storage.SourceZero] = 40
	bySource[storage.SourceMmap] = 80
	bySource[storage.SourceGCS] = 3
	timeBySource[storage.SourceMmap] = 4 * time.Millisecond
	timeBySource[storage.SourceGCS] = 90 * time.Millisecond
	got := attrMap(startupSpanAttrs(userfaultfd.ServeSnapshot{
		BySource:          bySource,
		ServeTimeBySource: timeBySource,
	}))

	assert.Equal(t, int64(40), got["startup.source_pages.zero"], "zero-filled pages have their own source")
	assert.Equal(t, int64(80), got["startup.source_pages.mmap"])
	assert.Equal(t, int64(3), got["startup.source_pages.gcs"])
	assert.InDelta(t, 4, got["startup.source_serve_ms.mmap"], 1e-9)
	assert.InDelta(t, 90, got["startup.source_serve_ms.gcs"], 1e-9, "a few slow remote pages show on the trace")
	assert.NotContains(t, got, "startup.source_pages.nfs", "an untouched backend is left off")
	assert.NotContains(t, got, "startup.source_serve_ms.nfs")
}

type fakeCacheProbe map[int64]bool

func (f fakeCacheProbe) PageResident(_ context.Context, off int64) (bool, bool) { return f[off], true }

type countingCacheProbe struct {
	offsets     []int64
	localBefore int64
}

func (p *countingCacheProbe) PageResident(_ context.Context, off int64) (bool, bool) {
	p.offsets = append(p.offsets, off)

	return off < p.localBefore, true
}

type unknownCacheProbe struct{}

func (unknownCacheProbe) PageResident(context.Context, int64) (bool, bool) { return false, false }

func TestCachedPercent(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	mapping := &metadata.MemoryPrefetchMapping{Indices: []uint64{0, 1, 2, 3}, BlockSize: header.PageSize}
	probe := fakeCacheProbe{0: true, 2 * header.PageSize: true, 3 * header.PageSize: true}

	assert.InDelta(t, 75, cachedPercent(ctx, probe, mapping), 1e-9)
	assert.InDelta(t, -1, cachedPercent(ctx, probe, nil), 1e-9, "no mapping, no tier")
	assert.InDelta(t, -1, cachedPercent(ctx, struct{}{}, mapping), 1e-9, "a memfile that cannot say")
	assert.InDelta(t, -1, cachedPercent(ctx, unknownCacheProbe{}, mapping), 1e-9, "a layer that cannot tell is not cold")

	indices := make([]uint64, 1024)
	for i := range indices {
		indices[i] = uint64(i)
	}
	large := &metadata.MemoryPrefetchMapping{Indices: indices, BlockSize: header.PageSize}
	counting := &countingCacheProbe{localBefore: 512 * header.PageSize}
	assert.InDelta(t, 50, cachedPercent(ctx, counting, large), 1e-9)
	assert.Len(t, counting.offsets, 64, "large mappings use a bounded probe before prefetch")
}

func TestCachedPercentSamplesInsideLargeBlocks(t *testing.T) {
	t.Parallel()

	const blockSize = 2 << 20
	const start = 3 * blockSize
	mapping := &metadata.MemoryPrefetchMapping{Indices: []uint64{3}, BlockSize: blockSize}
	probe := &countingCacheProbe{localBefore: start + blockSize/2}
	assert.InDelta(t, 50, cachedPercent(t.Context(), probe, mapping), 1e-9,
		"a partly local block contributes its local page share")
	assert.Len(t, probe.offsets, 64)
	for _, off := range probe.offsets {
		assert.GreaterOrEqual(t, off, int64(start))
		assert.Less(t, off, int64(start+blockSize))
		assert.Zero(t, off%header.PageSize)
	}
}
