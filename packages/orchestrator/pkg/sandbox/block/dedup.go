//go:build linux

package block

import (
	"bytes"
	"context"
	"fmt"
	"maps"
	"math"
	"slices"
	"time"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"

	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

type dedupPlan struct {
	pageDirty    *roaring.Bitmap
	pageEmpty    *roaring.Bitmap
	exportedSize int64

	// Per-block window cap (fetchWindower.compact).
	promotedBlocks int64
	promotedPages  int64

	// Global cheap-frame promotion (promoteCheapFrames). parentFrames counts
	// distinct parent fetch keys before promotion: the predicted cold-restore
	// fetches contributed by this diff's deduped pages.
	parentFrames       int64
	promotedFrames     int64
	promotedFramePages int64
}

// DedupBudget caps fetch fragmentation of the deduped diff. When a block
// spans more than MaxFetchWindowsPerBlock backing fetch windows, the cheapest
// parent pages are promoted into the diff, up to
// MaxPromotedParentPagesPerBlock per block. Independently, a parent fetch key
// is promoted whole when its page count is at most MaxPagesPerPromotedFrame
// times the expected backing fetches removed — a scale-free price per fetch
// that holds across VM sizes. The expected value assumes blocks fault
// independently with probability BlockFaultPct/100; values outside (0,100)
// are strict: only frames referenced by nothing outside the diff promote.
// Zero values disable promotion; FetchRunWindowPages 0 uses the compression
// frame size.
type DedupBudget struct {
	MaxFetchWindowsPerBlock        int
	MaxPromotedParentPagesPerBlock int
	MaxPagesPerPromotedFrame       int
	BlockFaultPct                  int
	FetchRunWindowPages            int
}

// DedupOptions is how a memfile diff is deduplicated against its parent.
type DedupOptions struct {
	BestEffort bool
	DirectIO   bool
	Budget     DedupBudget
}

type dedupPageKind byte

const (
	dedupPageEmpty dedupPageKind = iota
	dedupPageParent
	dedupPageCurrent
)

// dedupFetchKey identifies the parent fetch window backing a deduped page.
// On a cold restore each distinct key costs one backing fetch.
type dedupFetchKey struct {
	buildID uuid.UUID
	window  int
}

type dedupPageInfo struct {
	kind dedupPageKind
	key  dedupFetchKey
}

// dedupCompare classifies each dirty page against base: zero pages become
// pageEmpty, pages differing from base become pageDirty (stored in the diff),
// and pages equal to base are deduped away to be served by the parent.
// Per-page IsCached so a single uncached neighbour can't poison cached pages
// of the same block when the parent header is page-granular.
//
// Two optional budget passes then promote deduped pages into pageDirty: a
// global cheap-frame promotion, then a per-block fetch-window cap (last, so
// its packed-position model sees the final layout). Promoted pages are
// byte-identical to the parent, so promotion never changes the restored
// image.
func dedupCompare(
	ctx context.Context,
	src func(absOff int64) ([]byte, error),
	base ReadonlyDevice,
	dirty *roaring.Bitmap,
	blockSize int64,
	bestEffort bool,
	budget DedupBudget,
) (*dedupPlan, error) {
	if budget.FetchRunWindowPages <= 0 {
		budget.FetchRunWindowPages = storage.DefaultCompressFrameSize / header.PageSize
	}
	windowBytes := int64(budget.FetchRunWindowPages) * header.PageSize

	plan := &dedupPlan{pageDirty: roaring.New(), pageEmpty: roaring.New()}

	parentByKey := make(map[dedupFetchKey]*roaring.Bitmap)
	trackParentPages := budget.MaxPagesPerPromotedFrame > 0

	baseHeader := base.Header()
	peeker, _ := base.(CachePeeker)

	for r := range BitsetRanges(dirty, blockSize) {
		plan.exportedSize += r.Size

		for chunkOff := int64(0); chunkOff < r.Size; chunkOff += blockSize {
			if err := ctx.Err(); err != nil {
				return nil, err
			}

			absOff := r.Start + chunkOff
			srcBuf, err := src(absOff)
			if err != nil {
				return nil, err
			}

			for i := int64(0); i < blockSize; i += header.PageSize {
				srcPage := srcBuf[i : i+header.PageSize]
				pageOff := absOff + i
				pageIdx := uint32(pageOff / header.PageSize)

				if header.IsZero(srcPage) {
					plan.pageEmpty.Add(pageIdx)

					continue
				}

				var mapped header.BuildMap
				hasMapping := false
				if baseHeader != nil {
					if m, mErr := baseHeader.GetShiftedMapping(ctx, pageOff); mErr == nil {
						mapped, hasMapping = m, true
					}
				}
				if hasMapping && mapped.BuildId == uuid.Nil && int64(mapped.Length) >= header.PageSize {
					plan.pageDirty.Add(pageIdx) // unbacked parent hole: store as current

					continue
				}
				if bestEffort && peeker != nil && !peeker.IsCached(ctx, pageOff, header.PageSize) {
					plan.pageDirty.Add(pageIdx) // uncached parent page: store as current

					continue
				}

				basePage, sErr := base.Slice(ctx, pageOff, header.PageSize)
				if sErr != nil {
					return nil, fmt.Errorf("slice base at %d: %w", pageOff, sErr)
				}
				if !bytes.Equal(srcPage, basePage) {
					plan.pageDirty.Add(pageIdx)

					continue
				}

				key := parentFetchKey(baseHeader, mapped, hasMapping, pageOff, windowBytes)
				bm, exists := parentByKey[key]
				if !exists {
					if trackParentPages {
						bm = roaring.New()
					}
					parentByKey[key] = bm
				}
				if bm != nil {
					bm.Add(pageIdx)
				}
			}
		}
	}

	plan.parentFrames = int64(len(parentByKey))
	if budget.MaxPagesPerPromotedFrame > 0 {
		extBlocks := countExternalBlocks(parentByKey, baseHeader, dirty, blockSize, windowBytes)
		plan.promoteCheapFrames(parentByKey, extBlocks, int(blockSize/header.PageSize), budget)
	}

	if err := compactBlockWindows(ctx, plan, baseHeader, dirty, blockSize, windowBytes, budget); err != nil {
		return nil, err
	}

	return plan, nil
}

// parentFetchKey is the fetch key of the parent window backing pageOff. When
// the parent build's frame table is known, the key is the exact frame restore
// would fetch (builds can be compressed with non-default frame sizes);
// otherwise offsets are bucketed by the configured window size.
func parentFetchKey(baseHeader *header.Header, mapped header.BuildMap, hasMapping bool, pageOff, windowBytes int64) dedupFetchKey {
	if !hasMapping {
		return dedupFetchKey{window: int(pageOff / windowBytes)}
	}

	var ft *storage.FrameTable
	if baseHeader != nil {
		ft = baseHeader.GetBuildFrameData(mapped.BuildId)
	}
	key, _ := fetchKeyAndEnd(ft, mapped.BuildId, int64(mapped.Offset), windowBytes)

	return key
}

// fetchKeyAndEnd returns the fetch key of the parent window containing the
// build-local (uncompressed) offset and the end of that window: the exact
// frame when the build's frame table is known, a windowBytes bucket
// otherwise. The end is always past off, so window walks terminate.
func fetchKeyAndEnd(ft *storage.FrameTable, buildID uuid.UUID, off, windowBytes int64) (dedupFetchKey, int64) {
	if ft.IsCompressed() {
		if u, err := ft.LocateUncompressed(off); err == nil && u.Offset+int64(u.Length) > off {
			return dedupFetchKey{buildID: buildID, window: int(u.Offset / header.PageSize)}, u.Offset + int64(u.Length)
		}
	}

	return dedupFetchKey{buildID: buildID, window: int(off / windowBytes)}, (off/windowBytes + 1) * windowBytes
}

// compactBlockWindows applies the per-block fetch-window cap. It runs after
// the global promotion so fetchWindower's packed-position model sees the
// final diff layout. It needs no IO: every page is already classified by the
// plan (dirty = stored, empty, else parent) and parent keys are recomputed
// from the in-memory header.
func compactBlockWindows(
	ctx context.Context,
	plan *dedupPlan,
	baseHeader *header.Header,
	dirty *roaring.Bitmap,
	blockSize int64,
	windowBytes int64,
	budget DedupBudget,
) error {
	if budget.MaxFetchWindowsPerBlock <= 0 || budget.MaxPromotedParentPagesPerBlock <= 0 {
		return nil
	}

	pages := make([]dedupPageInfo, blockSize/header.PageSize)
	var currentStoredPages int64
	for r := range BitsetRanges(dirty, blockSize) {
		for chunkOff := int64(0); chunkOff < r.Size; chunkOff += blockSize {
			if err := ctx.Err(); err != nil {
				return err
			}

			absOff := r.Start + chunkOff
			firstPage := uint32(absOff / header.PageSize)

			clear(pages)
			for p := range pages {
				pageIdx := firstPage + uint32(p)
				switch {
				case plan.pageDirty.Contains(pageIdx):
					pages[p].kind = dedupPageCurrent
				case plan.pageEmpty.Contains(pageIdx):
					// zero value of dedupPageInfo is empty
				default:
					pageOff := absOff + int64(p)*header.PageSize
					var mapped header.BuildMap
					hasMapping := false
					if baseHeader != nil {
						if m, mErr := baseHeader.GetShiftedMapping(ctx, pageOff); mErr == nil {
							mapped, hasMapping = m, true
						}
					}
					pages[p].kind = dedupPageParent
					pages[p].key = parentFetchKey(baseHeader, mapped, hasMapping, pageOff, windowBytes)
				}
			}

			w := fetchWindower{windowPages: budget.FetchRunWindowPages, currentStart: currentStoredPages}
			if n := w.compact(pages, budget.MaxFetchWindowsPerBlock, budget.MaxPromotedParentPagesPerBlock); n > 0 {
				plan.promotedBlocks++
				plan.promotedPages += int64(n)
			}

			for p, info := range pages {
				if info.kind == dedupPageCurrent {
					plan.pageDirty.Add(firstPage + uint32(p))
					currentStoredPages++
				}
			}
		}
	}

	return nil
}

// countExternalBlocks counts, per candidate fetch key, the distinct blocks
// outside the dirty set that also reference it. Those mappings survive the
// header merge unchanged, so they can force the frame fetch on a restore no
// matter what this diff stores. Walks the in-memory header only, no IO.
func countExternalBlocks(
	candidates map[dedupFetchKey]*roaring.Bitmap,
	baseHeader *header.Header,
	dirty *roaring.Bitmap,
	blockSize, windowBytes int64,
) map[dedupFetchKey]int {
	counts := make(map[dedupFetchKey]int)
	if baseHeader == nil || len(candidates) == 0 {
		return counts
	}

	lastBlock := make(map[dedupFetchKey]int64)
	for _, m := range baseHeader.Mapping.All() {
		if m.BuildId == uuid.Nil {
			continue
		}
		ft := baseHeader.GetBuildFrameData(m.BuildId)
		end := int64(m.Offset + m.Length)
		for block := int64(m.Offset) / blockSize; block*blockSize < end; block++ {
			if dirty.Contains(uint32(block)) {
				continue
			}
			// Clamp the block to the mapping and shift to build-local offsets.
			lo := max(block*blockSize, int64(m.Offset)) - int64(m.Offset) + int64(m.BuildStorageOffset)
			hi := min((block+1)*blockSize, end) - int64(m.Offset) + int64(m.BuildStorageOffset)
			for off := lo; off < hi; {
				key, next := fetchKeyAndEnd(ft, m.BuildId, off, windowBytes)
				if _, ok := candidates[key]; ok && lastBlock[key] != block+1 {
					counts[key]++
					lastBlock[key] = block + 1 // +1 so block 0 differs from the zero value
				}
				off = next
			}
		}
	}

	return counts
}

// promoteCheapFrames stores whole parent fetch-key page sets in the diff when
// they are cheap relative to their value: the chance any diff block
// referencing the key faults, discounted by the chance a block outside the
// diff (still mapped to the frame) forces the fetch anyway. The price is
// capped below one fetch window: bigger promotions would add at least as
// many diff frames as they remove.
func (p *dedupPlan) promoteCheapFrames(parentByKey map[dedupFetchKey]*roaring.Bitmap, extBlocks map[dedupFetchKey]int, pagesPerBlock int, budget DedupBudget) {
	maxPages := min(budget.MaxPagesPerPromotedFrame, budget.FetchRunWindowPages-1)
	pFault := 1.0 // outside (0,100): strict, only frames unreferenced outside the diff
	if budget.BlockFaultPct > 0 && budget.BlockFaultPct < 100 {
		pFault = float64(budget.BlockFaultPct) / 100
	}

	for k, bm := range parentByKey {
		value := math.Pow(1-pFault, float64(extBlocks[k])) *
			(1 - math.Pow(1-pFault, float64(distinctBlocks(bm, pagesPerBlock))))
		if n := int(bm.GetCardinality()); n > 0 && float64(n) <= float64(maxPages)*value {
			p.pageDirty.Or(bm)
			p.promotedFrames++
			p.promotedFramePages += int64(n)
		}
	}
}

// distinctBlocks counts the distinct blocks the bitmap's pages fall into.
func distinctBlocks(pages *roaring.Bitmap, pagesPerBlock int) int {
	n, last := 0, int64(-1)
	for it := pages.Iterator(); it.HasNext(); {
		if b := int64(it.Next()) / int64(pagesPerBlock); b != last {
			n, last = n+1, b
		}
	}

	return n
}

// fetchWindower models the distinct fetch windows a block's pages resolve to:
// parent pages by their backing fetch key, current pages by the window of
// their packed position in the diff (currentStart is the running count of
// pages stored so far).
type fetchWindower struct {
	windowPages  int
	currentStart int64
}

// compact promotes parent pages to current until the block fits within
// maxWindows fetch windows or the promotion budget is exhausted. Only whole
// fetch-key groups are promoted: a partially promoted key keeps its fetch
// window, so partial promotion never reduces the count. The resulting count
// depends only on how many pages are promoted, so the cheapest-first prefix
// scan considers the optimal whole-group selection for every affordable
// budget.
func (w fetchWindower) compact(pages []dedupPageInfo, maxWindows, maxPromoted int) int {
	if maxWindows <= 0 || maxPromoted <= 0 {
		return 0
	}

	nCur := 0
	for _, p := range pages {
		if p.kind == dedupPageCurrent {
			nCur++
		}
	}
	groups := parentKeyGroups(pages)

	best := len(groups) + w.currentWindows(nCur)
	if best <= maxWindows {
		return 0
	}

	// Commit the shortest cheapest-first prefix that meets maxWindows, or
	// failing that, the one with the lowest count. A zero-benefit prefix
	// (e.g. a lone promotion that trades a parent window for a current one)
	// is only kept when a longer prefix improves on it.
	chosen, promoted := 0, 0
	for g, n := 0, 0; g < len(groups); g++ {
		n += len(groups[g])
		if n > maxPromoted {
			break // groups are size-sorted, so no later group fits either
		}
		if c := len(groups) - (g + 1) + w.currentWindows(nCur+n); c < best {
			best, chosen, promoted = c, g+1, n
		}
		if best <= maxWindows {
			break
		}
	}

	for _, group := range groups[:chosen] {
		for _, i := range group {
			pages[i].kind = dedupPageCurrent
		}
	}

	return promoted
}

// currentWindows is the number of fetch windows covered by n pages packed
// into the diff starting at currentStart.
func (w fetchWindower) currentWindows(n int) int {
	if n == 0 {
		return 0
	}
	wp := int64(w.windowPages)

	return int((w.currentStart+int64(n)-1)/wp - w.currentStart/wp + 1)
}

// parentKeyGroups returns the parent page indices grouped by fetch key,
// cheapest group first (ties by first page index, so the selection is
// deterministic).
func parentKeyGroups(pages []dedupPageInfo) [][]int {
	idxByKey := make(map[dedupFetchKey][]int)
	for i, p := range pages {
		if p.kind == dedupPageParent {
			idxByKey[p.key] = append(idxByKey[p.key], i)
		}
	}

	groups := slices.Collect(maps.Values(idxByKey))
	slices.SortFunc(groups, func(a, b []int) int {
		if d := len(a) - len(b); d != 0 {
			return d
		}

		return a[0] - b[0]
	})

	return groups
}

// recordDedupAttrs takes emptyPages explicitly because the memfd path merges
// the whole-VM inputEmpty bitmap into plan.pageEmpty in place before this
// runs; the caller captures the scan-only count first.
func recordDedupAttrs(ctx context.Context, plan *dedupPlan, emptyPages int64, compareDur, writeDur time.Duration) {
	totalPages := plan.exportedSize / header.PageSize
	uniquePages := int64(plan.pageDirty.GetCardinality())
	dedupedPages := totalPages - uniquePages - emptyPages
	ratio := 0.0
	if totalPages > 0 {
		ratio = float64(dedupedPages) / float64(totalPages)
	}
	telemetry.SetAttributes(ctx,
		attribute.Int64("dedup.total_pages", totalPages),
		attribute.Int64("dedup.deduped_pages", dedupedPages),
		attribute.Int64("dedup.unique_pages", uniquePages),
		attribute.Int64("dedup.empty_pages", emptyPages),
		attribute.Int64("dedup.promoted_blocks", plan.promotedBlocks),
		attribute.Int64("dedup.promoted_pages", plan.promotedPages),
		attribute.Int64("dedup.parent_frames", plan.parentFrames),
		attribute.Int64("dedup.promoted_frames", plan.promotedFrames),
		attribute.Int64("dedup.promoted_frame_pages", plan.promotedFramePages),
		attribute.Float64("dedup.ratio", ratio),
		attribute.Int64("dedup.compare_ms", compareDur.Milliseconds()),
		attribute.Int64("dedup.write_ms", writeDur.Milliseconds()),
	)
}
