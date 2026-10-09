package main

import (
	"cmp"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync/atomic"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/metric/noop"
	"golang.org/x/sync/errgroup"

	"github.com/e2b-dev/infra/packages/orchestrator/cmd/internal/cmdutil"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block/metrics"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

// validateConcurrency is the worker count for the parallel chunk fetch in
// validate. Hardcoded — matches the prod orchestrator's typical fetch fanout.
const validateConcurrency = 10

// runValidate validates the target build through the production read path
// (block.Chunker) and, with recursive, every ancestor its mappings draw on.
// In recursive mode the target's Builds map is treated as authoritative: each
// ancestor's own recorded Size is cross-checked against the target's record.
func runValidate(ctx context.Context, storagePath, buildID, artifact string, recursive bool) error {
	if !recursive {
		return validateBuild(ctx, storagePath, buildID, artifact, 0)
	}

	chain, err := gatherChain(ctx, storagePath, buildID, artifact, true)
	if err != nil {
		return err
	}

	expected := map[uuid.UUID]int64{}
	for id, bd := range chain[0].h.Builds {
		if bd.Size > 0 {
			expected[id] = bd.Size
		}
	}

	var validated, failed int
	for _, c := range chain {
		if c.Usage != nil && c.Usage.UsedBytes == 0 {
			continue // an ancestor the target build doesn't draw on
		}

		id := c.Image.BuildID
		fmt.Printf("\n──── %s (%s) ────\n", id, roleOf(id, chain[0].h.Metadata))

		validated++
		if err := validateBuild(ctx, storagePath, id.String(), artifact, expected[id]); err != nil {
			failed++
			fmt.Fprintf(os.Stderr, "  %s\n", err)
		}
	}

	fmt.Printf("\n%d build(s) validated, %d failed\n", validated, failed)
	if failed > 0 {
		return fmt.Errorf("%d build(s) failed validation", failed)
	}

	return nil
}

// validateBuild reads a build's entire image through the production block
// Chunker — the same fetch+decompress path the orchestrator uses — and verifies
// the SHA-256 checksum and the frame table. expectedSize is the size the
// caller expects (e.g., a descendant's record of this build); 0 = no
// expectation. Mismatches are reported by reportValidation.
func validateBuild(ctx context.Context, storagePath, buildID, artifact string, expectedSize int64) error {
	headerData, _, err := cmdutil.ReadFile(ctx, storagePath, buildID, artifact+storage.HeaderSuffix)
	if err != nil {
		return fmt.Errorf("read header: %w", err)
	}
	h, err := header.DeserializeBytes(headerData)
	if err != nil {
		return fmt.Errorf("deserialize header: %w", err)
	}

	ft := h.GetBuildFrameData(h.Metadata.BuildId)
	// expected is the zero value when the build records no checksum (V3); the
	// fetch still runs and reportValidation shows the checksum as n/a.
	expected := h.Builds[h.Metadata.BuildId].Checksum

	chunker, upstream, size, cleanup, err := openChunker(ctx, storagePath, buildID, artifact, h, ft)
	if err != nil {
		return err
	}
	defer cleanup()
	if size <= 0 {
		return errors.New("build has no data to validate")
	}

	chunks := validationUnits(ft, size)
	blockSize := int64(h.Metadata.BlockSize)

	// Fetch with the production access pattern: one blockSize Slice at a time.
	// Chunker.Slice is block-granular — it blocks only until the requested block
	// lands — so a worker takes a whole chunk and slices it block by block, and
	// validateConcurrency workers keep that many chunks in flight.
	var nextChunk atomic.Int64
	eg, egCtx := errgroup.WithContext(ctx)
	for range validateConcurrency {
		eg.Go(func() error {
			for {
				ci := nextChunk.Add(1) - 1
				if ci >= int64(len(chunks)) {
					return nil
				}
				c := chunks[ci]
				for off := c.lo; off < c.hi; off += blockSize {
					if _, err := chunker.Slice(egCtx, off, min(blockSize, c.hi-off), upstream, ft); err != nil {
						return fmt.Errorf("fetch block at %d: %w", off, err)
					}
				}
			}
		})
	}
	if err := eg.Wait(); err != nil {
		return err
	}

	// The cache is warm — sweep it one block at a time to hash the image.
	hasher := sha256.New()
	for off := int64(0); off < size; off += blockSize {
		b, err := chunker.Slice(ctx, off, min(blockSize, size-off), upstream, ft)
		if err != nil {
			return fmt.Errorf("read block at %d: %w", off, err)
		}
		hasher.Write(b)
	}
	var got [32]byte
	copy(got[:], hasher.Sum(nil))

	return reportValidation(h, ft, got, expected, size, expectedSize)
}

// openChunker wires a production block.Chunker over the build's data file,
// returning the chunker, the image's uncompressed size, and a cleanup function.
func openChunker(ctx context.Context, storagePath, buildID, artifact string, h *header.Header, ft *storage.FrameTable) (*block.Chunker, storage.RangeOpener, int64, func(), error) {
	spec, err := cmdutil.TemplateSpec(storagePath)
	if err != nil {
		return nil, nil, 0, nil, err
	}
	provider, err := storage.NewProvider(ctx, spec)
	if err != nil {
		return nil, nil, 0, nil, fmt.Errorf("storage provider: %w", err)
	}

	dataPath := buildID + "/" + artifact
	if ft.IsCompressed() {
		dataPath += ft.CompressionType().Suffix()
	}
	obj, err := provider.OpenSeekable(ctx, dataPath)
	if err != nil {
		return nil, nil, 0, nil, fmt.Errorf("open data: %w", err)
	}

	// Mirror production (build/storage_diff.go:Init): the canonical uncompressed
	// size is h.Builds[self].Size — correct even when the serialized self frame
	// table is sparse-trimmed (V4+ can drop frames while preserving original U
	// offsets, so ft.UncompressedSize() would be smaller). Fall back to the
	// storage object's own Size() for V3 headers, which have no Builds map.
	var size int64
	if bd, ok := h.Builds[h.Metadata.BuildId]; ok && bd.Size > 0 {
		size = bd.Size
	} else if size, err = obj.Size(ctx); err != nil {
		return nil, nil, 0, nil, fmt.Errorf("get data size: %w", err)
	}

	flags, err := featureflags.NewClient("", "", "")
	if err != nil {
		return nil, nil, 0, nil, fmt.Errorf("feature flags: %w", err)
	}
	m, err := metrics.NewMetrics(noop.NewMeterProvider())
	if err != nil {
		return nil, nil, 0, nil, fmt.Errorf("metrics: %w", err)
	}
	cacheDir, err := os.MkdirTemp("", "inspect-build-validate-")
	if err != nil {
		return nil, nil, 0, nil, err
	}

	chunker, err := block.NewChunker(flags, size, int64(h.Metadata.BlockSize), filepath.Join(cacheDir, "cache"), m, seekableType(artifact))
	if err != nil {
		os.RemoveAll(cacheDir)

		return nil, nil, 0, nil, fmt.Errorf("chunker: %w", err)
	}

	cleanup := func() {
		chunker.Close()
		os.RemoveAll(cacheDir)
	}

	return chunker, obj, size, cleanup, nil
}

// reportValidation prints the VALIDATION block: size, checksum, frame-table
// verdicts. size is the canonical uncompressed image size; expectedSize is
// what a descendant's Builds map recorded for this build (0 = no expectation).
func reportValidation(h *header.Header, ft *storage.FrameTable, got, expected [32]byte, size, expectedSize int64) error {
	var ftProblems []string
	if ft.IsCompressed() {
		ftProblems = validateFrameTable(h, ft)
	}
	sizeMismatch := expectedSize > 0 && expectedSize != size

	fmt.Printf("\nVALIDATION\n")
	if ft.IsCompressed() {
		fmt.Printf("  Frames           %d\n", ft.NumFrames())
	}
	switch {
	case expectedSize == 0:
		fmt.Printf("  Size             %d (%s)\n", size, humanSize(size))
	case sizeMismatch:
		fmt.Printf("  Size             MISMATCH\n    expected %d (%s)\n    actual   %d (%s)\n",
			expectedSize, humanSize(expectedSize), size, humanSize(size))
	default:
		fmt.Printf("  Size             OK  %d (%s)  (matches descendant's record)\n", size, humanSize(size))
	}
	noChecksum := expected == ([32]byte{})
	switch {
	case noChecksum:
		fmt.Printf("  Checksum         n/a  %s  (no recorded checksum to verify)\n", checksumString(got))
	case got == expected:
		fmt.Printf("  Checksum         OK  %s\n", checksumString(got))
	default:
		fmt.Printf("  Checksum         MISMATCH\n    expected %s\n    actual   %s\n", checksumString(expected), checksumString(got))
	}
	if ft.IsCompressed() {
		if len(ftProblems) == 0 {
			fmt.Printf("  Frame table      OK  (covers all mappings, no extra frames)\n")
		} else {
			fmt.Printf("  Frame table      FAILED\n")
			for _, p := range ftProblems {
				fmt.Printf("    %s\n", p)
			}
		}
	}

	if sizeMismatch || (!noChecksum && got != expected) || len(ftProblems) > 0 {
		return errors.New("validation failed")
	}

	return nil
}

type byteRange struct{ lo, hi int64 }

// validationUnits returns a build's Chunker fetch units: one per frame for
// compressed builds, one per MemoryChunkSize chunk for uncompressed.
func validationUnits(ft *storage.FrameTable, size int64) []byteRange {
	if ft.IsCompressed() {
		units := make([]byteRange, ft.NumFrames())
		for i := range units {
			startU, endU, _, _ := ft.FrameAt(i)
			units[i] = byteRange{startU, endU}
		}

		return units
	}

	var units []byteRange
	chunk := int64(storage.MemoryChunkSize)
	for off := int64(0); off < size; off += chunk {
		units = append(units, byteRange{off, min(off+chunk, size)})
	}

	return units
}

// validateFrameTable verifies the current build's V4+ metadata: its Builds entry
// exists, and its stored frame table matches the ranges its mappings reference
// — every mapped byte is covered by a frame, and every frame is referenced
// (the TrimToRanges invariant, both ways).
func validateFrameTable(h *header.Header, ft *storage.FrameTable) []string {
	currentID := h.Metadata.BuildId

	var problems []string
	if h.Metadata.Version >= header.MetadataVersionV4 {
		if _, ok := h.Builds[currentID]; !ok {
			problems = append(problems, fmt.Sprintf("V4+ header is missing its current build entry %s in Builds", currentID))
		}
	}

	var refs []byteRange
	for _, m := range h.Mapping.All() {
		if m.BuildId == currentID {
			refs = append(refs, byteRange{int64(m.BuildStorageOffset), int64(m.BuildStorageOffset + m.Length)})
		}
	}
	refs = mergeRanges(refs)

	frames := make([]byteRange, ft.NumFrames())
	for i := range frames {
		startU, endU, _, _ := ft.FrameAt(i)
		frames[i] = byteRange{startU, endU}
	}

	for _, r := range refs {
		if !coveredBy(frames, r) {
			problems = append(problems, fmt.Sprintf("mapped range [0x%X,0x%X) not fully covered by frames", r.lo, r.hi))
		}
	}
	for _, f := range frames {
		if !intersectsAny(refs, f) {
			problems = append(problems, fmt.Sprintf("frame [0x%X,0x%X) is not referenced by any mapping", f.lo, f.hi))
		}
	}

	return problems
}

// mergeRanges sorts and coalesces overlapping/adjacent ranges.
func mergeRanges(ranges []byteRange) []byteRange {
	if len(ranges) == 0 {
		return nil
	}
	slices.SortFunc(ranges, func(a, b byteRange) int { return cmp.Compare(a.lo, b.lo) })

	merged := ranges[:1]
	for _, r := range ranges[1:] {
		last := &merged[len(merged)-1]
		if r.lo <= last.hi {
			last.hi = max(last.hi, r.hi)
		} else {
			merged = append(merged, r)
		}
	}

	return merged
}

// coveredBy reports whether sorted frames collectively cover all of r.
func coveredBy(frames []byteRange, r byteRange) bool {
	cur := r.lo
	for _, f := range frames {
		if f.lo > cur {
			break
		}
		if f.hi > cur {
			cur = f.hi
		}
	}

	return cur >= r.hi
}

func intersectsAny(ranges []byteRange, r byteRange) bool {
	for _, x := range ranges {
		if r.lo < x.hi && r.hi > x.lo {
			return true
		}
	}

	return false
}

func seekableType(artifact string) storage.SeekableObjectType {
	if artifact == storage.RootfsName {
		return storage.RootFSObjectType
	}

	return storage.MemfileObjectType
}
