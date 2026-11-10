//go:build linux

package block

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

func dedupPerformanceInput(tb testing.TB, blocks int, fragmented bool) ([]byte, *fakeOriginalDevice, *roaring.Bitmap) {
	tb.Helper()

	const blockSize = 2 * 1024 * 1024
	base := make([]byte, blocks*blockSize)
	for i := range base {
		base[i] = byte(i%251 + 1)
	}
	src := bytes.Clone(base)
	for page := range len(src) / header.PageSize {
		off := page * header.PageSize
		switch page % 16 {
		case 0:
			clear(src[off : off+header.PageSize])
		case 1:
			src[off] ^= 0xff
		}
	}

	buildID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	var mapping []header.BuildMap
	if fragmented {
		for off := 0; off < len(base); off += header.PageSize {
			mapping = append(mapping, header.BuildMap{
				Offset:             uint64(off),
				Length:             header.PageSize,
				BuildId:            buildID,
				BuildStorageOffset: uint64(off/header.PageSize) * storage.DefaultCompressFrameSize,
			})
		}
	}
	hdr, err := header.NewHeader(header.NewTemplateMetadata(buildID, blockSize, uint64(len(base))), mapping)
	require.NoError(tb, err)

	return src, &fakeOriginalDevice{data: base, hdr: hdr}, fullDirty(int64(len(base)), blockSize)
}

func TestCacheDedup_FragmentedParentPreservesImageAndFrameCount(t *testing.T) {
	t.Parallel()

	for _, fragmented := range []bool{false, true} {
		for _, global := range []bool{false, true} {
			for _, perBlock := range []bool{false, true} {
				t.Run(fmt.Sprintf("fragmented=%t/global=%t/per-block=%t", fragmented, global, perBlock), func(t *testing.T) {
					t.Parallel()

					const blockSize = 2 * 1024 * 1024
					src, parent, dirty := dedupPerformanceInput(t, 1, fragmented)
					budget := DedupBudget{FetchRunWindowPages: blockSize / header.PageSize}
					if global {
						budget.MaxPagesPerPromotedFrame = 1
					}
					if perBlock {
						budget.MaxFetchWindowsPerBlock = 1
						budget.MaxPromotedParentPagesPerBlock = 8
					}

					readSrc := func(off int64) ([]byte, error) { return src[off : off+blockSize], nil }
					plan, err := dedupCompare(t.Context(), readSrc, parent, dirty, blockSize, false, budget)
					require.NoError(t, err)
					frames := int64(1)
					if fragmented {
						frames = 448
					}
					require.Equal(t, frames, plan.parentFrames)
					require.EqualValues(t, blockSize, plan.exportedSize)
					require.EqualValues(t, 32, plan.pageEmpty.GetCardinality())
					if fragmented && global {
						require.EqualValues(t, 480, plan.pageDirty.GetCardinality())
						require.Equal(t, frames, plan.promotedFrames)
					} else {
						storedPages := 32
						if fragmented && perBlock {
							storedPages += 8
							require.EqualValues(t, 8, plan.promotedPages)
						}
						require.EqualValues(t, storedPages, plan.pageDirty.GetCardinality())
						require.Zero(t, plan.promotedFrames)
					}

					input := buildPackedSrcCache(t, src, dirty, blockSize)
					cache, meta, err := input.Dedup(t.Context(), parent, dirty, blockSize, t.TempDir()+"/dedup", false, false, budget)
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, cache.Close()) })
					require.True(t, plan.pageDirty.Equals(meta.Dirty))
					require.True(t, plan.pageEmpty.Equals(meta.Empty))

					var packed int64
					for page := range len(src) / header.PageSize {
						off := page * header.PageSize
						var restored []byte
						switch {
						case meta.Dirty.Contains(uint32(page)):
							restored = make([]byte, header.PageSize)
							_, err = cache.ReadAt(restored, packed)
							require.NoError(t, err)
							packed += header.PageSize
						case meta.Empty.Contains(uint32(page)):
							restored = make([]byte, header.PageSize)
						default:
							restored = parent.data[off : off+header.PageSize]
						}
						require.Equal(t, src[off:off+header.PageSize], restored, "page %d", page)
					}
					packedSize, err := cache.Size()
					require.NoError(t, err)
					require.Equal(t, packed, packedSize)
				})
			}
		}
	}
}

//nolint:paralleltest // AllocsPerRun changes process-wide GOMAXPROCS.
func TestDedupCompare_DisabledGlobalPromotionDoesNotAllocatePerParentFrame(t *testing.T) {
	const blockSize = 2 * 1024 * 1024
	src, parent, dirty := dedupPerformanceInput(t, 1, true)
	ctx := t.Context()
	source := &fakeOriginalDevice{data: src}
	readSrc := func(off int64) ([]byte, error) { return source.Slice(ctx, off, blockSize) }
	var plan *dedupPlan
	var err error
	allocs := testing.AllocsPerRun(10, func() {
		plan, err = dedupCompare(t.Context(), readSrc, parent, dirty, blockSize, false, DedupBudget{})
	})
	require.NoError(t, err)
	require.EqualValues(t, 448, plan.parentFrames)
	require.EqualValues(t, 32, plan.pageDirty.GetCardinality())
	require.EqualValues(t, 32, plan.pageEmpty.GetCardinality())
	require.Less(t, allocs, float64(448), "disabled global promotion must not allocate page bitmaps for each parent frame")
}

func BenchmarkDedupCompare(b *testing.B) {
	for _, fragmented := range []bool{false, true} {
		for _, global := range []bool{false, true} {
			b.Run(fmt.Sprintf("fragmented=%t/global=%t", fragmented, global), func(b *testing.B) {
				const blockSize = 2 * 1024 * 1024
				src, parent, dirty := dedupPerformanceInput(b, 8, fragmented)
				budget := DedupBudget{}
				if global {
					budget.MaxPagesPerPromotedFrame = 1
				}
				readSrc := func(off int64) ([]byte, error) { return src[off : off+blockSize], nil }
				b.SetBytes(int64(len(src)))
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					plan, err := dedupCompare(b.Context(), readSrc, parent, dirty, blockSize, false, budget)
					if err != nil {
						b.Fatal(err)
					}
					if plan.exportedSize != int64(len(src)) || plan.pageEmpty.GetCardinality() != 256 {
						b.Fatal("incorrect classification")
					}
				}
			})
		}
	}
}

func BenchmarkCacheDedup(b *testing.B) {
	for _, fragmented := range []bool{false, true} {
		b.Run(fmt.Sprintf("fragmented=%t", fragmented), func(b *testing.B) {
			const blockSize = 2 * 1024 * 1024
			src, parent, dirty := dedupPerformanceInput(b, 8, fragmented)
			input, err := NewCache(int64(len(src)), blockSize, b.TempDir()+"/source", false)
			require.NoError(b, err)
			b.Cleanup(func() { require.NoError(b, input.Close()) })
			_, err = input.WriteAt(src, 0)
			require.NoError(b, err)
			outPath := b.TempDir() + "/diff"
			b.SetBytes(int64(len(src)))
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				cache, meta, err := input.Dedup(b.Context(), parent, dirty, blockSize, outPath, false, false, DedupBudget{})
				if err != nil {
					b.Fatal(err)
				}
				if err := cache.Close(); err != nil {
					b.Fatal(err)
				}
				if meta.Dirty.GetCardinality() != 256 || meta.Empty.GetCardinality() != 256 {
					b.Fatal("incorrect classification")
				}
			}
		})
	}
}
