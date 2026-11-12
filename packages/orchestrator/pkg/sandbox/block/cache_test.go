//go:build linux

package block

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sys/unix"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/uffd/testutils"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

func allocateTestMemory(t *testing.T, size uint64, pageSize uint64) (addr uint64, expectedData []byte) {
	t.Helper()

	mem, memoryStart, err := testutils.NewPageMmap(t, size, pageSize)
	require.NoError(t, err)

	n, err := rand.Read(mem)
	require.NoError(t, err)
	require.Equal(t, len(mem), n)

	return uint64(memoryStart), mem
}

func TestCopyFromProcess_FullRange(t *testing.T) {
	t.Parallel()

	pageSize := int64(header.PageSize)
	size := pageSize * 30

	addr, mem := allocateTestMemory(t, uint64(size), uint64(pageSize))

	ranges := []Range{
		{Start: int64(addr), Size: size},
	}

	cache, err := NewCacheFromProcessMemory(
		t.Context(),
		pageSize,
		t.TempDir()+"/cache",
		os.Getpid(),
		ranges,
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		cache.Close()
	})

	data := make([]byte, size)
	n, err := cache.ReadAt(data, 0)
	require.NoError(t, err)
	require.Equal(t, int(size), n)

	require.NoError(t, compareData(data[:n], mem[:n]))
}

func TestCopyFromProcess_LargeRanges(t *testing.T) {
	t.Parallel()

	pageSize := uint64(header.PageSize)
	totalSize := pageSize * 5

	addr, mem := allocateTestMemory(t, totalSize, pageSize)

	ranges := []Range{
		{Start: int64(addr), Size: int64(pageSize)},
		{Start: int64(addr + pageSize*3), Size: int64(pageSize)},
		{Start: int64(addr + pageSize), Size: int64(pageSize)},
	}

	cache, err := NewCacheFromProcessMemory(
		t.Context(),
		int64(pageSize),
		t.TempDir()+"/cache",
		os.Getpid(),
		ranges,
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		cache.Close()
	})

	data1 := make([]byte, pageSize)
	n, err := cache.ReadAt(data1, 0)
	require.NoError(t, err)
	require.Equal(t, int(pageSize), n)
	require.NoError(t, compareData(data1[:n], mem[0:pageSize]))

	data2 := make([]byte, pageSize)
	n, err = cache.ReadAt(data2, int64(pageSize))
	require.NoError(t, err)
	require.Equal(t, int(pageSize), n)
	require.NoError(t, compareData(data2[:n], mem[pageSize*3:pageSize*4]))

	data3 := make([]byte, pageSize)
	n, err = cache.ReadAt(data3, int64(pageSize*2))
	require.NoError(t, err)
	require.Equal(t, int(pageSize), n)
	require.NoError(t, compareData(data3[:n], mem[pageSize:pageSize*2]))
}

func TestCopyFromProcess_MultipleRanges(t *testing.T) {
	t.Parallel()

	numRanges := 1500
	pageSize := uint64(header.PageSize)
	rangeSize := int64(pageSize * 64)

	totalSize := rangeSize * int64(numRanges)

	addr, mem := allocateTestMemory(t, uint64(totalSize), pageSize)

	ranges := make([]Range, numRanges)
	for i := range numRanges {
		ranges[i] = Range{
			Start: int64(addr) + int64(i)*rangeSize,
			Size:  rangeSize,
		}
	}

	cache, err := NewCacheFromProcessMemory(
		t.Context(),
		int64(pageSize),
		t.TempDir()+"/cache",
		os.Getpid(),
		ranges,
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		cache.Close()
	})

	checkCount := min(numRanges, 10)
	for i := range checkCount {
		actualOffset := int64(i) * rangeSize
		alignedOffset := (actualOffset / int64(pageSize)) * int64(pageSize)

		data := make([]byte, pageSize)

		n, err := cache.ReadAt(data, alignedOffset)
		require.NoError(t, err)
		require.Equal(t, int(pageSize), n)

		require.NoError(t, compareData(data[:n], mem[alignedOffset:alignedOffset+int64(pageSize)]))
	}
}

func TestCopyFromProcess_HugepageToRegularPage(t *testing.T) {
	t.Parallel()

	pageSize := int64(header.HugepageSize)
	size := pageSize * 10

	mem, addr, err := testutils.NewPageMmap(t, uint64(size), uint64(pageSize))
	require.NoError(t, err)

	n, err := rand.Read(mem)
	require.NoError(t, err)
	require.Equal(t, len(mem), n)

	ranges := []Range{
		{Start: int64(addr), Size: pageSize * 2},
		{Start: int64(addr) + pageSize*4, Size: pageSize * 4},
	}

	cache, err := NewCacheFromProcessMemory(
		t.Context(),
		// Regular 4KiB pages.
		header.PageSize,
		t.TempDir()+"/cache",
		os.Getpid(),
		ranges,
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		cache.Close()
	})

	data := make([]byte, pageSize*2)
	n, err = cache.ReadAt(data, 0)
	require.NoError(t, err)
	require.Equal(t, int(pageSize*2), n)
	require.NoError(t, compareData(data[:n], mem[0:pageSize*2]))

	data = make([]byte, pageSize*4)
	n, err = cache.ReadAt(data, pageSize*2)
	require.NoError(t, err)
	require.Equal(t, int(pageSize*4), n)
	require.NoError(t, compareData(data[:n], mem[pageSize*4:pageSize*8]))
}

func TestSliceDirectOutOfBoundsReturnsBytesNotAvailable(t *testing.T) {
	t.Parallel()

	cache, err := NewCache(16, 4, t.TempDir()+"/cache", false)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cache.Close() })

	_, err = cache.sliceDirect(16, 4)
	require.ErrorIs(t, err, BytesNotAvailableError{})

	_, err = cache.sliceDirect(32, 4)
	require.ErrorIs(t, err, BytesNotAvailableError{})
}

func TestEmptyRanges(t *testing.T) {
	t.Parallel()

	c, err := NewCacheFromProcessMemory(
		t.Context(),
		header.PageSize,
		t.TempDir()+"/cache",
		os.Getpid(),
		nil,
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		c.Close()
	})
}

func TestCacheExportToDiff_ZeroBlockRoutesToEmpty(t *testing.T) {
	t.Parallel()

	const blockSize = header.RootfsBlockSize
	cache, err := NewCache(blockSize, blockSize, t.TempDir()+"/cache", false)
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, cache.Close())
	})

	_, err = cache.WriteAt(make([]byte, blockSize), 0)
	require.NoError(t, err)

	out, err := os.CreateTemp(t.TempDir(), "diff-*")
	require.NoError(t, err)
	defer out.Close()

	diffMetadata, err := cache.ExportToDiff(t.Context(), out)
	require.NoError(t, err)

	require.EqualValues(t, 0, diffMetadata.Dirty.GetCardinality())
	require.EqualValues(t, 1, diffMetadata.Empty.GetCardinality())

	stat, err := out.Stat()
	require.NoError(t, err)
	require.EqualValues(t, 0, stat.Size())
}

func TestCacheExportToDiff_ZeroBlockMapsToEmpty(t *testing.T) {
	t.Parallel()

	const blockSize = header.RootfsBlockSize
	cache, err := NewCache(blockSize, blockSize, t.TempDir()+"/cache", false)
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, cache.Close())
	})

	_, err = cache.WriteAt(make([]byte, blockSize), 0)
	require.NoError(t, err)

	out, err := os.CreateTemp(t.TempDir(), "diff-*")
	require.NoError(t, err)
	defer out.Close()

	diffMetadata, err := cache.ExportToDiff(t.Context(), out)
	require.NoError(t, err)

	baseBuildID := uuid.New()
	originalHeader, err := header.NewHeader(
		header.NewTemplateMetadata(baseBuildID, uint64(blockSize), uint64(blockSize)),
		nil,
	)
	require.NoError(t, err)

	diffHeader, err := diffMetadata.ToDiffHeader(t.Context(), originalHeader, uuid.New())
	require.NoError(t, err)

	mapped, err := diffHeader.GetShiftedMapping(t.Context(), 0)
	require.NoError(t, err)
	require.Equal(t, uuid.Nil, mapped.BuildId, "zero block should be exported as Empty (uuid.Nil)")
}

func TestCacheExportToDiff_MixedZeroBlockSplitsIntoEmptyAndDirty(t *testing.T) {
	t.Parallel()

	const blockSize = header.RootfsBlockSize
	const size = blockSize * 3

	cache, err := NewCache(size, blockSize, t.TempDir()+"/cache", false)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, cache.Close())
	})

	nonZeroBlock := bytes.Repeat([]byte{0xAB}, int(blockSize))

	_, err = cache.WriteAt(make([]byte, blockSize), 0)
	require.NoError(t, err)

	_, err = cache.WriteAt(nonZeroBlock, blockSize)
	require.NoError(t, err)

	out, err := os.CreateTemp(t.TempDir(), "diff-*")
	require.NoError(t, err)
	defer out.Close()

	diffMetadata, err := cache.ExportToDiff(t.Context(), out)
	require.NoError(t, err)

	require.EqualValues(t, 1, diffMetadata.Dirty.GetCardinality())
	require.EqualValues(t, 1, diffMetadata.Empty.GetCardinality())
	require.True(t, diffMetadata.Empty.Contains(0))
	require.True(t, diffMetadata.Dirty.Contains(1))

	baseBuildID := uuid.New()
	originalHeader, err := header.NewHeader(
		header.NewTemplateMetadata(baseBuildID, uint64(blockSize), uint64(size)),
		nil,
	)
	require.NoError(t, err)

	snapshotBuildID := uuid.New()
	diffHeader, err := diffMetadata.ToDiffHeader(t.Context(), originalHeader, snapshotBuildID)
	require.NoError(t, err)

	firstBlock, err := diffHeader.GetShiftedMapping(t.Context(), 0)
	require.NoError(t, err)
	require.Equal(t, uuid.Nil, firstBlock.BuildId, "zero block should map to Empty")

	secondBlock, err := diffHeader.GetShiftedMapping(t.Context(), blockSize)
	require.NoError(t, err)
	require.Equal(t, snapshotBuildID, secondBlock.BuildId, "non-zero dirty block should map to the snapshot diff")

	thirdBlock, err := diffHeader.GetShiftedMapping(t.Context(), 2*blockSize)
	require.NoError(t, err)
	require.Equal(t, baseBuildID, thirdBlock.BuildId, "clean blocks should keep the base mapping")
}

func TestCacheWriteZeroesAt_AlignedRangeMapsToEmpty(t *testing.T) {
	t.Parallel()

	const blockSize = header.RootfsBlockSize
	cache, err := NewCache(blockSize*4, blockSize, t.TempDir()+"/cache", false)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cache.Close()) })

	_, err = cache.WriteAt(bytes.Repeat([]byte{0xAB}, int(blockSize)), blockSize)
	require.NoError(t, err)

	n, err := cache.WriteZeroesAt(0, 2*blockSize)
	require.NoError(t, err)
	require.Equal(t, 2*blockSize, n)

	got := make([]byte, 2*blockSize)
	_, err = cache.ReadAt(got, 0)
	require.NoError(t, err)
	require.True(t, header.IsZero(got), "punched range must read back as zero")

	out, err := os.CreateTemp(t.TempDir(), "diff-*")
	require.NoError(t, err)
	defer out.Close()

	diffMetadata, err := cache.ExportToDiff(t.Context(), out)
	require.NoError(t, err)

	require.EqualValues(t, 0, diffMetadata.Dirty.GetCardinality())
	require.EqualValues(t, 2, diffMetadata.Empty.GetCardinality())
}

func TestCacheExportToDiff_NonContiguousDirtyBlocksPreserveRangeOrder(t *testing.T) {
	t.Parallel()

	const blockSize = header.RootfsBlockSize
	const size = blockSize * 5

	cache, err := NewCache(size, blockSize, t.TempDir()+"/cache", false)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, cache.Close())
	})

	firstBlock := bytes.Repeat([]byte{0x11}, int(blockSize))
	secondBlock := bytes.Repeat([]byte{0x22}, int(blockSize))

	_, err = cache.WriteAt(firstBlock, 0)
	require.NoError(t, err)

	_, err = cache.WriteAt(secondBlock, 3*blockSize)
	require.NoError(t, err)

	out, err := os.CreateTemp(t.TempDir(), "diff-*")
	require.NoError(t, err)
	defer out.Close()

	diffMetadata, err := cache.ExportToDiff(t.Context(), out)
	require.NoError(t, err)

	require.EqualValues(t, 2, diffMetadata.Dirty.GetCardinality())
	require.True(t, diffMetadata.Dirty.Contains(0))
	require.True(t, diffMetadata.Dirty.Contains(3))
	require.EqualValues(t, 0, diffMetadata.Empty.GetCardinality())

	_, err = out.Seek(0, io.SeekStart)
	require.NoError(t, err)
	exported, err := io.ReadAll(out)
	require.NoError(t, err)
	require.Equal(t, append(firstBlock, secondBlock...), exported)
}

func TestCache_ZeroLengthIsCachedAndSetIsCached(t *testing.T) {
	t.Parallel()

	const blockSize int64 = 4096
	const size int64 = blockSize * 10

	cache, err := NewCache(size, blockSize, t.TempDir()+"/cache", false)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, cache.Close())
	})

	// Before any writes, isCached(0, blockSize) should be false
	require.False(t, cache.isCached(0, blockSize), "block 0 should not be cached initially")

	// Zero-length isCached should return true (vacuous truth) and NOT check dirty map
	require.True(t, cache.isCached(0, 0), "zero-length isCached should return true (no-op)")

	// Zero-length setIsCached should be a no-op and NOT mark anything as cached
	cache.setIsCached(0, 0)

	// Block 0 should still not be cached after zero-length setIsCached
	require.False(t, cache.isCached(0, blockSize), "block 0 should still not be cached after zero-length setIsCached")

	// Test with various offsets to ensure zero-length is always a no-op
	cache.setIsCached(blockSize, 0)
	require.False(t, cache.isCached(blockSize, blockSize), "block 1 should not be cached after zero-length setIsCached")

	cache.setIsCached(blockSize*5, 0)
	require.False(t, cache.isCached(blockSize*5, blockSize), "block 5 should not be cached after zero-length setIsCached")
}

func compareData(readBytes []byte, expectedBytes []byte) error {
	// The bytes.Equal is the first place in this flow that actually touches the uffd managed memory and triggers the pagefault, so any deadlocks will manifest here.
	if !bytes.Equal(readBytes, expectedBytes) {
		idx, want, got := testutils.FirstDifferentByte(readBytes, expectedBytes)

		return fmt.Errorf("content mismatch: want '%x, got %x at index %d", want, got, idx)
	}

	return nil
}

func TestSplitOversizedRanges(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		ranges   []Range
		maxSize  int64
		expected []Range
	}{
		{
			name:     "empty input",
			ranges:   nil,
			maxSize:  100,
			expected: nil,
		},
		{
			name: "all ranges within limit",
			ranges: []Range{
				{Start: 0, Size: 50},
				{Start: 100, Size: 50},
			},
			maxSize: 100,
			expected: []Range{
				{Start: 0, Size: 50},
				{Start: 100, Size: 50},
			},
		},
		{
			name: "range exactly at limit",
			ranges: []Range{
				{Start: 0, Size: 100},
			},
			maxSize: 100,
			expected: []Range{
				{Start: 0, Size: 100},
			},
		},
		{
			name: "single oversized range splits evenly",
			ranges: []Range{
				{Start: 0, Size: 300},
			},
			maxSize: 100,
			expected: []Range{
				{Start: 0, Size: 100},
				{Start: 100, Size: 100},
				{Start: 200, Size: 100},
			},
		},
		{
			name: "single oversized range with remainder",
			ranges: []Range{
				{Start: 0, Size: 250},
			},
			maxSize: 100,
			expected: []Range{
				{Start: 0, Size: 100},
				{Start: 100, Size: 100},
				{Start: 200, Size: 50},
			},
		},
		{
			name: "mixed ranges - some need splitting",
			ranges: []Range{
				{Start: 0, Size: 50},
				{Start: 100, Size: 250},
				{Start: 400, Size: 80},
			},
			maxSize: 100,
			expected: []Range{
				{Start: 0, Size: 50},
				{Start: 100, Size: 100},
				{Start: 200, Size: 100},
				{Start: 300, Size: 50},
				{Start: 400, Size: 80},
			},
		},
		{
			name: "range just over limit",
			ranges: []Range{
				{Start: 0, Size: 101},
			},
			maxSize: 100,
			expected: []Range{
				{Start: 0, Size: 100},
				{Start: 100, Size: 1},
			},
		},
		{
			name: "preserves start addresses correctly",
			ranges: []Range{
				{Start: 1000, Size: 250},
			},
			maxSize: 100,
			expected: []Range{
				{Start: 1000, Size: 100},
				{Start: 1100, Size: 100},
				{Start: 1200, Size: 50},
			},
		},
		{
			name: "demonstrate unoptimal split",
			ranges: []Range{
				{Start: 1000, Size: 250},
				{Start: 1250, Size: 250},
			},
			maxSize: 240,
			expected: []Range{
				{Start: 1000, Size: 240},
				{Start: 1240, Size: 10},
				{Start: 1250, Size: 240},
				{Start: 1490, Size: 10},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := splitOversizedRanges(tt.ranges, tt.maxSize)
			require.Equal(t, tt.expected, result)
		})
	}
}

// This test is used to verify that the code correctly splits the ranges when the total size exceeds MAX_RW_COUNT.
func TestCopyFromProcess_Exceed_MAX_RW_COUNT(t *testing.T) {
	t.Parallel()

	pageSize := int64(header.PageSize)
	// We allocate more than MAX_RW_COUNT to trigger the MAX_RW_COUNT error if the ranges are not split correctly.
	size := ((MAX_RW_COUNT + 4*pageSize + pageSize - 1) / pageSize) * pageSize

	// Initialize the memory we will copy from.
	mem, addr, err := testutils.NewPageMmap(t, uint64(size), uint64(pageSize))
	require.NoError(t, err)

	n, err := rand.Read(mem)
	require.NoError(t, err)
	require.Equal(t, len(mem), n)

	ranges := []Range{
		// We make it so that at least one of the ranges is larger than MAX_RW_COUNT.
		{Start: int64(addr), Size: ((MAX_RW_COUNT + 2*pageSize + pageSize - 1) / pageSize) * pageSize},
		{Start: int64(addr) + ((MAX_RW_COUNT+2*pageSize+pageSize-1)/pageSize)*pageSize, Size: ((2*pageSize + pageSize - 1) / pageSize) * pageSize},
	}

	cache, err := NewCacheFromProcessMemory(
		t.Context(),
		// Regular 4KiB pages for the cache/mmap we will copy to.
		header.PageSize,
		t.TempDir()+"/cache",
		os.Getpid(),
		ranges,
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		cache.Close()
	})

	data := make([]byte, size)
	n, err = cache.ReadAt(data, 0)
	require.NoError(t, err)
	require.Equal(t, int(size), n)
	require.NoError(t, compareData(data[:n], mem[0:size]))
}

// Tests for a misalignment of the block size and the MAX_RW_COUNT that causes incorrect dirty tracking.
func TestCopyFromProcess_MAX_RW_COUNT_Misalignment_Hugepage(t *testing.T) {
	t.Parallel()

	pageSize := int64(header.HugepageSize)
	// We allocate more than MAX_RW_COUNT/pageSize + 2 to misalign the dirty tracking if the range split is unaligned to the block size.
	size := ((MAX_RW_COUNT/pageSize + 2) * pageSize)

	mem, addr, err := testutils.NewPageMmap(t, uint64(size), uint64(pageSize))
	require.NoError(t, err)

	n, err := rand.Read(mem)
	require.NoError(t, err)
	require.Equal(t, len(mem), n)

	ranges := []Range{
		{Start: int64(addr), Size: size},
	}

	cache, err := NewCacheFromProcessMemory(
		t.Context(),
		header.HugepageSize,
		t.TempDir()+"/cache",
		os.Getpid(),
		ranges,
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		cache.Close()
	})

	for _, offset := range header.BlocksOffsets(size, pageSize) {
		buf := make([]byte, pageSize)
		n, err := cache.ReadAt(buf, offset)
		require.NoError(t, err)
		require.Equal(t, int(pageSize), n)
		require.NoError(t, compareData(buf, mem[offset:offset+pageSize]))
	}
}

func BenchmarkCopyFromHugepagesFile(b *testing.B) {
	pageSize := int64(header.HugepageSize)
	size := pageSize * 500

	b.StopTimer()
	for {
		l := int(math.Ceil(float64(size)/float64(pageSize)) * float64(pageSize))
		mem, err := syscall.Mmap(
			-1,
			0,
			l,
			syscall.PROT_READ|syscall.PROT_WRITE,
			syscall.MAP_PRIVATE|syscall.MAP_ANONYMOUS|unix.MAP_HUGETLB|unix.MAP_HUGE_2MB,
		)

		require.NoError(b, err)

		addr := uintptr(unsafe.Pointer(&mem[0]))

		n, err := rand.Read(mem)
		require.NoError(b, err)
		require.Equal(b, len(mem), n)

		var totalCovered int64
		numRanges := 40
		ranges := make([]Range, 0, numRanges)
		cur := int64(addr)

		for i := range numRanges {
			sizePages := int64(1 + (i % 5)) // pseudo-random but deterministic
			sizeR := sizePages * pageSize
			if totalCovered+sizeR > size*8/10 && i > 0 { // Stop if we have covered ~80% total
				break
			}
			ranges = append(ranges, Range{
				Start: cur,
				Size:  sizeR,
			})
			cur += sizeR + pageSize // GAP of 1 page between each range
			totalCovered += sizeR
		}

		pid := os.Getpid()

		filePath := b.TempDir() + "/cache"

		size := GetSize(ranges)

		cache, err := NewCache(size, header.PageSize, filePath, false)
		require.NoError(b, err)

		b.StartTimer()
		if !b.Loop() {
			b.StopTimer()
			err = cache.Close()
			require.NoError(b, err)
			err = syscall.Munmap(mem)
			require.NoError(b, err)

			break
		}

		err = cache.copyProcessMemory(b.Context(), pid, ranges)
		require.NoError(b, err)

		b.StopTimer()

		err = cache.Close()
		require.NoError(b, err)

		err = syscall.Munmap(mem)
		require.NoError(b, err)

		b.SetBytes(GetSize(ranges))
	}
}

// buildPackedSrcCache packs mem's dirty ranges in BitsetRanges order.
func buildPackedSrcCache(t *testing.T, mem []byte, dirty *roaring.Bitmap, blockSize int64) *Cache {
	t.Helper()

	var packed []byte
	for r := range BitsetRanges(dirty, blockSize) {
		packed = append(packed, mem[r.Start:r.Start+r.Size]...)
	}

	cache, err := NewCache(int64(len(packed)), blockSize, t.TempDir()+"/src-cache", false)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cache.Close() })

	if len(packed) > 0 {
		_, err = cache.WriteAt(packed, 0)
		require.NoError(t, err)
	}

	return cache
}

func runDedup(t *testing.T, srcMem, baseMem []byte, dirty *roaring.Bitmap, blockSize int64) (*Cache, *header.DiffMetadata) {
	t.Helper()

	src := buildPackedSrcCache(t, srcMem, dirty, blockSize)
	cache, meta, err := src.Dedup(t.Context(), &fakeOriginalDevice{data: baseMem}, dirty, blockSize, t.TempDir()+"/dedup", false, false, DedupBudget{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cache.Close() })

	return cache, meta
}

func TestCacheDedup_AllPagesDiffer(t *testing.T) {
	t.Parallel()

	pageSize := int64(header.PageSize)
	blockSize := 4 * pageSize
	size := blockSize * 2

	srcData := make([]byte, size)
	_, err := rand.Read(srcData)
	require.NoError(t, err)
	origData := make([]byte, size)

	cache, meta := runDedup(t, srcData, origData, fullDirty(size, blockSize), blockSize)

	numPages := size / pageSize
	require.EqualValues(t, numPages, meta.Dirty.GetCardinality())
	for i := range numPages {
		got := make([]byte, pageSize)
		_, err := cache.ReadAt(got, i*pageSize)
		require.NoError(t, err)
		require.Equal(t, srcData[i*pageSize:(i+1)*pageSize], got, "page %d", i)
	}
}

// Regression: pageIdx must use the absolute guest offset (r.Start+chunkOff+i),
// and cacheOff must advance correctly across non-contiguous Ranges.
func TestCacheDedup_NonZeroRangeStartAndMultipleRanges(t *testing.T) {
	t.Parallel()

	pageSize := int64(header.PageSize)
	blockSize := 4 * pageSize
	size := blockSize * 4

	origData := make([]byte, size)
	_, err := rand.Read(origData)
	require.NoError(t, err)
	srcData := make([]byte, size)
	copy(srcData, origData)

	p1 := bytes.Repeat([]byte{0xD1}, int(pageSize))
	p13 := bytes.Repeat([]byte{0xD2}, int(pageSize))
	copy(srcData[1*pageSize:], p1)
	copy(srcData[13*pageSize:], p13)

	dirty := roaring.New()
	dirty.AddMany([]uint32{0, 3})

	cache, meta := runDedup(t, srcData, origData, dirty, blockSize)

	require.EqualValues(t, 2, meta.Dirty.GetCardinality())
	require.True(t, meta.Dirty.Contains(1))
	require.True(t, meta.Dirty.Contains(13))

	got := make([]byte, pageSize)
	_, err = cache.ReadAt(got, 0)
	require.NoError(t, err)
	require.Equal(t, p1, got)
	_, err = cache.ReadAt(got, pageSize)
	require.NoError(t, err)
	require.Equal(t, p13, got)
}

// Regression: BitsetRanges merges contiguous bits into one Range; the inner
// loop must walk r.Size, not blockSize.
func TestCacheDedup_ContiguousDirtyBlocks(t *testing.T) {
	t.Parallel()

	pageSize := int64(header.PageSize)
	blockSize := 4 * pageSize
	size := blockSize * 5

	origData := make([]byte, size)
	_, err := rand.Read(origData)
	require.NoError(t, err)
	srcData := make([]byte, size)
	copy(srcData, origData)

	pages := [3][]byte{
		bytes.Repeat([]byte{0xA1}, int(pageSize)),
		bytes.Repeat([]byte{0xA2}, int(pageSize)),
		bytes.Repeat([]byte{0xA3}, int(pageSize)),
	}
	for i, idx := range []int64{4, 9, 14} {
		copy(srcData[idx*pageSize:], pages[i])
	}

	dirty := roaring.New()
	dirty.AddMany([]uint32{1, 2, 3})

	cache, meta := runDedup(t, srcData, origData, dirty, blockSize)

	require.EqualValues(t, 3, meta.Dirty.GetCardinality())
	for i, want := range pages {
		got := make([]byte, pageSize)
		_, err := cache.ReadAt(got, int64(i)*pageSize)
		require.NoError(t, err)
		require.Equal(t, want, got, "packed page %d", i)
	}
}

func TestCacheDedup_ContextCancellation(t *testing.T) {
	t.Parallel()

	blockSize := 4 * int64(header.PageSize)
	size := blockSize * 4
	data := make([]byte, size)
	_, err := rand.Read(data)
	require.NoError(t, err)

	dirty := fullDirty(size, blockSize)
	src := buildPackedSrcCache(t, data, dirty, blockSize)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, _, err = src.Dedup(ctx, &fakeOriginalDevice{data: data}, dirty, blockSize, t.TempDir()+"/dedup", false, false, DedupBudget{})
	require.ErrorIs(t, err, context.Canceled)
}

func TestCacheDedup_OriginalMemfileReadError(t *testing.T) {
	t.Parallel()

	blockSize := 4 * int64(header.PageSize)
	data := make([]byte, blockSize)
	_, err := rand.Read(data)
	require.NoError(t, err)

	dirty := fullDirty(blockSize, blockSize)
	src := buildPackedSrcCache(t, data, dirty, blockSize)

	sentinel := errors.New("base read failed")
	_, _, err = src.Dedup(
		t.Context(),
		&erroringOriginalDevice{fakeOriginalDevice: fakeOriginalDevice{data: data}, err: sentinel},
		dirty,
		blockSize,
		t.TempDir()+"/dedup",
		false,
		false,
		DedupBudget{},
	)
	require.ErrorIs(t, err, sentinel)
}

// Empty parent mapping: dedup must skip base.ReadAt entirely.
func TestCacheDedup_EmptyParentMappingSkipsBaseReadAt(t *testing.T) {
	t.Parallel()

	pageSize := int64(header.PageSize)
	blockSize := 4 * pageSize
	size := blockSize * 2

	// Header reports Empty, so base bytes don't matter.
	srcData := make([]byte, size)
	_, err := rand.Read(srcData)
	require.NoError(t, err)

	// Block 1 is zero source → exercises pageDirty + pageEmpty in one run.
	clear(srcData[blockSize : blockSize*2])

	hdr, err := header.NewHeader(
		header.NewTemplateMetadata(uuid.Nil, uint64(blockSize), uint64(size)),
		[]header.BuildMap{{Offset: 0, Length: uint64(size), BuildId: uuid.Nil}},
	)
	require.NoError(t, err)

	dirty := fullDirty(size, blockSize)
	src := buildPackedSrcCache(t, srcData, dirty, blockSize)

	// Non-zero base junk: a regression that read base would misclassify pages.
	junk := bytes.Repeat([]byte{0xFF}, int(size))
	base := &fakeOriginalDevice{data: junk, hdr: hdr}

	cache, meta, err := src.Dedup(t.Context(), base, dirty, blockSize, t.TempDir()+"/dedup", false, false, DedupBudget{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cache.Close() })

	require.Zero(t, base.reads, "uuid.Nil parent fast path must not call base.ReadAt")

	require.EqualValues(t, blockSize/pageSize, meta.Dirty.GetCardinality())
	require.EqualValues(t, blockSize/pageSize, meta.Empty.GetCardinality())

	for i := range blockSize / pageSize {
		got := make([]byte, pageSize)
		_, err := cache.ReadAt(got, i*pageSize)
		require.NoError(t, err)
		require.Equal(t, srcData[i*pageSize:(i+1)*pageSize], got, "dirty page %d", i)
	}
}

// Zero matches → pageEmpty (uuid.Nil); non-zero matches → drop to parent.
func TestCacheDedup_ZeroMatchingPagesGoIntoEmpty(t *testing.T) {
	t.Parallel()

	pageSize := int64(header.PageSize)
	blockSize := 4 * pageSize
	size := blockSize * 2

	// Base: pages 0..3 zero, pages 4..7 non-zero. Source matches base exactly.
	origData := make([]byte, size)
	_, err := rand.Read(origData[4*pageSize:])
	require.NoError(t, err)
	srcData := make([]byte, size)
	copy(srcData, origData)

	cache, meta := runDedup(t, srcData, origData, fullDirty(size, blockSize), blockSize)

	require.EqualValues(t, 0, meta.Dirty.GetCardinality())
	require.EqualValues(t, header.PageSize, meta.BlockSize)
	sz, err := cache.Size()
	require.NoError(t, err)
	require.EqualValues(t, 0, sz)

	require.EqualValues(t, 4, meta.Empty.GetCardinality())
	for i := range uint32(4) {
		require.True(t, meta.Empty.Contains(i))
	}
	for i := uint32(4); i < 8; i++ {
		require.False(t, meta.Empty.Contains(i))
	}
}

func TestCacheDedup_ZeroPagesOverrideNonZeroBase(t *testing.T) {
	t.Parallel()

	pageSize := int64(header.PageSize)
	blockSize := 4 * pageSize
	size := blockSize

	origData := make([]byte, size)
	_, err := rand.Read(origData)
	require.NoError(t, err)
	srcData := make([]byte, size)

	cache, meta := runDedup(t, srcData, origData, fullDirty(size, blockSize), blockSize)

	require.EqualValues(t, 0, meta.Dirty.GetCardinality())
	require.EqualValues(t, size/pageSize, meta.Empty.GetCardinality())
	sz, err := cache.Size()
	require.NoError(t, err)
	require.Zero(t, sz)
}

// Best-effort + base uncached: skip base.ReadAt, write every non-zero
// page through.
func TestCacheDedup_BestEffortUncachedSkipsBaseReadAt(t *testing.T) {
	t.Parallel()

	pageSize := int64(header.PageSize)
	blockSize := 4 * pageSize
	size := blockSize * 2

	srcData := make([]byte, size)
	_, err := rand.Read(srcData)
	require.NoError(t, err)
	// Zero the last page so we exercise the empty routing.
	clear(srcData[size-pageSize : size])

	// Identical base catches accidental fallthrough to the compare path.
	baseData := make([]byte, size)
	copy(baseData, srcData)

	dirty := fullDirty(size, blockSize)
	src := buildPackedSrcCache(t, srcData, dirty, blockSize)
	base := &peekingOriginalDevice{fakeOriginalDevice: fakeOriginalDevice{data: baseData}, cached: false}

	cache, meta, err := src.Dedup(t.Context(), base, dirty, blockSize, t.TempDir()+"/dedup", true, false, DedupBudget{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cache.Close() })

	require.Zero(t, base.reads, "best-effort uncached path must not call base.ReadAt")

	totalPages := uint64(size / pageSize)
	require.Equal(t, totalPages-1, meta.Dirty.GetCardinality(), "non-zero pages written as dirty")
	require.EqualValues(t, 1, meta.Empty.GetCardinality(), "zero page still routed to Empty")
	require.True(t, meta.Empty.Contains(uint32(totalPages-1)))
}

// Best-effort + cache hit must behave like the normal compare path.
func TestCacheDedup_BestEffortCachedMatchesNormalPath(t *testing.T) {
	t.Parallel()

	pageSize := int64(header.PageSize)
	blockSize := 4 * pageSize
	size := blockSize * 2

	srcData := make([]byte, size)
	_, err := rand.Read(srcData)
	require.NoError(t, err)
	baseData := make([]byte, size)
	copy(baseData, srcData)
	// Force one differing page in block 0.
	srcData[pageSize] ^= 0xFF

	dirty := fullDirty(size, blockSize)
	src := buildPackedSrcCache(t, srcData, dirty, blockSize)
	base := &peekingOriginalDevice{fakeOriginalDevice: fakeOriginalDevice{data: baseData}, cached: true}

	cache, meta, err := src.Dedup(t.Context(), base, dirty, blockSize, t.TempDir()+"/dedup", true, false, DedupBudget{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cache.Close() })

	require.EqualValues(t, 1, meta.Dirty.GetCardinality(), "only the genuinely differing page is dirty")
	require.EqualValues(t, 0, meta.Empty.GetCardinality())
}

type perPagePeeker struct {
	fakeOriginalDevice

	cachedPages map[uint32]bool
}

func (p *perPagePeeker) IsCached(_ context.Context, off, _ int64) bool {
	return p.cachedPages[uint32(off/int64(header.PageSize))]
}

// Best-effort decides per page, not per block: when the parent has cached and
// uncached pages inside the same dedup block, only the uncached ones get
// written through. The earlier block-granular check gave up dedup on the
// whole block as soon as any page inside it was uncached.
func TestCacheDedup_BestEffortPerPageCacheCheck(t *testing.T) {
	t.Parallel()

	pageSize := int64(header.PageSize)
	blockSize := 4 * pageSize
	size := blockSize

	srcData := make([]byte, size)
	_, err := rand.Read(srcData)
	require.NoError(t, err)
	baseData := make([]byte, size)
	copy(baseData, srcData)

	dirty := fullDirty(size, blockSize)
	src := buildPackedSrcCache(t, srcData, dirty, blockSize)
	base := &perPagePeeker{
		fakeOriginalDevice: fakeOriginalDevice{data: baseData},
		cachedPages:        map[uint32]bool{0: true, 1: false, 2: true, 3: false},
	}

	cache, meta, err := src.Dedup(t.Context(), base, dirty, blockSize, t.TempDir()+"/dedup", true, false, DedupBudget{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cache.Close() })

	require.EqualValues(t, 2, meta.Dirty.GetCardinality())
	require.True(t, meta.Dirty.Contains(1))
	require.True(t, meta.Dirty.Contains(3))
}

// TestCache_FileSize_MatchesActualAllocation writes a known number of full
// blocks through Cache and verifies that FileSize reports the on-disk
// allocation in bytes — i.e. one block written ⇒ one block on disk.
//
// This is a regression test for using the wrong block-size constant in the
// stat.Blocks math. Before the fix, FileSize multiplied stat.Blocks (POSIX
// 512-byte units) by statfs.Bsize (4096 on ext4), inflating the reported
// usage by 8× and stalling the build-cache disk-pressure eviction loop.
func TestCache_FileSize_MatchesActualAllocation(t *testing.T) {
	t.Parallel()

	const (
		blockSize = int64(4096)
		nBlocks   = int64(4)
		fileSize  = blockSize * 64 // sparse, only nBlocks actually allocated
	)

	cache, err := NewCache(fileSize, blockSize, t.TempDir()+"/cache", false)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cache.Close() })

	buf := make([]byte, blockSize)
	_, err = rand.Read(buf)
	require.NoError(t, err)
	for i := range nBlocks {
		_, err = cache.WriteAt(buf, i*blockSize)
		require.NoError(t, err)
	}

	got, err := cache.FileSize(t.Context())
	require.NoError(t, err)

	expected := nBlocks * blockSize
	t.Logf("wrote %d blocks of %d B; FileSize=%d B (expected %d B)", nBlocks, blockSize, got, expected)

	require.Equal(t, expected, got,
		"FileSize must report on-disk allocation in bytes; a value ~%d× expected suggests stat.Blocks was multiplied by statfs.Bsize instead of the POSIX 512",
		int64(4096)/int64(512))
}

// TestCacheExportToDiffWithMetadata_ProceedsUnderReadLock pins the lock
// contract of the deferred-seal export: ExportToDiffWithMetadata takes only the
// cache's READ lock (the cache it exports is frozen), so the live sandbox's
// miss-reads that fall through to the sealing layer proceed concurrently
// instead of stalling behind the full copy. The test holds c.mu.RLock itself —
// standing in for such a miss-read — and requires the export to complete while
// the lock is held: under the correct RLock the export proceeds; under an
// exclusive Lock it blocks on the held reader and the deadline fails the test.
func TestCacheExportToDiffWithMetadata_ProceedsUnderReadLock(t *testing.T) {
	t.Parallel()

	blockSize := int64(header.PageSize)
	numBlocks := int64(64)
	size := blockSize * numBlocks

	c, err := NewCache(size, blockSize, t.TempDir()+"/frozen.cow", false)
	require.NoError(t, err)
	defer c.Close()

	want := make([]byte, blockSize)
	for i := range want {
		want[i] = 0xAB
	}
	for i := range numBlocks {
		_, err = c.WriteAt(want, i*blockSize)
		require.NoError(t, err)
	}

	meta, err := c.DiffMetadata()
	require.NoError(t, err)

	// The exporter goroutine owns its output file end to end: if the timeout
	// branch below Fatals with the export still in flight (the regression this
	// test exists to catch), the test goroutine must not close a file the
	// exporter is writing — that buries the real failure under a data race
	// pointing at production code.
	outPath := t.TempDir() + "/diff"

	// A concurrent miss-read of the frozen sealing layer holds the read lock
	// for the whole export.
	c.mu.RLock()
	done := make(chan error, 1)
	go func() {
		out, oerr := os.Create(outPath)
		if oerr != nil {
			done <- oerr

			return
		}
		defer out.Close()
		_, eerr := c.ExportToDiffWithMetadata(t.Context(), out, meta)
		done <- eerr
	}()

	// Release the read lock before any assertion can FailNow: the deferred
	// c.Close needs the write lock, so failing while the RLock is still held
	// would hang the test instead of failing it.
	var exportErr error
	select {
	case exportErr = <-done:
	case <-time.After(10 * time.Second):
		c.mu.RUnlock()
		require.Fail(t, "export blocked behind a held read lock: ExportToDiffWithMetadata must take RLock, not Lock")
	}
	c.mu.RUnlock()
	require.NoError(t, exportErr)

	// Smoke-check the exported bytes. Every block holds the same pattern, so
	// this cannot distinguish packed from identity offsets — the range-packing
	// layout is pinned by
	// TestCacheExportToDiff_NonContiguousDirtyBlocksPreserveRangeOrder.
	got := make([]byte, blockSize)
	outFile, err := os.Open(outPath)
	require.NoError(t, err)
	defer outFile.Close()
	_, err = outFile.ReadAt(got, (numBlocks/2)*blockSize)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

// TestCacheWriteAtShared pins the shared-lock write contract the CoW window's
// sink relies on: content lands like WriteAt, concurrent single-writer-per-
// block writers proceed in parallel, and a write after Close fails with
// ErrCacheClosed — never a write into unmapped memory (Close holds the write
// lock across the unmap, so the shared lock excludes it structurally).
func TestCacheWriteAtShared(t *testing.T) {
	t.Parallel()

	const blockSize = int64(header.PageSize)
	const nBlocks = int64(8)
	cache, err := NewCache(blockSize*nBlocks, blockSize, t.TempDir()+"/cache", false)
	require.NoError(t, err)

	content := make([]byte, blockSize*nBlocks)
	_, err = rand.Read(content)
	require.NoError(t, err)
	// One all-zero block so the detect-zeroes punch path runs under the
	// shared lock too.
	clear(content[2*blockSize : 3*blockSize])

	var eg errgroup.Group
	for i := range nBlocks {
		eg.Go(func() error {
			off := i * blockSize
			_, writeErr := cache.WriteAtShared(content[off:off+blockSize], off)

			return writeErr
		})
	}
	require.NoError(t, eg.Wait())

	got := make([]byte, blockSize*nBlocks)
	_, err = cache.ReadAt(got, 0)
	require.NoError(t, err)
	require.True(t, bytes.Equal(content, got), "shared-lock writes must land like WriteAt")

	require.NoError(t, cache.Close())
	_, err = cache.WriteAtShared(content[:blockSize], 0)
	var closedErr *CacheClosedError
	require.ErrorAs(t, err, &closedErr, "a straggler write after Close must fail closed")
}
