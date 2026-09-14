//go:build linux

package block

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/edsrzf/mmap-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"
	"golang.org/x/sys/unix"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

const (
	oomMinBackoff = 100 * time.Millisecond
	oomMaxJitter  = 100 * time.Millisecond
)

var tracer = otel.Tracer("github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block")

type CacheClosedError struct {
	filePath string
}

func (e *CacheClosedError) Error() string {
	return fmt.Sprintf("block cache already closed for path %s", e.filePath)
}

func NewErrCacheClosed(filePath string) *CacheClosedError {
	return &CacheClosedError{
		filePath: filePath,
	}
}

type Cache struct {
	filePath  string
	size      int64
	blockSize int64
	mmap      *mmap.MMap
	mu        sync.RWMutex
	tracker   *Tracker // Dirty: payload in mmap; Zero: punched, emitted as Empty in the diff
	dirtyFile bool
	closed    atomic.Bool
}

func NewCache(size, blockSize int64, filePath string, dirtyFile bool) (*Cache, error) {
	f, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("error opening file: %w", err)
	}

	defer f.Close()

	if size == 0 {
		return &Cache{
			filePath:  filePath,
			size:      size,
			blockSize: blockSize,
			dirtyFile: dirtyFile,
			tracker:   NewTracker(),
		}, nil
	}

	// This should create a sparse file on Linux.
	err = f.Truncate(size)
	if err != nil {
		return nil, fmt.Errorf("error allocating file: %w", err)
	}

	if size > math.MaxInt {
		return nil, fmt.Errorf("size too big: %d > %d", size, math.MaxInt)
	}

	mm, err := mmap.MapRegion(f, int(size), mmap.RDWR, 0, 0)
	if err != nil {
		return nil, fmt.Errorf("error mapping file: %w", err)
	}

	return &Cache{
		mmap:      &mm,
		filePath:  filePath,
		size:      size,
		blockSize: blockSize,
		dirtyFile: dirtyFile,
		tracker:   NewTracker(),
	}, nil
}

func (c *Cache) isClosed() bool {
	return c.closed.Load()
}

// DiffMetadata returns the dirty/empty diff metadata from the tracker without
// copying any block data. It lets a deferred/background seal build the diff
// header (and scheduling metadata) synchronously while the actual reflink copy
// (ExportToDiff) runs off the critical path. The result matches what
// ExportToDiff computes internally, provided the cache is frozen (no writes) in
// between — which is guaranteed once the sandbox has been stopped and the cache
// ejected.
func (c *Cache) DiffMetadata() (*header.DiffMetadata, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.isClosed() {
		return nil, NewErrCacheClosed(c.filePath)
	}

	if c.mmap == nil {
		return header.NewDiffMetadata(c.blockSize, nil, nil), nil
	}

	dirty, empty := c.tracker.Export()

	return header.NewDiffMetadata(c.blockSize, dirty, empty), nil
}

func (c *Cache) ExportToDiff(ctx context.Context, out *os.File) (*header.DiffMetadata, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.exportToDiffLocked(ctx, out, nil)
}

// ExportToDiffWithMetadata copies the dirty ranges described by meta to out,
// using meta's bitmap instead of re-reading the tracker. Callers that captured a
// DiffMetadata earlier (e.g. the deferred rootfs seal, which reads it at setup
// and exports later in the background) use this so the copied ranges are
// guaranteed to match a header built from the same bitmap read.
func (c *Cache) ExportToDiffWithMetadata(ctx context.Context, out *os.File, meta *header.DiffMetadata) (*header.DiffMetadata, error) {
	// Read lock, not exclusive: this entry point exports a FROZEN cache (the
	// sealing layer after a swap) while the live sandbox's miss-reads still
	// fall through to it — an exclusive lock would stall every such read for
	// the full export. The export only reads cache state and copies to out;
	// writers (WriteAt, Close) still take the write lock, so they remain
	// excluded for the duration either way.
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.exportToDiffLocked(ctx, out, meta)
}

func (c *Cache) exportToDiffLocked(ctx context.Context, out *os.File, meta *header.DiffMetadata) (*header.DiffMetadata, error) {
	ctx, childSpan := tracer.Start(ctx, "export-to-diff")
	defer childSpan.End()

	if c.isClosed() {
		return nil, NewErrCacheClosed(c.filePath)
	}

	if c.mmap == nil {
		if meta != nil {
			return meta, nil
		}

		return header.NewDiffMetadata(c.blockSize, nil, nil), nil
	}

	f, err := os.Open(c.filePath)
	if err != nil {
		return nil, fmt.Errorf("error opening file: %w", err)
	}
	defer f.Close()

	src := int(f.Fd())

	// Explicit mmap flush is not necessary, because the kernel will handle that as part of the copy_file_range syscall.
	// Calling sync_file_range marks the range for writeback and starts it early.
	// This is just an optimization, so if it fails just log a warning and let copy_file_range do the actual work.
	err = unix.SyncFileRange(src, 0, c.size, unix.SYNC_FILE_RANGE_WRITE)
	if err != nil {
		logger.L().Warn(ctx, "error syncing file", zap.Error(err))
	}

	diffMetadata := meta
	if diffMetadata == nil {
		dirty, empty := c.tracker.Export()
		diffMetadata = header.NewDiffMetadata(c.blockSize, dirty, empty)
	}

	dst := int(out.Fd())
	var writeOffset int64
	var totalRanges int64
	fallback := false

	copyStart := time.Now()
	for r := range BitsetRanges(diffMetadata.Dirty, diffMetadata.BlockSize) {
		totalRanges++
		remaining := int(r.Size)
		readOffset := r.Start

		// The kernel may return short writes (e.g. capped at MAX_RW_COUNT on non-reflink filesystems),
		// so we loop until the full range is copied. The offset pointers are advanced by the kernel.
		for remaining > 0 {
			if !fallback {
				// On XFS this uses reflink automatically.
				n, err := unix.CopyFileRange(
					src,
					&readOffset,
					dst,
					&writeOffset,
					remaining,
					0,
				)
				switch {
				case errors.Is(err, syscall.EXDEV) || errors.Is(err, syscall.EOPNOTSUPP) || errors.Is(err, syscall.ENOSYS):
					fallback = true
					logger.L().Warn(ctx, "copy_file_range unsupported, falling back to normal copy", zap.Error(err))
				case err != nil:
					return nil, fmt.Errorf("error copying file range: %w", err)
				case n == 0:
					return nil, fmt.Errorf("copy_file_range returned 0 with %d bytes remaining", remaining)
				default:
					remaining -= n
				}
			}

			// CopyFileRange failed. Falling back to normal copy
			if fallback && remaining > 0 {
				if _, err := out.Seek(writeOffset, io.SeekStart); err != nil {
					return nil, fmt.Errorf("error seeking: %w", err)
				}
				sr := io.NewSectionReader(f, readOffset, int64(remaining))
				if _, err := io.Copy(out, sr); err != nil {
					return nil, fmt.Errorf("error copying file range. %w", err)
				}

				writeOffset += int64(remaining)
				remaining = 0
			}
		}
	}

	telemetry.SetAttributes(ctx,
		attribute.Int64("copy_ms", time.Since(copyStart).Milliseconds()),
		attribute.Int64("total_size_bytes", c.size),
		attribute.Int64("dirty_size_bytes", int64(diffMetadata.Dirty.GetCardinality())*c.blockSize),
		attribute.Int64("empty_size_bytes", int64(diffMetadata.Empty.GetCardinality())*c.blockSize),
		attribute.Int64("total_ranges", totalRanges),
	)

	return diffMetadata, nil
}

// dedupDrain writes pageDirty pages from src to outPath packed at PageSize.
func dedupDrain(
	ctx context.Context,
	src func(absOff int64) ([]byte, error),
	pageDirty *roaring.Bitmap,
	blockSize int64,
	outPath string,
	directIO bool,
) (*Cache, error) {
	openFlags := os.O_RDWR | os.O_CREATE
	if directIO {
		openFlags |= unix.O_DIRECT
	}
	f, err := os.OpenFile(outPath, openFlags, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open dedup cache: %w", err)
	}
	if want := int64(pageDirty.GetCardinality()) * header.PageSize; directIO && want > 0 {
		if fErr := unix.Fallocate(int(f.Fd()), 0, 0, want); fErr != nil {
			logger.L().Warn(ctx, "fallocate dedup cache; proceeding without preallocation", zap.Error(fErr))
		}
	}

	fileOff, err := drainDirtyPages(ctx, int(f.Fd()), src, pageDirty, blockSize)
	if err != nil {
		return nil, errors.Join(err, f.Close(), os.Remove(outPath))
	}

	if directIO {
		if err := f.Truncate(fileOff); err != nil {
			return nil, errors.Join(fmt.Errorf("truncate dedup cache: %w", err), f.Close(), os.Remove(outPath))
		}
	}
	if err := f.Close(); err != nil {
		return nil, errors.Join(err, os.Remove(outPath))
	}

	cache, err := NewCache(fileOff, header.PageSize, outPath, false)
	if err != nil {
		return nil, errors.Join(err, os.Remove(outPath))
	}
	cache.setIsCached(0, fileOff)

	return cache, nil
}

// drainDirtyPages packs pageDirty pages from src into fd. Mirrors
// Cache.copyProcessMemory: coalesce contiguous pages into ranges, carve at
// source-block boundaries, pre-split over MAX_RW_COUNT, then drainIovs.
func drainDirtyPages(ctx context.Context, fd int, src func(absOff int64) ([]byte, error), pageDirty *roaring.Bitmap, blockSize int64) (int64, error) {
	var ranges []Range
	for r := range BitsetRanges(pageDirty, header.PageSize) {
		for off := r.Start; off < r.End(); {
			blockOff := (off / blockSize) * blockSize
			chunkEnd := min(r.End(), blockOff+blockSize)
			ranges = append(ranges, Range{Start: off, Size: chunkEnd - off})
			off = chunkEnd
		}
	}
	ranges = splitOversizedRanges(ranges, getAlignedMaxRwCount(header.PageSize))

	if err := drainIovs(ranges, func(r Range) int64 { return r.Size }, header.PageSize,
		func(destOff int64, batch []Range, _ int64) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			iovs := make([][]byte, len(batch))
			for i, r := range batch {
				blockOff := (r.Start / blockSize) * blockSize
				buf, srcErr := src(blockOff)
				if srcErr != nil {
					return fmt.Errorf("slice src at %d: %w", blockOff, srcErr)
				}
				iovs[i] = buf[r.Start-blockOff : r.Start-blockOff+r.Size]
			}
			if err := pwritevAll(fd, destOff, iovs); err != nil {
				return fmt.Errorf("pwritev dedup pages: %w", err)
			}

			return nil
		}); err != nil {
		return 0, err
	}

	return GetSize(ranges), nil
}

// Dedup writes pages from c that differ from base, packed at PageSize, to
// outPath. bestEffort skips uncached blocks; directIO uses O_DIRECT.
func (c *Cache) Dedup(
	ctx context.Context,
	base ReadonlyDevice,
	dirty *roaring.Bitmap,
	blockSize int64,
	outPath string,
	bestEffort bool,
	directIO bool,
	budget DedupBudget,
) (*Cache, *header.DiffMetadata, error) {
	ctx, span := tracer.Start(ctx, "dedup-pages")
	defer span.End()

	// c is packed in BitsetRanges order; map abs offset → packed offset.
	packed := make(map[int64]int64, dirty.GetCardinality())
	var cum int64
	for r := range BitsetRanges(dirty, blockSize) {
		for chunkOff := int64(0); chunkOff < r.Size; chunkOff += blockSize {
			packed[r.Start+chunkOff] = cum
			cum += blockSize
		}
	}
	src := func(absOff int64) ([]byte, error) {
		idx, ok := packed[absOff]
		if !ok {
			return nil, fmt.Errorf("dedup src: %d not packed", absOff)
		}

		return c.Slice(idx, blockSize)
	}

	compareStart := time.Now()
	var plan *dedupPlan
	if err := RunFaultSafe(ctx, func() error {
		var compareErr error
		plan, compareErr = dedupCompare(ctx, src, base, dirty, blockSize, bestEffort, budget)

		return compareErr
	}); err != nil {
		if faultErr, ok := errors.AsType[*MemoryFaultError](err); ok {
			logger.L().Error(ctx, "memory fault comparing pages during dedup",
				zap.Error(err),
				zap.String("cache_path", c.filePath),
				zap.Uintptr("fault_addr", faultErr.Addr),
			)
		}

		return nil, nil, err
	}
	compareDur := time.Since(compareStart)

	writeStart := time.Now()
	cache, err := dedupDrain(ctx, src, plan.pageDirty, blockSize, outPath, directIO)
	if err != nil {
		return nil, nil, err
	}
	recordDedupAttrs(ctx, plan, int64(plan.pageEmpty.GetCardinality()), compareDur, time.Since(writeStart))

	return cache, &header.DiffMetadata{
		Dirty:     plan.pageDirty,
		Empty:     plan.pageEmpty,
		BlockSize: header.PageSize,
	}, nil
}

func (c *Cache) ReadAt(b []byte, off int64) (int, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.mmap == nil {
		return 0, nil
	}

	if c.isClosed() {
		return 0, NewErrCacheClosed(c.filePath)
	}

	slice, err := c.Slice(off, int64(len(b)))
	if err != nil {
		return 0, fmt.Errorf("error slicing mmap: %w", err)
	}

	return copy(b, slice), nil
}

func (c *Cache) WriteAt(b []byte, off int64) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.mmap == nil {
		return 0, nil
	}

	if c.isClosed() {
		return 0, NewErrCacheClosed(c.filePath)
	}

	return c.WriteAtWithoutLock(b, off)
}

func (c *Cache) Close() (e error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.mmap == nil {
		return os.RemoveAll(c.filePath)
	}

	succ := c.closed.CompareAndSwap(false, true)
	if !succ {
		return NewErrCacheClosed(c.filePath)
	}

	err := c.mmap.Unmap()
	if err != nil {
		e = errors.Join(e, fmt.Errorf("error unmapping mmap: %w", err))
	}

	// TODO: Move to to the scope of the caller
	e = errors.Join(e, os.RemoveAll(c.filePath))

	return e
}

func (c *Cache) Size() (int64, error) {
	if c.isClosed() {
		return 0, NewErrCacheClosed(c.filePath)
	}

	return c.size, nil
}

// Slice returns a slice of the mmap.
// When using Slice you must ensure thread safety, ideally by only writing to the same block once and the exposing the slice.
func (c *Cache) Slice(off, length int64) ([]byte, error) {
	if c.isClosed() {
		return nil, NewErrCacheClosed(c.filePath)
	}

	if c.mmap == nil {
		return nil, nil
	}

	if c.dirtyFile || c.isCached(off, length) {
		end := min(off+length, c.size)

		return (*c.mmap)[off:end], nil
	}

	return nil, BytesNotAvailableError{}
}

// sliceDirect returns a slice of the mmap without checking isCached.
// Used by the streaming chunker after the waiter mechanism has confirmed data availability.
func (c *Cache) sliceDirect(off, length int64) ([]byte, error) {
	if c.isClosed() {
		return nil, NewErrCacheClosed(c.filePath)
	}

	if c.mmap == nil {
		return nil, nil
	}

	if off < 0 || off >= c.size {
		return nil, BytesNotAvailableError{}
	}

	end := min(off+length, c.size)

	return (*c.mmap)[off:end], nil
}

// Zero blocks are treated as cached: the mmap region reads back as zero (punched).
func (c *Cache) isCached(off, length int64) bool {
	start := uint32(header.BlockIdx(off, c.blockSize))
	end := uint32(header.BlockCeilIdx(min(off+length, c.size), c.blockSize))

	return c.tracker.Present(start, end)
}

func (c *Cache) setIsCached(off, length int64) {
	start := uint32(header.BlockIdx(off, c.blockSize))
	end := uint32(header.BlockCeilIdx(off+length, c.blockSize))

	c.tracker.SetRange(start, end, Dirty)
}

// punchHole frees backing pages; clear() fallback if MADV_REMOVE is unsupported.
func (c *Cache) punchHole(off, length int64) {
	if err := unix.Madvise((*c.mmap)[off:off+length], unix.MADV_REMOVE); err != nil {
		clear((*c.mmap)[off : off+length])
	}
}

// WriteAtShared is WriteAt under the read (shared) lock: callers that
// already guarantee exactly one writer per block — the CoW window's claim
// map — proceed in parallel with each other, while Close, which takes the
// write lock before unmapping, is structurally excluded. The exclusion is
// load-bearing: Close unmaps without nil'ing c.mmap, so WriteAtWithoutLock's
// guards cannot detect a closed cache, and a write racing the unmap is a
// SIGBUS that kills the whole process, not a failed write.
func (c *Cache) WriteAtShared(b []byte, off int64) (int, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.mmap == nil {
		return 0, nil
	}

	if c.isClosed() {
		return 0, NewErrCacheClosed(c.filePath)
	}

	return c.WriteAtWithoutLock(b, off)
}

// When using WriteAtWithoutLock you must ensure thread safety, ideally by only
// writing to the same block once and then exposing the slice. The caller must
// also exclude a concurrent Close for the write's whole duration (hold c.mu in
// some mode, as WriteAt/WriteAtShared do): Close unmaps without nil'ing
// c.mmap, so the guards below cannot detect it, and a write landing after the
// unmap is a process-fatal SIGBUS.
func (c *Cache) WriteAtWithoutLock(b []byte, off int64) (int, error) {
	if c.isClosed() {
		return 0, NewErrCacheClosed(c.filePath)
	}

	if c.mmap == nil {
		return 0, nil
	}

	if int64(len(b))%c.blockSize != 0 || off%c.blockSize != 0 {
		return 0, fmt.Errorf("misaligned write: len=%d off=%d block=%d", len(b), off, c.blockSize)
	}

	end := min(off+int64(len(b)), c.size)
	if end <= off {
		return 0, nil
	}

	// detect-zeroes=unmap: coalesce contiguous same-state blocks into one bulk
	// copy or punchHole call. Caller must pass a block-aligned write (NBD invariant).
	flush := func(runStart, runEnd int64, runZero bool) {
		startIdx := uint32(header.BlockIdx(runStart, c.blockSize))
		endIdx := uint32(header.BlockCeilIdx(runEnd, c.blockSize))
		if runZero {
			c.punchHole(runStart, runEnd-runStart)
			c.tracker.SetRange(startIdx, endIdx, Zero)
		} else {
			copy((*c.mmap)[runStart:runEnd], b[runStart-off:runEnd-off])
			c.tracker.SetRange(startIdx, endIdx, Dirty)
		}
	}

	runStart := off
	runZero := header.IsZero(b[:c.blockSize])
	for i := off + c.blockSize; i < end; i += c.blockSize {
		z := header.IsZero(b[i-off : i-off+c.blockSize])
		if z == runZero {
			continue
		}
		flush(runStart, i, runZero)
		runStart = i
		runZero = z
	}
	flush(runStart, end, runZero)

	return int(end - off), nil
}

// WriteZeroesAt punches the range and marks all touched blocks Zero.
// Caller must pass a block-aligned offset/length (NBD invariant).
func (c *Cache) WriteZeroesAt(off, length int64) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.mmap == nil {
		return 0, nil
	}

	if c.isClosed() {
		return 0, NewErrCacheClosed(c.filePath)
	}

	end := min(off+length, c.size)
	if end <= off {
		return 0, nil
	}

	c.punchHole(off, end-off)
	c.tracker.SetRange(
		uint32(header.BlockIdx(off, c.blockSize)),
		uint32(header.BlockCeilIdx(end, c.blockSize)),
		Zero,
	)

	return int(end - off), nil
}

// writeAtIfAbsent writes b at off only if the block is not already present in
// this cache, so a fold never overwrites a newer write the guest made after the
// swap. Callers must hold no other lock on this cache.
func (c *Cache) writeAtIfAbsent(b []byte, off int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.mmap == nil {
		return nil
	}

	if c.isClosed() {
		return NewErrCacheClosed(c.filePath)
	}

	if c.isCached(off, int64(len(b))) {
		return nil
	}

	_, err := c.WriteAtWithoutLock(b, off)

	return err
}

// FillMissingFrom copies every block present (Dirty or Zero) in older into this
// cache, but only where this cache does not already hold that block. After it
// returns, this cache is a superset of older ∪ itself — i.e. a complete diff
// again once older held "everything written before the swap" and this cache held
// "everything written after". Reading a Zero block from older yields zeroes,
// which writeAtIfAbsent re-detects and punches, so a single pass over the union
// handles both dirty and zero blocks.
//
// older must be frozen (no concurrent writes); this cache may be written
// concurrently by the guest — writeAtIfAbsent keeps each block's newest writer.
func (c *Cache) FillMissingFrom(older *Cache) error {
	if older == nil {
		return nil
	}

	meta, err := older.DiffMetadata()
	if err != nil {
		return fmt.Errorf("reading older cache metadata: %w", err)
	}

	present := meta.Dirty.Clone()
	present.Or(meta.Empty)

	buf := make([]byte, c.blockSize)
	for r := range BitsetRanges(present, c.blockSize) {
		for off := r.Start; off < r.End(); off += c.blockSize {
			if _, err := older.ReadAt(buf, off); err != nil {
				return fmt.Errorf("reading older block at %d: %w", off, err)
			}
			if err := c.writeAtIfAbsent(buf, off); err != nil {
				return fmt.Errorf("filling block at %d: %w", off, err)
			}
		}
	}

	return nil
}

// FileSize returns the size of the cache on disk.
// The size might differ from the dirty size, as it may not be fully on disk.
func (c *Cache) FileSize(_ context.Context) (int64, error) {
	var stat syscall.Stat_t
	if err := syscall.Stat(c.filePath, &stat); err != nil {
		return 0, fmt.Errorf("failed to get file stats: %w", err)
	}

	// Per POSIX (and Linux man 2 stat), stat.Blocks is always reported in
	// 512-byte units, regardless of the underlying filesystem's block size.
	const stBlockSize = 512

	return stat.Blocks * stBlockSize, nil
}

func (c *Cache) address(off int64) (*byte, error) {
	if c.mmap == nil {
		return nil, nil
	}

	if off >= c.size {
		return nil, fmt.Errorf("offset %d is out of bounds", off)
	}

	return &(*c.mmap)[off], nil
}

// addressBytes returns a slice of the mmap and a function to release the read lock which blocks the cache from being closed.
func (c *Cache) addressBytes(off, length int64) ([]byte, func(), error) {
	c.mu.RLock()

	if c.mmap == nil {
		c.mu.RUnlock()

		return nil, func() {}, nil
	}

	if c.isClosed() {
		c.mu.RUnlock()

		return nil, func() {}, NewErrCacheClosed(c.filePath)
	}

	if off >= c.size {
		c.mu.RUnlock()

		return nil, func() {}, fmt.Errorf("offset %d is out of bounds", off)
	}

	releaseCacheCloseLock := func() {
		c.mu.RUnlock()
	}

	end := min(off+length, c.size)

	return (*c.mmap)[off:end], releaseCacheCloseLock, nil
}

func (c *Cache) BlockSize() int64 {
	return c.blockSize
}

func (c *Cache) Path(_ context.Context) (string, error) {
	return c.filePath, nil
}

func NewCacheFromProcessMemory(
	ctx context.Context,
	blockSize int64,
	filePath string,
	pid int,
	ranges []Range,
) (*Cache, error) {
	size := GetSize(ranges)

	cache, err := NewCache(size, blockSize, filePath, false)
	if err != nil {
		return nil, err
	}

	if size == 0 {
		return cache, nil
	}

	err = cache.copyProcessMemory(ctx, pid, ranges)
	if err != nil {
		return nil, fmt.Errorf("failed to copy process memory: %w", errors.Join(err, cache.Close()))
	}

	return cache, nil
}

func (c *Cache) copyProcessMemory(
	ctx context.Context,
	pid int,
	rs []Range,
) error {
	// Pre-split so no single iov exceeds MAX_RW_COUNT.
	ranges := splitOversizedRanges(rs, getAlignedMaxRwCount(c.blockSize))

	return drainIovs(ranges, func(r Range) int64 { return r.Size }, c.blockSize,
		func(off int64, batch []Range, batchBytes int64) error {
			remote := make([]unix.RemoteIovec, len(batch))
			for i, r := range batch {
				remote[i] = unix.RemoteIovec{Base: uintptr(r.Start), Len: int(r.Size)}
			}
			address, err := c.address(off)
			if err != nil {
				return fmt.Errorf("failed to get address: %w", err)
			}
			local := []unix.Iovec{{Base: address, Len: uint64(batchBytes)}}

			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}

				n, err := unix.ProcessVMReadv(pid, local, remote, 0)
				if errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EINTR) {
					continue
				}
				if errors.Is(err, unix.ENOMEM) {
					time.Sleep(oomMinBackoff + time.Duration(rand.Intn(int(oomMaxJitter.Milliseconds())))*time.Millisecond)

					continue
				}
				if err != nil {
					return fmt.Errorf("failed to read memory: %w", err)
				}
				if int64(n) != batchBytes {
					return fmt.Errorf("failed to read memory: expected %d bytes, got %d", batchBytes, n)
				}

				c.setIsCached(off, batchBytes)

				return nil
			}
		})
}

// Split ranges so there are no ranges larger than maxSize.
// This is not an optimal split—ideally we would split the ranges so that we can fill each call to unix.ProcessVMReadv to the max size.
// This is though a very simple split that will work and the syscalls overhead here is not very high as opposed to the other things.
func splitOversizedRanges(ranges []Range, maxSize int64) (result []Range) {
	for _, r := range ranges {
		if r.Size <= maxSize {
			result = append(result, r)

			continue
		}

		for offset := int64(0); offset < r.Size; offset += maxSize {
			result = append(result, Range{
				Start: r.Start + offset,
				Size:  min(r.Size-offset, maxSize),
			})
		}
	}

	return result
}
