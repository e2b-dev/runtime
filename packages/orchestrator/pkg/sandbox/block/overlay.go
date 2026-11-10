//go:build linux

package block

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"sync/atomic"

	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

type Overlay struct {
	device ReadonlyDevice
	// cache is the writable target. It is loaded atomically on every read/write
	// so it can be swapped for a fresh cache (SwapCache) while the overlay stays
	// live.
	cache atomic.Pointer[Cache]
	// sealing holds the previous writable cache after a SwapCache: it is frozen
	// (no new writes land in it) and kept as a read-only fallback so blocks
	// written before the swap still resolve, until it has been sealed to a diff
	// and released (ReleaseSealing).
	sealing      atomic.Pointer[Cache]
	cacheEjected atomic.Bool
	blockSize    int64
}

var _ Device = (*Overlay)(nil)

func NewOverlay(device ReadonlyDevice, cache *Cache) *Overlay {
	o := &Overlay{
		device:    device,
		blockSize: device.BlockSize(),
	}
	o.cache.Store(cache)

	return o
}

// isCacheClosed reports whether err is a *CacheClosedError. A sealing cache can
// be closed by a concurrent fold/collapse after ReadAt has loaded its pointer.
// Whoever closed it guarantees its blocks are already reachable elsewhere
// (ReleaseSealing contract) — but WHERE differs by flow: an in-place fold
// merges them into the WRITABLE CACHE, a collapse into the base. A reader that
// hits this must therefore re-consult the writable cache before falling
// through to the base device, or a pre-swap dirty block folded during the
// read would resolve to stale template bytes.
func isCacheClosed(err error) bool {
	var closed *CacheClosedError

	return errors.As(err, &closed)
}

func (o *Overlay) ReadAt(ctx context.Context, p []byte, off int64) (int, error) {
	// Snapshot both cache pointers once so a concurrent SwapCache/collapse can't
	// change the layering mid-read.
	cache := o.cache.Load()
	sealing := o.sealing.Load()

	blocks := header.BlocksOffsets(int64(len(p)), o.blockSize)

	for _, blockOff := range blocks {
		buf := p[blockOff : blockOff+o.blockSize]
		blockAbsOff := off + blockOff

		// 1. writable cache
		n, err := cache.ReadAt(buf, blockAbsOff)
		if err == nil {
			continue
		}
		if !errors.As(err, &BytesNotAvailableError{}) {
			return n, fmt.Errorf("error reading from cache: %w", err)
		}

		// 2. sealing (frozen previous) cache, if a swap is outstanding
		if sealing != nil {
			n, err = sealing.ReadAt(buf, blockAbsOff)
			if err == nil {
				continue
			}
			if isCacheClosed(err) {
				// A concurrent fold/collapse closed the sealing cache after we
				// loaded its pointer. Its blocks are now guaranteed reachable
				// through the writable cache (fold) or the base (collapse), so
				// re-read the cache — our first attempt predates the fold —
				// before falling through to base (see isCacheClosed).
				n, err = cache.ReadAt(buf, blockAbsOff)
				if err == nil {
					continue
				}
				if !errors.As(err, &BytesNotAvailableError{}) {
					return n, fmt.Errorf("error re-reading cache after sealing close: %w", err)
				}
			} else if !errors.As(err, &BytesNotAvailableError{}) {
				return n, fmt.Errorf("error reading from sealing cache: %w", err)
			}
		}

		// 3. read-only base device
		n, err = o.device.ReadAt(ctx, buf, blockAbsOff)
		if err != nil {
			return n, fmt.Errorf("error reading from device: %w", err)
		}
	}

	return len(p), nil
}

// SwapCache installs newCache as the writable target and moves the previous
// writable cache into the read-only "sealing" slot, returning it. Reads of
// blocks written before the swap keep resolving through the sealing cache; new
// writes land in newCache, so the returned cache is frozen and safe to seal
// (reflink/export) in the background while the VM keeps running.
//
// Only one cache may be sealing at a time — SwapCache errors if a previous one
// has not yet been released via ReleaseSealing. The caller must ensure no writes
// are in flight during the swap (the VM is FC-paused at snapshot time).
func (o *Overlay) SwapCache(newCache *Cache) (*Cache, error) {
	if o.cacheEjected.Load() {
		return nil, errors.New("cache ejected")
	}

	if o.sealing.Load() != nil {
		return nil, errors.New("a previous cache is still sealing")
	}

	old := o.cache.Load()
	// Publish the sealing pointer before repointing the writable target so a
	// concurrent reader never sees a gap where a pre-swap block resolves to
	// neither cache.
	o.sealing.Store(old)
	o.cache.Store(newCache)

	return old, nil
}

// ReleaseSealing detaches the sealing cache (if any) and clears the slot so a
// subsequent SwapCache can proceed. It returns the detached cache for the caller
// to Close; nil if none is sealing. Callers must guarantee the writable cache or
// base device can already serve the sealing cache's blocks (see FoldSealing)
// before closing it, so in-flight reads that fall through don't miss data.
func (o *Overlay) ReleaseSealing() *Cache {
	return o.sealing.Swap(nil)
}

// FoldSealing folds the sealing cache into the writable cache (copying only the
// blocks the writable cache doesn't already hold), then detaches and returns the
// sealing cache for the caller to Close. After a successful fold the writable
// cache is a complete diff again, so reads no longer need the sealing layer and
// a subsequent SwapCache can proceed. Returns (nil, nil) if nothing is sealing.
//
// On fold error the sealing cache is left attached (reads stay correct via the
// sealing fallback) and the error is returned; the caller must not close it.
func (o *Overlay) FoldSealing() (*Cache, error) {
	sealing := o.sealing.Load()
	if sealing == nil {
		return nil, nil
	}

	if err := o.cache.Load().FillMissingFrom(sealing); err != nil {
		return nil, err
	}

	return o.ReleaseSealing(), nil
}

// CacheSize reports what the writable cache and, while one is sealing, the
// sealing cache occupy on disk together: the bytes a rootfs export copies. A
// cache whose file is already gone counts as nothing.
func (o *Overlay) CacheSize(ctx context.Context) (int64, error) {
	var total int64
	for _, c := range []*Cache{o.cache.Load(), o.sealing.Load()} {
		if c == nil {
			continue
		}

		size, err := c.FileSize(ctx)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return 0, err
		}
		total += size
	}

	return total, nil
}

func (o *Overlay) EjectCache() (*Cache, error) {
	if !o.cacheEjected.CompareAndSwap(false, true) {
		return nil, errors.New("cache already ejected")
	}

	return o.cache.Load(), nil
}

// ExportDiffInPlace writes the overlay's dirty blocks to `out` without detaching
// the cache. The overlay stays usable for a sandbox that keeps running after the
// export.
func (o *Overlay) ExportDiffInPlace(ctx context.Context, out *os.File) (*header.DiffMetadata, error) {
	if o.cacheEjected.Load() {
		return nil, errors.New("cache ejected")
	}

	return o.cache.Load().ExportToDiff(ctx, out)
}

// This method will not be very optimal if the length is not the same as the block size, because we cannot be just exposing the cache slice,
// but creating and copying the bytes from the cache and device to the new slice.
//
// When we are implementing this we might want to just enforce the length to be the same as the block size.
func (o *Overlay) Slice(_ context.Context, _, _ int64) ([]byte, error) {
	return nil, errors.New("not implemented")
}

func (o *Overlay) WriteAt(p []byte, off int64) (int, error) {
	return o.cache.Load().WriteAt(p, off)
}

func (o *Overlay) WriteZeroesAt(off, length int64) (int, error) {
	return o.cache.Load().WriteZeroesAt(off, length)
}

func (o *Overlay) Size(_ context.Context) (int64, error) {
	return o.cache.Load().Size()
}

func (o *Overlay) BlockSize() int64 {
	return o.blockSize
}

func (o *Overlay) Close() error {
	var errs []error

	// Close any outstanding sealing cache first; it is never ejected (only the
	// writable cache is), so it always belongs to the overlay.
	if sealing := o.sealing.Swap(nil); sealing != nil {
		errs = append(errs, sealing.Close())
	}

	if !o.cacheEjected.Load() {
		errs = append(errs, o.cache.Load().Close())
	}

	return errors.Join(errs...)
}

func (o *Overlay) Header() *header.Header {
	return o.device.Header()
}

func (o *Overlay) SwapHeader(h *header.Header) {
	o.device.SwapHeader(h)
}
