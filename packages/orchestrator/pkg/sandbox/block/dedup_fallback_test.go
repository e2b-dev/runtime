//go:build linux

package block

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"unsafe"

	"github.com/edsrzf/mmap-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

// DedupOrRaw keeps the raw diff where Dedup fails, never on a memory fault or once the pause's ctx is done.
func TestCacheDedupOrRaw_ParentReadError(t *testing.T) {
	t.Parallel()

	blockSize := 4 * int64(header.PageSize)
	data := make([]byte, blockSize)
	_, err := rand.Read(data)
	require.NoError(t, err)

	dirty := fullDirty(blockSize, blockSize)
	src := buildPackedSrcCache(t, data, dirty, blockSize)
	sentinel := errors.New("base read failed")
	base := &erroringOriginalDevice{fakeOriginalDevice: fakeOriginalDevice{data: data}, err: sentinel}
	out := t.TempDir() + "/dedup"

	_, _, err = src.Dedup(t.Context(), base, dirty, blockSize, out, false, false, DedupBudget{})
	require.ErrorIs(t, err, sentinel)

	kept, meta, err := src.DedupOrRaw(t.Context(), base, dirty, blockSize, out, false, false, DedupBudget{})
	require.NoError(t, err)
	assert.Same(t, src, kept)
	assert.Nil(t, meta)
	_, statErr := os.Stat(out)
	require.ErrorIs(t, statErr, os.ErrNotExist, "a failed dedup leaves no output file")

	got := make([]byte, blockSize)
	_, err = kept.ReadAt(got, 0)
	require.NoError(t, err)
	assert.Equal(t, data, got)
}

func TestKeepRawOnDedupError(t *testing.T) {
	t.Parallel()

	blockSize := int64(header.PageSize)
	dirty := fullDirty(blockSize, blockSize)
	src := buildPackedSrcCache(t, make([]byte, blockSize), dirty, blockSize)
	inside := uintptr(unsafe.Pointer(unsafe.SliceData(*src.mmap))) + 16

	live := t.Context()
	done, cancel := context.WithCancel(t.Context())
	cancel()
	read := errors.New("base read failed")
	fetchTimeout := fmt.Errorf("fetch: %w", context.DeadlineExceeded)
	faultIn := fmt.Errorf("compare: %w", &MemoryFaultError{Addr: inside})
	faultOut := fmt.Errorf("compare: %w", &MemoryFaultError{Addr: inside + uintptr(len(*src.mmap))})
	faultUnknown := fmt.Errorf("compare: %w", &MemoryFaultError{})

	assert.True(t, src.keepRawOnDedupError(live, read))
	assert.True(t, src.keepRawOnDedupError(live, fetchTimeout), "a parent fetch's own timeout is an ordinary dedup failure")
	assert.True(t, src.keepRawOnDedupError(live, faultOut), "a fault reading the parent leaves the raw diff whole")
	assert.False(t, src.keepRawOnDedupError(live, faultIn), "a fault in the raw diff's own mapping may leave it unreadable")
	assert.False(t, src.keepRawOnDedupError(live, faultUnknown), "a fault at an unknown address is treated as the raw diff's")
	assert.False(t, src.keepRawOnDedupError(done, read))
	assert.False(t, src.keepRawOnDedupError(done, fetchTimeout))
}

// A real SIGBUS while reading the parent keeps the raw diff: the fault is not in it.
func TestCacheDedupOrRaw_ParentFaultKeepsTheRawDiff(t *testing.T) {
	t.Parallel()

	blockSize := 4 * int64(header.PageSize)
	data := make([]byte, blockSize)
	_, err := rand.Read(data)
	require.NoError(t, err)
	dirty := fullDirty(blockSize, blockSize)
	src := buildPackedSrcCache(t, data, dirty, blockSize)

	// Mapped pages beyond EOF raise SIGBUS on access, like a bad sector under the parent's cache file.
	path := filepath.Join(t.TempDir(), "parent")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })
	require.NoError(t, f.Truncate(blockSize))
	parent, err := mmap.MapRegion(f, int(blockSize), mmap.RDWR, 0, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = parent.Unmap() })
	require.NoError(t, os.Truncate(path, 0))

	_, _, err = src.Dedup(t.Context(), &fakeOriginalDevice{data: parent}, dirty, blockSize, t.TempDir()+"/dedup", false, false, DedupBudget{})
	var fault *MemoryFaultError
	require.ErrorAs(t, err, &fault, "the parent's fault reaches the dedup as a memory fault")

	kept, meta, err := src.DedupOrRaw(t.Context(), &fakeOriginalDevice{data: parent}, dirty, blockSize, t.TempDir()+"/dedup", false, false, DedupBudget{})
	require.NoError(t, err)
	assert.Same(t, src, kept)
	assert.Nil(t, meta)

	got := make([]byte, blockSize)
	_, err = kept.ReadAt(got, 0)
	require.NoError(t, err)
	assert.Equal(t, data, got)
}

// A pause whose context is cancelled during the dedup fails instead of uploading the raw diff.
func TestCacheDedupOrRaw_CancelledPause(t *testing.T) {
	t.Parallel()

	blockSize := 4 * int64(header.PageSize)
	data := make([]byte, blockSize)
	_, err := rand.Read(data)
	require.NoError(t, err)

	dirty := fullDirty(blockSize, blockSize)
	src := buildPackedSrcCache(t, data, dirty, blockSize)
	ctx, cancel := context.WithCancel(t.Context())
	base := &cancellingOriginalDevice{erroringOriginalDevice: erroringOriginalDevice{fakeOriginalDevice: fakeOriginalDevice{data: data}, err: context.Canceled}, cancel: cancel}

	_, _, err = src.DedupOrRaw(ctx, base, dirty, blockSize, t.TempDir()+"/dedup", false, false, DedupBudget{})
	require.ErrorIs(t, err, context.Canceled)
}

// cancellingOriginalDevice cancels the pause's ctx on its first read, then fails like erroringOriginalDevice.
type cancellingOriginalDevice struct {
	erroringOriginalDevice

	cancel context.CancelFunc
}

func (c *cancellingOriginalDevice) Slice(ctx context.Context, off, length int64) ([]byte, error) {
	c.cancel()

	return c.erroringOriginalDevice.Slice(ctx, off, length)
}

func (c *cancellingOriginalDevice) ReadAt(ctx context.Context, p []byte, off int64) (int, error) {
	c.cancel()

	return c.erroringOriginalDevice.ReadAt(ctx, p, off)
}
