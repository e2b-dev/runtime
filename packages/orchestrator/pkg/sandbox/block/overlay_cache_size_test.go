//go:build linux

package block

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

// cacheSizeDevice is the smallest ReadonlyDevice an overlay needs.
type cacheSizeDevice struct {
	blockSize int64
}

func (d *cacheSizeDevice) ReadAt(context.Context, []byte, int64) (int, error)  { return 0, nil }
func (d *cacheSizeDevice) Slice(context.Context, int64, int64) ([]byte, error) { return nil, nil }
func (d *cacheSizeDevice) Size(context.Context) (int64, error)                 { return 0, nil }
func (d *cacheSizeDevice) Close() error                                        { return nil }
func (d *cacheSizeDevice) BlockSize() int64                                    { return d.blockSize }
func (d *cacheSizeDevice) Header() *header.Header                              { return nil }
func (d *cacheSizeDevice) SwapHeader(*header.Header)                           {}
func (d *cacheSizeDevice) DurableHeaderNow() (*header.Header, bool)            { return nil, false }
func (d *cacheSizeDevice) DurableHeader(context.Context) (*header.Header, error) {
	return nil, nil
}

// CacheSize is what the writable cache and, after a swap, the sealing cache
// occupy on disk together: both are copied by a rootfs export.
func TestOverlayCacheSizeCountsTheWritableAndSealingCaches(t *testing.T) {
	t.Parallel()

	const blockSize = int64(4096)
	dir := t.TempDir()

	writable, err := NewCache(16*blockSize, blockSize, filepath.Join(dir, "writable"), false)
	require.NoError(t, err)
	defer writable.Close()
	o := NewOverlay(&cacheSizeDevice{blockSize: blockSize}, writable)

	size, err := o.CacheSize(t.Context())
	require.NoError(t, err)
	assert.Zero(t, size, "a fresh sparse cache occupies nothing")

	_, err = writable.WriteAt(make([]byte, 2*blockSize), 0)
	require.NoError(t, err)
	writableSize, err := writable.FileSize(t.Context())
	require.NoError(t, err)
	size, err = o.CacheSize(t.Context())
	require.NoError(t, err)
	assert.Equal(t, writableSize, size)

	fresh, err := NewCache(16*blockSize, blockSize, filepath.Join(dir, "fresh"), false)
	require.NoError(t, err)
	defer fresh.Close()
	_, err = o.SwapCache(fresh)
	require.NoError(t, err)
	_, err = fresh.WriteAt(make([]byte, blockSize), 4*blockSize)
	require.NoError(t, err)

	freshSize, err := fresh.FileSize(t.Context())
	require.NoError(t, err)
	size, err = o.CacheSize(t.Context())
	require.NoError(t, err)
	assert.Equal(t, writableSize+freshSize, size, "the sealing cache is counted with the writable one")
}
