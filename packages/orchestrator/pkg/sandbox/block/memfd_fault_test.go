//go:build linux

package block

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
)

// A memfd copy whose cache file cannot back its pages (a full filesystem, here a
// truncated file) returns a memory fault instead of killing the process. If the
// conversion regresses, this test crashes the whole test binary.
func TestCopyFromMemfd_CacheFaultIsAnError(t *testing.T) {
	t.Parallel()

	blockSize := int64(header.PageSize)
	size := 4 * blockSize
	memfd, _ := newTestMemfd(t, size)
	t.Cleanup(func() { _ = memfd.Close() })

	path := filepath.Join(t.TempDir(), "cache")
	cache, err := NewCache(size, blockSize, path, false)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cache.Close() })
	require.NoError(t, os.Truncate(path, 0))

	err = copyFromMemfd(t.Context(), cache, memfd, fullDirty(size, blockSize), blockSize)
	var fault *MemoryFaultError
	require.ErrorAs(t, err, &fault)
}
