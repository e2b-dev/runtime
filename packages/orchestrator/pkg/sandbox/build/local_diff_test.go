//go:build linux

package build

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestLocalDiffFileCloseToDiffRemovesPartialOnError verifies CloseToDiff does not
// leave the partial cache file behind when it fails to produce a usable Diff.
// Nothing registers such a file in the DiffStore, so a leaked orphan would sit in
// the cache dir unreclaimable by disk-pressure eviction until process restart.
func TestLocalDiffFileCloseToDiffRemovesPartialOnError(t *testing.T) {
	t.Parallel()

	f, err := NewLocalDiffFile(t.TempDir(), "build-test-id", Rootfs)
	require.NoError(t, err)

	// Non-empty so we're past the zero-size NoDiff branch and into materialization.
	_, err = f.File.WriteAt(make([]byte, 128), 0)
	require.NoError(t, err)

	cachePath := f.cachePath
	require.FileExists(t, cachePath)

	// Force a materialization failure: closing the fd makes the Stat inside
	// CloseToDiff fail, driving the error path.
	require.NoError(t, f.File.Close())

	diff, err := f.CloseToDiff(blockSize)
	require.Error(t, err)
	require.Nil(t, diff)
	require.NoFileExists(t, cachePath, "partial diff file must be removed on materialization failure")
}

// TestLocalDiffFileCloseToDiffRemovesEmptyCacheFile verifies the zero-size NoDiff
// branch removes the cache file: NoDiff owns no path, so nothing downstream can.
func TestLocalDiffFileCloseToDiffRemovesEmptyCacheFile(t *testing.T) {
	t.Parallel()

	f, err := NewLocalDiffFile(t.TempDir(), "build-test-id", Rootfs)
	require.NoError(t, err)

	cachePath := f.cachePath
	require.FileExists(t, cachePath)

	// Nothing written, so CloseToDiff takes the zero-size NoDiff branch.
	diff, err := f.CloseToDiff(blockSize)
	require.NoError(t, err)
	require.IsType(t, &NoDiff{}, diff)
	require.NoFileExists(t, cachePath, "empty diff cache file must be removed")
}

// TestLocalDiffFileCloseToDiffServesWrittenBytes verifies the diff serves the
// bytes written through the file, as a copy that cannot reflink leaves them.
func TestLocalDiffFileCloseToDiffServesWrittenBytes(t *testing.T) {
	t.Parallel()

	f, err := NewLocalDiffFile(t.TempDir(), "build-test-id", Rootfs)
	require.NoError(t, err)

	payload := bytes.Repeat([]byte{0xA5, 0x5A}, 1<<19)
	_, err = f.File.WriteAt(payload, 0)
	require.NoError(t, err)

	diff, err := f.CloseToDiff(blockSize)
	require.NoError(t, err)
	t.Cleanup(func() { _ = diff.Close() })

	data, err := diff.Slice(t.Context(), 0, int64(len(payload)), nil)
	require.NoError(t, err)
	require.Equal(t, payload, data)
}
