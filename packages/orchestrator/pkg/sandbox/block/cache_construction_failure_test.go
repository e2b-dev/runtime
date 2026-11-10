//go:build linux

package block

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestCacheConstructionFailureRemovesNewOutput(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "new-output")
	cache, err := NewCache(-1, int64(os.Getpagesize()), path, false)
	require.Nil(t, cache)
	require.ErrorIs(t, err, unix.EINVAL, "the real truncate failure must remain visible")
	_, statErr := os.Stat(path)
	require.ErrorIs(t, statErr, os.ErrNotExist, "failed construction must remove its newly created output")
}

func TestCacheConstructionFailurePreservesExistingOutput(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"regular", "symlink", "dangling-symlink"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			target := filepath.Join(dir, "caller-owned")
			path := target
			data := []byte("already written diff")
			if mode == "dangling-symlink" {
				data = nil
			} else {
				require.NoError(t, os.WriteFile(target, data, 0o600))
			}
			if mode != "regular" {
				path = filepath.Join(dir, "existing-link")
				require.NoError(t, os.Symlink(target, path))
			}
			before, err := os.Lstat(path)
			require.NoError(t, err)
			cache, err := NewCache(-1, int64(os.Getpagesize()), path, false)
			require.Nil(t, cache)
			require.ErrorIs(t, err, unix.EINVAL)
			after, err := os.Lstat(path)
			require.NoError(t, err, "failed construction must not remove a pre-existing caller-owned path")
			require.True(t, os.SameFile(before, after))
			actual, err := os.ReadFile(target)
			require.NoError(t, err)
			assert.Equal(t, string(data), string(actual))
		})
	}
}

func TestCacheConstructionExistingOutputTransfersOnSuccess(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "written-diff")
	data := bytes.Repeat([]byte{0x6b}, os.Getpagesize())
	require.NoError(t, os.WriteFile(path, data, 0o600))
	cache, err := NewCache(int64(len(data)), int64(os.Getpagesize()), path, true)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cache.Close() })
	actual := make([]byte, len(data))
	n, err := cache.ReadAt(actual, 0)
	require.NoError(t, err)
	require.Equal(t, len(data), n)
	require.Equal(t, data, actual)
	require.NoError(t, cache.Close())
	_, err = os.Stat(path)
	require.ErrorIs(t, err, os.ErrNotExist, "successful constructor transfers ordinary Close ownership")
}
