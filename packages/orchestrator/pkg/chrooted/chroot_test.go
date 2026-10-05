//go:build linux

package chrooted

import (
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFileSystemsAreIsolated(t *testing.T) {
	t.Parallel()

	oneSrc := t.TempDir()
	one, err := Chroot(oneSrc)
	require.NoError(t, err)
	t.Cleanup(func() {
		err := one.Close()
		assert.NoError(t, err)
	})

	results, err := one.ReadDir("/")
	require.NoError(t, err)
	require.Empty(t, results)

	fileName := "test.txt"
	fullFilepath := "/" + fileName

	file, err := one.OpenFile(fullFilepath, os.O_CREATE|os.O_WRONLY, 0o644)
	require.NoError(t, err)
	_, err = io.WriteString(file, "hello one")
	require.NoError(t, err)
	err = file.Close()
	require.NoError(t, err)

	file, err = one.Open(fullFilepath)
	require.NoError(t, err)
	data, err := io.ReadAll(file)
	require.NoError(t, err)
	assert.Equal(t, "hello one", string(data))
	err = file.Close()
	require.NoError(t, err)

	results, err = one.ReadDir("/")
	require.NoError(t, err)
	assert.Len(t, results, 1)
	assert.Equal(t, fileName, results[0].Name())

	twoSrc := t.TempDir()
	two, err := Chroot(twoSrc)
	require.NoError(t, err)
	t.Cleanup(func() {
		err := two.Close()
		assert.NoError(t, err)
	})

	_, err = two.Open(fullFilepath)
	require.ErrorIs(t, err, os.ErrNotExist)
}
