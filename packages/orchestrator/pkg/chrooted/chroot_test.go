//go:build linux

package chrooted

import (
	"io"
	"os"
	"sync"
	"testing"
	"time"

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

func TestWaitClosedJoinsOriginalCloseAfterTimeout(t *testing.T) {
	t.Parallel()

	fs, err := Chroot(t.TempDir())
	require.NoError(t, err)
	fs.mu.RLock() // Simulate one operation holding the original root descriptor.
	var unlock sync.Once
	release := func() { unlock.Do(fs.mu.RUnlock) }
	t.Cleanup(release)

	closeDone := make(chan error, 1)
	go func() { closeDone <- fs.closeWithTimeout(5 * time.Millisecond) }()
	require.ErrorIs(t, <-closeDone, ErrCloseTimeout)
	waitDone := make(chan error, 1)
	go func() { waitDone <- fs.WaitClosed(t.Context()) }()
	select {
	case err := <-waitDone:
		t.Fatalf("original root descriptor joined before the operation released it: %v", err)
	default:
	}
	release()
	require.NoError(t, <-waitDone)
	require.NoError(t, fs.WaitClosed(t.Context()))
	require.ErrorIs(t, fs.Close(), ErrClosed, "a waiter must not retry the original close")
}

func TestCloseAsyncJoinsOriginalDrain(t *testing.T) {
	t.Parallel()

	fs, err := Chroot(t.TempDir())
	require.NoError(t, err)
	fs.mu.RLock()
	var unlock sync.Once
	release := func() { unlock.Do(fs.mu.RUnlock) }
	t.Cleanup(release)
	require.NoError(t, fs.CloseAsync())
	require.ErrorIs(t, fs.CloseAsync(), ErrClosed)
	select {
	case <-fs.closeDone:
		t.Fatal("descriptor closed while an operation still owns it")
	default:
	}
	release()
	require.NoError(t, fs.WaitClosed(t.Context()))
	require.NoError(t, fs.WaitClosed(t.Context()))
	require.ErrorIs(t, fs.Close(), ErrClosed)
}
