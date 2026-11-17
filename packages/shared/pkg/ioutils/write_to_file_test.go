package ioutils

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"testing/iotest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteToFileFromReaderAtomicallyReplacesExistingFile(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "metadata.json")
	require.NoError(t, os.WriteFile(path, []byte("old metadata"), 0o600))

	reader := &blockingReader{
		data:    []byte("new metadata"),
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	errCh := make(chan error, 1)
	go func() {
		errCh <- WriteToFileFromReaderAtomically(path, reader)
	}()

	released := false
	finished := false
	defer func() {
		if !released {
			close(reader.release)
		}
		if !finished {
			waitForWrite(t, errCh)
		}
	}()

	select {
	case <-reader.started:
	case err := <-errCh:
		finished = true
		require.Failf(t, "writer completed before reading input", "error: %v", err)
	case <-time.After(5 * time.Second):
		require.Fail(t, "writer did not read input")
	}

	contents, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, []byte("old metadata"), contents)

	close(reader.release)
	released = true
	err = waitForWrite(t, errCh)
	finished = true
	require.NoError(t, err)

	contents, err = os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, []byte("new metadata"), contents)

	info, err := os.Stat(path)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), info.Mode().Perm())
}

func TestWriteToFileFromReaderFailureKeepsExistingFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "metadata.json")
	require.NoError(t, os.WriteFile(path, []byte("old metadata"), 0o644))

	readErr := errors.New("read failed")
	err := WriteToFileFromReaderAtomically(path, io.MultiReader(strings.NewReader("partial"), iotest.ErrReader(readErr)))
	require.ErrorIs(t, err, readErr)

	contents, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, []byte("old metadata"), contents)

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "metadata.json", entries[0].Name())
}

func TestWriteToFileFromReaderAtomicallyRequiresExistingRegularFile(t *testing.T) {
	t.Parallel()

	t.Run("missing file", func(t *testing.T) {
		t.Parallel()

		path := filepath.Join(t.TempDir(), "metadata.json")
		err := WriteToFileFromReaderAtomically(path, strings.NewReader("metadata"))
		require.ErrorIs(t, err, os.ErrNotExist)
	})

	t.Run("directory", func(t *testing.T) {
		t.Parallel()

		path := t.TempDir()
		err := WriteToFileFromReaderAtomically(path, strings.NewReader("metadata"))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "is not a regular file")
	})
}

func waitForWrite(t *testing.T, errCh <-chan error) error {
	t.Helper()

	select {
	case err := <-errCh:
		return err
	case <-time.After(5 * time.Second):
		require.Fail(t, "writer did not finish")

		return nil
	}
}

type blockingReader struct {
	data    []byte
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (r *blockingReader) Read(p []byte) (int, error) {
	r.once.Do(func() { close(r.started) })
	<-r.release
	if len(r.data) == 0 {
		return 0, io.EOF
	}

	n := copy(p, r.data)
	r.data = r.data[n:]

	return n, nil
}
