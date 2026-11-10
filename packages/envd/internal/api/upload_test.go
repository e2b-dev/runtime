package api

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/filesystem"
)

func TestProcessFile(t *testing.T) {
	t.Parallel()
	uid := os.Getuid()
	gid := os.Getgid()

	newRequest := func(content []byte) (*http.Request, io.Reader) {
		request := &http.Request{
			ContentLength: int64(len(content)),
		}
		buffer := bytes.NewBuffer(content)

		return request, buffer
	}

	var emptyReq http.Request
	var emptyPart *bytes.Buffer
	var emptyLogger zerolog.Logger

	t.Run("failed to ensure directories", func(t *testing.T) {
		t.Parallel()
		// A parent that is a regular file cannot be made a directory on any platform.
		notDir := filepath.Join(t.TempDir(), "file")
		require.NoError(t, os.WriteFile(notDir, nil, 0o644))
		httpStatus, err := processFile(&emptyReq, filepath.Join(notDir, "sub", "not-real"), emptyPart, uid, gid, nil, emptyLogger)
		require.Error(t, err)
		assert.Equal(t, http.StatusInternalServerError, httpStatus)
		assert.ErrorContains(t, err, "error ensuring directories: ")
	})

	t.Run("attempt to replace directory with a file", func(t *testing.T) {
		t.Parallel()
		tempDir := t.TempDir()

		httpStatus, err := processFile(&emptyReq, tempDir, emptyPart, uid, gid, nil, emptyLogger)
		require.Error(t, err)
		assert.Equal(t, http.StatusBadRequest, httpStatus, err.Error())
		assert.ErrorContains(t, err, "path is a directory: ")
	})

	t.Run("fail to create file", func(t *testing.T) {
		t.Parallel()
		// /proc as a virtual filesystem with specific error semantics is Linux-only.
		if runtime.GOOS != "linux" {
			t.Skip("relies on Linux /proc virtual filesystem semantics")
		}
		httpStatus, err := processFile(&emptyReq, "/proc/invalid-filename", emptyPart, uid, gid, nil, emptyLogger)
		require.Error(t, err)
		assert.Equal(t, http.StatusInternalServerError, httpStatus)
		assert.ErrorContains(t, err, "error opening file: ")
	})

	t.Run("out of disk space", func(t *testing.T) {
		t.Parallel()
		// make a tiny tmpfs mount
		mountSize := 1024
		tempDir := createTmpfsMount(t, mountSize)

		// create test file
		firstFileSize := mountSize / 2
		tempFile1 := filepath.Join(tempDir, "test-file-1")

		// fill it up
		cmd := exec.CommandContext(t.Context(),
			"dd", "if=/dev/zero", "of="+tempFile1, fmt.Sprintf("bs=%d", firstFileSize), "count=1")
		err := cmd.Run()
		require.NoError(t, err)

		// create a new file that would fill up the
		secondFileContents := make([]byte, mountSize*2)
		for index := range secondFileContents {
			secondFileContents[index] = 'a'
		}

		// try to replace it
		request, buffer := newRequest(secondFileContents)
		tempFile2 := filepath.Join(tempDir, "test-file-2")
		httpStatus, err := processFile(request, tempFile2, buffer, uid, gid, nil, emptyLogger)
		require.Error(t, err)
		assert.Equal(t, http.StatusInsufficientStorage, httpStatus)
		assert.ErrorContains(t, err, "attempted to write 2048 bytes: not enough disk space")
	})

	t.Run("happy path", func(t *testing.T) {
		t.Parallel()
		tempDir := t.TempDir()
		tempFile := filepath.Join(tempDir, "test-file")

		content := []byte("test-file-contents")
		request, buffer := newRequest(content)

		httpStatus, err := processFile(request, tempFile, buffer, uid, gid, nil, emptyLogger)
		require.NoError(t, err)
		assert.Equal(t, http.StatusNoContent, httpStatus)

		data, err := os.ReadFile(tempFile)
		require.NoError(t, err)
		assert.Equal(t, content, data)
	})

	t.Run("overwrite file on full disk", func(t *testing.T) {
		t.Parallel()
		// make a tiny tmpfs mount
		sizeInBytes := 1024
		tempDir := createTmpfsMount(t, 1024)

		// create test file
		tempFile := filepath.Join(tempDir, "test-file")

		// fill it up
		cmd := exec.CommandContext(t.Context(), "dd", "if=/dev/zero", "of="+tempFile, fmt.Sprintf("bs=%d", sizeInBytes), "count=1")
		err := cmd.Run()
		require.NoError(t, err)

		// try to replace it
		content := []byte("test-file-contents")
		request, buffer := newRequest(content)
		httpStatus, err := processFile(request, tempFile, buffer, uid, gid, nil, emptyLogger)
		require.NoError(t, err)
		assert.Equal(t, http.StatusNoContent, httpStatus)
	})

	t.Run("write new file on full disk", func(t *testing.T) {
		t.Parallel()
		// make a tiny tmpfs mount
		sizeInBytes := 1024
		tempDir := createTmpfsMount(t, 1024)

		// create test file
		tempFile1 := filepath.Join(tempDir, "test-file")

		// fill it up
		cmd := exec.CommandContext(t.Context(), "dd", "if=/dev/zero", "of="+tempFile1, fmt.Sprintf("bs=%d", sizeInBytes), "count=1")
		err := cmd.Run()
		require.NoError(t, err)

		// try to write a new file
		tempFile2 := filepath.Join(tempDir, "test-file-2")
		content := []byte("test-file-contents")
		request, buffer := newRequest(content)
		httpStatus, err := processFile(request, tempFile2, buffer, uid, gid, nil, emptyLogger)
		require.ErrorContains(t, err, "not enough disk space available")
		assert.Equal(t, http.StatusInsufficientStorage, httpStatus)
	})

	t.Run("write new file with no inodes available", func(t *testing.T) {
		t.Parallel()
		// make a tiny tmpfs mount
		tempDir := createTmpfsMountWithInodes(t, 1024, 2)

		// create test file
		tempFile1 := filepath.Join(tempDir, "test-file")

		// fill it up
		cmd := exec.CommandContext(t.Context(), "dd", "if=/dev/zero", "of="+tempFile1, fmt.Sprintf("bs=%d", 100), "count=1")
		err := cmd.Run()
		require.NoError(t, err)

		// try to write a new file
		tempFile2 := filepath.Join(tempDir, "test-file-2")
		content := []byte("test-file-contents")
		request, buffer := newRequest(content)
		httpStatus, err := processFile(request, tempFile2, buffer, uid, gid, nil, emptyLogger)
		require.ErrorContains(t, err, "not enough inodes available")
		assert.Equal(t, http.StatusInsufficientStorage, httpStatus)
	})

	t.Run("update sysfs or other virtual fs", func(t *testing.T) {
		t.Parallel()
		if os.Geteuid() != 0 {
			t.Skip("skipping sysfs updates: Operation not permitted with non-root user")
		}

		filePath := "/sys/fs/cgroup/user.slice/cpu.weight"
		newContent := []byte("102\n")
		request, buffer := newRequest(newContent)

		httpStatus, err := processFile(request, filePath, buffer, uid, gid, nil, emptyLogger)
		require.NoError(t, err)
		assert.Equal(t, http.StatusNoContent, httpStatus)

		data, err := os.ReadFile(filePath)
		require.NoError(t, err)
		assert.Equal(t, newContent, data)
	})

	t.Run("replace file", func(t *testing.T) {
		t.Parallel()
		tempDir := t.TempDir()
		tempFile := filepath.Join(tempDir, "test-file")

		err := os.WriteFile(tempFile, []byte("old-contents"), 0o644)
		require.NoError(t, err)

		newContent := []byte("new-file-contents")
		request, buffer := newRequest(newContent)

		httpStatus, err := processFile(request, tempFile, buffer, uid, gid, nil, emptyLogger)
		require.NoError(t, err)
		assert.Equal(t, http.StatusNoContent, httpStatus)

		data, err := os.ReadFile(tempFile)
		require.NoError(t, err)
		assert.Equal(t, newContent, data)
	})

	t.Run("overwrite clears old metadata", func(t *testing.T) {
		t.Parallel()
		tempDir := t.TempDir()
		tempFile := filepath.Join(tempDir, "test-file")

		initialContent := []byte("old-contents")
		request, buffer := newRequest(initialContent)
		httpStatus, err := processFile(request, tempFile, buffer, uid, gid, map[string]string{"purpose": "old"}, emptyLogger)
		require.NoError(t, err)
		assert.Equal(t, http.StatusNoContent, httpStatus)

		metadata, err := filesystem.ReadMetadata(tempFile)
		require.NoError(t, err)
		if len(metadata) == 0 {
			t.Skip("filesystem does not support xattrs")
		}
		require.Equal(t, "old", metadata["purpose"])

		newContent := []byte("new-contents")
		request, buffer = newRequest(newContent)
		httpStatus, err = processFile(request, tempFile, buffer, uid, gid, nil, emptyLogger)
		require.NoError(t, err)
		assert.Equal(t, http.StatusNoContent, httpStatus)

		metadata, err = filesystem.ReadMetadata(tempFile)
		require.NoError(t, err)
		assert.Empty(t, metadata)
	})

	t.Run("overwrite replaces old metadata", func(t *testing.T) {
		t.Parallel()
		tempDir := t.TempDir()
		tempFile := filepath.Join(tempDir, "test-file")

		initialContent := []byte("old-contents")
		request, buffer := newRequest(initialContent)
		httpStatus, err := processFile(request, tempFile, buffer, uid, gid, map[string]string{"author": "old", "purpose": "old"}, emptyLogger)
		require.NoError(t, err)
		assert.Equal(t, http.StatusNoContent, httpStatus)

		metadata, err := filesystem.ReadMetadata(tempFile)
		require.NoError(t, err)
		if len(metadata) == 0 {
			t.Skip("filesystem does not support xattrs")
		}
		require.Equal(t, map[string]string{"author": "old", "purpose": "old"}, metadata)

		newContent := []byte("new-contents")
		request, buffer = newRequest(newContent)
		httpStatus, err = processFile(request, tempFile, buffer, uid, gid, map[string]string{"author": "new"}, emptyLogger)
		require.NoError(t, err)
		assert.Equal(t, http.StatusNoContent, httpStatus)

		metadata, err = filesystem.ReadMetadata(tempFile)
		require.NoError(t, err)
		assert.Equal(t, map[string]string{"author": "new"}, metadata)
	})
}

func createTmpfsMount(t *testing.T, sizeInBytes int) string {
	t.Helper()

	return createTmpfsMountWithInodes(t, sizeInBytes, 5)
}

func createTmpfsMountWithInodes(t *testing.T, sizeInBytes, inodesCount int) string {
	t.Helper()

	if os.Geteuid() != 0 {
		t.Skip("skipping sysfs updates: Operation not permitted with non-root user")
	}

	tempDir := t.TempDir()

	cmd := exec.CommandContext(t.Context(),
		"mount",
		"tmpfs",
		tempDir,
		"-t", "tmpfs",
		"-o", fmt.Sprintf("size=%d,nr_inodes=%d", sizeInBytes, inodesCount))
	err := cmd.Run()
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx := context.WithoutCancel(t.Context())
		cmd := exec.CommandContext(ctx, "umount", tempDir)
		if output, err := cmd.CombinedOutput(); err != nil {
			cmd = exec.CommandContext(ctx, "umount", "--lazy", tempDir)
			lazyOutput, lazyErr := cmd.CombinedOutput()
			require.NoError(t, lazyErr, "umount failed: %s; lazy umount failed: %s", output, lazyOutput)
		}
	})

	return tempDir
}
