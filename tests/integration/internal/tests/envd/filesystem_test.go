package envd

import (
	"context"
	"fmt"
	"net/http"
	"path"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/envd/filesystem"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/envd/process"
	envdAPI "github.com/e2b-dev/infra/tests/integration/internal/envd"
	"github.com/e2b-dev/infra/tests/integration/internal/setup"
	"github.com/e2b-dev/infra/tests/integration/internal/utils"
)

const (
	userHome           = "/home/user"
	testFolder         = "/test"
	relativeTestFolder = "test"
)

func TestListDir(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	c := setup.GetAPIClient()
	sbx := utils.SetupSandboxWithCleanup(t, c, utils.WithTimeout(120))
	envdClient := setup.GetEnvdClient(t, ctx)

	utils.CreateDir(t, sbx, testFolder)
	utils.CreateDir(t, sbx, fmt.Sprintf("%s/test-dir", testFolder))
	utils.CreateDir(t, sbx, fmt.Sprintf("%s/test-dir/sub-dir-1", testFolder))
	utils.CreateDir(t, sbx, fmt.Sprintf("%s/test-dir/sub-dir-2", testFolder))

	filePath := fmt.Sprintf("%s/test-dir/sub-dir-1/file.txt", testFolder)
	utils.UploadFile(t, ctx, sbx, envdClient, filePath, "Hello, World!")

	tests := []struct {
		name          string
		depth         uint32
		expectedPaths []string
	}{
		{
			name:  "depth 0 lists only root directory",
			depth: 0,
			expectedPaths: []string{
				fmt.Sprintf("%s/test-dir", testFolder),
			},
		},
		{
			name:  "depth 1 lists root directory",
			depth: 1,
			expectedPaths: []string{
				fmt.Sprintf("%s/test-dir", testFolder),
			},
		},
		{
			name:  "depth 2 lists first level of subdirectories (in this case the root directory)",
			depth: 2,
			expectedPaths: []string{
				fmt.Sprintf("%s/test-dir", testFolder),
				fmt.Sprintf("%s/test-dir/sub-dir-1", testFolder),
				fmt.Sprintf("%s/test-dir/sub-dir-2", testFolder),
			},
		},
		{
			name:  "depth 3 lists all directories and files",
			depth: 3,
			expectedPaths: []string{
				fmt.Sprintf("%s/test-dir", testFolder),
				fmt.Sprintf("%s/test-dir/sub-dir-1", testFolder),
				fmt.Sprintf("%s/test-dir/sub-dir-2", testFolder),
				fmt.Sprintf("%s/test-dir/sub-dir-1/file.txt", testFolder),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			req := connect.NewRequest(&filesystem.ListDirRequest{
				Path:  testFolder,
				Depth: tt.depth,
			})
			setup.SetSandboxHeader(t, req.Header(), sbx)
			setup.SetUserHeader(t, req.Header(), "user")
			folderListResp, err := envdClient.FilesystemClient.ListDir(ctx, req)
			require.NoError(t, err)

			assert.NotEmpty(t, folderListResp.Msg)
			assert.Len(t, folderListResp.Msg.GetEntries(), len(tt.expectedPaths))

			actualPaths := make([]string, len(folderListResp.Msg.GetEntries()))
			for i, entry := range folderListResp.Msg.GetEntries() {
				actualPaths[i] = entry.GetPath()
			}
			assert.ElementsMatch(t, tt.expectedPaths, actualPaths)
		})
	}
}

func TestFilePermissions(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	c := setup.GetAPIClient()
	sbx := utils.SetupSandboxWithCleanup(t, c)

	envdClient := setup.GetEnvdClient(t, ctx)
	req := connect.NewRequest(&process.StartRequest{
		Process: &process.ProcessConfig{
			Cmd:  "ls",
			Args: []string{"-la", userHome},
		},
	})
	setup.SetSandboxHeader(t, req.Header(), sbx)
	setup.SetUserHeader(t, req.Header(), "user")
	stream, err := envdClient.ProcessClient.Start(
		ctx,
		req,
	)

	require.NoError(t, err)

	defer stream.Close()

	out := []string{}

	for stream.Receive() {
		msg := stream.Msg()
		out = append(out, msg.String())
	}

	// in the output, we should see the files .bashrc and .profile, and they should have the correct permissions
	for _, line := range out {
		if strings.Contains(line, ".bashrc") || strings.Contains(line, ".profile") {
			assert.Contains(t, line, "-rw-r--r--")
			assert.Contains(t, line, "user user")
		}
	}
}

func TestStat(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	c := setup.GetAPIClient()
	sbx := utils.SetupSandboxWithCleanup(t, c)

	envdClient := setup.GetEnvdClient(t, ctx)
	filePath := "/home/user/test.txt"
	utils.UploadFile(t, ctx, sbx, envdClient, filePath, "Hello, World!")

	req := connect.NewRequest(&filesystem.StatRequest{
		Path: filePath,
	})
	setup.SetSandboxHeader(t, req.Header(), sbx)
	setup.SetUserHeader(t, req.Header(), "user")
	statResp, err := envdClient.FilesystemClient.Stat(ctx, req)
	require.NoError(t, err)

	// Verify the stat response
	require.NotNil(t, statResp.Msg)
	require.NotNil(t, statResp.Msg.GetEntry())
	entry := statResp.Msg.GetEntry()

	// Verify basic file info
	assert.Equal(t, "test.txt", entry.GetName())
	assert.Equal(t, filePath, entry.GetPath())
	assert.Equal(t, filesystem.FileType_FILE_TYPE_FILE, entry.GetType())

	// Verify permissions and ownership
	assert.Equal(t, uint32(0o644), entry.GetMode())
	assert.Equal(t, "-rw-r--r--", entry.GetPermissions())
	assert.Equal(t, "user", entry.GetOwner())
	assert.Equal(t, "user", entry.GetGroup())

	// Verify file size
	assert.Equal(t, int64(13), entry.GetSize())

	// Verify modified time
	require.NotNil(t, entry.GetModifiedTime())
}

func TestListDirFileEntry(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	c := setup.GetAPIClient()
	sbx := utils.SetupSandboxWithCleanup(t, c)
	envdClient := setup.GetEnvdClient(t, ctx)

	// Create test directory and file
	testDir := "/test-file-entry"
	filePath := fmt.Sprintf("%s/test.txt", testDir)

	utils.CreateDir(t, sbx, testDir)

	// Create a text file
	utils.UploadFile(t, ctx, sbx, envdClient, filePath, "Hello, World!")

	// List the directory
	req := connect.NewRequest(&filesystem.ListDirRequest{
		Path:  testDir,
		Depth: 1,
	})
	setup.SetSandboxHeader(t, req.Header(), sbx)
	setup.SetUserHeader(t, req.Header(), "user")
	folderListResp, err := envdClient.FilesystemClient.ListDir(ctx, req)
	require.NoError(t, err)

	// Verify response
	require.NotEmpty(t, folderListResp.Msg)
	require.Len(t, folderListResp.Msg.GetEntries(), 1)

	// Get the file entry
	fileEntry := folderListResp.Msg.GetEntries()[0]

	// Verify file entry
	assert.Equal(t, "test.txt", fileEntry.GetName())
	assert.Equal(t, filePath, fileEntry.GetPath())
	assert.Equal(t, filesystem.FileType_FILE_TYPE_FILE, fileEntry.GetType())
	assert.Equal(t, uint32(0o644), fileEntry.GetMode())
	assert.Equal(t, "-rw-r--r--", fileEntry.GetPermissions())
	assert.Equal(t, "user", fileEntry.GetOwner())
	assert.Equal(t, "user", fileEntry.GetGroup())
	assert.Equal(t, int64(13), fileEntry.GetSize()) // "Hello, World!" is 13 bytes
	require.NotNil(t, fileEntry.GetModifiedTime())
}

func TestListDirEntry(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	c := setup.GetAPIClient()
	sbx := utils.SetupSandboxWithCleanup(t, c)
	envdClient := setup.GetEnvdClient(t, ctx)

	// Create test directories
	testDir := "/test-entry-info"
	subDir := fmt.Sprintf("%s/subdir", testDir)

	utils.CreateDir(t, sbx, testDir)
	utils.CreateDir(t, sbx, subDir)

	// List the directory
	req := connect.NewRequest(&filesystem.ListDirRequest{
		Path:  testDir,
		Depth: 1,
	})
	setup.SetSandboxHeader(t, req.Header(), sbx)
	setup.SetUserHeader(t, req.Header(), "user")
	folderListResp, err := envdClient.FilesystemClient.ListDir(ctx, req)
	require.NoError(t, err)

	// Verify response
	require.NotEmpty(t, folderListResp.Msg)
	require.Len(t, folderListResp.Msg.GetEntries(), 1)

	// Get the subdirectory entry
	entry := folderListResp.Msg.GetEntries()[0]

	// Verify EntryInfo
	assert.Equal(t, "subdir", entry.GetName())
	assert.Equal(t, subDir, entry.GetPath())
	assert.Equal(t, filesystem.FileType_FILE_TYPE_DIRECTORY, entry.GetType())
	assert.Equal(t, uint32(0o755), entry.GetMode())
	assert.Equal(t, "drwxr-xr-x", entry.GetPermissions())
	assert.Equal(t, "user", entry.GetOwner())
	assert.Equal(t, "user", entry.GetGroup())
	require.NotNil(t, entry.GetModifiedTime())
}

func TestListDirMixedEntries(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	c := setup.GetAPIClient()
	sbx := utils.SetupSandboxWithCleanup(t, c)
	envdClient := setup.GetEnvdClient(t, ctx)

	// Create test directories and files
	testDir := "/test-mixed-entries"
	subDir := fmt.Sprintf("%s/subdir", testDir)
	filePath := fmt.Sprintf("%s/test.txt", testDir)

	utils.CreateDir(t, sbx, testDir)
	utils.CreateDir(t, sbx, subDir)

	// Create a text file
	utils.UploadFile(t, ctx, sbx, envdClient, filePath, "Hello, World!")

	// List the directory
	req := connect.NewRequest(&filesystem.ListDirRequest{
		Path:  testDir,
		Depth: 1,
	})
	setup.SetSandboxHeader(t, req.Header(), sbx)
	setup.SetUserHeader(t, req.Header(), "user")
	folderListResp, err := envdClient.FilesystemClient.ListDir(ctx, req)
	require.NoError(t, err)

	// Verify response
	require.NotEmpty(t, folderListResp.Msg)
	require.Len(t, folderListResp.Msg.GetEntries(), 2)

	// Create a map of entries by name for easier verification
	entries := make(map[string]*filesystem.EntryInfo)
	for _, entry := range folderListResp.Msg.GetEntries() {
		entries[entry.GetName()] = entry
	}

	// Verify directory entry
	dirEntry, exists := entries["subdir"]
	require.True(t, exists)
	assert.Equal(t, subDir, dirEntry.GetPath())
	assert.Equal(t, filesystem.FileType_FILE_TYPE_DIRECTORY, dirEntry.GetType())
	assert.Equal(t, uint32(0o755), dirEntry.GetMode())
	assert.Equal(t, "drwxr-xr-x", dirEntry.GetPermissions())
	assert.Equal(t, "user", dirEntry.GetOwner())
	assert.Equal(t, "user", dirEntry.GetGroup())
	require.NotNil(t, dirEntry.GetModifiedTime())

	// Verify file entry
	fileEntry, exists := entries["test.txt"]
	require.True(t, exists)
	assert.Equal(t, filePath, fileEntry.GetPath())
	assert.Equal(t, filesystem.FileType_FILE_TYPE_FILE, fileEntry.GetType())
	assert.Equal(t, uint32(0o644), fileEntry.GetMode())
	assert.Equal(t, "-rw-r--r--", fileEntry.GetPermissions())
	assert.Equal(t, "user", fileEntry.GetOwner())
	assert.Equal(t, "user", fileEntry.GetGroup())
	assert.Equal(t, int64(13), fileEntry.GetSize()) // "Hello, World!" is 13 bytes
	require.NotNil(t, fileEntry.GetModifiedTime())
}

func TestRelativePath(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	c := setup.GetAPIClient()
	sbx := utils.SetupSandboxWithCleanup(t, c)
	envdClient := setup.GetEnvdClient(t, ctx)

	utils.CreateDir(t, sbx, path.Join(userHome, relativeTestFolder))
	utils.UploadFile(t, ctx, sbx, envdClient, path.Join(userHome, relativeTestFolder, "test.txt"), "Hello, World!")

	req := connect.NewRequest(&filesystem.ListDirRequest{
		Path:  relativeTestFolder,
		Depth: 0,
	})
	setup.SetSandboxHeader(t, req.Header(), sbx)
	setup.SetUserHeader(t, req.Header(), "user")
	folderListResp, err := envdClient.FilesystemClient.ListDir(ctx, req)
	require.NoError(t, err)

	require.NotEmpty(t, folderListResp.Msg)
	assert.Len(t, folderListResp.Msg.GetEntries(), 1)

	assert.Equal(t, path.Join(userHome, relativeTestFolder, "test.txt"), folderListResp.Msg.GetEntries()[0].GetPath())
}

func TestConcurrentFileUpload(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	c := setup.GetAPIClient()
	sbx := utils.SetupSandboxWithCleanup(t, c)
	envdClient := setup.GetEnvdClient(t, ctx)

	baseDir := "/home/user/concurrent-test/nested/dir"

	// Upload multiple files concurrently to the same nested directory.
	// This exercises the race condition fix in EnsureDirs where concurrent
	// Mkdir calls are tolerated via os.IsExist checks.
	g, gCtx := errgroup.WithContext(ctx)
	for i := 1; i <= 4; i++ {
		g.Go(func() error {
			filePath := fmt.Sprintf("%s/test_%d.txt", baseDir, i)
			content := fmt.Sprintf("content of test_%d\n", i)

			buffer, contentType := utils.CreateTextFile(t, filePath, content)
			reqEditors := []envdAPI.RequestEditorFn{setup.WithSandbox(t, sbx)}

			writeRes, err := envdClient.HTTPClient.PostFilesWithBodyWithResponse(
				gCtx,
				&envdAPI.PostFilesParams{Path: &filePath, Username: new("user")},
				contentType,
				buffer,
				reqEditors...,
			)
			if err != nil {
				return fmt.Errorf("test_%d.txt: %w", i, err)
			}

			if writeRes.StatusCode() != http.StatusOK {
				return fmt.Errorf("test_%d.txt: unexpected status %d: %s", i, writeRes.StatusCode(), string(writeRes.Body))
			}

			return nil
		})
	}

	require.NoError(t, g.Wait())

	// Verify all files exist
	req := connect.NewRequest(&filesystem.ListDirRequest{
		Path:  baseDir,
		Depth: 1,
	})
	setup.SetSandboxHeader(t, req.Header(), sbx)
	setup.SetUserHeader(t, req.Header(), "user")
	listResp, err := envdClient.FilesystemClient.ListDir(ctx, req)
	require.NoError(t, err)
	require.Len(t, listResp.Msg.GetEntries(), 4)
}
