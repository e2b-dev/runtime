package api

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/user"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/envd/internal/execcontext"
	"github.com/e2b-dev/infra/packages/envd/internal/services/cgroups"
	"github.com/e2b-dev/infra/packages/envd/internal/utils"
)

func newComposeTestAPI(t *testing.T) (*API, *user.User) {
	t.Helper()

	currentUser, err := user.Current()
	require.NoError(t, err)

	logger := zerolog.Nop()
	defaults := &execcontext.Defaults{
		EnvVars: utils.NewEnvVars(),
		User:    currentUser.Username,
	}

	return New(&logger, defaults, nil, false, cgroups.NewWorkloadFreezer(cgroups.NewNoopManager()), nil), currentUser
}

func writeSourceFile(t *testing.T, dir string, name string, data []byte) string {
	t.Helper()

	path := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(path, data, 0o644))

	return path
}

func callCompose(t *testing.T, api *API, req ComposeRequest) *httptest.ResponseRecorder {
	t.Helper()

	body, err := json.Marshal(req)
	require.NoError(t, err)

	httpReq := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/files/compose", bytes.NewReader(body))
	httpReq.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	api.PostFilesCompose(w, httpReq)

	return w
}

func TestCompose_ConcatenatesFiles(t *testing.T) {
	t.Parallel()

	api, currentUser := newComposeTestAPI(t)
	srcDir := t.TempDir()
	destPath := filepath.Join(t.TempDir(), "composed.txt")

	src0 := writeSourceFile(t, srcDir, "part0", []byte("Hello, "))
	src1 := writeSourceFile(t, srcDir, "part1", []byte("World"))
	src2 := writeSourceFile(t, srcDir, "part2", []byte("!"))

	w := callCompose(t, api, ComposeRequest{
		SourcePaths: []string{src0, src1, src2},
		Destination: destPath,
		Username:    &currentUser.Username,
	})

	resp := w.Result()
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	var result EntryInfo
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
	assert.Equal(t, destPath, result.Path)
	assert.Equal(t, "composed.txt", result.Name)
	assert.Equal(t, File, result.Type)

	data, err := os.ReadFile(destPath)
	require.NoError(t, err)
	assert.Equal(t, []byte("Hello, World!"), data)
}

func TestCompose_DeletesSourceFiles(t *testing.T) {
	t.Parallel()

	api, currentUser := newComposeTestAPI(t)
	srcDir := t.TempDir()
	destPath := filepath.Join(t.TempDir(), "composed.txt")

	src0 := writeSourceFile(t, srcDir, "part0", []byte("aaa"))
	src1 := writeSourceFile(t, srcDir, "part1", []byte("bbb"))

	w := callCompose(t, api, ComposeRequest{
		SourcePaths: []string{src0, src1},
		Destination: destPath,
		Username:    &currentUser.Username,
	})
	require.Equal(t, http.StatusOK, w.Result().StatusCode)

	_, err := os.Stat(src0)
	assert.True(t, os.IsNotExist(err), "source file 0 should be deleted after compose")

	_, err = os.Stat(src1)
	assert.True(t, os.IsNotExist(err), "source file 1 should be deleted after compose")
}

func TestCompose_RequiresSourcePaths(t *testing.T) {
	t.Parallel()

	api, currentUser := newComposeTestAPI(t)

	w := callCompose(t, api, ComposeRequest{
		SourcePaths: []string{},
		Destination: "/tmp/dest.txt",
		Username:    &currentUser.Username,
	})

	assert.Equal(t, http.StatusBadRequest, w.Result().StatusCode)
}

func TestCompose_RequiresDestination(t *testing.T) {
	t.Parallel()

	api, currentUser := newComposeTestAPI(t)

	w := callCompose(t, api, ComposeRequest{
		SourcePaths: []string{"/tmp/something"},
		Destination: "",
		Username:    &currentUser.Username,
	})

	assert.Equal(t, http.StatusBadRequest, w.Result().StatusCode)
}

func TestCompose_SourceEqualsDestination(t *testing.T) {
	t.Parallel()

	api, currentUser := newComposeTestAPI(t)
	srcDir := t.TempDir()
	src := writeSourceFile(t, srcDir, "file.txt", []byte("data"))

	w := callCompose(t, api, ComposeRequest{
		SourcePaths: []string{src},
		Destination: src,
		Username:    &currentUser.Username,
	})

	assert.Equal(t, http.StatusBadRequest, w.Result().StatusCode)
}

func TestCompose_SourceIsDirectory(t *testing.T) {
	t.Parallel()

	api, currentUser := newComposeTestAPI(t)

	w := callCompose(t, api, ComposeRequest{
		SourcePaths: []string{t.TempDir()},
		Destination: filepath.Join(t.TempDir(), "dest.txt"),
		Username:    &currentUser.Username,
	})

	assert.Equal(t, http.StatusBadRequest, w.Result().StatusCode)
}

func TestCompose_SourceNotFound(t *testing.T) {
	t.Parallel()

	api, currentUser := newComposeTestAPI(t)

	w := callCompose(t, api, ComposeRequest{
		SourcePaths: []string{"/tmp/nonexistent-file-12345"},
		Destination: filepath.Join(t.TempDir(), "dest.txt"),
		Username:    &currentUser.Username,
	})

	assert.Equal(t, http.StatusNotFound, w.Result().StatusCode)
}

func TestCompose_CreatesParentDirs(t *testing.T) {
	t.Parallel()

	api, currentUser := newComposeTestAPI(t)
	srcDir := t.TempDir()
	destPath := filepath.Join(t.TempDir(), "nested", "dir", "file.txt")

	src := writeSourceFile(t, srcDir, "part0", []byte("content"))

	w := callCompose(t, api, ComposeRequest{
		SourcePaths: []string{src},
		Destination: destPath,
		Username:    &currentUser.Username,
	})
	require.Equal(t, http.StatusOK, w.Result().StatusCode)

	data, err := os.ReadFile(destPath)
	require.NoError(t, err)
	assert.Equal(t, []byte("content"), data)
}

func TestCompose_LargeFile(t *testing.T) {
	t.Parallel()

	api, currentUser := newComposeTestAPI(t)
	srcDir := t.TempDir()
	destPath := filepath.Join(t.TempDir(), "large.bin")

	partSize := 1024 * 1024
	var sources []string
	var expectedTotal int64

	for i := range 10 {
		data := bytes.Repeat([]byte{byte(i)}, partSize)
		src := writeSourceFile(t, srcDir, filepath.Base(t.TempDir())+string(rune('0'+i)), data)
		sources = append(sources, src)
		expectedTotal += int64(partSize)
	}

	w := callCompose(t, api, ComposeRequest{
		SourcePaths: sources,
		Destination: destPath,
		Username:    &currentUser.Username,
	})

	resp := w.Result()
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	var result EntryInfo
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
	assert.Equal(t, destPath, result.Path)
	assert.Equal(t, File, result.Type)

	data, err := os.ReadFile(destPath)
	require.NoError(t, err)
	assert.Len(t, data, int(expectedTotal))

	for i := range 10 {
		offset := i * partSize
		assert.Equal(t, byte(i), data[offset], "first byte of part %d should match", i)
		assert.Equal(t, byte(i), data[offset+partSize-1], "last byte of part %d should match", i)
	}
}

func TestCompose_RoundTripWithDownload(t *testing.T) {
	t.Parallel()

	api, currentUser := newComposeTestAPI(t)
	srcDir := t.TempDir()
	destPath := filepath.Join(t.TempDir(), "roundtrip.txt")

	src0 := writeSourceFile(t, srcDir, "part0", []byte("round"))
	src1 := writeSourceFile(t, srcDir, "part1", []byte("trip"))

	w := callCompose(t, api, ComposeRequest{
		SourcePaths: []string{src0, src1},
		Destination: destPath,
		Username:    &currentUser.Username,
	})
	require.Equal(t, http.StatusOK, w.Result().StatusCode)

	downloadReq := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/files?path="+url.QueryEscape(destPath), nil)
	downloadW := httptest.NewRecorder()
	api.GetFiles(downloadW, downloadReq, GetFilesParams{
		Path:     &destPath,
		Username: &currentUser.Username,
	})

	downloadResp := downloadW.Result()
	defer downloadResp.Body.Close()

	require.Equal(t, http.StatusOK, downloadResp.StatusCode)

	body, err := io.ReadAll(downloadResp.Body)
	require.NoError(t, err)
	assert.Equal(t, []byte("roundtrip"), body)
}

func TestCompose_PreservesExistingFileOnFailure(t *testing.T) {
	t.Parallel()

	if os.Getuid() == 0 {
		t.Skip("test requires non-root to enforce directory permissions")
	}

	api, currentUser := newComposeTestAPI(t)
	destDir := t.TempDir()
	destPath := filepath.Join(destDir, "existing.txt")

	require.NoError(t, os.WriteFile(destPath, []byte("original content"), 0o644))

	srcDir := t.TempDir()
	src := writeSourceFile(t, srcDir, "part0", []byte("new content"))

	// Make destination directory read-only so temp file creation fails.
	require.NoError(t, os.Chmod(destDir, 0o500))
	defer os.Chmod(destDir, 0o755)

	w := callCompose(t, api, ComposeRequest{
		SourcePaths: []string{src},
		Destination: destPath,
		Username:    &currentUser.Username,
	})
	assert.Equal(t, http.StatusInternalServerError, w.Result().StatusCode)

	// Restore permissions and verify original file is intact.
	require.NoError(t, os.Chmod(destDir, 0o755))

	data, err := os.ReadFile(destPath)
	require.NoError(t, err)
	assert.Equal(t, []byte("original content"), data)

	// Source file should still exist since compose failed.
	_, err = os.Stat(src)
	assert.NoError(t, err, "source file should be preserved when compose fails")
}

func TestCompose_SingleFile(t *testing.T) {
	t.Parallel()

	api, currentUser := newComposeTestAPI(t)
	srcDir := t.TempDir()
	destPath := filepath.Join(t.TempDir(), "single.txt")

	src := writeSourceFile(t, srcDir, "only", []byte("solo"))

	w := callCompose(t, api, ComposeRequest{
		SourcePaths: []string{src},
		Destination: destPath,
		Username:    &currentUser.Username,
	})

	resp := w.Result()
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	var result EntryInfo
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
	assert.Equal(t, destPath, result.Path)
	assert.Equal(t, "single.txt", result.Name)
	assert.Equal(t, File, result.Type)

	data, err := os.ReadFile(destPath)
	require.NoError(t, err)
	assert.Equal(t, []byte("solo"), data)
}
