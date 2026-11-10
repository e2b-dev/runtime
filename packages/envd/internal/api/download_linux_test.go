//go:build linux

package api

import (
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/user"
	"strconv"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/envd/internal/execcontext"
	"github.com/e2b-dev/infra/packages/envd/internal/services/cgroups"
	"github.com/e2b-dev/infra/packages/envd/internal/services/cpus"
	"github.com/e2b-dev/infra/packages/envd/internal/utils"
)

// Virtual files (procfs, sysfs) report stat size 0 but yield content when
// read. The identity path must serve their real content, not an empty body.
func TestGetFiles_IdentityDownload_VirtualFile(t *testing.T) {
	t.Parallel()

	virtualFile := "/proc/version"

	stat, err := os.Stat(virtualFile)
	require.NoError(t, err)
	if stat.Size() != 0 {
		t.Skipf("expected %s to report stat size 0, got %d", virtualFile, stat.Size())
	}

	expectedContent, err := os.ReadFile(virtualFile)
	require.NoError(t, err)
	require.NotEmpty(t, expectedContent)

	currentUser, err := user.Current()
	require.NoError(t, err)

	logger := zerolog.Nop()
	defaults := &execcontext.Defaults{
		EnvVars: utils.NewEnvVars(),
		User:    currentUser.Username,
	}
	api := New(&logger, defaults, nil, false, cgroups.NewWorkloadFreezer(cgroups.NewNoopManager()), nil, cpus.NewNoopManager())

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/files?path="+url.QueryEscape(virtualFile), nil)
	w := httptest.NewRecorder()

	params := GetFilesParams{
		Path:     &virtualFile,
		Username: &currentUser.Username,
	}
	api.GetFiles(w, req, params)

	resp := w.Result()
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, expectedContent, body)
	assert.Equal(t, strconv.Itoa(len(expectedContent)), resp.Header.Get("Content-Length"))
}

func TestGetFiles_IdentityRangeDownload_VirtualFile(t *testing.T) {
	t.Parallel()

	virtualFile := "/proc/version"

	expectedContent, err := os.ReadFile(virtualFile)
	require.NoError(t, err)
	require.Greater(t, len(expectedContent), 5)

	currentUser, err := user.Current()
	require.NoError(t, err)

	logger := zerolog.Nop()
	defaults := &execcontext.Defaults{
		EnvVars: utils.NewEnvVars(),
		User:    currentUser.Username,
	}
	api := New(&logger, defaults, nil, false, cgroups.NewWorkloadFreezer(cgroups.NewNoopManager()), nil, cpus.NewNoopManager())

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/files?path="+url.QueryEscape(virtualFile), nil)
	req.Header.Set("Range", "bytes=0-4")
	w := httptest.NewRecorder()

	params := GetFilesParams{
		Path:     &virtualFile,
		Username: &currentUser.Username,
	}
	api.GetFiles(w, req, params)

	resp := w.Result()
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	assert.Equal(t, http.StatusPartialContent, resp.StatusCode)
	assert.Equal(t, expectedContent[:5], body)
}

// Devices report stat size 0 too, but endless ones like /dev/zero must not be
// read to the end.
func TestGetFiles_IdentityDownload_CharDevice(t *testing.T) {
	t.Parallel()

	device := "/dev/zero"

	currentUser, err := user.Current()
	require.NoError(t, err)

	logger := zerolog.Nop()
	defaults := &execcontext.Defaults{
		EnvVars: utils.NewEnvVars(),
		User:    currentUser.Username,
	}
	api := New(&logger, defaults, nil, false, cgroups.NewWorkloadFreezer(cgroups.NewNoopManager()), nil, cpus.NewNoopManager())

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/files?path="+url.QueryEscape(device), nil)
	w := httptest.NewRecorder()

	params := GetFilesParams{
		Path:     &device,
		Username: &currentUser.Username,
	}
	api.GetFiles(w, req, params)

	resp := w.Result()
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Empty(t, body)
}

// sysfs files report a page size regardless of their content.
func TestGetFiles_IdentityDownload_SysfsFile(t *testing.T) {
	t.Parallel()

	sysfsFile := "/sys/kernel/mm/transparent_hugepage/enabled"

	stat, err := os.Stat(sysfsFile)
	if err != nil {
		t.Skipf("%s not available: %v", sysfsFile, err)
	}

	expectedContent, err := os.ReadFile(sysfsFile)
	require.NoError(t, err)
	require.NotEmpty(t, expectedContent)
	if stat.Size() == int64(len(expectedContent)) {
		t.Skipf("expected %s stat size to differ from its content, both are %d", sysfsFile, stat.Size())
	}

	currentUser, err := user.Current()
	require.NoError(t, err)

	logger := zerolog.Nop()
	defaults := &execcontext.Defaults{
		EnvVars: utils.NewEnvVars(),
		User:    currentUser.Username,
	}
	api := New(&logger, defaults, nil, false, cgroups.NewWorkloadFreezer(cgroups.NewNoopManager()), nil, cpus.NewNoopManager())

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/files?path="+url.QueryEscape(sysfsFile), nil)
	w := httptest.NewRecorder()

	params := GetFilesParams{
		Path:     &sysfsFile,
		Username: &currentUser.Username,
	}
	api.GetFiles(w, req, params)

	resp := w.Result()
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, expectedContent, body)
	assert.Equal(t, strconv.Itoa(len(expectedContent)), resp.Header.Get("Content-Length"))
}

func getFileIdentity(t *testing.T, path string, header http.Header) (int, http.Header, []byte) {
	t.Helper()

	currentUser, err := user.Current()
	require.NoError(t, err)

	logger := zerolog.Nop()
	defaults := &execcontext.Defaults{
		EnvVars: utils.NewEnvVars(),
		User:    currentUser.Username,
	}
	api := New(&logger, defaults, nil, false, cgroups.NewWorkloadFreezer(cgroups.NewNoopManager()), nil, cpus.NewNoopManager())

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/files?path="+url.QueryEscape(path), nil)
	maps.Copy(req.Header, header)
	w := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		defer close(done)
		api.GetFiles(w, req, GetFilesParams{Path: &path, Username: &currentUser.Username})
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		require.FailNow(t, "GetFiles did not return", path)
	}

	resp := w.Result()
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return resp.StatusCode, resp.Header, body
}

// Files like trace_pipe or /proc/kmsg block until the kernel produces data;
// the download must return what is there instead of waiting.
func TestGetFiles_IdentityDownload_BlockingVirtualFile(t *testing.T) {
	t.Parallel()

	pipe := "/sys/kernel/tracing/trace_pipe"
	f, err := os.Open(pipe)
	if err != nil {
		t.Skipf("%s not readable: %v", pipe, err)
	}
	f.Close()

	status, _, _ := getFileIdentity(t, pipe, nil)

	assert.Equal(t, http.StatusOK, status)
}

// A virtual file's mtime doesn't follow its content, so a conditional request
// must still get the current content.
func TestGetFiles_IdentityDownload_VirtualFileIgnoresConditional(t *testing.T) {
	t.Parallel()

	virtualFile := "/proc/version"
	expectedContent, err := os.ReadFile(virtualFile)
	require.NoError(t, err)

	status, header, body := getFileIdentity(t, virtualFile, http.Header{"If-Modified-Since": {"Fri, 01 Jan 2100 00:00:00 GMT"}})

	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, expectedContent, body)
	assert.Empty(t, header.Get("Last-Modified"))
}

// Large virtual files that report their real size keep length and Range.
func TestGetFiles_IdentityRangeDownload_LargeSizedVirtualFile(t *testing.T) {
	t.Parallel()

	btf := "/sys/kernel/btf/vmlinux"
	stat, err := os.Stat(btf)
	if err != nil {
		t.Skipf("%s not available: %v", btf, err)
	}
	if stat.Size() <= maxFileSizeForIdentityFallback {
		t.Skipf("%s is %d bytes, not above the buffer cap", btf, stat.Size())
	}

	f, err := os.Open(btf)
	require.NoError(t, err)
	expected := make([]byte, 4)
	_, err = io.ReadFull(f, expected)
	f.Close()
	require.NoError(t, err)

	status, header, body := getFileIdentity(t, btf, http.Header{"Range": {"bytes=0-3"}})

	assert.Equal(t, http.StatusPartialContent, status)
	assert.Equal(t, expected, body)
	assert.Equal(t, "bytes 0-3/"+strconv.FormatInt(stat.Size(), 10), header.Get("Content-Range"))
}
