package api

import (
	"bytes"
	"compress/gzip"
	"io"
	"mime/multipart"
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
	"github.com/e2b-dev/infra/packages/envd/internal/services/cpus"
	"github.com/e2b-dev/infra/packages/envd/internal/utils"
)

func TestGetFilesContentDisposition(t *testing.T) {
	t.Parallel()

	currentUser, err := user.Current()
	require.NoError(t, err)

	tests := []struct {
		name           string
		filename       string
		expectedHeader string
	}{
		{
			name:           "simple filename",
			filename:       "test.txt",
			expectedHeader: `inline; filename=test.txt`,
		},
		{
			name:           "filename with extension",
			filename:       "presentation.pptx",
			expectedHeader: `inline; filename=presentation.pptx`,
		},
		{
			name:           "filename with multiple dots",
			filename:       "archive.tar.gz",
			expectedHeader: `inline; filename=archive.tar.gz`,
		},
		{
			name:           "filename with spaces",
			filename:       "my document.pdf",
			expectedHeader: `inline; filename="my document.pdf"`,
		},
		{
			name:           "filename with quotes",
			filename:       `file"name.txt`,
			expectedHeader: `inline; filename="file\"name.txt"`,
		},
		{
			name:           "filename with backslash",
			filename:       `file\name.txt`,
			expectedHeader: `inline; filename="file\\name.txt"`,
		},
		{
			name:           "unicode filename",
			filename:       "\u6587\u6863.pdf", // 文档.pdf in Chinese
			expectedHeader: "inline; filename*=utf-8''%E6%96%87%E6%A1%A3.pdf",
		},
		{
			name:           "dotfile preserved",
			filename:       ".env",
			expectedHeader: `inline; filename=.env`,
		},
		{
			name:           "dotfile with extension preserved",
			filename:       ".gitignore",
			expectedHeader: `inline; filename=.gitignore`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Create a temp directory and file
			tempDir := t.TempDir()
			tempFile := filepath.Join(tempDir, tt.filename)
			err := os.WriteFile(tempFile, []byte("test content"), 0o644)
			require.NoError(t, err)

			// Create test API
			logger := zerolog.Nop()
			defaults := &execcontext.Defaults{
				EnvVars: utils.NewEnvVars(),
				User:    currentUser.Username,
			}
			api := New(&logger, defaults, nil, false, cgroups.NewWorkloadFreezer(cgroups.NewNoopManager()), nil, cpus.NewNoopManager())

			// Create request and response recorder
			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/files?path="+url.QueryEscape(tempFile), nil)
			w := httptest.NewRecorder()

			// Call the handler
			params := GetFilesParams{
				Path:     &tempFile,
				Username: &currentUser.Username,
			}
			api.GetFiles(w, req, params)

			// Check response
			resp := w.Result()
			defer resp.Body.Close()

			assert.Equal(t, http.StatusOK, resp.StatusCode)

			// Verify Content-Disposition header
			contentDisposition := resp.Header.Get("Content-Disposition")
			assert.Equal(t, tt.expectedHeader, contentDisposition, "Content-Disposition header should be set with correct filename")
		})
	}
}

func TestGetFilesContentDispositionWithNestedPath(t *testing.T) {
	t.Parallel()

	currentUser, err := user.Current()
	require.NoError(t, err)

	// Create a temp directory with nested structure
	tempDir := t.TempDir()
	nestedDir := filepath.Join(tempDir, "subdir", "another")
	err = os.MkdirAll(nestedDir, 0o755)
	require.NoError(t, err)

	filename := "document.pdf"
	tempFile := filepath.Join(nestedDir, filename)
	err = os.WriteFile(tempFile, []byte("test content"), 0o644)
	require.NoError(t, err)

	// Create test API
	logger := zerolog.Nop()
	defaults := &execcontext.Defaults{
		EnvVars: utils.NewEnvVars(),
		User:    currentUser.Username,
	}
	api := New(&logger, defaults, nil, false, cgroups.NewWorkloadFreezer(cgroups.NewNoopManager()), nil, cpus.NewNoopManager())

	// Create request and response recorder
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/files?path="+url.QueryEscape(tempFile), nil)
	w := httptest.NewRecorder()

	// Call the handler
	params := GetFilesParams{
		Path:     &tempFile,
		Username: &currentUser.Username,
	}
	api.GetFiles(w, req, params)

	// Check response
	resp := w.Result()
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Verify Content-Disposition header uses only the base filename, not the full path
	contentDisposition := resp.Header.Get("Content-Disposition")
	assert.Equal(t, `inline; filename=document.pdf`, contentDisposition, "Content-Disposition should contain only the filename, not the path")
}

func TestGetFiles_GzipEncoding_ExplicitIdentityOffWithRange(t *testing.T) {
	t.Parallel()

	currentUser, err := user.Current()
	require.NoError(t, err)

	// Create a temp directory with a test file
	tempDir := t.TempDir()
	filename := "document.pdf"
	tempFile := filepath.Join(tempDir, filename)
	err = os.WriteFile(tempFile, []byte("test content"), 0o644)
	require.NoError(t, err)

	// Create test API
	logger := zerolog.Nop()
	defaults := &execcontext.Defaults{
		EnvVars: utils.NewEnvVars(),
		User:    currentUser.Username,
	}
	api := New(&logger, defaults, nil, false, cgroups.NewWorkloadFreezer(cgroups.NewNoopManager()), nil, cpus.NewNoopManager())

	// Create request and response recorder
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/files?path="+url.QueryEscape(tempFile), nil)
	req.Header.Set("Accept-Encoding", "gzip; q=1,*; q=0")
	req.Header.Set("Range", "bytes=0-4") // Request first 5 bytes
	w := httptest.NewRecorder()

	// Call the handler
	params := GetFilesParams{
		Path:     &tempFile,
		Username: &currentUser.Username,
	}
	api.GetFiles(w, req, params)

	// Check response
	resp := w.Result()
	defer resp.Body.Close()

	assert.Equal(t, http.StatusNotAcceptable, resp.StatusCode)
}

func TestGetFiles_GzipDownload(t *testing.T) {
	t.Parallel()

	currentUser, err := user.Current()
	require.NoError(t, err)

	originalContent := []byte("hello world, this is a test file for gzip compression")

	// Create a temp file with known content
	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, "test.txt")
	err = os.WriteFile(tempFile, originalContent, 0o644)
	require.NoError(t, err)

	logger := zerolog.Nop()
	defaults := &execcontext.Defaults{
		EnvVars: utils.NewEnvVars(),
		User:    currentUser.Username,
	}
	api := New(&logger, defaults, nil, false, cgroups.NewWorkloadFreezer(cgroups.NewNoopManager()), nil, cpus.NewNoopManager())

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/files?path="+url.QueryEscape(tempFile), nil)
	req.Header.Set("Accept-Encoding", "gzip")
	w := httptest.NewRecorder()

	params := GetFilesParams{
		Path:     &tempFile,
		Username: &currentUser.Username,
	}
	api.GetFiles(w, req, params)

	resp := w.Result()
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "gzip", resp.Header.Get("Content-Encoding"))
	assert.Equal(t, "text/plain; charset=utf-8", resp.Header.Get("Content-Type"))

	// Decompress the gzip response body
	gzReader, err := gzip.NewReader(resp.Body)
	require.NoError(t, err)
	defer gzReader.Close()

	decompressed, err := io.ReadAll(gzReader)
	require.NoError(t, err)

	assert.Equal(t, originalContent, decompressed)
}

func TestPostFiles_GzipUpload(t *testing.T) {
	t.Parallel()

	currentUser, err := user.Current()
	require.NoError(t, err)

	originalContent := []byte("hello world, this is a test file uploaded with gzip")

	// Build a multipart body
	var multipartBuf bytes.Buffer
	mpWriter := multipart.NewWriter(&multipartBuf)
	part, err := mpWriter.CreateFormFile("file", "uploaded.txt")
	require.NoError(t, err)
	_, err = part.Write(originalContent)
	require.NoError(t, err)
	err = mpWriter.Close()
	require.NoError(t, err)

	// Gzip-compress the entire multipart body
	var gzBuf bytes.Buffer
	gzWriter := gzip.NewWriter(&gzBuf)
	_, err = gzWriter.Write(multipartBuf.Bytes())
	require.NoError(t, err)
	err = gzWriter.Close()
	require.NoError(t, err)

	// Create test API
	tempDir := t.TempDir()
	destPath := filepath.Join(tempDir, "uploaded.txt")

	logger := zerolog.Nop()
	defaults := &execcontext.Defaults{
		EnvVars: utils.NewEnvVars(),
		User:    currentUser.Username,
	}
	api := New(&logger, defaults, nil, false, cgroups.NewWorkloadFreezer(cgroups.NewNoopManager()), nil, cpus.NewNoopManager())

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/files?path="+url.QueryEscape(destPath), &gzBuf)
	req.Header.Set("Content-Type", mpWriter.FormDataContentType())
	req.Header.Set("Content-Encoding", "gzip")
	w := httptest.NewRecorder()

	params := PostFilesParams{
		Path:     &destPath,
		Username: &currentUser.Username,
	}
	api.PostFiles(w, req, params)

	resp := w.Result()
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Verify the file was written with the original (decompressed) content
	data, err := os.ReadFile(destPath)
	require.NoError(t, err)
	assert.Equal(t, originalContent, data)
}

func TestPostFiles_RawBodyUpload(t *testing.T) {
	t.Parallel()

	currentUser, err := user.Current()
	require.NoError(t, err)

	originalContent := []byte("raw body upload content")

	tempDir := t.TempDir()
	destPath := filepath.Join(tempDir, "raw-uploaded.txt")

	logger := zerolog.Nop()
	defaults := &execcontext.Defaults{
		EnvVars: utils.NewEnvVars(),
		User:    currentUser.Username,
	}
	api := New(&logger, defaults, nil, false, cgroups.NewWorkloadFreezer(cgroups.NewNoopManager()), nil, cpus.NewNoopManager())

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/files?path="+url.QueryEscape(destPath), bytes.NewReader(originalContent))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()

	params := PostFilesParams{
		Path:     &destPath,
		Username: &currentUser.Username,
	}
	api.PostFiles(w, req, params)

	resp := w.Result()
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	data, err := os.ReadFile(destPath)
	require.NoError(t, err)
	assert.Equal(t, originalContent, data)
}

func TestPostFiles_RawBodyUploadCreatesDirectories(t *testing.T) {
	t.Parallel()

	currentUser, err := user.Current()
	require.NoError(t, err)

	originalContent := []byte("nested dir content")

	tempDir := t.TempDir()
	destPath := filepath.Join(tempDir, "a", "b", "c", "nested.txt")

	logger := zerolog.Nop()
	defaults := &execcontext.Defaults{
		EnvVars: utils.NewEnvVars(),
		User:    currentUser.Username,
	}
	api := New(&logger, defaults, nil, false, cgroups.NewWorkloadFreezer(cgroups.NewNoopManager()), nil, cpus.NewNoopManager())

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/files?path="+url.QueryEscape(destPath), bytes.NewReader(originalContent))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()

	params := PostFilesParams{
		Path:     &destPath,
		Username: &currentUser.Username,
	}
	api.PostFiles(w, req, params)

	resp := w.Result()
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	data, err := os.ReadFile(destPath)
	require.NoError(t, err)
	assert.Equal(t, originalContent, data)
}

func TestPostFiles_RawBodyUploadRequiresPath(t *testing.T) {
	t.Parallel()

	currentUser, err := user.Current()
	require.NoError(t, err)

	logger := zerolog.Nop()
	defaults := &execcontext.Defaults{
		EnvVars: utils.NewEnvVars(),
		User:    currentUser.Username,
	}
	api := New(&logger, defaults, nil, false, cgroups.NewWorkloadFreezer(cgroups.NewNoopManager()), nil, cpus.NewNoopManager())

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/files", bytes.NewReader([]byte("some content")))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()

	params := PostFilesParams{
		Username: &currentUser.Username,
	}
	api.PostFiles(w, req, params)

	resp := w.Result()
	defer resp.Body.Close()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestPostFiles_RawBodyUploadOverwritesExisting(t *testing.T) {
	t.Parallel()

	currentUser, err := user.Current()
	require.NoError(t, err)

	tempDir := t.TempDir()
	destPath := filepath.Join(tempDir, "existing.txt")
	err = os.WriteFile(destPath, []byte("old content"), 0o644)
	require.NoError(t, err)

	newContent := []byte("new content via raw upload")

	logger := zerolog.Nop()
	defaults := &execcontext.Defaults{
		EnvVars: utils.NewEnvVars(),
		User:    currentUser.Username,
	}
	api := New(&logger, defaults, nil, false, cgroups.NewWorkloadFreezer(cgroups.NewNoopManager()), nil, cpus.NewNoopManager())

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/files?path="+url.QueryEscape(destPath), bytes.NewReader(newContent))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()

	params := PostFilesParams{
		Path:     &destPath,
		Username: &currentUser.Username,
	}
	api.PostFiles(w, req, params)

	resp := w.Result()
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	data, err := os.ReadFile(destPath)
	require.NoError(t, err)
	assert.Equal(t, newContent, data)
}

func TestPostFiles_RawBodyGzipUpload(t *testing.T) {
	t.Parallel()

	currentUser, err := user.Current()
	require.NoError(t, err)

	originalContent := []byte("raw body gzip upload content")

	// Gzip-compress the content
	var gzBuf bytes.Buffer
	gzWriter := gzip.NewWriter(&gzBuf)
	_, err = gzWriter.Write(originalContent)
	require.NoError(t, err)
	err = gzWriter.Close()
	require.NoError(t, err)

	tempDir := t.TempDir()
	destPath := filepath.Join(tempDir, "raw-gzip.txt")

	logger := zerolog.Nop()
	defaults := &execcontext.Defaults{
		EnvVars: utils.NewEnvVars(),
		User:    currentUser.Username,
	}
	api := New(&logger, defaults, nil, false, cgroups.NewWorkloadFreezer(cgroups.NewNoopManager()), nil, cpus.NewNoopManager())

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/files?path="+url.QueryEscape(destPath), &gzBuf)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Content-Encoding", "gzip")
	w := httptest.NewRecorder()

	params := PostFilesParams{
		Path:     &destPath,
		Username: &currentUser.Username,
	}
	api.PostFiles(w, req, params)

	resp := w.Result()
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	data, err := os.ReadFile(destPath)
	require.NoError(t, err)
	assert.Equal(t, originalContent, data)
}

func TestPostFiles_UnsupportedContentType(t *testing.T) {
	t.Parallel()

	currentUser, err := user.Current()
	require.NoError(t, err)

	logger := zerolog.Nop()
	defaults := &execcontext.Defaults{
		EnvVars: utils.NewEnvVars(),
		User:    currentUser.Username,
	}
	api := New(&logger, defaults, nil, false, cgroups.NewWorkloadFreezer(cgroups.NewNoopManager()), nil, cpus.NewNoopManager())

	tempDir := t.TempDir()
	destPath := filepath.Join(tempDir, "test.txt")

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/files?path="+url.QueryEscape(destPath), bytes.NewReader([]byte(`{"key":"value"}`)))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	params := PostFilesParams{
		Path:     &destPath,
		Username: &currentUser.Username,
	}
	api.PostFiles(w, req, params)

	resp := w.Result()
	defer resp.Body.Close()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestPostFiles_MultipartStillWorksWithoutContentType(t *testing.T) {
	t.Parallel()

	currentUser, err := user.Current()
	require.NoError(t, err)

	originalContent := []byte("multipart backwards compat test")

	var multipartBuf bytes.Buffer
	mpWriter := multipart.NewWriter(&multipartBuf)
	part, err := mpWriter.CreateFormFile("file", "compat.txt")
	require.NoError(t, err)
	_, err = part.Write(originalContent)
	require.NoError(t, err)
	err = mpWriter.Close()
	require.NoError(t, err)

	tempDir := t.TempDir()
	destPath := filepath.Join(tempDir, "compat.txt")

	logger := zerolog.Nop()
	defaults := &execcontext.Defaults{
		EnvVars: utils.NewEnvVars(),
		User:    currentUser.Username,
	}
	api := New(&logger, defaults, nil, false, cgroups.NewWorkloadFreezer(cgroups.NewNoopManager()), nil, cpus.NewNoopManager())

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/files?path="+url.QueryEscape(destPath), &multipartBuf)
	req.Header.Set("Content-Type", mpWriter.FormDataContentType())
	w := httptest.NewRecorder()

	params := PostFilesParams{
		Path:     &destPath,
		Username: &currentUser.Username,
	}
	api.PostFiles(w, req, params)

	resp := w.Result()
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	data, err := os.ReadFile(destPath)
	require.NoError(t, err)
	assert.Equal(t, originalContent, data)
}

func TestGzipUploadThenGzipDownload(t *testing.T) {
	t.Parallel()

	currentUser, err := user.Current()
	require.NoError(t, err)

	originalContent := []byte("round-trip gzip test: upload compressed, download compressed, verify match")

	// --- Upload with gzip ---

	// Build a multipart body
	var multipartBuf bytes.Buffer
	mpWriter := multipart.NewWriter(&multipartBuf)
	part, err := mpWriter.CreateFormFile("file", "roundtrip.txt")
	require.NoError(t, err)
	_, err = part.Write(originalContent)
	require.NoError(t, err)
	err = mpWriter.Close()
	require.NoError(t, err)

	// Gzip-compress the entire multipart body
	var gzBuf bytes.Buffer
	gzWriter := gzip.NewWriter(&gzBuf)
	_, err = gzWriter.Write(multipartBuf.Bytes())
	require.NoError(t, err)
	err = gzWriter.Close()
	require.NoError(t, err)

	tempDir := t.TempDir()
	destPath := filepath.Join(tempDir, "roundtrip.txt")

	logger := zerolog.Nop()
	defaults := &execcontext.Defaults{
		EnvVars: utils.NewEnvVars(),
		User:    currentUser.Username,
	}
	api := New(&logger, defaults, nil, false, cgroups.NewWorkloadFreezer(cgroups.NewNoopManager()), nil, cpus.NewNoopManager())

	uploadReq := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/files?path="+url.QueryEscape(destPath), &gzBuf)
	uploadReq.Header.Set("Content-Type", mpWriter.FormDataContentType())
	uploadReq.Header.Set("Content-Encoding", "gzip")
	uploadW := httptest.NewRecorder()

	uploadParams := PostFilesParams{
		Path:     &destPath,
		Username: &currentUser.Username,
	}
	api.PostFiles(uploadW, uploadReq, uploadParams)

	uploadResp := uploadW.Result()
	defer uploadResp.Body.Close()

	require.Equal(t, http.StatusOK, uploadResp.StatusCode)

	// --- Download with gzip ---

	downloadReq := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/files?path="+url.QueryEscape(destPath), nil)
	downloadReq.Header.Set("Accept-Encoding", "gzip")
	downloadW := httptest.NewRecorder()

	downloadParams := GetFilesParams{
		Path:     &destPath,
		Username: &currentUser.Username,
	}
	api.GetFiles(downloadW, downloadReq, downloadParams)

	downloadResp := downloadW.Result()
	defer downloadResp.Body.Close()

	require.Equal(t, http.StatusOK, downloadResp.StatusCode)
	assert.Equal(t, "gzip", downloadResp.Header.Get("Content-Encoding"))

	// Decompress and verify content matches original
	gzReader, err := gzip.NewReader(downloadResp.Body)
	require.NoError(t, err)
	defer gzReader.Close()

	decompressed, err := io.ReadAll(gzReader)
	require.NoError(t, err)

	assert.Equal(t, originalContent, decompressed)
}
