package api

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"os/user"
	"path/filepath"
	"time"

	"github.com/e2b-dev/infra/packages/envd/internal/execcontext"
	"github.com/e2b-dev/infra/packages/envd/internal/logs"
	"github.com/e2b-dev/infra/packages/envd/internal/permissions"
)

// maxFileSizeForIdentityFallback is the maximum file size to read into memory
// when the stat size can't be trusted (virtual files like procfs/sysfs). Files
// that outgrow it are streamed without a known length instead of being truncated.
const maxFileSizeForIdentityFallback = 1 << 20 // 1 MiB

// needsBuffering reports whether http.ServeContent can't serve the file:
// it sizes the response by seeking to the end, which for virtual files doesn't
// match their content (procfs reports 0, sysfs a page size), so the body comes
// back empty, cut short, or as a seek error. Devices are left to ServeContent:
// block devices seek to their real size and /dev/zero must not be read.
// Virtual files over the cap already report their real size.
func needsBuffering(path string, stat os.FileInfo) (buffer, virtual bool) {
	if !stat.Mode().IsRegular() {
		return false, false
	}

	virtual = isVirtualFS(path)

	return stat.Size() == 0 || virtual && stat.Size() <= maxFileSizeForIdentityFallback, virtual
}

// serveBuffered reads the file into memory and serves a bytes.Reader, which
// keeps Range/conditional request semantics. It returns an error only before
// anything has been written.
func (a *API) serveBuffered(w http.ResponseWriter, r *http.Request, name string, file *os.File, stat os.FileInfo, virtual bool, operationID string) error {
	reader, err := newAvailableReader(file)
	if err != nil {
		return err
	}

	content, err := io.ReadAll(io.LimitReader(reader, maxFileSizeForIdentityFallback+1))
	if err != nil {
		return err
	}

	// A virtual file's mtime doesn't change with its content, so it must not
	// answer conditional requests.
	modTime := stat.ModTime()
	if virtual {
		modTime = time.Time{}
	}

	if len(content) <= maxFileSizeForIdentityFallback {
		http.ServeContent(w, r, name, modTime, bytes.NewReader(content))

		return nil
	}

	// The file outgrew the buffer; stream the rest without a known length,
	// like the gzip path does.
	contentType := mime.TypeByExtension(filepath.Ext(name))
	if contentType == "" {
		contentType = http.DetectContentType(content)
	}
	w.Header().Set("Content-Type", contentType)

	if _, err := w.Write(content); err != nil {
		a.logger.Error().Err(err).Str(string(logs.OperationIDKey), operationID).Msg("error writing response")

		return nil
	}

	if _, err := io.Copy(w, reader); err != nil {
		a.logger.Error().Err(err).Str(string(logs.OperationIDKey), operationID).Msg("error writing response")
	}

	return nil
}

func (a *API) GetFiles(w http.ResponseWriter, r *http.Request, params GetFilesParams) {
	defer r.Body.Close()

	var errorCode int
	var errMsg error

	var path string
	if params.Path != nil {
		path = *params.Path
	}

	operationID := logs.AssignOperationID()

	// signing authorization if needed
	err := a.validateSigning(r, params.Signature, params.SignatureExpiration, params.Username, path, SigningReadOperation)
	if err != nil {
		a.logger.Error().Err(err).Str(string(logs.OperationIDKey), operationID).Msg("error during auth validation")
		jsonError(w, http.StatusUnauthorized, err)

		return
	}

	username, err := execcontext.ResolveDefaultUsername(params.Username, a.defaults.User)
	if err != nil {
		a.logger.Error().Err(err).Str(string(logs.OperationIDKey), operationID).Msg("no user specified")
		jsonError(w, http.StatusBadRequest, err)

		return
	}

	defer func() {
		l := a.logger.
			Err(errMsg).
			Str("method", r.Method+" "+r.URL.Path).
			Str(string(logs.OperationIDKey), operationID).
			Str("path", path).
			Str("username", username)

		if errMsg != nil {
			l = l.Int("error_code", errorCode)
		}

		l.Msg("File read")
	}()

	u, err := user.Lookup(username)
	if err != nil {
		errMsg = fmt.Errorf("error looking up user '%s': %w", username, err)
		errorCode = http.StatusUnauthorized
		jsonError(w, errorCode, errMsg)

		return
	}

	resolvedPath, err := permissions.ExpandAndResolve(path, u, a.defaults.Workdir)
	if err != nil {
		errMsg = fmt.Errorf("error expanding and resolving path '%s': %w", path, err)
		errorCode = http.StatusBadRequest
		jsonError(w, errorCode, errMsg)

		return
	}

	stat, err := os.Stat(resolvedPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			errMsg = fmt.Errorf("path '%s' does not exist", resolvedPath)
			errorCode = http.StatusNotFound
			jsonError(w, errorCode, errMsg)

			return
		}

		errMsg = fmt.Errorf("error checking if path exists '%s': %w", resolvedPath, err)
		errorCode = http.StatusInternalServerError
		jsonError(w, errorCode, errMsg)

		return
	}

	if stat.IsDir() {
		errMsg = fmt.Errorf("path '%s' is a directory", resolvedPath)
		errorCode = http.StatusBadRequest
		jsonError(w, errorCode, errMsg)

		return
	}

	// Validate Accept-Encoding header
	encoding, err := parseAcceptEncoding(r)
	if err != nil {
		errMsg = fmt.Errorf("error parsing Accept-Encoding: %w", err)
		errorCode = http.StatusNotAcceptable
		jsonError(w, errorCode, errMsg)

		return
	}

	// Tell caches to store separate variants for different Accept-Encoding values
	w.Header().Set("Vary", "Accept-Encoding")

	// Fall back to identity for Range or conditional requests to preserve http.ServeContent
	// behavior (206 Partial Content, 304 Not Modified). However, we must check if identity
	// is acceptable per the Accept-Encoding header.
	hasRangeOrConditional := r.Header.Get("Range") != "" ||
		r.Header.Get("If-Modified-Since") != "" ||
		r.Header.Get("If-None-Match") != "" ||
		r.Header.Get("If-Range") != ""
	if hasRangeOrConditional {
		if !isIdentityAcceptable(r) {
			errMsg = errors.New("identity encoding not acceptable for Range or conditional request")
			errorCode = http.StatusNotAcceptable
			jsonError(w, errorCode, errMsg)

			return
		}
		encoding = EncodingIdentity
	}

	file, err := os.Open(resolvedPath)
	if err != nil {
		errMsg = fmt.Errorf("error opening file '%s': %w", resolvedPath, err)
		errorCode = http.StatusInternalServerError
		jsonError(w, errorCode, errMsg)

		return
	}
	defer file.Close()

	w.Header().Set("Content-Disposition", mime.FormatMediaType("inline", map[string]string{"filename": filepath.Base(resolvedPath)}))

	// Serve with gzip encoding if requested.
	if encoding == EncodingGzip {
		w.Header().Set("Content-Encoding", EncodingGzip)

		// Set Content-Type based on file extension, preserving the original type
		contentType := mime.TypeByExtension(filepath.Ext(path))
		if contentType == "" {
			contentType = "application/octet-stream"
		}
		w.Header().Set("Content-Type", contentType)

		gw := gzip.NewWriter(w)
		defer gw.Close()

		_, err = io.Copy(gw, file)
		if err != nil {
			a.logger.Error().Err(err).Str(string(logs.OperationIDKey), operationID).Msg("error writing gzip response")
		}

		return
	}

	if buffer, virtual := needsBuffering(resolvedPath, stat); buffer {
		if err := a.serveBuffered(w, r, path, file, stat, virtual, operationID); err != nil {
			errMsg = fmt.Errorf("error reading file '%s': %w", resolvedPath, err)
			errorCode = http.StatusInternalServerError
			jsonError(w, errorCode, errMsg)
		}

		return
	}

	http.ServeContent(w, r, path, stat.ModTime(), file)
}
