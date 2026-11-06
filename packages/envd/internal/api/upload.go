package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/rs/zerolog"

	"github.com/e2b-dev/infra/packages/envd/internal/execcontext"
	"github.com/e2b-dev/infra/packages/envd/internal/logs"
	"github.com/e2b-dev/infra/packages/envd/internal/permissions"
	"github.com/e2b-dev/infra/packages/envd/internal/utils"
	"github.com/e2b-dev/infra/packages/shared/pkg/filesystem"
)

var ErrNoDiskSpace = errors.New("not enough disk space available")

// metadataHeaderPrefix is the request-header prefix used to attach
// user-defined metadata to file uploads. Each `<metadataHeaderPrefix><key>:
// <value>` header becomes a `user.e2b.<key>` xattr on the uploaded file.
const metadataHeaderPrefix = "X-Metadata-"

// extractMetadataHeaders returns all `X-Metadata-*` headers from h, with the
// prefix stripped and keys lowercased. Returns nil if none are present.
func extractMetadataHeaders(h http.Header) map[string]string {
	var metadata map[string]string
	for name, values := range h {
		if len(values) == 0 || !strings.HasPrefix(name, metadataHeaderPrefix) {
			continue
		}
		key := strings.ToLower(strings.TrimPrefix(name, metadataHeaderPrefix))
		if key == "" {
			continue
		}
		if metadata == nil {
			metadata = make(map[string]string)
		}
		metadata[key] = values[0]
	}

	return metadata
}

func processFile(r *http.Request, path string, part io.Reader, uid, gid int, metadata map[string]string, logger zerolog.Logger) (int, error) {
	logger.Debug().
		Str("path", path).
		Msg("File processing")

	err := permissions.EnsureDirs(filepath.Dir(path), uid, gid)
	if err != nil {
		err := fmt.Errorf("error ensuring directories: %w", err)

		return http.StatusInternalServerError, err
	}

	canBePreChowned := false
	stat, err := os.Stat(path)
	if err != nil && !os.IsNotExist(err) {
		errMsg := fmt.Errorf("error getting file info: %w", err)

		return http.StatusInternalServerError, errMsg
	} else if err == nil {
		if stat.IsDir() {
			err := fmt.Errorf("path is a directory: %s", path)

			return http.StatusBadRequest, err
		}
		canBePreChowned = true
	}

	hasBeenChowned := false
	if canBePreChowned {
		err = os.Chown(path, uid, gid)
		if err != nil {
			if !os.IsNotExist(err) {
				err = fmt.Errorf("error changing file ownership: %w", err)

				return http.StatusInternalServerError, err
			}
		} else {
			hasBeenChowned = true
		}
	}

	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o666)
	if err != nil {
		if errors.Is(err, syscall.ENOSPC) {
			err = fmt.Errorf("not enough inodes available: %w", err)

			return http.StatusInsufficientStorage, err
		}

		err := fmt.Errorf("error opening file: %w", err)

		return http.StatusInternalServerError, err
	}

	defer file.Close()

	if !hasBeenChowned {
		err = file.Chown(uid, gid)
		if err != nil {
			err := fmt.Errorf("error changing file ownership: %w", err)

			return http.StatusInternalServerError, err
		}
	}

	_, err = file.ReadFrom(part)
	if err != nil {
		if errors.Is(err, syscall.ENOSPC) {
			err = ErrNoDiskSpace
			if r.ContentLength > 0 {
				err = fmt.Errorf("attempted to write %d bytes: %w", r.ContentLength, err)
			}

			return http.StatusInsufficientStorage, err
		}

		err = fmt.Errorf("error writing file: %w", err)

		return http.StatusInternalServerError, err
	}

	// Always (re)write metadata, even with an empty/nil map, so that
	// overwriting a file replaces its full metadata set: keys absent from
	// this request are cleared (O_TRUNC truncates the body but preserves
	// xattrs from a prior upload).
	if err := filesystem.WriteMetadata(file, metadata); err != nil {
		switch {
		case filesystem.IsXattrUnsupported(err):
			// Filesystem doesn't support xattrs. ext4 (the sandbox rootfs)
			// always supports them; this branch only triggers for virtual
			// filesystems such as /sys and /proc that the upload API also
			// supports. The file body was already persisted, so we log and
			// continue; the response EntryInfo reads xattrs back from disk
			// so it won't falsely claim metadata was persisted.
			logger.Warn().Str("path", path).Err(err).Msg("filesystem does not support xattrs; metadata not persisted")
		case errors.Is(err, syscall.ENOSPC) || errors.Is(err, syscall.EDQUOT):
			return http.StatusInsufficientStorage, fmt.Errorf("not enough space for file metadata: %w", err)
		default:
			return http.StatusInternalServerError, fmt.Errorf("error writing file metadata: %w", err)
		}
	}

	return http.StatusNoContent, nil
}

// resolvePath resolves a part's destination and refuses one this request already
// wrote: the same path, or another name for the same file (a hard or symbolic
// link), which opening it would truncate.
func resolvePath(part *multipart.Part, paths *UploadSuccess, u *user.User, defaultPath *string, params PostFilesParams) (string, error) {
	var pathToResolve string

	if params.Path != nil {
		pathToResolve = *params.Path
	} else {
		var err error
		customPart := utils.NewCustomPart(part)
		pathToResolve, err = customPart.FileNameWithPath()
		if err != nil {
			return "", fmt.Errorf("error getting multipart custom part file name: %w", err)
		}
	}

	filePath, err := permissions.ExpandAndResolve(pathToResolve, u, defaultPath)
	if err != nil {
		return "", fmt.Errorf("error resolving path: %w", err)
	}

	var target os.FileInfo
	if len(*paths) > 0 {
		target, _ = os.Stat(filePath)
	}
	for _, entry := range *paths {
		if entry.Path == filePath || sameFile(target, entry.Path) {
			var alreadyUploaded []string
			for _, uploadedFile := range *paths {
				if uploadedFile.Path != entry.Path {
					alreadyUploaded = append(alreadyUploaded, uploadedFile.Path)
				}
			}

			errMsg := fmt.Errorf("you cannot upload multiple files to the same path '%s' in one upload request, only the first specified file was uploaded", entry.Path)

			if len(alreadyUploaded) > 1 {
				errMsg = fmt.Errorf("%w, also the following files were uploaded: %v", errMsg, strings.Join(alreadyUploaded, ", "))
			}

			return "", errMsg
		}
	}

	return filePath, nil
}

// sameFile reports whether path names the file target describes.
func sameFile(target os.FileInfo, path string) bool {
	if target == nil {
		return false
	}
	info, err := os.Stat(path)

	return err == nil && os.SameFile(target, info)
}

func (a *API) handlePart(r *http.Request, part *multipart.Part, paths UploadSuccess, u *user.User, uid, gid int, metadata map[string]string, operationID string, params PostFilesParams) (*EntryInfo, int, error) {
	defer part.Close()

	if part.FormName() != "file" {
		return nil, http.StatusOK, nil
	}

	filePath, err := resolvePath(part, &paths, u, a.defaults.Workdir, params)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	logger := a.logger.
		With().
		Str(string(logs.OperationIDKey), operationID).
		Str("event_type", "file_processing").
		Logger()

	status, err := processFile(r, filePath, part, uid, gid, metadata, logger)
	if err != nil {
		return nil, status, err
	}

	entry := &EntryInfo{
		Path: filePath,
		Name: filepath.Base(filePath),
		Type: File,
	}
	persisted, err := filesystem.ReadMetadata(filePath)
	if err != nil {
		logger.Warn().Str("path", filePath).Err(err).Msg("failed to read back file metadata for upload response")
	}
	if len(persisted) > 0 {
		entry.Metadata = &persisted
	}

	return entry, http.StatusOK, nil
}

func (a *API) PostFiles(w http.ResponseWriter, r *http.Request, params PostFilesParams) {
	// Capture original body to ensure it's always closed
	originalBody := r.Body
	defer originalBody.Close()

	var errorCode int
	var errMsg error

	var path string
	if params.Path != nil {
		path = *params.Path
	}

	operationID := logs.AssignOperationID()

	// signing authorization if needed
	err := a.validateSigning(r, params.Signature, params.SignatureExpiration, params.Username, path, SigningWriteOperation)
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

		l.Msg("File write")
	}()

	// Handle gzip-encoded request body
	body, err := getDecompressedBody(r)
	if err != nil {
		errMsg = fmt.Errorf("error decompressing request body: %w", err)
		errorCode = http.StatusBadRequest
		jsonError(w, errorCode, errMsg)

		return
	}
	defer body.Close()
	r.Body = body

	u, err := user.Lookup(username)
	if err != nil {
		errMsg = fmt.Errorf("error looking up user '%s': %w", username, err)
		errorCode = http.StatusUnauthorized

		jsonError(w, errorCode, errMsg)

		return
	}

	uid, gid, err := permissions.GetUserIdInts(u)
	if err != nil {
		errMsg = fmt.Errorf("error getting user ids: %w", err)

		jsonError(w, http.StatusInternalServerError, errMsg)

		return
	}

	metadata := extractMetadataHeaders(r.Header)
	if err := filesystem.ValidateMetadata(metadata); err != nil {
		errMsg = fmt.Errorf("invalid metadata: %w", err)
		errorCode = http.StatusBadRequest
		jsonError(w, errorCode, errMsg)

		return
	}

	// Use raw body upload only for application/octet-stream, default to multipart for backwards compatibility
	contentType := r.Header.Get("Content-Type")
	mediaType, _, _ := mime.ParseMediaType(contentType)

	var paths UploadSuccess

	switch {
	case mediaType == "application/octet-stream":
		paths, errorCode, errMsg = a.handleRawUpload(r, u, uid, gid, metadata, operationID, params)
	case strings.HasPrefix(mediaType, "multipart/"):
		paths, errorCode, errMsg = a.handleMultipartUpload(r, u, uid, gid, metadata, operationID, params)
	default:
		errorCode = http.StatusBadRequest
		errMsg = fmt.Errorf("unsupported content type: %s, expected multipart/form-data or application/octet-stream", contentType)
	}

	if errMsg != nil {
		jsonError(w, errorCode, errMsg)

		return
	}

	data, err := json.Marshal(paths)
	if err != nil {
		errMsg = fmt.Errorf("error marshaling response: %w", err)
		errorCode = http.StatusInternalServerError
		jsonError(w, errorCode, errMsg)

		return
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

func (a *API) handleMultipartUpload(r *http.Request, u *user.User, uid, gid int, metadata map[string]string, operationID string, params PostFilesParams) (UploadSuccess, int, error) {
	f, err := r.MultipartReader()
	if err != nil {
		return nil, http.StatusInternalServerError, fmt.Errorf("error parsing multipart form: %w", err)
	}

	paths := UploadSuccess{}

	for {
		part, partErr := f.NextPart()

		if partErr == io.EOF {
			break
		} else if partErr != nil {
			return nil, http.StatusInternalServerError, fmt.Errorf("error reading form: %w", partErr)
		}

		entry, status, err := a.handlePart(r, part, paths, u, uid, gid, metadata, operationID, params)
		if err != nil {
			return nil, status, err
		}

		if entry != nil {
			paths = append(paths, *entry)
		}
	}

	return paths, http.StatusOK, nil
}

func (a *API) handleRawUpload(r *http.Request, u *user.User, uid, gid int, metadata map[string]string, operationID string, params PostFilesParams) (UploadSuccess, int, error) {
	if params.Path == nil {
		return nil, http.StatusBadRequest, errors.New("path query parameter is required for raw body upload")
	}

	filePath, err := permissions.ExpandAndResolve(*params.Path, u, a.defaults.Workdir)
	if err != nil {
		return nil, http.StatusBadRequest, fmt.Errorf("error resolving path: %w", err)
	}

	logger := a.logger.
		With().
		Str(string(logs.OperationIDKey), operationID).
		Str("event_type", "file_processing").
		Logger()

	status, err := processFile(r, filePath, r.Body, uid, gid, metadata, logger)
	if err != nil {
		return nil, status, err
	}

	entry := EntryInfo{
		Path: filePath,
		Name: filepath.Base(filePath),
		Type: File,
	}
	persisted, err := filesystem.ReadMetadata(filePath)
	if err != nil {
		logger.Warn().Str("path", filePath).Err(err).Msg("failed to read back file metadata for upload response")
	}
	if len(persisted) > 0 {
		entry.Metadata = &persisted
	}

	return UploadSuccess{entry}, http.StatusOK, nil
}
