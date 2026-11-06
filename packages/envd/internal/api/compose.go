package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/user"
	"path/filepath"
	"syscall"

	"github.com/google/uuid"

	"github.com/e2b-dev/infra/packages/envd/internal/execcontext"
	"github.com/e2b-dev/infra/packages/envd/internal/logs"
	"github.com/e2b-dev/infra/packages/envd/internal/permissions"
)

func (a *API) PostFilesCompose(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	operationID := logs.AssignOperationID()

	var req ComposeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, http.StatusBadRequest, fmt.Errorf("invalid request body: %w", err))

		return
	}

	if len(req.SourcePaths) == 0 {
		jsonError(w, http.StatusBadRequest, errors.New("source_paths must not be empty"))

		return
	}

	if req.Destination == "" {
		jsonError(w, http.StatusBadRequest, errors.New("destination is required"))

		return
	}

	username, err := execcontext.ResolveDefaultUsername(req.Username, a.defaults.User)
	if err != nil {
		a.logger.Error().Err(err).Str(string(logs.OperationIDKey), operationID).Msg("no user specified")
		jsonError(w, http.StatusBadRequest, err)

		return
	}

	u, err := user.Lookup(username)
	if err != nil {
		errMsg := fmt.Errorf("error looking up user '%s': %w", username, err)
		a.logger.Error().Err(errMsg).Str(string(logs.OperationIDKey), operationID).Msg("user lookup failed")
		jsonError(w, http.StatusUnauthorized, errMsg)

		return
	}

	uid, gid, err := permissions.GetUserIdInts(u)
	if err != nil {
		errMsg := fmt.Errorf("error getting user ids: %w", err)
		a.logger.Error().Err(errMsg).Str(string(logs.OperationIDKey), operationID).Msg("failed to get user ids")
		jsonError(w, http.StatusInternalServerError, errMsg)

		return
	}

	destPath, err := permissions.ExpandAndResolve(req.Destination, u, a.defaults.Workdir)
	if err != nil {
		errMsg := fmt.Errorf("error resolving destination path: %w", err)
		a.logger.Error().Err(errMsg).Str(string(logs.OperationIDKey), operationID).Msg("path resolution failed")
		jsonError(w, http.StatusBadRequest, errMsg)

		return
	}

	// Lstat: a destination that is itself a link to a source is replaced,
	// not aliased.
	destInfo, destErr := os.Lstat(destPath)
	resolvedSources := make([]string, len(req.SourcePaths))
	for i, src := range req.SourcePaths {
		resolved, err := permissions.ExpandAndResolve(src, u, a.defaults.Workdir)
		if err != nil {
			jsonError(w, http.StatusBadRequest, fmt.Errorf("error resolving source path %q: %w", src, err))

			return
		}

		if resolved == destPath {
			jsonError(w, http.StatusBadRequest, fmt.Errorf("source path %q cannot be the same as destination", src))

			return
		}

		info, err := os.Stat(resolved)
		if err != nil {
			jsonError(w, http.StatusNotFound, fmt.Errorf("source file not found: %s", src))

			return
		}

		if !info.Mode().IsRegular() {
			jsonError(w, http.StatusBadRequest, fmt.Errorf("source path is not a regular file: %s", src))

			return
		}

		if destErr == nil && os.SameFile(info, destInfo) {
			jsonError(w, http.StatusBadRequest, fmt.Errorf("source path %q cannot be the same as destination", src))

			return
		}

		resolvedSources[i] = resolved
	}

	err = permissions.EnsureDirs(filepath.Dir(destPath), uid, gid)
	if err != nil {
		jsonError(w, http.StatusInternalServerError, fmt.Errorf("error ensuring directories: %w", err))

		return
	}

	// Write to a temporary file and rename on success to avoid destroying
	// any pre-existing file at destPath if assembly fails midway.
	tmpPath := destPath + ".e2b-compose." + uuid.New().String() + ".tmp"

	destFile, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o666)
	if err != nil {
		if errors.Is(err, syscall.ENOSPC) {
			jsonError(w, http.StatusInsufficientStorage, errors.New("not enough disk space available"))

			return
		}

		jsonError(w, http.StatusInternalServerError, fmt.Errorf("error creating destination file: %w", err))

		return
	}

	err = os.Chown(tmpPath, uid, gid)
	if err != nil {
		destFile.Close()
		os.Remove(tmpPath)
		jsonError(w, http.StatusInternalServerError, fmt.Errorf("error changing file ownership: %w", err))

		return
	}

	var totalSize int64

	for _, srcPath := range resolvedSources {
		srcFile, err := os.Open(srcPath)
		if err != nil {
			destFile.Close()
			os.Remove(tmpPath)
			jsonError(w, http.StatusInternalServerError, fmt.Errorf("error opening source file %s: %w", srcPath, err))

			return
		}

		// ReadFrom uses copy_file_range on Linux for zero-copy transfers
		// between regular files — data moves kernel-side without touching
		// userspace buffers.
		n, err := destFile.ReadFrom(srcFile)
		srcFile.Close()

		if err != nil {
			destFile.Close()
			os.Remove(tmpPath)

			if errors.Is(err, syscall.ENOSPC) {
				jsonError(w, http.StatusInsufficientStorage, errors.New("not enough disk space available"))

				return
			}

			jsonError(w, http.StatusInternalServerError, fmt.Errorf("error composing source %s: %w", srcPath, err))

			return
		}

		totalSize += n
	}

	if err := destFile.Close(); err != nil {
		os.Remove(tmpPath)
		jsonError(w, http.StatusInternalServerError, fmt.Errorf("error closing destination file: %w", err))

		return
	}

	if err := os.Rename(tmpPath, destPath); err != nil {
		os.Remove(tmpPath)
		jsonError(w, http.StatusInternalServerError, fmt.Errorf("error finalizing compose: %w", err))

		return
	}

	// Lstat: a source that is a link to the composed file is removed as the
	// link it is, and only the composed file's own entry is kept.
	composed, composedErr := os.Stat(destPath)
	for _, srcPath := range resolvedSources {
		if info, err := os.Lstat(srcPath); err == nil && composedErr == nil && os.SameFile(info, composed) {
			continue
		}
		os.Remove(srcPath)
	}

	a.logger.Info().
		Str(string(logs.OperationIDKey), operationID).
		Str("path", destPath).
		Int("sources", len(resolvedSources)).
		Int64("size", totalSize).
		Msg("File compose completed")

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	if err := json.NewEncoder(w).Encode(EntryInfo{
		Path: destPath,
		Name: filepath.Base(destPath),
		Type: File,
	}); err != nil {
		a.logger.Error().Err(err).Str(string(logs.OperationIDKey), operationID).Msg("failed to encode compose response")
	}
}
