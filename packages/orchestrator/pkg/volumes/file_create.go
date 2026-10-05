//go:build linux

package volumes

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

var ErrExpectedStart = errors.New("expected start message")

var ErrUnexpectedStart = errors.New("unexpected start message")

func (s *Service) CreateFile(server orchestrator.VolumeService_CreateFileServer) (err error) {
	ctx, span := tracer.Start(server.Context(), "create file in volume")
	defer func() {
		setSpanStatus(span, err)
		span.End()
	}()

	req, err := server.Recv()
	if err != nil {
		return fmt.Errorf("failed to receive start message: %w", err)
	}

	start := req.GetStart()
	if start == nil {
		return ErrExpectedStart
	}

	fs, path, errResponse := s.getFilesystemAndPath(ctx, start)
	if errResponse != nil {
		return errResponse.Err()
	}
	defer fs.Close()

	uid := utils.DerefOrDefault(start.Uid, defaultOwnerID)            //nolint:protogetter
	gid := utils.DerefOrDefault(start.Gid, defaultGroupID)            //nolint:protogetter
	mode := utils.DerefOrDefault(start.Mode, uint32(defaultFileMode)) //nolint:protogetter

	span.AddEvent("creating file", trace.WithAttributes(
		attribute.String("path", path),
		attribute.Int64("uid", int64(uid)),
		attribute.Int64("gid", int64(gid)),
		attribute.Int64("mode", int64(mode)),
	))

	dirName := filepath.Dir(path)
	if start.GetForce() {
		if err = ensureDirs(fs, dirName, uid, gid); err != nil {
			return fmt.Errorf("failed to prepare parent directories: %w", err)
		}
	}

	var flags int
	if start.GetForce() {
		flags = os.O_CREATE | os.O_WRONLY | os.O_TRUNC
	} else {
		flags = os.O_CREATE | os.O_WRONLY | os.O_EXCL
	}

	file, err := fs.OpenFile(path, flags, os.FileMode(mode).Perm())
	if err != nil {
		return processError(ctx, "failed to open file for create", err)
	}

	defer func() {
		closeErr := file.Close()
		if closeErr != nil {
			logger.L().Error(ctx, "failed to close file", zap.Error(closeErr))
		}
	}()

	for {
		req, err := server.Recv()
		if err != nil {
			return fmt.Errorf("failed to receive chunk: %w", err)
		}

		switch m := req.GetMessage().(type) {
		case *orchestrator.CreateFileRequest_Content:
			if _, err := file.Write(m.Content.GetContent()); err != nil {
				return fmt.Errorf("failed to write file content: %w", err)
			}

		case *orchestrator.CreateFileRequest_Finish:
			if err = file.Sync(); err != nil {
				return fmt.Errorf("failed to sync file to disk: %w", err)
			}

			if err := fs.Chown(path, int(uid), int(gid)); err != nil {
				return fmt.Errorf("failed to set file ownership: %w", err)
			}

			// we do this again to avoid the process' umask from automatically 'fixing' our requests.
			if err := fs.Chmod(path, os.FileMode(mode)); err != nil {
				return fmt.Errorf("failed to set file mode: %w", err)
			}

			fi, err := fs.Stat(path)
			if err != nil {
				return fmt.Errorf("failed to stat created file: %w", err)
			}

			entry := toEntry(fs, path, fi)

			return server.SendAndClose(&orchestrator.CreateFileResponse{
				Entry: entry,
			})

		default:
			return ErrUnexpectedStart
		}
	}
}
