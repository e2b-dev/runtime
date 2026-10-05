//go:build linux

package volumes

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"time"

	"go.opentelemetry.io/otel"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/chrooted"
	"github.com/e2b-dev/infra/packages/shared/pkg/filesystem"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
)

const (
	defaultDirMode  os.FileMode = 0o777
	defaultFileMode os.FileMode = 0o666
	defaultOwnerID  uint32      = 9090
	defaultGroupID  uint32      = 9090
)

var tracer = otel.Tracer("github.com/e2b-dev/infra/packages/orchestrator/pkg/volumes")

type Service struct {
	orchestrator.UnimplementedVolumeServiceServer

	builder *chrooted.Builder
	config  cfg.Config
}

var _ orchestrator.VolumeServiceServer = (*Service)(nil)

func New(config cfg.Config, builder *chrooted.Builder) *Service {
	return &Service{config: config, builder: builder}
}

type volumePathRequest interface {
	GetVolume() *orchestrator.VolumeInfo
	GetPath() string
}

func (s *Service) getVolumeRootPath(ctx context.Context, volume *orchestrator.VolumeInfo) (string, error) {
	volumeType := volume.GetVolumeType()

	teamID, ok := pkg.TryParseUUID(volume.GetTeamId())
	if !ok {
		return "", newAPIError(ctx, codes.InvalidArgument, http.StatusBadRequest, orchestrator.UserErrorCode_INVALID_REQUEST, "invalid team ID %q", volume.GetTeamId()).Err()
	}

	volumeID, ok := pkg.TryParseUUID(volume.GetVolumeId())
	if !ok {
		return "", newAPIError(ctx, codes.InvalidArgument, http.StatusBadRequest, orchestrator.UserErrorCode_INVALID_REQUEST, "invalid volume ID %q", volume.GetVolumeId()).Err()
	}

	path, err := s.builder.BuildVolumePath(volumeType, teamID, volumeID)
	if err != nil {
		if errors.Is(err, chrooted.ErrVolumeTypeNotFound) {
			return "", newAPIError(ctx, codes.Internal, http.StatusInternalServerError, orchestrator.UserErrorCode_NOT_SUPPORTED, "unknown volume type").Err()
		}

		return "", newAPIError(ctx, codes.Internal, http.StatusInternalServerError, orchestrator.UserErrorCode_UNKNOWN_USER_ERROR_CODE, "failed to build volume path: %v", err).Err()
	}

	return path, nil
}

func (s *Service) getFilesystemAndPath(ctx context.Context, request volumePathRequest) (*chrooted.Chrooted, string, *status.Status) {
	volume := request.GetVolume()
	volumeType := volume.GetVolumeType()

	teamID, ok := pkg.TryParseUUID(volume.GetTeamId())
	if !ok {
		return nil, "", newAPIError(ctx,
			codes.InvalidArgument,
			http.StatusBadRequest,
			orchestrator.UserErrorCode_INVALID_REQUEST,
			"invalid team ID %q", volume.GetTeamId(),
		)
	}

	volumeID, ok := pkg.TryParseUUID(volume.GetVolumeId())
	if !ok {
		return nil, "", newAPIError(ctx,
			codes.InvalidArgument,
			http.StatusBadRequest,
			orchestrator.UserErrorCode_INVALID_REQUEST,
			"invalid volume ID %q", volume.GetVolumeId(),
		)
	}

	chroot, err := s.builder.Chroot(volumeType, teamID, volumeID)
	if err != nil {
		if errors.Is(err, chrooted.ErrVolumeTypeNotFound) {
			return nil, "", newAPIError(ctx,
				codes.Internal,
				http.StatusInternalServerError,
				orchestrator.UserErrorCode_NOT_SUPPORTED,
				"volume type not found",
			)
		}

		return nil, "", newAPIError(ctx,
			codes.Internal,
			http.StatusInternalServerError,
			orchestrator.UserErrorCode_UNKNOWN_USER_ERROR_CODE,
			"failed to get chroot: %v", err,
		)
	}

	path := request.GetPath()
	if !filepath.IsAbs(path) {
		path = "/" + path
	}
	path = filepath.Clean(path)

	return chroot, path, nil
}

func (s *Service) isRoot(path string) bool {
	return path == "/"
}

// toEntry describes fileInfo, taken at fullVolumePath inside fs. A symlink's
// target is resolved inside the volume, never against the host.
func toEntry(fs *chrooted.Chrooted, fullVolumePath string, fileInfo os.FileInfo) *orchestrator.EntryInfo {
	return fromEntryInfo(fullVolumePath, fs.EntryInfo(fullVolumePath, fileInfo))
}

func fromEntryInfo(fullVolumePath string, entryInfo filesystem.EntryInfo) *orchestrator.EntryInfo {
	return &orchestrator.EntryInfo{
		Name:          entryInfo.Name,
		Type:          toType(entryInfo.Type),
		Path:          fullVolumePath,
		Size:          entryInfo.Size,
		Mode:          uint32(entryInfo.Mode & os.ModePerm),
		Uid:           entryInfo.UID,
		Gid:           entryInfo.GID,
		ModifiedTime:  toTimestampFromTime(entryInfo.ModifiedTime),
		SymlinkTarget: entryInfo.SymlinkTarget,
		CreatedTime:   toTimestampFromTime(entryInfo.CreatedTime),
		AccessedTime:  toTimestampFromTime(entryInfo.AccessedTime),
	}
}

func toType(fileType filesystem.FileType) orchestrator.FileType {
	switch fileType {
	case filesystem.DirectoryFileType:
		return orchestrator.FileType_FILE_TYPE_DIRECTORY
	case filesystem.FileFileType:
		return orchestrator.FileType_FILE_TYPE_FILE
	case filesystem.SymlinkFileType:
		return orchestrator.FileType_FILE_TYPE_SYMLINK
	default:
		return orchestrator.FileType_FILE_TYPE_UNSPECIFIED
	}
}

func toTimestampFromTime(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}

	return timestamppb.New(t)
}

func setSpanStatus(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, err.Error())

		return
	}

	span.SetStatus(otelcodes.Ok, "")
}

func ensureDirs(fs *chrooted.Chrooted, dirPath string, uid, gid uint32) error {
	if dirPath == "" {
		return nil
	}

	// Determine which parent directories do not exist yet, up to the volume root.
	var needsUpdates []string
	cur := dirPath
	for {
		if _, err := fs.Stat(cur); err == nil {
			if cur == dirPath {
				// there's nothing for us to do here
				return nil
			}

			break
		} else if !os.IsNotExist(err) {
			return fmt.Errorf("failed to stat parent directory %q: %w", cur, err)
		}

		needsUpdates = append(needsUpdates, cur)

		next := filepath.Clean(filepath.Dir(cur))
		if next == cur { // reached filesystem root just in case
			break
		}
		cur = next
	}

	if err := fs.MkdirAll(dirPath, defaultDirMode); err != nil {
		return fmt.Errorf("failed to create parent directories: %w", err)
	}

	// Only chmod the directories that were created by this call (precomputed above).
	// Iterate from highest parent to deepest child for determinism.
	for _, p := range slices.Backward(needsUpdates) {
		if err := fs.Chmod(p, defaultDirMode); err != nil {
			return fmt.Errorf("failed chmod for created parent directory %q: %w", p, err)
		}

		if err := fs.Chown(p, int(uid), int(gid)); err != nil {
			return fmt.Errorf("failed chown for created parent directory %q: %w", p, err)
		}
	}

	return nil
}
