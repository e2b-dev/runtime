//go:build linux

package volumes

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/chrooted"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

const (
	minDepth = 1
	maxDepth = 10
)

func (s *Service) ListDir(
	ctx context.Context,
	request *orchestrator.ListDirRequest,
) (r *orchestrator.ListDirResponse, err error) {
	ctx, span := tracer.Start(ctx, "list directory in volume")
	defer func() {
		setSpanStatus(span, err)
		span.End()
	}()

	fs, path, errResponse := s.getFilesystemAndPath(ctx, request)
	if errResponse != nil {
		return nil, errResponse.Err()
	}
	defer fs.Close()

	span.AddEvent("listing directory", trace.WithAttributes(
		attribute.String("path", path),
	))

	depth := int(request.GetDepth())
	depth = max(depth, minDepth)

	if depth > maxDepth {
		return nil, newAPIError(ctx,
			codes.InvalidArgument,
			http.StatusBadRequest,
			orchestrator.UserErrorCode_DEPTH_OUT_OF_RANGE,
			"depth must be between %d and %d", minDepth, maxDepth,
		).Err()
	}

	results, err := s.listRecursive(ctx, fs, path, depth)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, newAPIError(ctx,
				codes.NotFound,
				http.StatusNotFound,
				orchestrator.UserErrorCode_PATH_NOT_FOUND,
				"failed to read: %q not found.",
				request.GetPath(),
			).Err()
		}

		return nil, fmt.Errorf("failed to read directory %q: %w", path, err)
	}

	return &orchestrator.ListDirResponse{Files: results}, nil
}

func (s *Service) listRecursive(
	ctx context.Context,
	fs *chrooted.Chrooted,
	path string,
	depth int,
) ([]*orchestrator.VolumeDirectoryItem, error) {
	if depth <= 0 {
		return nil, nil
	}

	items, err := fs.ReadDir(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory %q: %w", path, err)
	}

	var results []*orchestrator.VolumeDirectoryItem
	for _, item := range items {
		itemPath := filepath.Join(path, item.Name())
		results = append(results, &orchestrator.VolumeDirectoryItem{
			Entry: toEntry(fs, itemPath, item),
		})

		if item.IsDir() && depth > 1 {
			children, err := s.listRecursive(ctx, fs, itemPath, depth-1)
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					logger.L().Warn(ctx, "directory deleted during traversal",
						zap.String("path", itemPath),
					)

					continue
				}

				return nil, err
			}

			results = append(results, children...)
		}
	}

	return results, nil
}
