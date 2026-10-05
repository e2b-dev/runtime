//go:build linux

package server

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/storage/paths"
	templatemanager "github.com/e2b-dev/infra/packages/shared/pkg/grpc/template-manager"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/templates"
)

const signedUrlExpiration = time.Minute * 30

func (s *ServerStore) InitLayerFileUpload(ctx context.Context, in *templatemanager.InitLayerFileUploadRequest) (*templatemanager.InitLayerFileUploadResponse, error) {
	ctx, childSpan := tracer.Start(ctx, "template-create")
	defer childSpan.End()

	if err := templates.ValidateFilesHash(in.GetHash()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	// default to scope by template ID
	cacheScope := in.GetTemplateID()
	if in.CacheScope != nil {
		cacheScope = in.GetCacheScope()
	}

	path := paths.GetLayerFilesCachePath(cacheScope, in.GetHash())
	obj, err := s.buildStorage.OpenBlob(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("failed to open layer files cache: %w", err)
	}

	exists, err := obj.Exists(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to check if layer files exists: %w", err)
	}

	uploadRequest, err := s.buildStorage.UploadSignedURL(ctx, path, signedUrlExpiration)
	if err != nil {
		// A cache hit needs no upload URL, so any signing failure — a provider that cannot sign, or a transient credential/network error — is fatal only on a miss; the request carries no force-upload intent, so a forced re-upload degrades to using the cached entry here.
		if exists {
			logger.L().Warn(ctx, "serving layer-file cache hit without an upload URL: signing the upload URL failed", zap.Error(err))

			return &templatemanager.InitLayerFileUploadResponse{Present: true}, nil
		}

		return nil, fmt.Errorf("failed to get signed url: %w", err)
	}

	return &templatemanager.InitLayerFileUploadResponse{
		Present: exists,
		Url:     &uploadRequest.URL,
		Headers: uploadRequest.Headers,
	}, nil
}
