package template_manager

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/e2b-dev/infra/packages/api/internal/utils"
	templatemanager "github.com/e2b-dev/infra/packages/shared/pkg/grpc/template-manager"
)

func (tm *TemplateManager) InitLayerFileUpload(ctx context.Context, clusterID uuid.UUID, nodeID string, teamID uuid.UUID, templateID string, hash string) (*templatemanager.InitLayerFileUploadResponse, error) {
	client, err := tm.GetClusterBuildClient(clusterID, nodeID)
	if err != nil {
		return nil, fmt.Errorf("failed to get build client for template '%s': %w", templateID, err)
	}

	resp, err := client.Template.InitLayerFileUpload(
		ctx, &templatemanager.InitLayerFileUploadRequest{
			CacheScope: new(cacheScopeFor(teamID)),
			TemplateID: templateID,
			Hash:       hash,
		},
	)

	err = utils.UnwrapGRPCError(err)
	if err != nil {
		return nil, fmt.Errorf("failed to request layer files upload for template '%s' with hash '%s': %w", templateID, hash, err)
	}

	return resp, nil
}
