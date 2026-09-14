package artifacts_registry

import (
	"context"
	"fmt"

	artifactregistry "cloud.google.com/go/artifactregistry/apiv1"
	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
)

type GCPArtifactsRegistry struct {
	registry *artifactregistry.Client
}

func NewGCPArtifactsRegistry(ctx context.Context) (*GCPArtifactsRegistry, error) {
	registry, err := artifactregistry.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("error creating artifact registry client: %w", err)
	}

	return &GCPArtifactsRegistry{registry: registry}, nil
}

func (g *GCPArtifactsRegistry) Delete(ctx context.Context, templateId string, buildId string) error {
	tagPath := g.getDockerImageTagPath(templateId, buildId)
	err := g.registry.DeleteTag(ctx, &artifactregistrypb.DeleteTagRequest{Name: tagPath})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return ErrImageNotExists
		}

		return fmt.Errorf("error deleting tag %s: %w", tagPath, err)
	}

	return nil
}

func (g *GCPArtifactsRegistry) getDockerImagePath(templateId string) string {
	// DockerImagesURL is the URL to the docker images in the artifact registry
	return fmt.Sprintf("projects/%s/locations/%s/repositories/%s/packages/%s", consts.GCPProject, consts.GCPRegion, consts.DockerRegistry, templateId)
}

func (g *GCPArtifactsRegistry) getDockerImageTagPath(templateId string, buildId string) string {
	return fmt.Sprintf("%s/tags/%s", g.getDockerImagePath(templateId), buildId)
}
