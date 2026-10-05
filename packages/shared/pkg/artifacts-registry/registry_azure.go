package artifacts_registry

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/containers/azcontainerregistry"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/azure"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

// azureRegistryAPI is the subset of *azcontainerregistry.Client that Delete uses.
type azureRegistryAPI interface {
	GetTagProperties(ctx context.Context, name string, reference string, options *azcontainerregistry.ClientGetTagPropertiesOptions) (azcontainerregistry.ClientGetTagPropertiesResponse, error)
	DeleteTag(ctx context.Context, name string, reference string, options *azcontainerregistry.ClientDeleteTagOptions) (azcontainerregistry.ClientDeleteTagResponse, error)
	GetManifestProperties(ctx context.Context, name string, digest string, options *azcontainerregistry.ClientGetManifestPropertiesOptions) (azcontainerregistry.ClientGetManifestPropertiesResponse, error)
	DeleteManifest(ctx context.Context, name string, digest string, options *azcontainerregistry.ClientDeleteManifestOptions) (azcontainerregistry.ClientDeleteManifestResponse, error)
}

type AzureArtifactsRegistry struct {
	repositoryName string
	client         azureRegistryAPI
}

func NewAzureArtifactsRegistry(_ context.Context) (*AzureArtifactsRegistry, error) {
	registryName := consts.AzureContainerRegistryName()
	if registryName == "" {
		return nil, fmt.Errorf("%s environment variable is not set", consts.AzureContainerRegistryNameEnvVar)
	}
	if strings.ContainsAny(registryName, "./:") {
		return nil, fmt.Errorf("%s must be the bare registry name (got %q; the .azurecr.io suffix is appended here)", consts.AzureContainerRegistryNameEnvVar, registryName)
	}

	repositoryName := consts.AzureDockerRepositoryName()
	if repositoryName == "" {
		return nil, fmt.Errorf("%s environment variable is not set", consts.AzureDockerRepositoryNameEnvVar)
	}

	credential, err := azure.DefaultCredential()
	if err != nil {
		return nil, err
	}

	// Public Azure cloud only; sovereign clouds use a different suffix and ACR audience (pkg/acr).
	loginServer := fmt.Sprintf("%s.azurecr.io", registryName)

	client, err := azcontainerregistry.NewClient(fmt.Sprintf("https://%s", loginServer), credential, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating azure container registry client: %w", err)
	}

	return &AzureArtifactsRegistry{
		repositoryName: repositoryName,
		client:         client,
	}, nil
}

func (g *AzureArtifactsRegistry) Delete(ctx context.Context, _ string, buildId string) error {
	// Deleting only the tag would leave the manifest and its layers behind
	// (ACR's untagged-manifest retention is Premium-only and off by default).
	props, err := g.client.GetTagProperties(ctx, g.repositoryName, buildId, nil)
	if err != nil {
		if isAzureNotFound(err) {
			return ErrImageNotExists
		}

		return fmt.Errorf("failed to get tag properties from azure acr: %w", err)
	}

	if props.Tag == nil || props.Tag.Digest == nil {
		return errors.New("azure acr tag properties did not contain a digest")
	}

	digest := *props.Tag.Digest

	// A cache-hit rebuild lands on this digest under its own tag, so the
	// manifest goes only when a post-delete re-read shows no tags remain.
	// The SDK treats 404 on both deletes as success; not-found detection is
	// GetTagProperties above. Accepted race: a tag attached between the
	// re-read and DeleteManifest (no precondition exists) is lost with the
	// manifest — serializing delete-vs-push is the build coordinator's job.
	if _, err := g.client.DeleteTag(ctx, g.repositoryName, buildId, nil); err != nil {
		return fmt.Errorf("failed to delete tag from azure acr: %w", err)
	}

	// The tag is gone, so the delete has succeeded; manifest cleanup is
	// best-effort — an error here would report a completed deletion as failed.
	manifest, err := g.client.GetManifestProperties(ctx, g.repositoryName, digest, nil)
	if err != nil {
		if isAzureNotFound(err) {
			return nil
		}

		logger.L().Warn(ctx, "azure acr manifest cleanup skipped: could not re-read manifest after tag delete",
			zap.String("repository", g.repositoryName), zap.String("digest", digest), zap.Error(err))

		return nil
	}

	if manifest.Manifest != nil {
		for _, tag := range manifest.Manifest.Tags {
			if tag != nil {
				// Another build's tag still references the manifest.
				return nil
			}
		}
	}

	if _, err := g.client.DeleteManifest(ctx, g.repositoryName, digest, nil); err != nil {
		logger.L().Warn(ctx, "azure acr manifest cleanup skipped: could not delete untagged manifest",
			zap.String("repository", g.repositoryName), zap.String("digest", digest), zap.Error(err))
	}

	return nil
}

func isAzureNotFound(err error) bool {
	if respErr, ok := errors.AsType[*azcore.ResponseError](err); ok {
		return respErr.StatusCode == http.StatusNotFound
	}

	return false
}
