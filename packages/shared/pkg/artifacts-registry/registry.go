package artifacts_registry

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/e2b-dev/infra/packages/shared/pkg/env"
)

type RegistryProvider string

const (
	GCPStorageProvider   RegistryProvider = "GCP_ARTIFACTS"
	AWSStorageProvider   RegistryProvider = "AWS_ECR"
	AzureStorageProvider RegistryProvider = "AZURE_ACR"
	LocalStorageProvider RegistryProvider = "Local"

	DefaultRegistryProvider RegistryProvider = GCPStorageProvider

	storageProviderEnv = "ARTIFACTS_REGISTRY_PROVIDER"
)

var ErrImageNotExists = errors.New("image does not exist")

type ArtifactsRegistry interface {
	Delete(ctx context.Context, templateId string, buildId string) error
}

func GetArtifactsRegistryProvider(ctx context.Context) (ArtifactsRegistry, error) {
	provider := RegistryProvider(env.GetEnv(storageProviderEnv, string(DefaultRegistryProvider)))

	setupCtx, setupCtxCancel := context.WithTimeout(ctx, 10*time.Second)
	defer setupCtxCancel()

	switch provider {
	case AWSStorageProvider:
		return NewAWSArtifactsRegistry(setupCtx)
	case GCPStorageProvider:
		return NewGCPArtifactsRegistry(setupCtx)
	case AzureStorageProvider:
		return NewAzureArtifactsRegistry(setupCtx)
	case LocalStorageProvider:
		return NewLocalArtifactsRegistry()
	}

	return nil, fmt.Errorf("unknown artifacts registry provider: %s", provider)
}
