package dockerhub

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/go-containerregistry/pkg/name"
	containerregistry "github.com/google/go-containerregistry/pkg/v1"

	"github.com/e2b-dev/infra/packages/shared/pkg/env"
)

type RemoteRepositoryProvider string

// ErrRepositoryAccessDenied identifies failures of the platform's registry credentials.
var ErrRepositoryAccessDenied = errors.New("configured image proxy is unavailable")

const (
	GCPStorageProvider   RemoteRepositoryProvider = "GCP_REMOTE_REPOSITORY"
	AWSStorageProvider   RemoteRepositoryProvider = "AWS_ECR"
	AzureStorageProvider RemoteRepositoryProvider = "AZURE_ACR"
	LocalStorageProvider RemoteRepositoryProvider = "Local"

	DefaultRegistryProvider RemoteRepositoryProvider = GCPStorageProvider

	storageProviderEnv         = "DOCKERHUB_REMOTE_REPOSITORY_PROVIDER"
	storageRemoteRepositoryURL = "DOCKERHUB_REMOTE_REPOSITORY_URL"

	setupTimeout = 10 * time.Second
)

type RemoteRepository interface {
	GetImage(ctx context.Context, tag string, platform containerregistry.Platform) (containerregistry.Image, error)
	Close() error
}

func GetRemoteRepository(ctx context.Context) (RemoteRepository, error) {
	provider := RemoteRepositoryProvider(env.GetEnv(storageProviderEnv, string(DefaultRegistryProvider)))

	dockerRemoteRepositoryURL := env.GetEnv(storageRemoteRepositoryURL, "")
	if dockerRemoteRepositoryURL == "" {
		return NewNoopRemoteRepository(), nil
	}

	setupCtx, setupCtxCancel := context.WithTimeout(ctx, setupTimeout)
	defer setupCtxCancel()

	switch provider {
	case AWSStorageProvider:
		return NewAWSRemoteRepository(setupCtx, dockerRemoteRepositoryURL)
	case GCPStorageProvider:
		return NewGCPRemoteRepository(setupCtx, dockerRemoteRepositoryURL)
	case AzureStorageProvider:
		return NewAzureRemoteRepository(setupCtx, dockerRemoteRepositoryURL)
	case LocalStorageProvider:
		return NewNoopRemoteRepository(), nil
	}

	return nil, fmt.Errorf("unknown dockerhub remote repository provider: %s", provider)
}

func removeRegistryFromTag(tag string) (string, error) {
	ref, err := name.ParseReference(tag)
	if err != nil {
		return "", fmt.Errorf("invalid image reference: %w", err)
	}

	registry := ref.Context().RegistryStr()
	withoutRegistry := strings.TrimPrefix(ref.Name(), registry)
	withoutRegistry = strings.TrimPrefix(withoutRegistry, "/")

	return withoutRegistry, nil
}
