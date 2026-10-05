package artifacts_registry

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ecr"
	"github.com/aws/aws-sdk-go-v2/service/ecr/types"
)

type AWSArtifactsRegistry struct {
	repositoryName string
	client         *ecr.Client
}

var (
	AwsRepositoryNameEnvVar = "AWS_DOCKER_REPOSITORY_NAME"
	AwsRepositoryName       = os.Getenv(AwsRepositoryNameEnvVar)
)

func NewAWSArtifactsRegistry(ctx context.Context) (*AWSArtifactsRegistry, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}

	if AwsRepositoryName == "" {
		return nil, fmt.Errorf("%s environment variable is not set", AwsRepositoryNameEnvVar)
	}

	client := ecr.NewFromConfig(cfg)

	return &AWSArtifactsRegistry{
		repositoryName: AwsRepositoryName,
		client:         client,
	}, nil
}

func (g *AWSArtifactsRegistry) Delete(ctx context.Context, _ string, buildId string) error {
	imageIds := []types.ImageIdentifier{
		{ImageTag: &buildId},
	}

	// for AWS implementation we are using only build id as image tag
	res, err := g.client.BatchDeleteImage(ctx, &ecr.BatchDeleteImageInput{RepositoryName: &g.repositoryName, ImageIds: imageIds})
	if err != nil {
		return fmt.Errorf("failed to delete image from aws ecr: %w", err)
	}

	if len(res.Failures) > 0 {
		if res.Failures[0].FailureCode == types.ImageFailureCodeImageNotFound {
			return ErrImageNotExists
		}

		return errors.New("failed to delete image from aws ecr")
	}

	return nil
}
