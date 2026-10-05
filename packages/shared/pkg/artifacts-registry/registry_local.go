package artifacts_registry

import "context"

type LocalArtifactsRegistry struct{}

func NewLocalArtifactsRegistry() (*LocalArtifactsRegistry, error) {
	return &LocalArtifactsRegistry{}, nil
}

func (g *LocalArtifactsRegistry) Delete(context.Context, string, string) error {
	// for now, just assume local image can be deleted manually
	return nil
}
