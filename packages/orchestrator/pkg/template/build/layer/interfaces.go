//go:build linux

package layer

import (
	"context"
	"time"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	sbxtemplate "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
)

const (
	waitEnvdTimeout = 60 * time.Second
)

// SandboxCreator creates sandboxes for layer building
type SandboxCreator interface {
	Sandbox(
		ctx context.Context,
		layerExecutor *LayerExecutor,
		template sbxtemplate.Template,
	) (*sandbox.Sandbox, error)
}

// ActionExecutor executes actions within a sandbox during layer building
type ActionExecutor interface {
	Execute(ctx context.Context, sbx *sandbox.Sandbox, meta metadata.Template) (metadata.Template, error)
}

// SourceTemplateProvider provides the source template for the layer build
type SourceTemplateProvider interface {
	// Get returns the source template and the release of the pin the caller
	// holds on it, which must be called once the layer no longer reads it.
	Get(ctx context.Context, templateCache *sbxtemplate.Cache) (sbxtemplate.Template, func(), error)
}

// LayerBuildCommand encapsulates all parameters needed for building a layer
type LayerBuildCommand struct {
	SourceTemplate SourceTemplateProvider
	CurrentLayer   metadata.Template
	Hash           string
	UpdateEnvd     bool
	SandboxCreator SandboxCreator
	ActionExecutor ActionExecutor
	// BuildOrigin is the object-metadata origin for the uploaded layer:
	// template_build for the final layer, template_build_cache for intermediates.
	BuildOrigin storage.ObjectOrigin
}
