//go:build linux

package layer

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	sbxtemplate "github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/template"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/config"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
)

// ResumeSandbox creates sandboxes for resuming existing templates
type ResumeSandbox struct {
	config         *sandbox.Config
	sandboxFactory *sandbox.Factory
	timeout        time.Duration
}

var _ SandboxCreator = (*ResumeSandbox)(nil)

func NewResumeSandbox(config *sandbox.Config, sandboxFactory *sandbox.Factory, timeout time.Duration) *ResumeSandbox {
	return &ResumeSandbox{config: config, sandboxFactory: sandboxFactory, timeout: timeout}
}

func (rs *ResumeSandbox) Sandbox(
	ctx context.Context,
	layerExecutor *LayerExecutor,
	template sbxtemplate.Template,
) (*sandbox.Sandbox, error) {
	sbx, err := rs.sandboxFactory.ResumeSandbox(
		ctx,
		template,
		rs.config,
		sandboxtypes.RuntimeMetadata{
			TemplateID:  layerExecutor.Config.TemplateID,
			SandboxID:   config.InstanceBuildPrefix + id.Generate(),
			ExecutionID: uuid.NewString(),
			TeamID:      layerExecutor.Config.TeamID,
			BuildID:     layerExecutor.Template.BuildID,
			SandboxType: sandboxtypes.SandboxTypeBuild,
		},
		time.Now(),
		time.Now().Add(rs.timeout),
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("resume sandbox: %w", err)
	}

	return sbx, nil
}
