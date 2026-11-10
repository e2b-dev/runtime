//go:build linux

package commands

import (
	"context"
	"errors"
	"fmt"

	"go.uber.org/zap/zapcore"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/proxy"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/sandboxtools"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
	templatemanager "github.com/e2b-dev/infra/packages/shared/pkg/grpc/template-manager"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

type Run struct{}

var _ Command = (*Run)(nil)

func (r *Run) Execute(
	ctx context.Context,
	logger logger.Logger,
	lvl zapcore.Level,
	proxy *proxy.SandboxProxy,
	sandboxID string,
	prefix string,
	step *templatemanager.TemplateStep,
	cmdMetadata metadata.Context,
) (metadata.Context, error) {
	args := step.GetArgs()
	// args: [command optional_user]
	if len(args) < 1 {
		return metadata.Context{}, errors.New("RUN requires command argument")
	}

	originalMetadata := cmdMetadata

	// If a custom command user is specified, use it
	if len(args) >= 2 {
		cmdMetadata.User = args[1]
	}

	cmd := args[0]
	err := sandboxtools.RunCommandWithLogger(
		ctx,
		proxy,
		logger,
		lvl,
		prefix,
		sandboxID,
		cmd,
		cmdMetadata,
	)
	if err != nil {
		// The command is not repeated here: the build log already shows it
		// on the step's own line, and this error is also logged by the
		// orchestrator, where a user's command does not belong.
		return metadata.Context{}, fmt.Errorf("failed to run command: %w", err)
	}

	return originalMetadata, nil
}
