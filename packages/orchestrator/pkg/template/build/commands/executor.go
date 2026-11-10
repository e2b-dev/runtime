//go:build linux

package commands

import (
	"context"
	"fmt"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap/zapcore"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/proxy"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/buildcontext"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
	templatemanager "github.com/e2b-dev/infra/packages/shared/pkg/grpc/template-manager"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

var tracer = otel.Tracer("github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/commands")

type CommandExecutor struct {
	buildcontext.BuildContext

	buildStorage storage.StorageProvider
	proxy        *proxy.SandboxProxy
}

func NewCommandExecutor(
	buildContext buildcontext.BuildContext,
	buildStorage storage.StorageProvider,
	proxy *proxy.SandboxProxy,
) *CommandExecutor {
	return &CommandExecutor{
		BuildContext: buildContext,

		buildStorage: buildStorage,
		proxy:        proxy,
	}
}

func (ce *CommandExecutor) getCommand(
	step *templatemanager.TemplateStep,
) (Command, error) {
	cmdType := strings.ToUpper(step.GetType())

	if renderer := ce.Config.GuestTemplate().Renderer; renderer != nil {
		return &Rendered{
			Renderer:     renderer,
			FilesStorage: ce.buildStorage,
			CacheScope:   ce.CacheScope,
		}, nil
	}

	var cmd Command
	switch cmdType {
	case "ADD", "COPY":
		cmd = &Copy{
			FilesStorage: ce.buildStorage,
			CacheScope:   ce.CacheScope,
		}
	case "RUN":
		cmd = &Run{}
	case "USER":
		cmd = &User{}
	case "WORKDIR":
		cmd = &Workdir{}
	case "ENV", "ARG":
		cmd = &Env{}
	}

	if cmd == nil {
		return nil, fmt.Errorf("command type %s is not implemented", cmdType)
	}

	return cmd, nil
}

func (ce *CommandExecutor) Execute(
	ctx context.Context,
	userLogger logger.Logger,
	lvl zapcore.Level,
	sbx *sandbox.Sandbox,
	prefix string,
	step *templatemanager.TemplateStep,
	cmdMetadata metadata.Context,
) (metadata.Context, error) {
	ctx, span := tracer.Start(ctx, "apply-command", trace.WithAttributes(
		attribute.String("prefix", prefix),
		attribute.String("sandbox.id", sbx.Runtime.SandboxID),
		// The step's arguments are the user's and stay out of traces.
		attribute.String("step.type", step.GetType()),
		attribute.String("step.files.hash", utils.Sprintp(step.FilesHash)), //nolint:protogetter // we need the nil check too
	))
	defer span.End()

	cmd, err := ce.getCommand(step)
	if err != nil {
		return metadata.Context{}, fmt.Errorf("failed to get command for step %s: %w", step.GetType(), err)
	}

	cmdMetadata, err = cmd.Execute(
		ctx,
		userLogger,
		lvl,
		ce.proxy,
		sbx.Runtime.SandboxID,
		prefix,
		step,
		cmdMetadata,
	)
	if err != nil {
		return metadata.Context{}, err
	}

	return cmdMetadata, nil
}
