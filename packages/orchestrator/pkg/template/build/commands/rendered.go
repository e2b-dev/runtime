//go:build linux

package commands

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strings"

	"go.uber.org/zap/zapcore"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/proxy"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/guest"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/sandboxtools"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
	templatemanager "github.com/e2b-dev/infra/packages/shared/pkg/grpc/template-manager"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// Rendered is a build step of a guest whose profile renders the steps: the
// profile turns the step into one guest command and the build state after it,
// and this runs the command, after uploading the step's archive where the
// rendering asks for it.
type Rendered struct {
	Renderer     guest.StepRenderer
	FilesStorage storage.StorageProvider
	CacheScope   string
}

var _ Command = (*Rendered)(nil)

func (r *Rendered) Execute(
	ctx context.Context,
	logger logger.Logger,
	lvl zapcore.Level,
	proxy *proxy.SandboxProxy,
	sandboxID string,
	prefix string,
	step *templatemanager.TemplateStep,
	cmdMetadata metadata.Context,
) (metadata.Context, error) {
	cmdType := strings.ToUpper(step.GetType())

	rendered, err := r.Renderer.Render(
		guest.Step{Type: cmdType, Args: step.GetArgs(), FilesHash: step.GetFilesHash()},
		guest.StepState{
			User:    cmdMetadata.User,
			WorkDir: utils.DerefOrDefault(cmdMetadata.WorkDir, ""),
			Env:     maps.Clone(cmdMetadata.EnvVars),
		},
	)
	if err != nil {
		return metadata.Context{}, err
	}

	for _, warning := range rendered.Warnings {
		logger.Warn(ctx, fmt.Sprintf("[%s] %s", prefix, warning))
	}

	run := runContext(cmdMetadata, rendered)
	next := cmdMetadata
	next.User = rendered.State.User
	next.EnvVars = rendered.State.Env
	if next.EnvVars == nil {
		next.EnvVars = map[string]string{}
	}
	next.WorkDir = nil
	if rendered.State.WorkDir != "" {
		next.WorkDir = &rendered.State.WorkDir
	}

	if rendered.ArchivePath != "" {
		if step.FilesHash == nil || step.GetFilesHash() == "" {
			return metadata.Context{}, fmt.Errorf("%s requires files hash to be set", cmdType)
		}
		if err := uploadLayerFiles(ctx, r.FilesStorage, r.CacheScope, proxy, sandboxID, run.User, step.GetFilesHash(), rendered.ArchivePath); err != nil {
			return metadata.Context{}, err
		}
	}

	if rendered.Command == "" {
		return next, nil
	}

	switch {
	case rendered.WorkDirFromOutput:
		cmdCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		out := newBoundedOutput(maxWorkDirOutput, cancel)
		err = sandboxtools.RunCommandWithOutput(
			cmdCtx,
			proxy,
			sandboxID,
			rendered.Command,
			// No working directory: the command may be the one that creates it.
			metadata.Context{User: run.User, EnvVars: run.EnvVars},
			func(stdout, _ string) { out.add(stdout) },
		)
		if out.overflowed {
			return metadata.Context{}, fmt.Errorf("failed to resolve workdir: the command printed more than %d bytes, and a working directory is one path", maxWorkDirOutput)
		}
		if err != nil {
			return metadata.Context{}, fmt.Errorf("failed to create workdir: %w", err)
		}
		workdir := strings.TrimSpace(out.String())
		if workdir == "" {
			return metadata.Context{}, errors.New("failed to resolve workdir: the command printed no path")
		}
		logger.Log(ctx, lvl, fmt.Sprintf("[%s] %s %s", prefix, cmdType, workdir))
		next.WorkDir = &workdir
	case rendered.ArchivePath != "":
		err = sandboxtools.RunCommandWithLogger(ctx, proxy, logger, zapcore.DebugLevel, "unpack", sandboxID, rendered.Command, run)
		if err != nil {
			return metadata.Context{}, fmt.Errorf("failed to move files in sandbox: %w", err)
		}
	default:
		err = sandboxtools.RunCommandWithLogger(ctx, proxy, logger, lvl, prefix, sandboxID, rendered.Command, run)
		if err != nil {
			// The command is not repeated here, as for a default RUN step.
			return metadata.Context{}, fmt.Errorf("failed to run command: %w", err)
		}
	}

	return next, nil
}

// runContext is the context a rendered step's upload and command run in: the
// build's, as the rendering's user when it names one.
func runContext(cmdMetadata metadata.Context, rendered guest.RenderedStep) metadata.Context {
	if rendered.User != "" {
		cmdMetadata.User = rendered.User
	}

	return cmdMetadata
}

// maxWorkDirOutput bounds what a WORKDIR command may print: one path.
const maxWorkDirOutput = 4096

// boundedOutput collects a command's output up to a limit; past it, it stops
// collecting and cancels the command.
type boundedOutput struct {
	buf        strings.Builder
	limit      int
	cancel     context.CancelFunc
	overflowed bool
}

func newBoundedOutput(limit int, cancel context.CancelFunc) *boundedOutput {
	return &boundedOutput{limit: limit, cancel: cancel}
}

func (b *boundedOutput) add(s string) {
	if b.overflowed {
		return
	}
	if b.buf.Len()+len(s) > b.limit {
		b.overflowed = true
		b.cancel()

		return
	}
	b.buf.WriteString(s)
}

func (b *boundedOutput) String() string { return b.buf.String() }
