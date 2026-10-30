//go:build linux

package commands

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"go.uber.org/zap/zapcore"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/proxy"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/sandboxtools"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
	templatemanager "github.com/e2b-dev/infra/packages/shared/pkg/grpc/template-manager"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

const defaultRelativeToAbsoluteWorkdir = "/"

type Workdir struct{}

var _ Command = (*Workdir)(nil)

func (w *Workdir) Execute(
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
	// args: [path]
	if len(args) < 1 {
		return metadata.Context{}, errors.New("WORKDIR requires a path argument")
	}

	workDir := defaultRelativeToAbsoluteWorkdir
	if cmdMetadata.WorkDir != nil {
		workDir = *cmdMetadata.WorkDir
	}

	workdirArg := args[0]
	if !filepath.IsAbs(workdirArg) {
		workdirArg = filepath.Join(workDir, workdirArg)
	}

	err := sandboxtools.RunCommandWithLogger(
		ctx,
		proxy,
		logger,
		lvl,
		prefix,
		sandboxID,
		// Use mkdir -p to create any missing parents
		// Find the first existing parent and chown from there
		// This ensures that if we have e.g. /home/user and we create /home/user/project/test
		// we only chown /home/user/project (including /home/user/project/test) and not /home or /home/user
		fmt.Sprintf(`
			target="%s"

			# Exit early if target already exists
			if [ -d "$target" ]; then
			    exit 0
			fi

			# Find the first non-existent parent
			first_new=""
			check="$target"
			while [ ! -d "$check" ]; do
			    first_new="$check"
			    check=$(dirname "$check")
			done

			# Create and chown from the top (only if we found new directories to create)
			if [ -n "$first_new" ]; then
			    mkdir -p "$target" && chown -R %s:%s "$first_new"
			fi
    `, workdirArg, cmdMetadata.User, cmdMetadata.User),
		metadata.Context{
			User:    "root",
			EnvVars: cmdMetadata.EnvVars,
			// Workdir can't be set here to not error when current workdir is deleted
		},
	)
	if err != nil {
		return metadata.Context{}, fmt.Errorf("failed to create workdir: %w", err)
	}

	return saveWorkdirMeta(ctx, proxy, sandboxID, cmdMetadata, workdirArg)
}

func saveWorkdirMeta(
	ctx context.Context,
	proxy *proxy.SandboxProxy,
	sandboxID string,
	cmdMetadata metadata.Context,
	workdir string,
) (m metadata.Context, e error) {
	var stdout strings.Builder
	var stderr strings.Builder
	err := sandboxtools.RunCommandWithOutput(
		ctx,
		proxy,
		sandboxID,
		fmt.Sprintf(`cd "%s" && printf '\n%s%%s\n' "$PWD"`, workdir, workdirOutputMarker),
		metadata.Context{
			User:    "root",
			EnvVars: cmdMetadata.EnvVars,
			// Workdir can't be set here to not error when current workdir is deleted
		},
		func(out, errOut string) {
			_, _ = stdout.WriteString(out)
			_, _ = stderr.WriteString(errOut)
		},
	)
	return saveWorkdirMetaFromOutput(cmdMetadata, workdir, stdout.String(), stderr.String(), err)
}

const workdirOutputMarker = "__E2B_WORKDIR__:"

func saveWorkdirMetaFromOutput(
	cmdMetadata metadata.Context,
	workdir, stdout, stderr string,
	commandErr error,
) (metadata.Context, error) {
	resolved, err := absoluteWorkdirFromOutput(stdout, stderr, commandErr)
	if err != nil {
		return metadata.Context{}, fmt.Errorf("failed to save workdir %s: %w", workdir, err)
	}
	cmdMetadata.WorkDir = &resolved
	return cmdMetadata, nil
}

// absoluteWorkdirFromOutput uses a marked line so unrelated login-hook output
// cannot be mistaken for pwd. The caller collects all stream chunks first.
func absoluteWorkdirFromOutput(stdout, stderr string, commandErr error) (string, error) {
	if commandErr != nil {
		return "", fmt.Errorf("workdir command failed: %w", commandErr)
	}
	marker := strings.LastIndex(stdout, workdirOutputMarker)
	if marker >= 0 {
		path := strings.TrimSpace(strings.SplitN(stdout[marker+len(workdirOutputMarker):], "\n", 2)[0])
		if filepath.IsAbs(path) {
			return path, nil
		}
	}
	return "", fmt.Errorf("error getting absolute path of workdir: stdout=%q stderr=%q", stdout, stderr)
}
