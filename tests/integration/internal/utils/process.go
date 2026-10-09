package utils

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"

	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/envd/process"
	"github.com/e2b-dev/infra/tests/integration/internal/api"
	"github.com/e2b-dev/infra/tests/integration/internal/setup"
)

const sandboxStateProbeTimeout = 5 * time.Second

func ExecCommand(tb testing.TB, ctx context.Context, sbx *api.Sandbox, envdClient *setup.EnvdClient, command string, args ...string) error {
	tb.Helper()

	return ExecCommandWithOptions(tb, ctx, sbx, envdClient, nil, "user", command, args...)
}

func ExecCommandWithCwd(tb testing.TB, ctx context.Context, sbx *api.Sandbox, envdClient *setup.EnvdClient, cwd *string, command string, args ...string) error {
	tb.Helper()

	return ExecCommandWithOptions(tb, ctx, sbx, envdClient, cwd, "user", command, args...)
}

func ExecCommandAsRoot(tb testing.TB, ctx context.Context, sbx *api.Sandbox, envdClient *setup.EnvdClient, command string, args ...string) error {
	tb.Helper()

	return ExecCommandWithOptions(tb, ctx, sbx, envdClient, nil, "root", command, args...)
}

func ExecCommandAsRootWithOutput(tb testing.TB, ctx context.Context, sbx *api.Sandbox, envdClient *setup.EnvdClient, command string, args ...string) (string, error) {
	tb.Helper()

	return ExecCommandWithOutput(tb, ctx, sbx, envdClient, nil, "root", command, args...)
}

// ExecCommandAsDefaultUserWithOutput runs a command without sending a user
// header, so envd falls back to the default user set at /init time. This is the
// path that exercises the template's default user/workdir (it differs from
// passing an explicit "user").
func ExecCommandAsDefaultUserWithOutput(tb testing.TB, ctx context.Context, sbx *api.Sandbox, envdClient *setup.EnvdClient, command string, args ...string) (string, error) {
	tb.Helper()

	return ExecCommandWithOutput(tb, ctx, sbx, envdClient, nil, "", command, args...)
}

func ExecCommandWithOptions(tb testing.TB, ctx context.Context, sbx *api.Sandbox, envdClient *setup.EnvdClient, cwd *string, user string, command string, args ...string) error {
	tb.Helper()

	_, err := ExecCommandWithOutput(tb, ctx, sbx, envdClient, cwd, user, command, args...)

	return err
}

func ExecCommandWithOutput(tb testing.TB, ctx context.Context, sbx *api.Sandbox, envdClient *setup.EnvdClient, cwd *string, user string, command string, args ...string) (string, error) {
	tb.Helper()

	f := false
	req := connect.NewRequest(&process.StartRequest{
		Process: &process.ProcessConfig{
			Cmd:  command,
			Args: args,
			Cwd:  cwd,
		},
		Stdin: &f,
	})

	setup.SetSandboxHeader(tb, req.Header(), sbx)
	// An empty user omits the auth header so envd uses its default user.
	if user != "" {
		setup.SetUserHeader(tb, req.Header(), user)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream, err := envdClient.ProcessClient.Start(ctx, req)
	if err != nil {
		return "", err
	}
	contextInfo := ""
	if cwd != nil {
		contextInfo += fmt.Sprintf(" (cwd: %s)", *cwd)
	}
	if user != "user" {
		contextInfo += fmt.Sprintf(" (user: %s)", user)
	}
	fmt.Printf("Executing command %s in sandbox %s%s\n", command, sbx.SandboxID, contextInfo)
	defer func() {
		streamErr := stream.Close()
		if streamErr != nil {
			tb.Logf("Error closing stream: %v", streamErr)
		}
	}()

	var output strings.Builder
	for stream.Receive() {
		msg := stream.Msg()
		tb.Logf("Command [%s] output: %s", command, msg.String())

		if msg.GetEvent().GetData() != nil {
			if stdout := msg.GetEvent().GetData().GetStdout(); stdout != nil {
				output.Write(stdout)
			}

			if stderr := msg.GetEvent().GetData().GetStderr(); stderr != nil {
				output.Write(stderr)
			}
		}

		if msg.GetEvent().GetEnd() != nil {
			if msg.GetEvent().GetEnd().GetExitCode() != 0 {
				return output.String(), fmt.Errorf("command %s in sandbox %s failed with exit code %d", command, sbx.SandboxID, msg.GetEvent().GetEnd().GetExitCode())
			}
			tb.Logf("Command [%s] completed successfully in sandbox %s", command, sbx.SandboxID)

			return output.String(), nil
		}
	}

	if err := stream.Err(); err != nil {
		return output.String(), diagnoseStreamErr(ctx, sbx, command, err)
	}

	return output.String(), nil
}

// Killing a sandbox resets the envd connection mid-frame, so a TTL expiry
// surfaces as a Connect framing error ("incomplete envelope: unexpected EOF")
// that names neither the sandbox nor the timeout.
func diagnoseStreamErr(ctx context.Context, sbx *api.Sandbox, command string, streamErr error) error {
	if cause := asContextFailure(ctx, streamErr, context.Canceled, connect.CodeCanceled); cause != nil {
		return fmt.Errorf("failed to execute command %s in sandbox %s: %w", command, sbx.SandboxID, cause)
	}

	if cause := asContextFailure(ctx, streamErr, context.DeadlineExceeded, connect.CodeDeadlineExceeded); cause != nil {
		return fmt.Errorf("command %s in sandbox %s did not finish before its deadline: %w", command, sbx.SandboxID, cause)
	}

	base := fmt.Errorf("failed to execute command %s in sandbox %s: %w", command, sbx.SandboxID, streamErr)

	// The caller's context is usually spent by the time the stream breaks.
	probeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sandboxStateProbeTimeout)
	defer cancel()

	res, err := setup.GetAPIClient().GetSandboxesSandboxIDWithResponse(probeCtx, sbx.SandboxID, setup.WithAPIKey())
	if err != nil {
		return fmt.Errorf("%w (sandbox state unknown: %w)", base, err)
	}

	if res.StatusCode() == http.StatusNotFound {
		return fmt.Errorf("command %s did not finish before sandbox %s stopped and was removed, so the error below is the teardown and not the command result: %w", command, sbx.SandboxID, streamErr)
	}

	detail := res.JSON200
	if detail == nil {
		return fmt.Errorf("%w (sandbox state unknown: HTTP %d)", base, res.StatusCode())
	}

	if detail.State == api.Running && time.Now().Before(detail.EndAt) {
		return base
	}

	return fmt.Errorf("command %s did not finish while sandbox %s was usable (state=%s, startedAt=%s, endAt=%s), so the error below is the teardown and not the command result: %w",
		command, sbx.SandboxID, detail.State,
		detail.StartedAt.Format(time.RFC3339), detail.EndAt.Format(time.RFC3339),
		streamErr)
}

// connect may report a context failure in the code alone, leaving the sentinel
// out of the chain that callers match on. The code alone also covers a peer's
// RST_STREAM CANCEL, so only ctx tells our own cancellation from the far side's.
func asContextFailure(ctx context.Context, err error, sentinel error, code connect.Code) error {
	switch {
	case errors.Is(err, sentinel):
		return err
	case connect.CodeOf(err) == code && errors.Is(ctx.Err(), sentinel):
		return fmt.Errorf("%w: %w", err, sentinel)
	}

	return nil
}
