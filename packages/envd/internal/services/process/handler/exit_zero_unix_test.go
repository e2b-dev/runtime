//go:build unix

package handler

import (
	"context"
	"os/exec"
)

func exitZeroCommand(ctx context.Context) *exec.Cmd {
	return exec.CommandContext(ctx, "/bin/sh", "-c", "exit 0")
}
