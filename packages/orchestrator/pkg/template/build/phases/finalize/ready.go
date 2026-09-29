//go:build linux

package finalize

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap/zapcore"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/sandboxtools"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

const (
	defaultReadyWait = 20 * time.Second

	readyCommandRetryInterval = 2 * time.Second
	readyCommandTimeout       = 10 * time.Minute
)

func (ppb *PostProcessingBuilder) runReadyCommand(
	ctx context.Context,
	userLogger logger.Logger,
	sandboxID string,
	readyCmd string,
	cmdMetadata metadata.Context,
) error {
	ctx, span := tracer.Start(ctx, "run-ready-command")
	defer span.End()

	userLogger.Info(ctx, fmt.Sprintf("Waiting for template to be ready: %s", readyCmd))

	startTime := time.Now()
	ctx, cancel := context.WithTimeout(ctx, readyCommandTimeout)
	defer cancel()

	// Start the ready check
	for {
		err := sandboxtools.RunCommandWithLogger(
			ctx,
			ppb.proxy,
			userLogger,
			zapcore.DebugLevel,
			"ready",
			sandboxID,
			readyCmd,
			cmdMetadata,
		)

		if err == nil {
			userLogger.Info(ctx, "Template is ready")

			return nil
		}

		userLogger.Debug(ctx, fmt.Sprintf("Template is not ready: %v", err))

		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return fmt.Errorf("ready command timed out after %s", time.Since(startTime))
			}
			// Template is ready, the start command finished before the ready command
			userLogger.Info(ctx, "Template is ready")

			return nil
		case <-time.After(readyCommandRetryInterval):
			// Wait for readyCommandRetryInterval time before retrying the ready command
		}
	}
}

// GetDefaultReadyCommand returns a ready command that sleeps for the given duration.
// Pass 0 to use the built-in default (20s).
func GetDefaultReadyCommand(startCmdTimeout time.Duration) string {
	if startCmdTimeout <= 0 {
		startCmdTimeout = defaultReadyWait
	}

	return fmt.Sprintf("sleep %d", int(startCmdTimeout.Seconds()))
}
