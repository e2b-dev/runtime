//go:build linux

package factories

import (
	"context"
	"errors"
	"net/http"
	"time"

	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

const pprofShutdownTimeout = 5 * time.Second

func closePprofServer(ctx context.Context, server *http.Server) error {
	if ctx.Err() != nil {
		return server.Close()
	}

	shutdownCtx, cancel := context.WithTimeout(ctx, pprofShutdownTimeout)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.L().Warn(ctx, "pprof graceful shutdown interrupted", zap.Error(err))
		closeErr := server.Close()
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return closeErr
		}

		return errors.Join(err, closeErr)
	}

	return nil
}
