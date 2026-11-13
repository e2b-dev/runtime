package logged

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

func logStart(ctx context.Context, s string, args ...any) func(context.Context, error, ...any) {
	start := time.Now()
	requestID := uuid.NewString()

	l := logger.L().With(zap.String("request_id", requestID))
	l.Debug(ctx, fmt.Sprintf("[nfs proxy] %s: start", s), zap.String("operation", s))

	return func(ctx context.Context, err error, result ...any) {
		args := []zap.Field{
			zap.Duration("dur", time.Since(start)),
			zap.Any("args", args),
			zap.Any("result", result),
		}

		var log func(context.Context, string, ...zap.Field)
		if err == nil {
			log = l.Debug
		} else {
			log = l.Warn
			args = append(args, zap.Error(err))
			// args = append(args, zap.Stack("stack"))
		}

		log(ctx, fmt.Sprintf("[nfs proxy] %s: end", s), args...)
	}
}
