package sharedriver

import (
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

func TestRiverLogsReachTheServiceLoggerFromWarnUp(t *testing.T) {
	t.Parallel()

	core, logged := observer.New(zapcore.DebugLevel)
	riverLog := NewLogger(logger.NewTracedLoggerFromCore(core)).With(slog.String("client_id", "worker-1"))

	riverLog.InfoContext(t.Context(), "JobExecutor: Job errored; retrying", slog.Int64("job_id", 1))
	riverLog.WarnContext(t.Context(), "JobExecutor: Job appears to be stuck", slog.Int64("job_id", 7), slog.Duration("timeout", time.Minute))
	riverLog.WithGroup("elector").ErrorContext(t.Context(), "Elector: Error attempting to elect", slog.String("error", "connection reset"))

	entries := logged.All()
	require.Len(t, entries, 2, "River's Info lines stay below the threshold its default logger keeps")

	assert.Equal(t, zapcore.WarnLevel, entries[0].Level)
	assert.Equal(t, "JobExecutor: Job appears to be stuck", entries[0].Message)
	assert.Equal(t, map[string]any{"client_id": "worker-1", "job_id": int64(7), "timeout": time.Minute}, entries[0].ContextMap())

	assert.Equal(t, zapcore.ErrorLevel, entries[1].Level)
	assert.Equal(t, map[string]any{"client_id": "worker-1", "elector.error": "connection reset"}, entries[1].ContextMap())
}
