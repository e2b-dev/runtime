package sharedriver

import (
	"context"
	"log/slog"
	"slices"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

// NewLogger returns the logger to set as river.Config.Logger, so River's own
// lines reach the service logger instead of River's default text handler on
// stdout. It keeps that default's Warn threshold: below it River reports every
// failed attempt, which OutboxJobTelemetry already logs with the attempt
// count, and routine maintenance.
func NewLogger(l logger.Logger) *slog.Logger {
	if l == nil {
		l = logger.NewNopLogger()
	}

	return slog.New(&logHandler{log: l})
}

type logHandler struct {
	log    logger.Logger
	fields []zap.Field
	group  string
}

func (*logHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= slog.LevelWarn
}

func (h *logHandler) Handle(ctx context.Context, record slog.Record) error {
	fields := slices.Grow(slices.Clone(h.fields), record.NumAttrs())
	record.Attrs(func(attr slog.Attr) bool {
		fields = appendAttr(fields, h.group, attr)

		return true
	})

	level := zapcore.WarnLevel
	if record.Level >= slog.LevelError {
		level = zapcore.ErrorLevel
	}
	h.log.Log(ctx, level, record.Message, fields...)

	return nil
}

func (h *logHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	fields := slices.Clone(h.fields)
	for _, attr := range attrs {
		fields = appendAttr(fields, h.group, attr)
	}

	return &logHandler{log: h.log, fields: fields, group: h.group}
}

func (h *logHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}

	return &logHandler{log: h.log, fields: h.fields, group: h.group + name + "."}
}

func appendAttr(fields []zap.Field, group string, attr slog.Attr) []zap.Field {
	value := attr.Value.Resolve()
	if value.Kind() == slog.KindGroup {
		if attr.Key != "" {
			group += attr.Key + "."
		}
		for _, member := range value.Group() {
			fields = appendAttr(fields, group, member)
		}

		return fields
	}
	if attr.Key == "" {
		return fields
	}

	return append(fields, zap.Any(group+attr.Key, value.Any()))
}
