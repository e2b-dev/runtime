package logger

import (
	"context"
	"errors"
	"fmt"
	"os"

	"go.opentelemetry.io/contrib/bridges/otelzap"
	"go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type edgeTraceIDContextKey struct{}

const edgeTraceIDField = "edge_trace_id"

type LoggerConfig struct {
	// ServiceName is the name of the service that the logger is being created for.
	// The service name is added to every log entry.
	ServiceName string
	// IsInternal differentiates between our (internal) logs, and user accessible (external) logs.
	IsInternal bool
	// IsDebug enables debug level logging, otherwise zap.InfoLevel level is used.
	IsDebug bool
	// DisableStacktrace disables stacktraces for the logger.
	DisableStacktrace bool

	// InitialFields fields that are added to every log entry.
	InitialFields []zap.Field
	// Cores additional processing cores for the logger.
	Cores []zapcore.Core
	// EnableConsole enables console logging.
	EnableConsole bool

	// Encoding sets the logger's encoding. If empty, defaults to "console".
	Encoding string
	// EncoderConfig optionally overrides the default encoder config.
	// If the encoding is set to "console," it defaults to GetConsoleEncoderConfig(); otherwise, it needs to be specified.
	EncoderConfig *zapcore.EncoderConfig
}

func NewLogger(loggerConfig LoggerConfig) (Logger, error) {
	var level zap.AtomicLevel
	if loggerConfig.IsDebug {
		level = zap.NewAtomicLevelAt(zap.DebugLevel)
	} else {
		level = zap.NewAtomicLevelAt(zap.InfoLevel)
	}

	encoding := loggerConfig.Encoding
	if encoding == "" {
		encoding = "console"
	}

	if encoding != "console" && loggerConfig.EncoderConfig == nil {
		return nil, errors.New("encoder config has to be provided when encoding is not console")
	}

	var encoderConfig zapcore.EncoderConfig
	if loggerConfig.EncoderConfig != nil {
		encoderConfig = *loggerConfig.EncoderConfig
	} else {
		encoderConfig = GetConsoleEncoderConfig()
	}

	config := zap.Config{
		DisableStacktrace: loggerConfig.DisableStacktrace,
		// Takes stacktraces more liberally
		Development: true,
		Sampling:    nil,

		Encoding:         encoding,
		EncoderConfig:    encoderConfig,
		Level:            level,
		OutputPaths:      []string{},
		ErrorOutputPaths: []string{},
	}
	if loggerConfig.EnableConsole {
		config.OutputPaths = []string{"stdout"}
		config.ErrorOutputPaths = []string{"stderr"}
	}

	cores := make([]zapcore.Core, 0)
	cores = append(cores, loggerConfig.Cores...)

	opts := []zap.Option{
		zap.WrapCore(func(c zapcore.Core) zapcore.Core {
			cores = append(cores, c)

			return zapcore.NewTee(cores...)
		}),
		zap.Fields(
			zap.String("service", loggerConfig.ServiceName),
			zap.Bool("internal", loggerConfig.IsInternal),
			zap.Int("pid", os.Getpid()),
		),
		zap.Fields(loggerConfig.InitialFields...),
	}
	if !loggerConfig.DisableStacktrace {
		opts = append([]zap.Option{zap.AddStacktrace(zapcore.ErrorLevel)}, opts...)
	}
	logger, err := config.Build(opts...)
	if err != nil {
		return nil, fmt.Errorf("error building logger: %w", err)
	}

	return NewTracedLogger(logger), nil
}

func GetConsoleEncoderConfig() zapcore.EncoderConfig {
	cfg := zap.NewDevelopmentEncoderConfig()
	cfg.EncodeLevel = zapcore.CapitalColorLevelEncoder
	cfg.CallerKey = zapcore.OmitKey
	cfg.ConsoleSeparator = "  "

	return cfg
}

func GetOTELCore(provider log.LoggerProvider, serviceName string) zapcore.Core {
	return otelzap.NewCore(serviceName, otelzap.WithLoggerProvider(provider))
}

type TracedLogger struct {
	innerLogger *zap.Logger
}

func NewTracedLoggerFromCore(core zapcore.Core) Logger {
	return &TracedLogger{innerLogger: zap.New(core)} //nolint:forbidigo // zap.New is used to create a new logger from a core
}

func NewTracedLogger(innerLogger *zap.Logger) Logger {
	return &TracedLogger{innerLogger: innerLogger}
}

func L() *TracedLogger {
	return &TracedLogger{innerLogger: zap.L()} //nolint:forbidigo // zap.L is used to get the global logger
}

func NewNopLogger() *TracedLogger {
	return &TracedLogger{innerLogger: zap.NewNop()} //nolint:forbidigo // zap.NewNop is used to create a new nop logger
}

func NewDevelopmentLogger() (Logger, error) {
	zl, err := zap.NewDevelopment() //nolint:forbidigo // zap.NewDevelopment is used to create a new development logger
	if err != nil {
		return nil, err
	}

	return NewTracedLogger(zl), nil
}

func (t *TracedLogger) With(fields ...zap.Field) Logger {
	return &TracedLogger{innerLogger: t.innerLogger.With(fields...)}
}

func (t *TracedLogger) Info(ctx context.Context, msg string, fields ...zap.Field) {
	t.innerLogger.Info(msg, t.generateFields(ctx, fields...)...)
}

func (t *TracedLogger) Warn(ctx context.Context, msg string, fields ...zap.Field) {
	t.innerLogger.Warn(msg, t.generateFields(ctx, fields...)...)
}

func (t *TracedLogger) Error(ctx context.Context, msg string, fields ...zap.Field) {
	t.innerLogger.Error(msg, t.generateFields(ctx, fields...)...)
}

func (t *TracedLogger) Fatal(ctx context.Context, msg string, fields ...zap.Field) {
	t.innerLogger.Fatal(msg, t.generateFields(ctx, fields...)...)
}

func (t *TracedLogger) Panic(ctx context.Context, msg string, fields ...zap.Field) {
	t.innerLogger.Panic(msg, t.generateFields(ctx, fields...)...)
}

func (t *TracedLogger) Debug(ctx context.Context, msg string, fields ...zap.Field) {
	t.innerLogger.Debug(msg, t.generateFields(ctx, fields...)...)
}

func (t *TracedLogger) Log(ctx context.Context, lvl zapcore.Level, msg string, fields ...zap.Field) {
	t.innerLogger.Log(lvl, msg, t.generateFields(ctx, fields...)...)
}

func (t *TracedLogger) WithOptions(opts ...zap.Option) Logger {
	return &TracedLogger{innerLogger: t.innerLogger.WithOptions(opts...)}
}

func (t *TracedLogger) Sync() error {
	return t.innerLogger.Sync()
}

func (t *TracedLogger) Detach(ctx context.Context) *zap.Logger {
	return t.innerLogger.With(t.generateFields(ctx)...)
}

func (t *TracedLogger) generateFields(ctx context.Context, fields ...zap.Field) []zap.Field {
	if ctx != nil {
		contextFields := make([]zap.Field, 0)

		if edgeTraceID, ok := GetEdgeTraceID(ctx); ok {
			contextFields = append(contextFields, zap.String(edgeTraceIDField, edgeTraceID))
		}

		span := trace.SpanFromContext(ctx)
		spanContext := span.SpanContext()
		if spanContext.HasTraceID() {
			contextFields = append(contextFields, zap.String("trace_id", spanContext.TraceID().String()))
		}
		if spanContext.HasSpanID() {
			contextFields = append(contextFields, zap.String("span_id", spanContext.SpanID().String()))
		}

		return append(contextFields, fields...)
	}

	return fields
}

func ContextWithEdgeTraceID(ctx context.Context, edgeTraceID string) context.Context {
	return context.WithValue(ctx, edgeTraceIDContextKey{}, edgeTraceID)
}

func GetEdgeTraceID(ctx context.Context) (string, bool) {
	edgeTraceID, ok := ctx.Value(edgeTraceIDContextKey{}).(string)
	if !ok || edgeTraceID == "" {
		return "", false
	}

	return edgeTraceID, true
}

func ReplaceGlobals(ctx context.Context, logger Logger) func() {
	return zap.ReplaceGlobals(logger.Detach(ctx))
}

type Logger = *TracedLogger
