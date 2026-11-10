package sbxlogger

import (
	"context"

	"go.opentelemetry.io/otel/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

type SandboxLoggerConfig struct {
	// ServiceName is the name of the service that the logger is being created for.
	// The service name is added to every log entry.
	ServiceName string
	// IsInternal differentiates between our (internal) logs, and user accessible (external) logs.
	// For external logger, we also disable stacktraces
	IsInternal       bool
	CollectorAddress string
	// FeatureFlags, when set for an external logger with a CollectorAddress,
	// enables LaunchDarkly-controlled log write routing (see
	// featureflags.LogsWriteConfigFlag), merged over CollectorAddress and the
	// LOGS_WRITE_* defaults. When nil the logger keeps writing only to
	// CollectorAddress (legacy behavior).
	FeatureFlags *featureflags.Client
}

func NewLogger(ctx context.Context, loggerProvider log.LoggerProvider, config SandboxLoggerConfig) logger.Logger {
	level := zap.NewAtomicLevelAt(zap.DebugLevel)

	enableConsole := false
	var core zapcore.Core
	if !config.IsInternal && config.CollectorAddress != "" {
		// Add Vector exporter to the core
		vectorEncoder := zapcore.NewJSONEncoder(GetSandboxEncoderConfig())
		httpWriter := newExternalLogWriter(ctx, config)
		core = zapcore.NewCore(
			vectorEncoder,
			httpWriter,
			level,
		)
	} else {
		core = logger.GetOTELCore(loggerProvider, config.ServiceName)
		enableConsole = true
	}

	lg, err := logger.NewLogger(logger.LoggerConfig{
		ServiceName:       config.ServiceName,
		IsInternal:        config.IsInternal,
		IsDebug:           true,
		DisableStacktrace: !config.IsInternal,
		InitialFields: []zap.Field{
			zap.String("logger", config.ServiceName),
		},
		Cores:         []zapcore.Core{core},
		EnableConsole: enableConsole,
	})
	if err != nil {
		panic(err)
	}

	return lg
}

// newExternalLogWriter builds the write syncer for external sandbox logs.
// With a FeatureFlags client it resolves destinations from
// featureflags.LogsWriteConfigFlag on each write, falling back to
// CollectorAddress and the LOGS_WRITE_* defaults; without one it uses the
// legacy fixed-address writer.
func newExternalLogWriter(ctx context.Context, config SandboxLoggerConfig) zapcore.WriteSyncer {
	if config.FeatureFlags == nil {
		return logger.NewHTTPWriter(ctx, config.CollectorAddress)
	}

	// Cache LaunchDarkly evaluations behind a short TTL so per-line writes on
	// the hot path don't evaluate the flag every time.
	resolver := featureflags.NewLogWriteConfigResolver(config.FeatureFlags, featureflags.LogWriteDefaultsFromEnv(config.CollectorAddress))
	resolve := func(ctx context.Context) logger.LogRoute {
		cfg := resolver.Resolve(ctx)

		return logger.LogRoute{
			PrimaryURL:              cfg.PrimaryURL,
			ShadowURLs:              cfg.ShadowURLs,
			Timeout:                 cfg.Timeout,
			MaxInflightShadowWrites: cfg.MaxInflightShadowWrites,
		}
	}

	return logger.NewDynamicHTTPWriter(ctx, config.CollectorAddress, resolve)
}

func GetSandboxEncoderConfig() zapcore.EncoderConfig {
	return zapcore.EncoderConfig{
		TimeKey:       "timestamp",
		MessageKey:    "message",
		LevelKey:      "level",
		EncodeLevel:   zapcore.LowercaseLevelEncoder,
		NameKey:       "logger",
		StacktraceKey: "stacktrace",
		EncodeTime:    zapcore.RFC3339NanoTimeEncoder,
		LineEnding:    zapcore.DefaultLineEnding,
	}
}
