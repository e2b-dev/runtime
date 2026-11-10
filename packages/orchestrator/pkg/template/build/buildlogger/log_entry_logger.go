package buildlogger

import (
	"fmt"
	"slices"
	"sync"

	"go.uber.org/zap/zapcore"
	"google.golang.org/protobuf/types/known/timestamppb"

	template_manager "github.com/e2b-dev/infra/packages/shared/pkg/grpc/template-manager"
)

// LogEntryLogger is a zapcore.Core that captures every log entry into an
// in-memory slice of template_manager.TemplateBuildLogEntry. It replaces the
// older io.Writer-based implementation that round-tripped log lines through
// JSON; consuming entries directly avoids that and removes the need for any
// fallback error-logging context.
type LogEntryLogger struct {
	zapcore.LevelEnabler

	mu    sync.Mutex
	lines []*template_manager.TemplateBuildLogEntry
}

// Compile-time assertion: LogEntryLogger implements zapcore.Core.
var _ zapcore.Core = (*LogEntryLogger)(nil)

func NewLogEntryLogger() *LogEntryLogger {
	return &LogEntryLogger{
		LevelEnabler: zapcore.DebugLevel,
		lines:        make([]*template_manager.TemplateBuildLogEntry, 0),
	}
}

// With returns a Core that shares this LogEntryLogger's capture buffer but
// carries additional accumulated fields. Returning a child type (rather than
// cloning the LogEntryLogger) keeps a single source of truth for Lines().
func (b *LogEntryLogger) With(fields []zapcore.Field) zapcore.Core {
	return &childCore{
		parent: b,
		with:   append([]zapcore.Field(nil), fields...),
	}
}

func (b *LogEntryLogger) Check(ent zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if b.Enabled(ent.Level) {
		return ce.AddCore(ent, b)
	}

	return ce
}

func (b *LogEntryLogger) Write(ent zapcore.Entry, fields []zapcore.Field) error {
	return b.write(ent, nil, fields)
}

func (b *LogEntryLogger) Sync() error {
	// In-memory capture; nothing to flush. Acquire the mutex so Sync acts as
	// a quiescence barrier: once it returns, no Write is in flight, which
	// callers (e.g. on build done/success) rely on to observe a settled
	// capture buffer.
	b.mu.Lock()
	defer b.mu.Unlock()

	return nil
}

func (b *LogEntryLogger) write(ent zapcore.Entry, accumulated, fields []zapcore.Field) error {
	// Hold the mutex for the entire write (encoding + append) so that Sync,
	// which acquires the same mutex, is a true quiescence barrier: once Sync
	// returns, no Write is in flight and the capture buffer is settled.
	b.mu.Lock()
	defer b.mu.Unlock()

	enc := zapcore.NewMapObjectEncoder()
	for _, f := range accumulated {
		f.AddTo(enc)
	}
	for _, f := range fields {
		f.AddTo(enc)
	}

	flat := make(map[string]string, len(enc.Fields))
	for k, v := range enc.Fields {
		switch t := v.(type) {
		case string:
			flat[k] = t
		case fmt.Stringer:
			flat[k] = t.String()
		case error:
			flat[k] = t.Error()
		default:
			flat[k] = fmt.Sprint(v)
		}
	}

	b.lines = append(b.lines, &template_manager.TemplateBuildLogEntry{
		Timestamp: timestamppb.New(ent.Time.UTC()),
		Message:   ent.Message,
		Level:     zapLevelToLogLevel(ent.Level),
		Fields:    flat,
	})

	return nil
}

func (b *LogEntryLogger) Lines() []*template_manager.TemplateBuildLogEntry {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Shallow copy of the slice (not the entries themselves)
	copied := make([]*template_manager.TemplateBuildLogEntry, len(b.lines))
	copy(copied, b.lines)

	return copied
}

// childCore is the result of LogEntryLogger.With. It shares the parent's
// capture state and only carries additional accumulated fields, so that
// zap's per-logger With(...) chains don't fork the capture buffer.
type childCore struct {
	parent *LogEntryLogger
	with   []zapcore.Field
}

func (c *childCore) Enabled(l zapcore.Level) bool { return c.parent.Enabled(l) }

func (c *childCore) With(fields []zapcore.Field) zapcore.Core {
	return &childCore{
		parent: c.parent,
		with:   slices.Concat(c.with, fields),
	}
}

func (c *childCore) Check(ent zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if c.Enabled(ent.Level) {
		return ce.AddCore(ent, c)
	}

	return ce
}

func (c *childCore) Write(ent zapcore.Entry, fields []zapcore.Field) error {
	return c.parent.write(ent, c.with, fields)
}

func (c *childCore) Sync() error { return c.parent.Sync() }

func zapLevelToLogLevel(level zapcore.Level) template_manager.LogLevel {
	switch level {
	case zapcore.DebugLevel:
		return template_manager.LogLevel_Debug
	case zapcore.InfoLevel:
		return template_manager.LogLevel_Info
	case zapcore.WarnLevel:
		return template_manager.LogLevel_Warn
	case zapcore.ErrorLevel, zapcore.DPanicLevel, zapcore.PanicLevel, zapcore.FatalLevel:
		return template_manager.LogLevel_Error
	default:
		return template_manager.LogLevel_Info
	}
}
