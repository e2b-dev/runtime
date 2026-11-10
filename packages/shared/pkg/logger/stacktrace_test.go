package logger

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

//nolint:paralleltest // t.Setenv cannot be used from a parallel test
func TestStacktracesCanBeDisabledByEnvironment(t *testing.T) {
	t.Setenv(stacktracesDisabledEnv, "true")

	core, logs := observer.New(zap.DebugLevel)
	l, err := NewLogger(LoggerConfig{ServiceName: "test", Cores: []zapcore.Core{core}})
	require.NoError(t, err)

	l.Error(t.Context(), "an error")

	require.Len(t, logs.All(), 1)
	assert.Empty(t, logs.All()[0].Stack)
}
