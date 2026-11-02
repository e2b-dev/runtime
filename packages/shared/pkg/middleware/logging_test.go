package middleware

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

func TestLevelForStatus(t *testing.T) {
	t.Parallel()

	const def = zapcore.InfoLevel

	tests := []struct {
		status int
		want   zapcore.Level
	}{
		{http.StatusOK, def},
		{http.StatusNoContent, def},
		{http.StatusMovedPermanently, def},
		{http.StatusNotFound, zapcore.InfoLevel},
		{http.StatusBadRequest, zapcore.WarnLevel},
		{http.StatusUnauthorized, zapcore.WarnLevel},
		{http.StatusForbidden, zapcore.WarnLevel},
		{http.StatusTooManyRequests, zapcore.WarnLevel},
		{http.StatusInternalServerError, zapcore.ErrorLevel},
		{http.StatusBadGateway, zapcore.ErrorLevel},
	}

	for _, tt := range tests {
		require.Equal(t, tt.want, levelForStatus(tt.status, def), "status %d", tt.status)
	}
}