package tracing

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

func TestARequestsLinesCarryItsEdgeTraceID(t *testing.T) {
	t.Parallel()

	const edgeTraceID = "0123456789abcdef0123456789abcdef"
	core, logs := observer.New(zap.InfoLevel)
	l := logger.NewTracedLoggerFromCore(core)
	router := gin.New()
	router.Use(Middleware(noop.NewTracerProvider(), "test"))
	router.GET("/sandboxes", func(c *gin.Context) {
		l.Info(c.Request.Context(), "handled")
		c.Status(http.StatusNoContent)
	})

	request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/sandboxes", nil)
	request.Header.Set(telemetry.GCPTraceContextHeader, edgeTraceID+"/1;o=1")
	router.ServeHTTP(httptest.NewRecorder(), request)

	require.Len(t, logs.All(), 1)
	assert.Equal(t, edgeTraceID, logs.All()[0].ContextMap()["edge_trace_id"])
}
