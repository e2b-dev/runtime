package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
)

// timeoutBackendStub is a timeoutOrchestrator whose KeepAliveFor always fails
// with the configured APIError.
type timeoutBackendStub struct {
	err *api.APIError
}

func (s timeoutBackendStub) KeepAliveFor(_ context.Context, _ uuid.UUID, _ string, _ time.Duration, _ bool) (*sandbox.Sandbox, *api.APIError) {
	return nil, s.err
}

// runTimeoutWithError drives PostSandboxesSandboxIDTimeout with a stubbed
// KeepAliveFor returning apiErr, recording spans so the caller can inspect
// whether the handler reported a span error. Returns the HTTP recorder and the
// single finished span.
func runTimeoutWithError(t *testing.T, apiErr *api.APIError) (*httptest.ResponseRecorder, sdktrace.ReadOnlySpan) {
	t.Helper()

	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	tracer := provider.Tracer("test")

	ctx, span := tracer.Start(context.Background(), "timeout")

	store := &APIStore{timeoutBackendOverride: timeoutBackendStub{err: apiErr}}

	rec := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(rec)

	body, err := json.Marshal(api.SandboxTimeoutRequest{Timeout: 30})
	require.NoError(t, err)

	ginCtx.Request = httptest.NewRequestWithContext(ctx, http.MethodPost, "/sandboxes/abc/timeout", bytes.NewReader(body))
	ginCtx.Request.Header.Set("Content-Type", "application/json")
	auth.SetTeamInfoForTest(t, ginCtx, minimalTeamInfo())

	store.PostSandboxesSandboxIDTimeout(ginCtx, "abc123")

	span.End()
	spans := recorder.Ended()
	require.Len(t, spans, 1)

	return rec, spans[0]
}

// spanHasErrorEvent reports whether span carries an exception event, which is
// what span.RecordError (via telemetry.ReportError) adds.
func spanHasErrorEvent(span sdktrace.ReadOnlySpan) bool {
	for _, e := range span.Events() {
		if e.Name == "exception" {
			return true
		}
	}

	return false
}

// TestSandboxTimeout_NotFoundIsQuiet verifies that a 404 from KeepAliveFor is
// treated as an expected outcome: the handler returns the 404 to the client but
// does NOT call telemetry.ReportError, so no span error event is recorded.
func TestSandboxTimeout_NotFoundIsQuiet(t *testing.T) {
	t.Parallel()

	rec, span := runTimeoutWithError(t, &api.APIError{
		Code:      http.StatusNotFound,
		ClientMsg: "sandbox not found",
		Err:       assert.AnError,
	})

	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.False(t, spanHasErrorEvent(span),
		"a 404 must not record a span error — it is an expected race, not a failure")
	assert.Equal(t, codes.Unset, span.Status().Code,
		"a 404 must not set the span status to Error")
}

// TestSandboxTimeout_ServerErrorIsReported is the counterpart: a non-404 error
// still flows through telemetry.ReportError and records a span error, so real
// failures remain visible.
func TestSandboxTimeout_ServerErrorIsReported(t *testing.T) {
	t.Parallel()

	rec, span := runTimeoutWithError(t, &api.APIError{
		Code:      http.StatusInternalServerError,
		ClientMsg: "boom",
		Err:       assert.AnError,
	})

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.True(t, spanHasErrorEvent(span),
		"a 5xx must still record a span error so real failures are not hidden")
}
