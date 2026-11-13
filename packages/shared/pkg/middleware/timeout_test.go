package middleware

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRequestTimeout_SetsDeadline(t *testing.T) {
	t.Parallel()

	r := gin.New()
	r.Use(RequestTimeout(500 * time.Millisecond))
	r.GET("/test", func(c *gin.Context) {
		deadline, ok := c.Request.Context().Deadline()
		assert.True(t, ok, "context should have a deadline")
		assert.WithinDuration(t, time.Now().Add(500*time.Millisecond), deadline, 100*time.Millisecond)
		c.Status(http.StatusOK)
	})

	w := httptest.NewRecorder()
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, "/test", nil)
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
}

func TestRequestTimeout_CancelsBlockingHandler(t *testing.T) {
	t.Parallel()

	r := gin.New()
	r.Use(RequestTimeout(100 * time.Millisecond))

	handlerDone := make(chan struct{})
	r.GET("/slow", func(c *gin.Context) {
		defer close(handlerDone)
		<-c.Request.Context().Done()
		c.Status(http.StatusServiceUnavailable)
	})

	w := httptest.NewRecorder()
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, "/slow", nil)

	start := time.Now()
	r.ServeHTTP(w, req)
	elapsed := time.Since(start)

	<-handlerDone
	assert.Less(t, elapsed, 500*time.Millisecond, "handler should have been unblocked by context timeout")
}

func TestRequestTimeout_NormalRequestContextNotCanceled(t *testing.T) {
	t.Parallel()

	// Simulate an outer middleware that reads CancelCause after c.Next().
	// The context itself will be canceled by defer cancel(), but CancelCause
	// should return nil for normal (non-timed-out) requests.
	var outerCause error
	outerMiddleware := func(c *gin.Context) {
		c.Next()
		outerCause = CancelCause(c)
	}

	r := gin.New()
	r.Use(outerMiddleware)
	r.Use(RequestTimeout(5 * time.Second))
	r.GET("/test", func(c *gin.Context) {
		c.Status(http.StatusOK)
	})

	w := httptest.NewRecorder()
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, "/test", nil)
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	assert.NoError(t, outerCause, "CancelCause should return nil for normal requests")
}

func TestRequestTimeout_TimeoutContextVisibleToOuterMiddleware(t *testing.T) {
	t.Parallel()

	var outerCause error
	outerMiddleware := func(c *gin.Context) {
		c.Next()
		outerCause = CancelCause(c)
	}

	r := gin.New()
	r.Use(outerMiddleware)
	r.Use(RequestTimeout(50 * time.Millisecond))
	r.GET("/slow", func(c *gin.Context) {
		<-c.Request.Context().Done()
	})

	w := httptest.NewRecorder()
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, "/slow", nil)
	r.ServeHTTP(w, req)

	require.ErrorIs(t, outerCause, ErrRequestTimeout,
		"outer middleware should see ErrRequestTimeout as the cause when the timeout fires")
}

func TestClassifyCancelCause(t *testing.T) {
	t.Parallel()

	for name, tt := range map[string]struct {
		cause            error
		wantClient       bool
		wantServerShutdn bool
	}{
		"nil":               {cause: nil, wantClient: false, wantServerShutdn: false},
		"client disconnect": {cause: context.Canceled, wantClient: true, wantServerShutdn: false},
		"wrapped client":    {cause: fmt.Errorf("read: %w", context.Canceled), wantClient: true, wantServerShutdn: false},
		"request timeout":   {cause: ErrRequestTimeout, wantClient: false, wantServerShutdn: false},
		"server shutdown":   {cause: ErrServerShuttingDown, wantClient: false, wantServerShutdn: true},
		"wrapped shutdown":  {cause: fmt.Errorf("ctx: %w", ErrServerShuttingDown), wantClient: false, wantServerShutdn: true},
		"deadline exceeded": {cause: context.DeadlineExceeded, wantClient: false, wantServerShutdn: false},
		"unrelated error":   {cause: errors.New("boom"), wantClient: false, wantServerShutdn: false},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.wantClient, IsClientDisconnect(tt.cause), "IsClientDisconnect")
			assert.Equal(t, tt.wantServerShutdn, IsServerShutdown(tt.cause), "IsServerShutdown")
		})
	}
}

// TestServerShutdownCausePropagatesToOuterMiddleware proves the end-to-end
// wiring: a request whose parent context is cancelled with
// ErrServerShuttingDown (as the server does on teardown via BaseContext)
// surfaces that cause to an outer middleware through the RequestTimeout
// wrapper, so it is classified as a server shutdown rather than a client
// disconnect. See issue #3685.
func TestServerShutdownCausePropagatesToOuterMiddleware(t *testing.T) {
	t.Parallel()

	baseCtx, cancelBase := context.WithCancelCause(t.Context())

	var outerCause error
	outerMiddleware := func(c *gin.Context) {
		c.Next()
		outerCause = CancelCause(c)
	}

	r := gin.New()
	r.Use(outerMiddleware)
	r.Use(RequestTimeout(5 * time.Second))
	r.GET("/slow", func(c *gin.Context) {
		// Server tears down while this request is in flight.
		cancelBase(ErrServerShuttingDown)
		<-c.Request.Context().Done()
	})

	w := httptest.NewRecorder()
	req, _ := http.NewRequestWithContext(baseCtx, http.MethodGet, "/slow", nil)
	r.ServeHTTP(w, req)

	require.ErrorIs(t, outerCause, ErrServerShuttingDown)
	assert.True(t, IsServerShutdown(outerCause), "a teardown-cut request must classify as server shutdown")
	assert.False(t, IsClientDisconnect(outerCause), "a teardown-cut request must not classify as a client disconnect")
}
