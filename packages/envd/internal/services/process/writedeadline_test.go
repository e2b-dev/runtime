package process

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// deadlineTracker is a minimal http.ResponseWriter that records every
// SetWriteDeadline call so tests can inspect the values passed.
type deadlineTracker struct {
	*httptest.ResponseRecorder
	deadlines []time.Time
}

func (d *deadlineTracker) SetWriteDeadline(t time.Time) error {
	d.deadlines = append(d.deadlines, t)
	return nil
}

func newDeadlineTracker() *deadlineTracker {
	return &deadlineTracker{ResponseRecorder: httptest.NewRecorder()}
}

// TestStreamDeadlineMiddleware_StoresResponseWriterInContext verifies that the
// middleware makes the ResponseWriter retrievable via responseWriterKey inside
// the request context, so connect.go / start.go can call setStreamWriteDeadline.
func TestStreamDeadlineMiddleware_StoresResponseWriterInContext(t *testing.T) {
	t.Parallel()

	var capturedCtx context.Context

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedCtx = r.Context()
	})

	rw := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", nil)

	streamDeadlineMiddleware(inner).ServeHTTP(rw, req)

	require.NotNil(t, capturedCtx)
	w, ok := capturedCtx.Value(responseWriterKey{}).(http.ResponseWriter)
	require.True(t, ok, "middleware must store the ResponseWriter in the request context")
	assert.NotNil(t, w)
}

// TestStreamDeadlineMiddleware_PreservesRequest verifies that the middleware
// does not alter the request method, path, or headers.
func TestStreamDeadlineMiddleware_PreservesRequest(t *testing.T) {
	t.Parallel()

	var capturedMethod, capturedPath, capturedHeader string

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedMethod = r.Method
		capturedPath = r.URL.Path
		capturedHeader = r.Header.Get("X-Custom")
		w.WriteHeader(http.StatusOK)
	})

	rw := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/test/path", nil)
	req.Header.Set("X-Custom", "value")

	streamDeadlineMiddleware(inner).ServeHTTP(rw, req)

	assert.Equal(t, http.MethodGet, capturedMethod)
	assert.Equal(t, "/test/path", capturedPath)
	assert.Equal(t, "value", capturedHeader)
	assert.Equal(t, http.StatusOK, rw.Code)
}

// TestSetStreamWriteDeadline_SetsDeadlineApproximatelyNowPlusInterval verifies
// that setStreamWriteDeadline calls SetWriteDeadline with a time of approximately
// now + streamWriteDeadline (within a 1-second tolerance for test execution).
func TestSetStreamWriteDeadline_SetsDeadlineApproximatelyNowPlusInterval(t *testing.T) {
	t.Parallel()

	tracker := newDeadlineTracker()
	ctx := context.WithValue(context.Background(), responseWriterKey{}, http.ResponseWriter(tracker))

	before := time.Now()
	setStreamWriteDeadline(ctx)
	after := time.Now()

	require.Len(t, tracker.deadlines, 1, "exactly one SetWriteDeadline call expected")

	deadline := tracker.deadlines[0]
	expectedMin := before.Add(streamWriteDeadline)
	expectedMax := after.Add(streamWriteDeadline)

	assert.False(t, deadline.Before(expectedMin),
		"deadline %v should be ≥ now+streamWriteDeadline (%v)", deadline, expectedMin)
	assert.False(t, deadline.After(expectedMax),
		"deadline %v should be ≤ now+streamWriteDeadline (%v)", deadline, expectedMax)
}

// TestClearStreamWriteDeadline_SetsZeroTime verifies that clearStreamWriteDeadline
// calls SetWriteDeadline with the zero time.Time, which is how Go's HTTP layer
// interprets "remove the deadline".
func TestClearStreamWriteDeadline_SetsZeroTime(t *testing.T) {
	t.Parallel()

	tracker := newDeadlineTracker()
	ctx := context.WithValue(context.Background(), responseWriterKey{}, http.ResponseWriter(tracker))

	clearStreamWriteDeadline(ctx)

	require.Len(t, tracker.deadlines, 1, "exactly one SetWriteDeadline call expected")
	assert.True(t, tracker.deadlines[0].IsZero(),
		"clearStreamWriteDeadline must pass the zero time.Time to disarm the deadline")
}

// TestSetAndClearStreamWriteDeadline_RoundTrip verifies that calling set then
// clear yields exactly two SetWriteDeadline calls: arm (future time) then disarm
// (zero time). This mirrors what connect.go and start.go do around each Send.
func TestSetAndClearStreamWriteDeadline_RoundTrip(t *testing.T) {
	t.Parallel()

	tracker := newDeadlineTracker()
	ctx := context.WithValue(context.Background(), responseWriterKey{}, http.ResponseWriter(tracker))

	setStreamWriteDeadline(ctx)
	clearStreamWriteDeadline(ctx)

	require.Len(t, tracker.deadlines, 2)
	assert.False(t, tracker.deadlines[0].IsZero(), "first call (arm) must set a future deadline")
	assert.True(t, tracker.deadlines[1].IsZero(), "second call (disarm) must set zero time")
}

// TestSetStreamWriteDeadline_NoopWhenNoResponseWriterInContext verifies that
// calling setStreamWriteDeadline with a plain context (no ResponseWriter)
// does not panic and is silently ignored. This is the path taken when
// streamDeadlineMiddleware is not mounted.
func TestSetStreamWriteDeadline_NoopWhenNoResponseWriterInContext(t *testing.T) {
	t.Parallel()

	assert.NotPanics(t, func() {
		setStreamWriteDeadline(context.Background())
	})
}

// TestClearStreamWriteDeadline_NoopWhenNoResponseWriterInContext is the
// symmetric test for clearStreamWriteDeadline.
func TestClearStreamWriteDeadline_NoopWhenNoResponseWriterInContext(t *testing.T) {
	t.Parallel()

	assert.NotPanics(t, func() {
		clearStreamWriteDeadline(context.Background())
	})
}

// TestSetStreamWriteDeadline_NoopWhenWriterDoesNotSupportDeadlines verifies
// that when the ResponseWriter in context does NOT implement SetWriteDeadline
// (http.NewResponseController returns ErrNotSupported, which we ignore), the
// helpers do not panic.
func TestSetStreamWriteDeadline_NoopWhenWriterDoesNotSupportDeadlines(t *testing.T) {
	t.Parallel()

	// httptest.ResponseRecorder does not implement SetWriteDeadline.
	plain := httptest.NewRecorder()
	ctx := context.WithValue(context.Background(), responseWriterKey{}, http.ResponseWriter(plain))

	assert.NotPanics(t, func() {
		setStreamWriteDeadline(ctx)
		clearStreamWriteDeadline(ctx)
	})
}

// TestStreamWriteDeadlineConstant guards against accidental misconfiguration of
// the deadline constant to zero or a tiny value that would false-positive on
// legitimate slow connections.
func TestStreamWriteDeadlineConstant(t *testing.T) {
	t.Parallel()

	assert.Positive(t, streamWriteDeadline,
		"streamWriteDeadline must be a positive duration")
	assert.GreaterOrEqual(t, streamWriteDeadline, 5*time.Second,
		"streamWriteDeadline should be ≥ 5 s to avoid false-positives on slow networks")
	assert.LessOrEqual(t, streamWriteDeadline, 60*time.Second,
		"streamWriteDeadline should be ≤ 60 s to detect dead peers within a keepalive cycle")
}

// TestStreamDeadlineMiddleware_WriterIsUsedForDeadlines is an end-to-end test
// verifying that the ResponseWriter stored by the middleware is the same one
// that setStreamWriteDeadline calls SetWriteDeadline on. It connects the two
// halves: middleware installation and deadline helper usage.
func TestStreamDeadlineMiddleware_WriterIsUsedForDeadlines(t *testing.T) {
	t.Parallel()

	tracker := newDeadlineTracker()

	// Replace the httptest.NewRecorder() inside the handler with our tracker
	// by having the middleware receive it as the outer ResponseWriter.
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The middleware stored w (our tracker) in context.
		// setStreamWriteDeadline should call SetWriteDeadline on it.
		setStreamWriteDeadline(r.Context())
		clearStreamWriteDeadline(r.Context())
	})

	req := httptest.NewRequest(http.MethodPost, "/", nil)
	streamDeadlineMiddleware(inner).ServeHTTP(tracker, req)

	require.Len(t, tracker.deadlines, 2, "set+clear should produce two SetWriteDeadline calls")
	assert.False(t, tracker.deadlines[0].IsZero(), "arm: non-zero deadline")
	assert.True(t, tracker.deadlines[1].IsZero(), "disarm: zero deadline")
}
