package idempotency

import (
	"bufio"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type transportSpy struct {
	*httptest.ResponseRecorder

	closed  chan bool
	hijacks int
	pushes  int
}

func (w *transportSpy) CloseNotify() <-chan bool { return w.closed }

func (w *transportSpy) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	w.hijacks++

	return nil, nil, http.ErrNotSupported
}

func (w *transportSpy) Push(string, *http.PushOptions) error {
	w.pushes++

	return nil
}

func TestResponseCaptureInterceptsGinWrites(t *testing.T) {
	t.Parallel()
	transport := &transportSpy{ResponseRecorder: httptest.NewRecorder(), closed: make(chan bool)}
	c, _ := gin.CreateTestContext(transport)
	original := c.Writer
	original.Header().Set("X-Removed", "before capture")
	capture := newResponseCapture(original)
	defer capture.release()
	capture.Header().Set("X-Captured", "yes")
	capture.Header().Del("X-Removed")
	original.Header().Set("X-Shared", "after capture")
	require.Equal(t, "after capture", capture.Header().Get("X-Shared"))
	capture.WriteHeader(http.StatusCreated)
	capture.WriteHeader(http.StatusAccepted)
	require.Equal(t, http.StatusAccepted, capture.Status())
	require.False(t, capture.Written())
	require.Equal(t, -1, capture.Size())

	n, err := capture.WriteString("string")
	require.NoError(t, err)
	require.Equal(t, 6, n)
	capture.WriteHeader(http.StatusInternalServerError)
	n, err = capture.Write([]byte("bytes"))
	require.NoError(t, err)
	require.Equal(t, 5, n)
	copied, err := io.Copy(capture, struct{ io.Reader }{strings.NewReader("stream")})
	require.NoError(t, err)
	require.EqualValues(t, 6, copied)
	require.NoError(t, http.NewResponseController(capture).Flush())

	require.True(t, capture.Written())
	require.Equal(t, 17, capture.Size())
	require.Equal(t, http.StatusAccepted, capture.Status())
	require.Equal(t, "stringbytesstream", string(capture.response().Body))
	require.Empty(t, capture.response().Headers)
	require.False(t, original.Written())
	require.Equal(t, http.StatusOK, original.Status())
	require.Empty(t, transport.Body.String())
	require.Equal(t, "yes", transport.Header().Get("X-Captured"))
	require.Empty(t, transport.Header().Get("X-Removed"))
	require.False(t, transport.Flushed)
	require.Equal(t, (<-chan bool)(transport.closed), capture.CloseNotify())

	_, _, err = capture.Hijack()
	require.ErrorContains(t, err, "cannot be hijacked")
	require.Zero(t, transport.hijacks)
	require.NotNil(t, original.Pusher())
	require.Nil(t, capture.Pusher())
	require.Zero(t, transport.pushes)
}

func TestResponseCaptureSnapshotsAllowedHeaders(t *testing.T) {
	t.Parallel()
	transport := httptest.NewRecorder()
	transport.Header().Set("Content-Type", "application/vnd.example+json")
	transport.Header().Set("RateLimit-Remaining", "5")
	c, _ := gin.CreateTestContext(transport)
	capture := newResponseCapture(c.Writer)
	defer capture.release()
	capture.Header().Set("Location", "/widgets/one")
	capture.Header().Set("ETag", `"created"`)
	capture.Header().Set("Set-Cookie", "session=example")
	capture.Header().Set("Access-Control-Allow-Origin", "*")
	_, err := capture.Write([]byte(`{"value":"one"}`))
	require.NoError(t, err)
	result := capture.response()
	want := http.Header{"Content-Type": {"application/vnd.example+json"}}
	require.Equal(t, want, result.Headers)
	capture.Header()["Content-Type"][0] = "text/plain"
	require.Equal(t, want, result.Headers)
}

func TestResponseCaptureBuffersHeaderOnlyResponse(t *testing.T) {
	t.Parallel()
	transport := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(transport)
	capture := newResponseCapture(c.Writer)
	defer capture.release()
	capture.WriteHeader(http.StatusNoContent)
	capture.WriteHeaderNow()
	capture.WriteHeader(http.StatusInternalServerError)
	require.True(t, capture.Written())
	require.Zero(t, capture.Size())
	require.Equal(t, http.StatusNoContent, capture.response().Status)
	require.Empty(t, capture.response().Body)
	require.Empty(t, capture.response().Headers)
	require.False(t, c.Writer.Written())
	require.Empty(t, transport.Body.String())
}

func TestResponseCaptureReleaseClearsBody(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		size int
	}{{"pooled", 32}, {"oversized", maxPooledResponseBytes + 1}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			capture := newResponseCapture(c.Writer)
			_, err := capture.WriteString(strings.Repeat("s", tc.size))
			require.NoError(t, err)
			body := capture.response().Body
			capture.release()
			if tc.size > maxPooledResponseBytes {
				require.Equal(t, make([]byte, tc.size), body)
			}
			require.Nil(t, capture.body)
			next := newResponseCapture(c.Writer)
			defer next.release()
			require.Empty(t, next.response().Body)
			_, err = next.WriteString("new")
			require.NoError(t, err)
			require.Equal(t, "new", string(next.response().Body))
		})
	}
}

func TestReplayPreservesHTTPDefaults(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		status int
		body   []byte
	}{
		{"text", http.StatusOK, []byte("plain text")},
		{"binary", http.StatusOK, []byte{0, 255, 'a'}},
		{"empty", http.StatusOK, nil},
		{"no content", http.StatusNoContent, nil},
		{"not modified", http.StatusNotModified, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := gin.New()
			r.GET("/original", func(c *gin.Context) {
				c.Status(tc.status)
				if _, err := c.Writer.Write(tc.body); err != nil {
					t.Error(err)
				}
			})
			r.GET("/replay", func(c *gin.Context) {
				replay(c, cachedResponse{Status: tc.status, Body: tc.body})
			})
			server := httptest.NewServer(r)
			defer server.Close()
			var originalHeaders http.Header
			for _, path := range []string{"/original", "/replay"} {
				req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL+path, nil)
				require.NoError(t, err)
				res, err := server.Client().Do(req)
				require.NoError(t, err)
				body, err := io.ReadAll(res.Body)
				require.NoError(t, err)
				require.NoError(t, res.Body.Close())
				require.Equal(t, tc.status, res.StatusCode)
				require.Equal(t, string(tc.body), string(body))
				if originalHeaders == nil {
					originalHeaders = res.Header
				} else {
					require.Equal(t, originalHeaders.Values("Content-Type"), res.Header.Values("Content-Type"))
					require.Equal(t, originalHeaders.Values("Content-Length"), res.Header.Values("Content-Length"))
				}
			}
		})
	}
}
