package middleware

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
)

// TestCORSExposesSpecResponseHeaders asserts that every response header the
// OpenAPI spec declares is also exposed via CORS. Browsers withhold custom
// response headers from JS unless they are listed, so drift here silently
// breaks browser SDKs (e.g. pagination stops after the first page).
func TestCORSExposesSpecResponseHeaders(t *testing.T) {
	t.Parallel()

	swagger, err := api.GetSpec()
	require.NoError(t, err)

	exposed := make(map[string]struct{}, len(exposedResponseHeaders))
	for _, header := range exposedResponseHeaders {
		exposed[strings.ToLower(header)] = struct{}{}
	}

	for path, item := range swagger.Paths.Map() {
		for method, operation := range item.Operations() {
			for status, response := range operation.Responses.Map() {
				for header := range response.Value.Headers {
					assert.Containsf(t, exposed, strings.ToLower(header),
						"%s %s (%s) declares response header %q, add it to exposedResponseHeaders",
						method, path, status, header)
				}
			}
		}
	}
}

// TestCORSSetsExposeHeaders asserts the middleware actually emits the
// Access-Control-Expose-Headers response header on a cross-origin request.
func TestCORSSetsExposeHeaders(t *testing.T) {
	t.Parallel()

	r := gin.New()
	r.Use(CORS())
	r.GET("/", func(c *gin.Context) { c.Status(http.StatusOK) })

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
	// Must differ from the request host, otherwise the middleware treats the
	// request as same-origin and skips the CORS headers entirely.
	req.Header.Set("Origin", "https://app.e2b.dev")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Header().Get("Access-Control-Expose-Headers"), "X-Next-Token")
}

func TestCORSAllowsIdempotencyKey(t *testing.T) {
	t.Parallel()
	r := gin.New()
	r.Use(CORS())
	r.POST("/v2/sandboxes", func(c *gin.Context) { c.Status(http.StatusCreated) })
	req := httptest.NewRequestWithContext(t.Context(), http.MethodOptions, "/v2/sandboxes", nil)
	req.Header.Set("Origin", "https://app.e2b.dev")
	req.Header.Set("Access-Control-Request-Method", http.MethodPost)
	req.Header.Set("Access-Control-Request-Headers", "content-type,idempotency-key")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusNoContent, w.Code)
	require.Contains(t, w.Header().Get("Access-Control-Allow-Headers"), "Idempotency-Key")
}
