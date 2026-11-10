package openapispec

import (
	"encoding/json"
	"maps"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	document        = `{"openapi":"3.0.0","info":{"title":"API","version":"1"},"paths":{"/sandboxes":{"post":{"security":[{"ApiKeyAuth":[]}],"responses":{"201":{"description":"Created"}}}}}}`
	changedDocument = `{"openapi":"3.0.0","info":{"title":"API","version":"1"},"paths":{}}`
)

// serve sends one request to a Handler serving doc at Path, for GET and HEAD
// as the services register it.
func serve(t *testing.T, doc string, method string, header http.Header) *httptest.ResponseRecorder {
	t.Helper()

	handler, err := Handler(json.RawMessage(doc))
	require.NoError(t, err)

	engine := gin.New()
	engine.GET(Path, handler)
	engine.HEAD(Path, handler)

	req := httptest.NewRequestWithContext(t.Context(), method, Path, nil)
	maps.Copy(req.Header, header)
	actualResponse := httptest.NewRecorder()
	engine.ServeHTTP(actualResponse, req)

	return actualResponse
}

func Test_Handler(t *testing.T) {
	t.Parallel()

	// A client reads the served document to learn the deployment's
	// operations and their security.
	t.Run("should serve the document as JSON", func(t *testing.T) {
		t.Parallel()

		// Act
		actualResponse := serve(t, document, http.MethodGet, nil)

		// Assert
		require.Equal(t, http.StatusOK, actualResponse.Code)
		assert.Equal(t, "application/json", actualResponse.Header().Get("Content-Type"))
		assert.Equal(t, "no-cache", actualResponse.Header().Get("Cache-Control"))
		assert.JSONEq(t, document, actualResponse.Body.String())
	})

	t.Run("should answer HEAD with the ETag and no body", func(t *testing.T) {
		t.Parallel()

		// Act
		actualResponse := serve(t, document, http.MethodHead, nil)

		// Assert
		assert.Equal(t, http.StatusOK, actualResponse.Code)
		assert.NotEmpty(t, actualResponse.Header().Get("ETag"))
		assert.Empty(t, actualResponse.Body.Bytes())
	})

	// A client refreshing the document pays a 304 until the document changes.
	t.Run("should answer 304 to its ETag until the document changes", func(t *testing.T) {
		t.Parallel()

		// Arrange
		etag := serve(t, document, http.MethodGet, nil).Header().Get("ETag")
		require.NotEmpty(t, etag)
		ifNoneMatch := http.Header{"If-None-Match": {etag}}

		// Act
		unchanged := serve(t, document, http.MethodGet, ifNoneMatch)
		changed := serve(t, changedDocument, http.MethodGet, ifNoneMatch)

		// Assert
		assert.Equal(t, http.StatusNotModified, unchanged.Code)
		assert.Empty(t, unchanged.Body.Bytes())
		assert.Equal(t, http.StatusOK, changed.Code)
		assert.NotEqual(t, etag, changed.Header().Get("ETag"))
	})
}
