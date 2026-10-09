package openapispec

import (
	"maps"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
)

func serve(t *testing.T, doc *openapi3.T, header http.Header) *httptest.ResponseRecorder {
	t.Helper()

	handler, err := Handler(doc)
	require.NoError(t, err)

	engine := gin.New()
	engine.GET(Path, handler)

	request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, Path, nil)
	maps.Copy(request.Header, header)
	recorder := httptest.NewRecorder()
	engine.ServeHTTP(recorder, request)

	return recorder
}

func validatedSpec(t *testing.T) *openapi3.T {
	t.Helper()

	doc, err := api.GetSwagger()
	require.NoError(t, err)

	return doc
}

// A client reads the served document to learn the deployment's operations
// and their security.
func TestHandlerServesTheValidatedSpec(t *testing.T) {
	t.Parallel()

	response := serve(t, validatedSpec(t), nil)

	require.Equal(t, http.StatusOK, response.Code)
	assert.Equal(t, "application/json", response.Header().Get("Content-Type"))
	served, err := openapi3.NewLoader().LoadFromData(response.Body.Bytes())
	require.NoError(t, err)
	createSandbox := served.Paths.Find("/sandboxes").Post
	require.NotNil(t, createSandbox, "POST /sandboxes is served")
	require.NotNil(t, createSandbox.Security)
	assert.Contains(t, *createSandbox.Security, openapi3.SecurityRequirement{"ApiKeyAuth": {}})
}

// A client refreshing the document pays a 304 until the document changes.
func TestHandlerRevalidatesByDocument(t *testing.T) {
	t.Parallel()

	first := serve(t, validatedSpec(t), nil)
	etag := first.Header().Get("ETag")
	require.NotEmpty(t, etag)

	changed := validatedSpec(t)
	changed.Paths.Delete("/sandboxes")

	unchanged := serve(t, validatedSpec(t), http.Header{"If-None-Match": {etag}})
	updated := serve(t, changed, http.Header{"If-None-Match": {etag}})

	assert.Equal(t, http.StatusNotModified, unchanged.Code)
	assert.Empty(t, unchanged.Body.Bytes())
	assert.Equal(t, http.StatusOK, updated.Code)
	assert.NotEqual(t, etag, updated.Header().Get("ETag"))
}
