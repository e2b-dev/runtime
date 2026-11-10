package middleware_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/gin-gonic/gin"
	oapimiddleware "github.com/oapi-codegen/gin-middleware"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/middleware"
	"github.com/e2b-dev/infra/packages/api/internal/utils"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
)

// limit is the body limit the router under test enforces.
const limit = 256

// newValidatedRouter is ReadRequestBody, enforcing limit, and the request
// validator in front of POST /v2/sandboxes, wired as main.go wires them. The handlers record the body they receive, and recorded
// collects the errors the request leaves on the gin context, which the
// logging and tracing middleware persist.
func newValidatedRouter(t *testing.T, received *[]byte, recorded *[]string) *gin.Engine {
	t.Helper()

	swagger, err := api.GetSpec()
	require.NoError(t, err)
	swagger.Servers = nil

	authenticationFunc := auth.CreateAuthenticationFunc(
		[]auth.Authenticator{
			auth.NewApiKeyAuthenticator(func(context.Context, *gin.Context, string) (*types.Team, *auth.APIError) {
				return &types.Team{}, nil
			}),
		},
		nil,
	)

	r := gin.New()
	r.Use(
		func(c *gin.Context) {
			c.Next()
			for _, ginErr := range c.Errors {
				*recorded = append(*recorded, ginErr.Error())
			}
		},
		middleware.ReadRequestBody(limit),
		oapimiddleware.OapiRequestValidatorWithOptions(swagger, &oapimiddleware.Options{
			DiscardValidatedValue: true,
			ErrorHandler: func(c *gin.Context, message string, fallbackStatusCode int) {
				statusCode := max(c.Writer.Status(), fallbackStatusCode)
				utils.ErrorHandler(c, message, statusCode)
			},
			MultiErrorHandler: utils.MultiErrorHandler,
			Options: openapi3filter.Options{
				AuthenticationFunc: authenticationFunc,
				MultiError:         true,
			},
			SilenceServersWarning: true,
		}),
	)
	handler := func(c *gin.Context) {
		body, err := io.ReadAll(c.Request.Body)
		require.NoError(t, err)
		*received = body
		c.Status(http.StatusCreated)
	}
	r.POST("/v2/sandboxes", handler)

	return r
}

// createBody is a valid POST /v2/sandboxes body of exactly size bytes.
func createBody(t *testing.T, size int) string {
	t.Helper()

	prefix, suffix := `{"templateID":"base","metadata":{"pad":"`, `"}}`
	require.GreaterOrEqual(t, size, len(prefix)+len(suffix))

	return prefix + strings.Repeat("x", size-len(prefix)-len(suffix)) + suffix
}

func serve(t *testing.T, r *gin.Engine, body io.Reader) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v2/sandboxes", body)
	req.Header.Set(auth.HeaderAPIKey, "e2b_00")
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	return rr
}

// countingReader counts the bytes read from r.
type countingReader struct {
	r io.Reader
	n int
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += n

	return n, err
}

// assertJSONError checks that the whole body is one API error object for
// status, and returns its message.
func assertJSONError(t *testing.T, rr *httptest.ResponseRecorder, status int) string {
	t.Helper()

	var body struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &body), "body: %q", rr.Body.String())
	assert.Equal(t, status, body.Code)
	assert.NotEmpty(t, body.Message)

	return body.Message
}

func TestReadRequestBody(t *testing.T) {
	t.Parallel()

	t.Run("should pass a body of exactly the limit to the handler whole", func(t *testing.T) {
		t.Parallel()

		var received []byte
		r := newValidatedRouter(t, &received, new([]string))
		body := createBody(t, limit)

		rr := serve(t, r, strings.NewReader(body))

		// The validator re-encodes the body with the spec's defaults, so
		// compare what was sent, not the bytes.
		require.Equal(t, http.StatusCreated, rr.Code)
		var sent, got struct {
			TemplateID string            `json:"templateID"`
			Metadata   map[string]string `json:"metadata"`
		}
		require.NoError(t, json.Unmarshal([]byte(body), &sent))
		require.NoError(t, json.Unmarshal(received, &got))
		assert.Equal(t, sent, got)
	})

	t.Run("should answer a body over the limit with a JSON 413", func(t *testing.T) {
		t.Parallel()

		var received []byte
		r := newValidatedRouter(t, &received, new([]string))

		rr := serve(t, r, strings.NewReader(createBody(t, limit+1)))

		assert.Equal(t, http.StatusRequestEntityTooLarge, rr.Code)
		assertJSONError(t, rr, http.StatusRequestEntityTooLarge)
		assert.Nil(t, received)
	})

	// An unknown path or an unserved method has no route, so it gets 404.
	// Neither this middleware nor the validation error handler's log may
	// read its body.
	for _, unrouted := range []struct{ method, path string }{
		{http.MethodPost, "/no-such-route"},
		{http.MethodPatch, "/v2/sandboxes"},
	} {
		t.Run("should answer "+unrouted.method+" "+unrouted.path+" with 404 without reading the body", func(t *testing.T) {
			t.Parallel()

			var received []byte
			r := newValidatedRouter(t, &received, new([]string))
			body := &countingReader{r: strings.NewReader(createBody(t, 4*limit))}
			req := httptest.NewRequestWithContext(t.Context(), unrouted.method, unrouted.path, body)
			req.Header.Set(auth.HeaderAPIKey, "e2b_00")
			req.Header.Set("Content-Type", "application/json")
			rr := httptest.NewRecorder()

			r.ServeHTTP(rr, req)

			assert.Equal(t, http.StatusNotFound, rr.Code)
			assert.Zero(t, body.n)
		})
	}

	// The read error's text can quote the request, e.g. the line of a
	// malformed chunked body, so it is never recorded where logs and traces
	// pick it up.
	t.Run("should not record the read error", func(t *testing.T) {
		t.Parallel()

		const sentinel = "sentinel-value-DO-NOT-LOG-0000"
		var received []byte
		var recorded []string
		r := newValidatedRouter(t, &received, &recorded)
		readErr := errors.New(`malformed MIME header line: {"value":"` + sentinel + `"}`)

		rr := serve(t, r, io.MultiReader(strings.NewReader(`{"templateID":`), iotest.ErrReader(readErr)))

		assert.Equal(t, http.StatusBadRequest, rr.Code)
		require.NotEmpty(t, recorded)
		for _, recordedErr := range recorded {
			assert.NotContains(t, recordedErr, sentinel)
		}
	})

	t.Run("should not echo the read error of a body that cannot be read", func(t *testing.T) {
		t.Parallel()

		var received []byte
		r := newValidatedRouter(t, &received, new([]string))
		readErr := errors.New("read tcp 192.0.2.10:50001->198.51.100.20:51364: i/o timeout")

		rr := serve(t, r, io.MultiReader(strings.NewReader(`{"templateID":`), iotest.ErrReader(readErr)))

		assert.Equal(t, http.StatusBadRequest, rr.Code)
		message := assertJSONError(t, rr, http.StatusBadRequest)
		assert.NotContains(t, message, "192.0.2.10")
		assert.NotContains(t, message, "i/o timeout")
		assert.Nil(t, received)
	})
}
