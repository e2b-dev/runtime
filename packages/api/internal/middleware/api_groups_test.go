package middleware

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/gin-gonic/gin"
	ginmiddleware "github.com/oapi-codegen/gin-middleware"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
)

func TestWithAPIGroupPopulatesGinContextFromMatchedOperation(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name, method, route, path, group string
		authCalls                        int
	}{
		{"list", http.MethodGet, "/sandboxes", "/sandboxes", "list", 1},
		{"sandbox creation", http.MethodPost, "/sandboxes", "/sandboxes", "", 1},
		{"v2 list", http.MethodGet, "/v2/sandboxes", "/v2/sandboxes", "list", 1},
		{"path parameter", http.MethodGet, "/templates/:templateID/tags", "/templates/template-a/tags", "list", 1},
		{"different Gin parameter name", http.MethodGet, "/templates/:template/tags", "/templates/template-a/tags", "list", 1},
		{"encoded path parameter", http.MethodGet, "/templates/:templateID/tags", "/templates/team%2Ftemplate-a/tags", "list", 1},
		{"invalid extension", http.MethodPost, "/invalid", "/invalid", "", 1},
		{"empty extension", http.MethodPost, "/empty", "/empty", "", 1},
		{"client supplied group", http.MethodPost, "/sandboxes", "/sandboxes?api-group=list", "", 1},
		{"public operation", http.MethodGet, "/public", "/public", "", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			spec, err := api.GetSpec()
			require.NoError(t, err)
			spec.Servers = nil
			spec.Security = openapi3.SecurityRequirements{{"ApiKeyAuth": {}}}
			spec.Paths.Set("/invalid", &openapi3.PathItem{
				Post: &openapi3.Operation{Extensions: map[string]any{APIGroupExtension: 42}},
			})
			spec.Paths.Set("/empty", &openapi3.PathItem{
				Post: &openapi3.Operation{Extensions: map[string]any{APIGroupExtension: ""}},
			})
			spec.Paths.Set("/public", &openapi3.PathItem{
				Get: &openapi3.Operation{
					Extensions: map[string]any{APIGroupExtension: "list"},
					Security:   &openapi3.SecurityRequirements{},
				},
			})
			type existingContextKey struct{}
			type authenticatedContextKey struct{}
			ctx := context.WithValue(t.Context(), existingContextKey{}, "preserved")
			request := httptest.NewRequestWithContext(ctx, tc.method, tc.path, nil)
			request.Header.Set("X-API-Group", "list")
			calls, authCalls := 0, 0
			r := gin.New()
			r.UseRawPath = true
			r.Use(ginmiddleware.OapiRequestValidatorWithOptions(spec, &ginmiddleware.Options{
				Options: openapi3filter.Options{
					ExcludeRequestBody: true,
					AuthenticationFunc: WithAPIGroup(func(authCtx context.Context, input *openapi3filter.AuthenticationInput) error {
						authCalls++
						assert.Equal(t, tc.method, input.RequestValidationInput.Route.Method)
						c := ginmiddleware.GetGinContext(authCtx)
						assert.Empty(t, APIGroupFromContext(c))
						c.Set(authenticatedContextKey{}, "authenticated")

						return nil
					}),
				},
			}))
			r.Handle(tc.method, tc.route, func(c *gin.Context) {
				calls++
				assert.Equal(t, tc.group, APIGroupFromContext(c))
				assert.Same(t, request, c.Request)
				assert.Equal(t, "preserved", c.Request.Context().Value(existingContextKey{}))
				assert.Equal(t, ctx.Done(), c.Request.Context().Done())
				if tc.authCalls > 0 {
					assert.Equal(t, "authenticated", c.GetString(authenticatedContextKey{}))
				}
				c.Status(http.StatusNoContent)
			})
			response := httptest.NewRecorder()
			r.ServeHTTP(response, request)
			assert.Equal(t, 1, calls)
			assert.Equal(t, tc.authCalls, authCalls)
			assert.Equal(t, http.StatusNoContent, response.Code)
			assert.Empty(t, response.Header().Get("RateLimit-Limit"))
			assert.Empty(t, response.Header().Get("Retry-After"))
			assert.Nil(t, ctx.Value(apiGroupContextKey{}))
		})
	}
}

func TestWithAPIGroupPreservesAuthenticationFailure(t *testing.T) {
	t.Parallel()

	spec, err := api.GetSpec()
	require.NoError(t, err)
	spec.Servers = nil
	authErr := errors.New("authentication failed")
	authCalls, handlerCalls := 0, 0
	r := gin.New()
	r.Use(ginmiddleware.OapiRequestValidatorWithOptions(spec, &ginmiddleware.Options{
		Options: openapi3filter.Options{
			AuthenticationFunc: WithAPIGroup(func(context.Context, *openapi3filter.AuthenticationInput) error {
				authCalls++

				return authErr
			}),
		},
		ErrorHandler: func(c *gin.Context, message string, _ int) {
			assert.Contains(t, message, authErr.Error())
			assert.Empty(t, APIGroupFromContext(c))
			c.AbortWithStatus(http.StatusUnauthorized)
		},
	}))
	r.GET("/sandboxes", func(c *gin.Context) {
		handlerCalls++
		c.Status(http.StatusNoContent)
	})
	response := httptest.NewRecorder()
	r.ServeHTTP(response, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/sandboxes", nil))
	assert.Equal(t, http.StatusUnauthorized, response.Code)
	assert.Positive(t, authCalls)
	assert.Zero(t, handlerCalls)
}

func TestWithAPIGroupPreservesValidatedRequestBody(t *testing.T) {
	t.Parallel()

	spec, err := api.GetSpec()
	require.NoError(t, err)
	spec.Servers = nil
	spec.Paths.Value("/sandboxes").Post.Extensions = map[string]any{APIGroupExtension: "test-create"}
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/sandboxes", strings.NewReader(`{"templateID":"template-a"}`))
	request.Header.Set("Content-Type", "application/json")
	r := gin.New()
	r.Use(ginmiddleware.OapiRequestValidatorWithOptions(spec, &ginmiddleware.Options{
		Options: openapi3filter.Options{AuthenticationFunc: WithAPIGroup(openapi3filter.NoopAuthenticationFunc)},
	}))
	r.POST("/sandboxes", func(c *gin.Context) {
		assert.Same(t, request, c.Request)
		var body struct {
			TemplateID string `json:"templateID"`
			Timeout    int    `json:"timeout"`
		}
		require.NoError(t, c.ShouldBindJSON(&body))
		assert.Equal(t, "template-a", body.TemplateID)
		assert.Equal(t, 15, body.Timeout)
		assert.Equal(t, "test-create", APIGroupFromContext(c))
		c.Status(http.StatusNoContent)
	})
	response := httptest.NewRecorder()
	r.ServeHTTP(response, request)
	assert.Equal(t, http.StatusNoContent, response.Code)
}

func TestListGroupCoversTeamScopedListOperations(t *testing.T) {
	t.Parallel()

	spec, err := api.GetSpec()
	require.NoError(t, err)
	expected := map[string]bool{
		"/sandboxes": true, "/v2/sandboxes": true, "/sandboxes/metrics": true,
		"/snapshots": true, "/templates": true, "/v2/templates": true,
		"/templates/{templateID}": true, "/templates/{templateID}/tags": true,
		"/api-keys": true, "/volumes": true, "/secrets": true,
	}
	count := 0
	for path, item := range spec.Paths.Map() {
		for method, operation := range item.Operations() {
			if method == http.MethodGet && expected[path] {
				assert.Equal(t, "list", operation.Extensions[APIGroupExtension], "%s %s", method, path)
				require.NotNil(t, operation.Security, path)
				require.NotEmpty(t, *operation.Security, path)
				for _, security := range *operation.Security {
					_, apiKey := security["ApiKeyAuth"]
					_, userTeam := security["AuthProviderTeamAuth"]
					_, adminTeam := security["AdminTeamAuth"]
					assert.True(t, apiKey || userTeam || adminTeam, "list auth must identify a team: %s", path)
				}
				count++
			} else {
				assert.NotContains(t, operation.Extensions, APIGroupExtension, "%s %s", method, path)
			}
		}
	}
	assert.Equal(t, len(expected), count)
}

func TestAPIGroupOperationsDeclareRateLimitResponses(t *testing.T) {
	t.Parallel()

	spec, err := api.GetSpec()
	require.NoError(t, err)
	for path, item := range spec.Paths.Map() {
		for method, operation := range item.Operations() {
			if _, grouped := operation.Extensions[APIGroupExtension]; !grouped {
				continue
			}
			t.Run(method+" "+path, func(t *testing.T) {
				t.Parallel()

				response := operation.Responses.Status(http.StatusTooManyRequests)
				require.NotNil(t, response, "grouped operations can reject requests with HTTP 429")
				require.NotNil(t, response.Value)
				header := response.Value.Headers["Retry-After"]
				require.NotNil(t, header, "missing Retry-After response header")
				require.NotNil(t, header.Value)
				assert.False(t, header.Value.Required)
				require.NoError(t, header.Value.Schema.Value.VisitJSON(float64(0)))
				require.NoError(t, header.Value.Schema.Value.VisitJSON(float64(30)))
				require.Error(t, header.Value.Schema.Value.VisitJSON(float64(-1)))
				require.Error(t, header.Value.Schema.Value.VisitJSON(0.5))
				require.NoError(t, response.Value.Content["application/json"].Schema.Value.VisitJSON(map[string]any{
					"code": float64(http.StatusTooManyRequests), "message": "Rate limit exceeded",
				}))
			})
		}
	}
}
