package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/gin-gonic/gin"
	middleware "github.com/oapi-codegen/gin-middleware"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
)

// The schema owns "a given source is non-empty"; "exactly one source" stays
// with the handler, because OpenAPI 3.0 cannot require one of two properties
// without turning the generated type into a union.
func TestTemplateBuildStartV2SourceValidation(t *testing.T) {
	t.Parallel()

	swagger, err := api.GetSpec()
	require.NoError(t, err)
	swagger.Servers = nil

	r := gin.New()
	r.Use(middleware.OapiRequestValidatorWithOptions(swagger, &middleware.Options{
		Options: openapi3filter.Options{
			AuthenticationFunc: openapi3filter.NoopAuthenticationFunc,
			MultiError:         true,
		},
	}))
	r.POST("/v2/templates/:templateID/builds/:buildID", func(c *gin.Context) { c.Status(http.StatusAccepted) })

	for _, tt := range []struct {
		name string
		body string
		code int
	}{
		{"empty fromImage", `{"fromImage":""}`, http.StatusBadRequest},
		{"empty fromTemplate", `{"fromTemplate":""}`, http.StatusBadRequest},
		{"both sources, one empty", `{"fromImage":"alpine","fromTemplate":""}`, http.StatusBadRequest},
		{"no source", `{}`, http.StatusAccepted},
		{"both sources", `{"fromImage":"alpine","fromTemplate":"base"}`, http.StatusAccepted},
		{"fromImage", `{"fromImage":"alpine"}`, http.StatusAccepted},
		{"fromTemplate", `{"fromTemplate":"base"}`, http.StatusAccepted},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v2/templates/tpl/builds/00000000-0000-0000-0000-000000000000", strings.NewReader(tt.body))
			req.Header.Set("Content-Type", "application/json")
			rr := httptest.NewRecorder()
			r.ServeHTTP(rr, req)

			require.Equal(t, tt.code, rr.Code, rr.Body.String())
			if tt.code == http.StatusBadRequest {
				require.Contains(t, rr.Body.String(), "TemplateBuildStartV2")
				require.Contains(t, rr.Body.String(), "minimum string length is 1")
			}
		})
	}
}
