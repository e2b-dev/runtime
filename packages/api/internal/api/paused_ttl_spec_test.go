package api

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/gin-gonic/gin"
	middleware "github.com/oapi-codegen/gin-middleware"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/ginutils"
)

// describePausedTTL names the TTL a handler receives.
func describePausedTTL(ttl *SandboxPausedRetentionSeconds) string {
	if ttl == nil {
		return "none"
	}

	return fmt.Sprint(*ttl)
}

func pausedTTLRoute[B any](router *gin.Engine, method, path string, ttl func(B) *SandboxPausedRetentionSeconds) {
	router.Handle(method, path, func(c *gin.Context) {
		body, err := ginutils.ParseBody[B](c.Request.Context(), c)
		if err != nil {
			return
		}
		c.String(http.StatusOK, describePausedTTL(ttl(body)))
	})
}

func TestPausedTTLRequestContract(t *testing.T) {
	t.Parallel()

	spec, err := GetSpec()
	require.NoError(t, err)
	spec.Servers = nil

	router := gin.New()
	router.Use(middleware.OapiRequestValidatorWithOptions(spec, &middleware.Options{
		Options: openapi3filter.Options{
			AuthenticationFunc: openapi3filter.NoopAuthenticationFunc,
			MultiError:         true,
		},
	}))
	pausedTTLRoute(router, http.MethodPost, "/v2/sandboxes", func(b NewSandboxV2) *SandboxPausedRetentionSeconds { return b.PausedRetentionSeconds })
	pausedTTLRoute(router, http.MethodPut, "/sandboxes/:sandboxID/paused-retention", func(b SandboxPausedRetentionRequest) *SandboxPausedRetentionSeconds { return b.PausedRetentionSeconds })

	type route struct {
		method, path, required string
		// omitted is the answer to a body without the field; empty for a 400.
		omitted string
	}
	routes := []route{
		{method: http.MethodPost, path: "/v2/sandboxes", required: `"templateID":"base"`, omitted: "none"},
		{method: http.MethodPut, path: "/sandboxes/abc/paused-retention"},
	}

	cases := []struct {
		field string
		want  string
	}{
		{field: `"pausedRetentionSeconds":null`, want: "none"},
		{field: `"pausedRetentionSeconds":3600`, want: "3600"},
		{field: `"pausedRetentionSeconds":3153600000`, want: "3153600000"},
		{field: `"pausedRetentionSeconds":3599`},
		{field: `"pausedRetentionSeconds":3153600001`},
		{field: `"pausedRetentionSeconds":0`},
		{field: `"pausedRetentionSeconds":"3600"`},
	}

	for _, r := range routes {
		for _, tc := range append(cases, struct{ field, want string }{want: r.omitted}) {
			fields := []string{}
			for _, f := range []string{r.required, tc.field} {
				if f != "" {
					fields = append(fields, f)
				}
			}
			body := "{" + strings.Join(fields, ",") + "}"

			t.Run(r.method+" "+r.path+" "+body, func(t *testing.T) {
				t.Parallel()

				req := httptest.NewRequestWithContext(t.Context(), r.method, r.path, strings.NewReader(body))
				req.Header.Set("Content-Type", "application/json")
				response := httptest.NewRecorder()
				router.ServeHTTP(response, req)

				if tc.want == "" {
					require.Equal(t, http.StatusBadRequest, response.Code, response.Body.String())

					return
				}
				require.Equal(t, http.StatusOK, response.Code, response.Body.String())
				require.Equal(t, tc.want, response.Body.String())
			})
		}
	}
}
