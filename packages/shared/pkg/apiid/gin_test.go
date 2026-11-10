package apiid_test

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/apiid"
)

func TestProjectIDGinBinding(t *testing.T) {
	t.Parallel()

	internal := uuid.MustParse("f47ac10b-58cc-4372-a567-0e02b2c3d479")
	testGinIDBinding(t, apiid.NewProjectID(internal), internal.String(),
		"prj_7mfb0gpp6c8dsaasre0asc7n3s", "wrk_7mfb0gpp6c8dsaasre0asc7n3s")
}

func TestWorkspaceIDGinBinding(t *testing.T) {
	t.Parallel()

	internal := uuid.MustParse("f47ac10b-58cc-4372-a567-0e02b2c3d479")
	testGinIDBinding(t, apiid.NewWorkspaceID(internal), internal.String(),
		"wrk_7mfb0gpp6c8dsaasre0asc7n3s", "prj_7mfb0gpp6c8dsaasre0asc7n3s")
}

func testGinIDBinding[T comparable](t *testing.T, want T, internal, public, wrongKind string) {
	t.Helper()

	for _, location := range []string{"uri", "query", "header"} {
		t.Run(location, func(t *testing.T) {
			t.Parallel()

			for _, tc := range []struct {
				name  string
				input string
				valid bool
			}{
				{name: "uuid", input: internal, valid: true},
				{name: "public", input: public, valid: true},
				{name: "wrong_kind", input: wrongKind},
				{name: "garbage", input: "not-an-id"},
				{name: "empty", input: ""},
				{name: "json_string", input: `"` + public + `"`},
			} {
				t.Run(tc.name, func(t *testing.T) {
					t.Parallel()

					c, _ := gin.CreateTestContext(httptest.NewRecorder())
					c.Request = httptest.NewRequestWithContext(t.Context(), http.MethodGet,
						"/?resource_id="+url.QueryEscape(tc.input), nil)
					c.Params = gin.Params{{Key: "resource_id", Value: tc.input}}
					c.Request.Header.Set("X-Resource-ID", tc.input)

					body := struct {
						ResourceID T `form:"resource_id" header:"X-Resource-ID" uri:"resource_id"`
					}{}
					if !tc.valid {
						body.ResourceID = want
					}

					var err error
					switch location {
					case "uri":
						err = c.ShouldBindUri(&body)
					case "query":
						err = c.ShouldBindQuery(&body)
					case "header":
						err = c.ShouldBindHeader(&body)
					}

					if tc.valid {
						require.NoError(t, err)
					} else {
						require.Error(t, err)
					}
					require.Equal(t, want, body.ResourceID)
				})
			}
		})
	}
}
