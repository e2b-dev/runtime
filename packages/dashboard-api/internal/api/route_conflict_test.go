package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStaticAndParamSiblingsCoexist guards against gin's router rejecting or
// misrouting the sibling pair POST /admin/users/bootstrap and
// POST /admin/users/:userId/bootstrap. The pair was flagged as a potential
// httprouter conflict; this verifies gin handles it correctly.
func TestStaticAndParamSiblingsCoexist(t *testing.T) {
	t.Parallel()

	r := gin.New()

	require.NotPanics(t, func() {
		r.POST("/admin/users/bootstrap", func(c *gin.Context) {
			c.String(http.StatusOK, "static")
		})
		r.POST("/admin/users/:userId/bootstrap", func(c *gin.Context) {
			c.String(http.StatusOK, "param:"+c.Param("userId"))
		})
	}, "gin must accept sibling static and parameter segments at the same level")

	cases := []struct {
		path string
		want string
	}{
		{"/admin/users/bootstrap", "static"},
		{"/admin/users/abc-123/bootstrap", "param:abc-123"},
	}

	for _, tc := range cases {
		t.Run(tc.path, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequestWithContext(
				t.Context(),
				http.MethodPost,
				tc.path,
				nil,
			)
			rec := httptest.NewRecorder()
			r.ServeHTTP(rec, req)

			assert.Equal(t, http.StatusOK, rec.Code)
			assert.Equal(t, tc.want, rec.Body.String())
		})
	}
}
