package permissions

import (
	"net/http"
	"net/http/httptest"
	"os/user"
	"testing"

	"connectrpc.com/authn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAuthenticateUsernameMiddleware(t *testing.T) {
	t.Parallel()

	current, err := user.Current()
	require.NoError(t, err)

	for _, tt := range []struct {
		name     string
		username string
		want     *user.User
	}{
		{name: "without credentials"},
		{name: "with credentials", username: current.Username, want: current},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var info any
			handler := authn.NewMiddleware(AuthenticateUsername).Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				info = authn.GetInfo(r.Context())
				w.WriteHeader(http.StatusNoContent)
			}))
			request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/", nil)
			if tt.username != "" {
				request.SetBasicAuth(tt.username, "")
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)

			require.Equal(t, http.StatusNoContent, response.Code)
			if tt.want == nil {
				assert.Nil(t, info)
			} else {
				got, ok := info.(*user.User)
				require.True(t, ok)
				assert.Equal(t, tt.want.Uid, got.Uid)
				assert.Equal(t, tt.want.Username, got.Username)
			}
		})
	}
}
