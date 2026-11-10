package apierrors

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/shared/pkg/apierrors/httperror"
)

// Clients read the status, code, message and, when set, error_code; the
// message is also recorded on the gin context for the access log.
func TestSendAPIErrorAnswersInTheAPIErrorShape(t *testing.T) {
	t.Parallel()

	testCases := map[string]struct {
		apiErr   *APIError
		wantBody string
	}{
		"message only": {
			apiErr:   &APIError{Code: http.StatusNotFound, ClientMsg: "template 'base' not found"},
			wantBody: `{"code":404,"message":"template 'base' not found"}`,
		},
		"with error code": {
			apiErr:   &APIError{Code: http.StatusServiceUnavailable, ClientMsg: "no capacity", ErrorCode: "sandbox_capacity_unavailable"},
			wantBody: `{"code":503,"error_code":"sandbox_capacity_unavailable","message":"no capacity"}`,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)

			SendAPIError(c, tc.apiErr)

			assert.Equal(t, tc.apiErr.Code, recorder.Code)
			assert.Equal(t, httperror.ContentType, recorder.Header().Get("Content-Type"))
			assert.JSONEq(t, tc.wantBody, recorder.Body.String())
			require.Len(t, c.Errors, 1)
			assert.Equal(t, tc.apiErr.ClientMsg, c.Errors[0].Error())
		})
	}
}

func TestSendAPIErrorSetsShouldRetryFromTheError(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		retryable Retryability
		existing  string
		want      string
	}{
		"undefined leaves absent header unset": {retryable: UndefinedRetry},
		"undefined preserves true":             {retryable: UndefinedRetry, existing: "true", want: "true"},
		"undefined preserves false":            {retryable: UndefinedRetry, existing: "false", want: "false"},
		"no retry":                             {retryable: NoRetry, existing: "true", want: "false"},
		"can retry":                            {retryable: CanRetry, existing: "false", want: "true"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			if tc.existing != "" {
				c.Header(ShouldRetryHeader, tc.existing)
			}

			SendAPIError(c, &APIError{Code: http.StatusServiceUnavailable, ClientMsg: "unavailable", Retryable: tc.retryable})

			header := recorder.Result().Header
			_, present := header[ShouldRetryHeader]
			assert.Equal(t, tc.want != "", present)
			assert.Equal(t, tc.want, header.Get(ShouldRetryHeader))
		})
	}
}
