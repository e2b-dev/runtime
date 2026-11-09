package utils

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sharedauth "github.com/e2b-dev/infra/packages/auth/pkg/auth"
)

func TestMultiErrorHandlerSecuritySelection(t *testing.T) {
	t.Parallel()

	missingHeader := fmt.Errorf("Invalid API key. %w", sharedauth.ErrNoAuthHeader)
	forbidden := &sharedauth.TeamForbiddenError{Message: "team is banned"}

	testCases := map[string]struct {
		errs []error
		want string
	}{
		"forbidden after missing header": {
			errs: []error{missingHeader, forbidden},
			want: sharedauth.ForbiddenErrPrefix + "team is banned",
		},
		"forbidden behind an attempted invalid credential": {
			errs: []error{errors.New("invalid key format"), forbidden},
			want: sharedauth.ForbiddenErrPrefix + "team is banned",
		},
		"all missing headers falls back to the first": {
			errs: []error{missingHeader, missingHeader},
			want: sharedauth.SecurityErrPrefix + missingHeader.Error(),
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got := MultiErrorHandler(openapi3.MultiError{
				&openapi3filter.SecurityRequirementsError{Errors: tc.errs},
			})
			assert.Equal(t, tc.want, got.Error())
		})
	}
}

// runErrorHandler feeds a MultiErrorHandler result through ErrorHandler the
// way the request-validator middleware does.
func runErrorHandler(t *testing.T, message string, statusCode int) (int, string) {
	t.Helper()

	result := refuse(t, http.MethodGet, "/sandboxes", "/sandboxes", "", message, statusCode)

	var body struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}
	require.NoError(t, json.Unmarshal(result.actualResponse.Body.Bytes(), &body))

	return result.actualResponse.Code, body.Message
}

func TestErrorHandlerSecurityStatusMapping(t *testing.T) {
	t.Parallel()

	forbidden := &sharedauth.TeamForbiddenError{Message: "team is banned"}

	testCases := map[string]struct {
		errs        []error
		wantCode    int
		wantMessage string
	}{
		"forbidden team maps to 403": {
			errs:        []error{fmt.Errorf("Invalid API key. %w", sharedauth.ErrNoAuthHeader), forbidden},
			wantCode:    http.StatusForbidden,
			wantMessage: "team is banned",
		},
		"forbidden behind an attempted invalid credential maps to 403": {
			errs:        []error{errors.New("invalid key format"), forbidden},
			wantCode:    http.StatusForbidden,
			wantMessage: "team is banned",
		},
		"attempted scheme keeps the middleware status": {
			errs:        []error{errors.New("invalid key format")},
			wantCode:    http.StatusUnauthorized,
			wantMessage: "invalid key format",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			selected := MultiErrorHandler(openapi3.MultiError{
				&openapi3filter.SecurityRequirementsError{Errors: tc.errs},
			})

			code, message := runErrorHandler(t, selected.Error(), http.StatusUnauthorized)
			assert.Equal(t, tc.wantCode, code)
			assert.Equal(t, tc.wantMessage, message)
		})
	}
}

// sentinel stands for a value a client sent that must never be returned or
// recorded.
const sentinel = "sentinel-value-DO-NOT-LOG-0000"

// refusal is what a request refused before its handler left behind.
type refusal struct {
	actualResponse *httptest.ResponseRecorder
	// recorded is every error left on the gin context, which the logging and
	// tracing middleware persist.
	recorded string
	// unread is the request body still readable after the refusal.
	unread []byte
}

// leaked reports whether the sentinel reached the response or the record.
func (r refusal) leaked() bool {
	return strings.Contains(r.actualResponse.Body.String(), sentinel) || strings.Contains(r.recorded, sentinel)
}

// refuse serves one request to route and refuses it with ErrorHandler.
func refuse(t *testing.T, method, route, target, body, message string, statusCode int) refusal {
	t.Helper()

	var result refusal
	engine := gin.New()
	engine.Handle(method, route, func(c *gin.Context) {
		ErrorHandler(c, message, statusCode)

		result.recorded = c.Errors.String()
		result.unread, _ = io.ReadAll(c.Request.Body)
	})

	result.actualResponse = httptest.NewRecorder()
	engine.ServeHTTP(result.actualResponse, httptest.NewRequestWithContext(t.Context(), method, target, strings.NewReader(body)))

	return result
}

func Test_ErrorHandler(t *testing.T) {
	t.Parallel()

	refusals := map[string]struct {
		message    string
		statusCode int
		wantCode   int
	}{
		"a validation error": {message: "request body has an error", statusCode: http.StatusBadRequest, wantCode: http.StatusBadRequest},
		"an unknown key":     {message: sharedauth.SecurityErrPrefix + "Invalid API key", statusCode: http.StatusUnauthorized, wantCode: http.StatusUnauthorized},
		"a forbidden team":   {message: sharedauth.ForbiddenErrPrefix + "team is banned", statusCode: http.StatusUnauthorized, wantCode: http.StatusForbidden},
	}
	for name, tc := range refusals {
		t.Run("should neither read nor record the body of a request refused for "+name, func(t *testing.T) {
			t.Parallel()

			// Act
			result := refuse(t, http.MethodPost, "/sandboxes", "/sandboxes",
				`{"templateID":"base","envVars":{"TOKEN":"`+sentinel+`"}}`, tc.message, tc.statusCode)

			// Assert
			assert.Equal(t, tc.wantCode, result.actualResponse.Code)
			assert.False(t, result.leaked(), "the body reached the response or the record")
			assert.Contains(t, string(result.unread), sentinel, "the error handler consumed the request body")
		})
	}

	t.Run("should return and record the validator's message", func(t *testing.T) {
		t.Parallel()

		// Act
		result := refuse(t, http.MethodPost, "/sandboxes", "/sandboxes", `{}`,
			`request body has an error: Error at "/templateID": property is missing`, http.StatusBadRequest)

		// Assert
		assert.Equal(t, http.StatusBadRequest, result.actualResponse.Code)
		assert.Contains(t, result.actualResponse.Body.String(), `Error at \"/templateID\"`)
		assert.Contains(t, result.recorded, `Error at "/templateID"`)
	})
}
