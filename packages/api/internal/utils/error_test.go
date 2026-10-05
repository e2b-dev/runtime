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
	"github.com/google/uuid"
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

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/sandboxes", nil)

	ErrorHandler(c, message, statusCode)

	var body struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))

	return w.Code, body.Message
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

// TestErrorHandlerOnSecretsRouteIsFixedAndBodyBlind proves the handler answers a
// secrets route with fixed text, records a fixed error, and never reads the
// request body to build one.
func TestErrorHandlerOnSecretsRouteIsFixedAndBodyBlind(t *testing.T) {
	t.Parallel()

	const sentinel = "sentinel-value-DO-NOT-LOG-0000"

	tests := []struct {
		name        string
		statusCode  int
		wantMessage string
	}{
		{name: "validation", statusCode: http.StatusBadRequest, wantMessage: "Invalid secrets request"},
		{name: "authentication", statusCode: http.StatusUnauthorized, wantMessage: "You are not authenticated"},
		{name: "forbidden", statusCode: http.StatusForbidden, wantMessage: "Secrets are not available for this team"},
		{name: "unmatched path", statusCode: http.StatusNotFound, wantMessage: "Not found"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/secrets/"+sentinel+"?metadata="+sentinel,
				strings.NewReader(`{"value":"`+sentinel+`"}`))

			ErrorHandler(c, "request body has an error: "+sentinel, test.statusCode)

			if recorder.Code != test.statusCode {
				t.Fatalf("got status %d, want %d", recorder.Code, test.statusCode)
			}

			if strings.Contains(recorder.Body.String(), sentinel) {
				t.Fatal("the response echoed confidential request material")
			}

			if !strings.Contains(recorder.Body.String(), test.wantMessage) {
				t.Fatalf("the response does not carry the fixed message %q", test.wantMessage)
			}

			for _, ginErr := range c.Errors {
				if strings.Contains(ginErr.Error(), sentinel) {
					t.Fatal("a gin error carried confidential request material")
				}
			}

			// The body was never consumed, so a handler could still read it.
			body, err := io.ReadAll(c.Request.Body)
			if err != nil {
				t.Fatalf("reading the request body: %v", err)
			}

			if len(body) == 0 {
				t.Fatal("the error handler consumed the request body")
			}
		})
	}
}

// The webhook create and update bodies carry the caller's signing secret,
// which is enough to forge a delivery for that team. A request these routes
// reject before the handler must not put that body in the response, the access
// log or the span — while keeping the validation message the spec documents.
func TestErrorHandlerOnWebhookRoutesIsBodyBlind(t *testing.T) {
	t.Parallel()

	const secret = "whsec-DO-NOT-LOG-0000"

	for _, test := range []struct {
		name   string
		method string
		target string
	}{
		{name: "create", method: http.MethodPost, target: "/events/webhooks"},
		{name: "update", method: http.MethodPatch, target: "/events/webhooks/" + uuid.NewString()},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequestWithContext(t.Context(), test.method, test.target,
				strings.NewReader(`{"name":"alerts","url":"nope","signatureSecret":"`+secret+`"}`))

			ErrorHandler(c, `request body has an error: doesn't match schema: Error at "/url"`, http.StatusBadRequest)

			assert.Equal(t, http.StatusBadRequest, recorder.Code)
			assert.NotContains(t, recorder.Body.String(), secret, "the response echoed the signing secret")

			// The caller still learns what was refused.
			assert.Contains(t, recorder.Body.String(), `Error at \"/url\"`)

			for _, ginErr := range c.Errors {
				assert.NotContains(t, ginErr.Error(), secret, "a gin error carried the signing secret")
			}

			// The body was never consumed, so a handler could still read it.
			body, err := io.ReadAll(c.Request.Body)
			require.NoError(t, err)
			assert.NotEmpty(t, body)
		})
	}
}

// A read route under the same prefix carries no secret, but the rule keys off
// the path family rather than the method, so it is covered too.
func TestErrorHandlerOnWebhookReadRoutesIsBodyBlind(t *testing.T) {
	t.Parallel()

	const sentinel = "sentinel-DO-NOT-LOG"

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequestWithContext(t.Context(), http.MethodGet,
		"/events/webhooks/x/deliveries", strings.NewReader(sentinel))

	ErrorHandler(c, "request has an error: limit", http.StatusBadRequest)

	for _, ginErr := range c.Errors {
		assert.NotContains(t, ginErr.Error(), sentinel)
	}
}

// Every other route keeps reporting the body, which is what makes a rejected
// request diagnosable.
func TestErrorHandlerStillRecordsTheBodyElsewhere(t *testing.T) {
	t.Parallel()

	const marker = "template-marker"

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/sandboxes",
		strings.NewReader(`{"templateID":"`+marker+`"}`))

	ErrorHandler(c, "request body has an error", http.StatusBadRequest)

	require.NotEmpty(t, c.Errors)
	assert.Contains(t, c.Errors[0].Error(), marker)
}
