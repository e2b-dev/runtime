package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	middleware "github.com/oapi-codegen/gin-middleware"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/utils"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
)

// A webhook create or update body carries the caller's signing secret, which
// is enough to forge a delivery for that team. This drives the real spec and
// the real validator, because what a rejected request reveals is decided by
// the layers together: the validator's message, the multi-error selection, and
// the error handler. The errors it records are checked alongside the response,
// since those are what reach the access log and the span.
func TestRejectedWebhookRequestNeverRevealsTheSigningSecret(t *testing.T) {
	t.Parallel()

	const secret = "whsec-DO-NOT-LOG-0000"

	for _, test := range []struct {
		name   string
		method string
		target string
		body   string
	}{
		{
			name: "create missing every other required field",
			// The failure is at the root, so the validator reports the object
			// it refused rather than one field of it.
			method: http.MethodPost, target: "/events/webhooks",
			body: `{"signatureSecret":"` + secret + `"}`,
		},
		{
			name:   "create with a malformed field",
			method: http.MethodPost, target: "/events/webhooks",
			body: `{"name":"alerts","url":"https://example.com","events":"not-a-list","signatureSecret":"` + secret + `"}`,
		},
		{
			name:   "update with a malformed field",
			method: http.MethodPatch, target: "/events/webhooks/" + uuid.NewString(),
			body: `{"enabled":"yes","signatureSecret":"` + secret + `"}`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			router, recorded := newValidatingRouter(t)

			request := httptest.NewRequestWithContext(t.Context(), test.method, test.target, strings.NewReader(test.body))
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("X-API-Key", "e2b_key")

			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, request)

			require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
			assert.NotContains(t, recorder.Body.String(), secret, "the response echoed the signing secret")

			require.NotEmpty(t, *recorded, "the rejection was not recorded at all")
			for _, err := range *recorded {
				assert.NotContains(t, err.Error(), secret, "a recorded error carried the signing secret")
			}
		})
	}
}

// newValidatingRouter serves the spec's own request validation, authenticating
// every caller, and returns the errors each request recorded. The webhook and
// template build routes are registered so a rejected request is answered by the
// validator rather than by gin's own not-found.
func newValidatingRouter(t *testing.T) (*gin.Engine, *[]error) {
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

	recorded := &[]error{}

	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Next()

		for _, ginErr := range c.Errors {
			*recorded = append(*recorded, ginErr)
		}
	})
	router.Use(middleware.OapiRequestValidatorWithOptions(swagger, &middleware.Options{
		ErrorHandler: func(c *gin.Context, message string, fallbackStatusCode int) {
			utils.ErrorHandler(c, message, max(c.Writer.Status(), fallbackStatusCode))
		},
		MultiErrorHandler: utils.MultiErrorHandler,
		Options: openapi3filter.Options{
			AuthenticationFunc: authenticationFunc,
			MultiError:         true,
		},
		SilenceServersWarning: true,
	}))

	reached := func(c *gin.Context) { c.Status(http.StatusOK) }
	router.POST("/events/webhooks", reached)
	router.PATCH("/events/webhooks/:webhookID", reached)
	router.POST("/v2/templates/:templateID/builds/:buildID", reached)

	return router, recorded
}
