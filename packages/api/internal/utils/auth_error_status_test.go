package utils

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	middleware "github.com/oapi-codegen/gin-middleware"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
)

func TestAdminJWTAlternativePreservesAdminTeamFailureStatus(t *testing.T) {
	t.Parallel()

	swagger, err := api.GetSpec()
	require.NoError(t, err)
	swagger.Servers = nil

	authenticationFunc := auth.CreateAuthenticationFunc(
		[]auth.Authenticator{
			auth.NewApiKeyAuthenticator(func(context.Context, *gin.Context, string) (*types.Team, *auth.APIError) {
				return nil, nil
			}),
			auth.NewAuthProviderBearerAuthenticator(func(context.Context, *gin.Context, string) (uuid.UUID, *auth.APIError) {
				return uuid.Nil, nil
			}),
			auth.NewAuthProviderTeamAuthenticator(func(context.Context, *gin.Context, string) (*types.Team, *auth.APIError) {
				return nil, nil
			}),
			auth.NewAdminApiKeyAuthenticator("admin-token"),
			auth.NewAdminJWTAuthenticator(nil),
			auth.NewAdminTeamAuthenticator(func(context.Context, *gin.Context, string) (*types.Team, *auth.APIError) {
				return nil, &auth.APIError{
					Code:      http.StatusNotFound,
					Err:       errors.New("team not found"),
					ClientMsg: "Team not found",
				}
			}),
		},
		nil,
	)

	r := gin.New()
	r.Use(middleware.OapiRequestValidatorWithOptions(swagger, &middleware.Options{
		ErrorHandler: func(c *gin.Context, message string, fallbackStatusCode int) {
			statusCode := max(c.Writer.Status(), fallbackStatusCode)
			ErrorHandler(c, message, statusCode)
		},
		MultiErrorHandler: MultiErrorHandler,
		Options: openapi3filter.Options{
			AuthenticationFunc: authenticationFunc,
			MultiError:         true,
		},
		SilenceServersWarning: true,
	}))
	r.GET("/api-keys", func(c *gin.Context) {
		c.Status(http.StatusOK)
	})

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api-keys", nil)
	req.Header.Set(auth.HeaderAdminToken, "admin-token")
	req.Header.Set(auth.HeaderTeamID, "unknown-team")
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusNotFound, rr.Code)
	require.Contains(t, rr.Body.String(), "Team not found")
}
