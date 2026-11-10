package auth

import (
	"context"

	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	internalauthmiddleware "github.com/e2b-dev/infra/packages/auth/pkg/auth/internal/middleware"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
)

var (
	ErrNoAuthHeader      = internalauthmiddleware.ErrNoAuthHeader
	ErrInvalidAuthHeader = internalauthmiddleware.ErrInvalidAuthHeader
)

type Authenticator = internalauthmiddleware.Authenticator

// AuthenticatorConfig describes a header-token security scheme for
// NewAuthenticator.
type AuthenticatorConfig[T any] = internalauthmiddleware.AuthenticatorConfig[T]

// NewAuthenticator builds an Authenticator for a scheme this package does not
// name itself. Services using one of the schemes below want that constructor
// instead — it already carries the right scheme name, header and context key.
func NewAuthenticator[T any](config AuthenticatorConfig[T]) Authenticator {
	return internalauthmiddleware.NewAuthenticator(config)
}

func NewApiKeyAuthenticator(validationFunc func(context.Context, *gin.Context, string) (*types.Team, *APIError)) Authenticator {
	return internalauthmiddleware.NewApiKeyAuthenticator(validationFunc)
}

func NewAuthProviderBearerAuthenticator(validationFunc func(context.Context, *gin.Context, string) (uuid.UUID, *APIError)) Authenticator {
	return internalauthmiddleware.NewAuthProviderBearerAuthenticator(validationFunc)
}

func NewAuthProviderTeamAuthenticator(validationFunc func(context.Context, *gin.Context, string) (*types.Team, *APIError)) Authenticator {
	return internalauthmiddleware.NewAuthProviderTeamAuthenticator(validationFunc)
}

func NewAdminJWTAuthenticator(verifier *JWKSVerifier) Authenticator {
	return internalauthmiddleware.NewAdminJWTAuthenticator(verifier)
}

func NewAdminApiKeyAuthenticator(adminToken string) Authenticator {
	return internalauthmiddleware.NewAdminApiKeyAuthenticator(adminToken)
}

func NewAdminTeamAuthenticator(validationFunc func(context.Context, *gin.Context, string) (*types.Team, *APIError)) Authenticator {
	return internalauthmiddleware.NewAdminTeamAuthenticator(validationFunc)
}

func CreateAuthenticationFunc(authenticators []Authenticator, preAuthHook func(*gin.Context)) openapi3filter.AuthenticationFunc {
	return internalauthmiddleware.CreateAuthenticationFunc(authenticators, preAuthHook)
}
