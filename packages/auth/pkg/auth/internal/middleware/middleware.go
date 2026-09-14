package middleware

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	middleware "github.com/oapi-codegen/gin-middleware"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	"github.com/e2b-dev/infra/packages/auth/pkg/auth/internal/authcontext"
	internalauthteam "github.com/e2b-dev/infra/packages/auth/pkg/auth/internal/team"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth/internal/token"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
	"github.com/e2b-dev/infra/packages/shared/pkg/apierrors"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

const (
	HeaderAPIKey        = "X-API-Key"
	HeaderAuthorization = "Authorization"
	HeaderTeamID        = "X-Team-ID"
	HeaderAdminToken    = "X-Admin-Token"
	PrefixAPIKey        = "e2b_"
	PrefixBearer        = "Bearer "
)

type APIError = apierrors.APIError

var tracer = otel.Tracer("github.com/e2b-dev/infra/packages/auth/pkg/auth/internal/middleware")

var (
	ErrNoAuthHeader      = errors.New("authorization header is missing")
	ErrInvalidAuthHeader = errors.New("authorization header is malformed")
	ErrMalformedAPIKey   = fmt.Errorf("API key is malformed: expected the %q prefix, visit https://docs.e2b.dev/api-key for more information", PrefixAPIKey)
)

// headerKey describes how to extract an authentication token from an HTTP request header.
type headerKey struct {
	name         string
	prefix       string
	removePrefix string

	// malformedError, when set, is returned instead of ErrInvalidAuthHeader
	// when the token is present but lacks prefix. Only set it for a scheme
	// that owns its header exclusively: a shared header must keep the
	// generic error so another scheme can claim the token.
	malformedError error
}

// Authenticator is implemented by types that can authenticate requests against a security scheme.
type Authenticator interface {
	Authenticate(ctx context.Context, ginCtx *gin.Context, input *openapi3filter.AuthenticationInput) error
	SecuritySchemeName() string
}

// commonAuthenticator implements Authenticator using a header-based token with a pluggable validation function.
type commonAuthenticator[T any] struct {
	schemeName     string
	header         headerKey
	validationFunc func(ctx context.Context, ginCtx *gin.Context, token string) (T, *APIError)
	setContextFunc func(ginCtx *gin.Context, value T)
	errorMessage   string
}

const authFailureStatusContextKey = "e2b.auth.middleware.failure_status"

// getHeaderKeysFromRequest extracts the token from the request header.
func (a *commonAuthenticator[T]) getHeaderKeysFromRequest(req *http.Request) (string, error) {
	key := req.Header.Get(a.header.name)
	if key == "" {
		return "", ErrNoAuthHeader
	}

	if a.header.removePrefix != "" {
		key = strings.TrimSpace(strings.TrimPrefix(key, a.header.removePrefix))
	}

	if a.header.prefix != "" && !strings.HasPrefix(key, a.header.prefix) {
		if a.header.malformedError != nil {
			return "", a.header.malformedError
		}

		return "", ErrInvalidAuthHeader
	}

	return key, nil
}

// Authenticate validates the request against the security scheme.
func (a *commonAuthenticator[T]) Authenticate(ctx context.Context, ginCtx *gin.Context, input *openapi3filter.AuthenticationInput) error {
	key, err := a.getHeaderKeysFromRequest(input.RequestValidationInput.Request)
	if err != nil {
		telemetry.ReportEvent(ctx, "auth scheme skipped",
			attribute.String("auth.scheme", a.schemeName),
			attribute.String("auth.reason", err.Error()),
		)

		// Gin's default 404 is not an authentication failure. This key records
		// errors returned by common authenticators across security alternatives.
		if _, hasAuthenticationFailure := ginCtx.Get(authFailureStatusContextKey); !hasAuthenticationFailure {
			ginCtx.Status(http.StatusUnauthorized)
			ginCtx.Set(authFailureStatusContextKey, struct{}{})
		}

		return err
	}

	telemetry.ReportEvent(ctx, "api key extracted")

	result, validationError := a.validationFunc(ctx, ginCtx, key)
	if validationError != nil {
		telemetry.ReportError(ctx,
			"validation error",
			validationError.Err,
			attribute.String("error.message", a.errorMessage),
			attribute.Int("http.status_code", validationError.Code),
			attribute.String("http.status_text", http.StatusText(validationError.Code)),
		)

		ginCtx.Status(validationError.Code)
		ginCtx.Set(authFailureStatusContextKey, struct{}{})

		if _, ok := errors.AsType[*internalauthteam.ForbiddenError](validationError.Err); ok {
			return validationError.Err
		}

		return fmt.Errorf("%s\n%s", a.errorMessage, validationError.ClientMsg)
	}

	telemetry.ReportEvent(ctx, "api key validated")

	if a.setContextFunc != nil {
		a.setContextFunc(ginCtx, result)
	}

	return nil
}

// SecuritySchemeName returns the name of the security scheme this authenticator handles.
func (a *commonAuthenticator[T]) SecuritySchemeName() string {
	return a.schemeName
}

func adminValidationFunction(adminToken string) func(ctx context.Context, ginCtx *gin.Context, token string) (struct{}, *APIError) {
	return func(_ context.Context, _ *gin.Context, token string) (struct{}, *APIError) {
		if subtle.ConstantTimeCompare([]byte(token), []byte(adminToken)) != 1 {
			return struct{}{}, &APIError{
				Code:      http.StatusUnauthorized,
				Err:       errors.New("invalid access token"),
				ClientMsg: "Invalid Access token.",
			}
		}

		return struct{}{}, nil
	}
}

// AuthenticatorConfig describes a header-token security scheme.
//
// The constructors below cover the schemes this package defines, each with
// its scheme name and context key fixed. This exists for a service that
// defines its own — one that verifies a token under a scheme of its own name,
// or records something other than a user or a team.
//
// Without it such a service reimplements the header handling and the 401
// stamping below, which is how a scheme ends up subtly different from every
// other one rather than merely differently named.
type AuthenticatorConfig[T any] struct {
	// SchemeName must match the securityScheme in the service's OpenAPI
	// document; the validator dispatches on it.
	SchemeName string

	// Header is the header carrying the token.
	Header string

	// RequiredPrefix, when set, is a prefix the token must carry for this
	// scheme to apply. A token without it is left to another authenticator.
	RequiredPrefix string

	// StrippedPrefix is removed before validation, e.g. "Bearer ".
	StrippedPrefix string

	// Validate turns a token into whatever the scheme establishes, or an
	// APIError carrying the status the caller should see.
	Validate func(ctx context.Context, ginCtx *gin.Context, token string) (T, *APIError)

	// SetContext records the result for handlers. Optional: a scheme that
	// only proves the caller may proceed has nothing to record.
	SetContext func(ginCtx *gin.Context, value T)

	// ErrorMessage prefixes the failure returned to the validator.
	ErrorMessage string
}

// NewAuthenticator builds an Authenticator for a scheme this package does not
// name itself.
func NewAuthenticator[T any](config AuthenticatorConfig[T]) Authenticator {
	return &commonAuthenticator[T]{
		schemeName: config.SchemeName,
		header: headerKey{
			name:         config.Header,
			prefix:       config.RequiredPrefix,
			removePrefix: config.StrippedPrefix,
		},
		validationFunc: config.Validate,
		setContextFunc: config.SetContext,
		errorMessage:   config.ErrorMessage,
	}
}

// NewApiKeyAuthenticator creates an authenticator for the ApiKeyAuth security scheme (X-API-Key header, e2b_ prefix).
func NewApiKeyAuthenticator(validationFunc func(ctx context.Context, ginCtx *gin.Context, token string) (*types.Team, *APIError)) Authenticator {
	return &commonAuthenticator[*types.Team]{
		schemeName: "ApiKeyAuth",
		header: headerKey{
			name:           HeaderAPIKey,
			prefix:         PrefixAPIKey,
			malformedError: ErrMalformedAPIKey,
		},
		validationFunc: validationFunc,
		setContextFunc: authcontext.SetTeamInfo,
		errorMessage:   "Invalid API key, please visit https://docs.e2b.dev/api-key for more information.",
	}
}

// NewAuthProviderBearerAuthenticator creates an authenticator for AuthProviderBearerAuth (Authorization Bearer token).
func NewAuthProviderBearerAuthenticator(validationFunc func(ctx context.Context, ginCtx *gin.Context, token string) (uuid.UUID, *APIError)) Authenticator {
	return &commonAuthenticator[uuid.UUID]{
		schemeName: "AuthProviderBearerAuth",
		header: headerKey{
			name:         HeaderAuthorization,
			removePrefix: PrefixBearer,
		},
		validationFunc: validationFunc,
		setContextFunc: authcontext.SetUserID,
		errorMessage:   "Invalid auth provider token.",
	}
}

// NewAuthProviderTeamAuthenticator creates an authenticator for the AuthProviderTeamAuth security scheme (X-Team-Id header).
func NewAuthProviderTeamAuthenticator(validationFunc func(ctx context.Context, ginCtx *gin.Context, token string) (*types.Team, *APIError)) Authenticator {
	return &commonAuthenticator[*types.Team]{
		schemeName: "AuthProviderTeamAuth",
		header: headerKey{
			name: HeaderTeamID,
		},
		validationFunc: validationFunc,
		setContextFunc: authcontext.SetTeamInfo,
		errorMessage:   "Invalid auth provider token teamID.",
	}
}

// NewAdminJWTAuthenticator creates an authenticator for the AdminJWTAuth security scheme.
func NewAdminJWTAuthenticator(verifier *token.JWKSVerifier) Authenticator {
	return &commonAuthenticator[string]{
		schemeName: "AdminJWTAuth",
		header: headerKey{
			name:         HeaderAuthorization,
			removePrefix: PrefixBearer,
		},
		validationFunc: func(ctx context.Context, _ *gin.Context, token string) (string, *APIError) {
			claims, err := verifier.Verify(ctx, token)
			if err != nil {
				return "", &APIError{Code: http.StatusUnauthorized, Err: err, ClientMsg: "Invalid service token."}
			}
			issuer, err := claims.GetIssuer()
			if err != nil {
				return "", &APIError{Code: http.StatusUnauthorized, Err: err, ClientMsg: "Invalid service token."}
			}

			return issuer, nil
		},
		setContextFunc: authcontext.SetServiceIssuer,
		errorMessage:   "Invalid service token.",
	}
}

// NewAdminApiKeyAuthenticator creates an authenticator for the AdminApiKeyAuth security scheme (X-Admin-Token header).
func NewAdminApiKeyAuthenticator(adminToken string) Authenticator {
	return newAdminApiKeyAuthenticator("AdminApiKeyAuth", adminToken)
}

func newAdminApiKeyAuthenticator(schemeName, adminToken string) Authenticator {
	return &commonAuthenticator[struct{}]{
		schemeName: schemeName,
		header: headerKey{
			name: HeaderAdminToken,
		},
		validationFunc: adminValidationFunction(adminToken),
		errorMessage:   "Invalid Access token.",
	}
}

// NewAdminTeamAuthenticator creates an authenticator for AdminTeamAuth (X-Team-ID header).
func NewAdminTeamAuthenticator(validationFunc func(ctx context.Context, ginCtx *gin.Context, teamID string) (*types.Team, *APIError)) Authenticator {
	return newAdminTeamAuthenticator("AdminTeamAuth", validationFunc)
}

func newAdminTeamAuthenticator(
	schemeName string,
	validationFunc func(ctx context.Context, ginCtx *gin.Context, teamID string) (*types.Team, *APIError),
) Authenticator {
	return &commonAuthenticator[*types.Team]{
		schemeName: schemeName,
		header: headerKey{
			name: HeaderTeamID,
		},
		validationFunc: validationFunc,
		setContextFunc: authcontext.SetTeamInfo,
		errorMessage:   "Invalid admin token teamID.",
	}
}

// CreateAuthenticationFunc creates an OpenAPI authentication function from a list of authenticators.
func CreateAuthenticationFunc(
	authenticators []Authenticator,
	preAuthHook func(*gin.Context),
) openapi3filter.AuthenticationFunc {
	return func(ctx context.Context, input *openapi3filter.AuthenticationInput) error {
		ginCtx := middleware.GetGinContext(ctx)

		if preAuthHook != nil {
			preAuthHook(ginCtx)
		}

		ctx, span := tracer.Start(ginCtx.Request.Context(), "authenticate")
		defer span.End()

		for _, validator := range authenticators {
			if input.SecuritySchemeName == validator.SecuritySchemeName() {
				//nolint:contextcheck // We use the gin request context here by design.
				return validator.Authenticate(ctx, ginCtx, input)
			}
		}

		return fmt.Errorf("invalid security scheme name '%s'", input.SecuritySchemeName)
	}
}
