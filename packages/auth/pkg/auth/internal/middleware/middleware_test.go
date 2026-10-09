package middleware

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/auth/pkg/auth/internal/authcontext"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
	authqueries "github.com/e2b-dev/infra/packages/db/pkg/auth/queries"
)

func TestAdminValidationFunction(t *testing.T) {
	t.Parallel()

	validate := adminValidationFunction("super-secret-token")

	t.Run("accepts matching token", func(t *testing.T) {
		t.Parallel()

		_, err := validate(t.Context(), nil, "super-secret-token")
		require.Nil(t, err)
	})

	t.Run("rejects non-matching token", func(t *testing.T) {
		t.Parallel()

		_, err := validate(t.Context(), nil, "super-secret-tokem")
		require.NotNil(t, err)
		require.Equal(t, 401, err.Code)
	})
}

func TestAdminTeamAuthenticatorSetsTeamContext(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	teamID := uuid.New()
	team := types.NewTeam(&authqueries.Team{ID: teamID}, &authqueries.TeamLimit{})

	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/", nil)
	req.Header.Set(HeaderTeamID, teamID.String())

	ginCtx, _ := gin.CreateTestContext(httptest.NewRecorder())
	authenticator := NewAdminTeamAuthenticator(func(_ context.Context, _ *gin.Context, gotTeamID string) (*types.Team, *APIError) {
		if gotTeamID != teamID.String() {
			return nil, &APIError{
				Err:       ErrInvalidAuthHeader,
				ClientMsg: "Invalid team ID",
				Code:      http.StatusBadRequest,
			}
		}

		return team, nil
	})
	if got, want := authenticator.SecuritySchemeName(), "AdminTeamAuth"; got != want {
		t.Fatalf("NewAdminTeamAuthenticator().SecuritySchemeName() = %q, want %q", got, want)
	}

	err := authenticator.Authenticate(ctx, ginCtx, &openapi3filter.AuthenticationInput{
		RequestValidationInput: &openapi3filter.RequestValidationInput{Request: req},
	})
	if err != nil {
		t.Fatalf("AdminTeamAuth.Authenticate(valid team ID) error: %v", err)
	}

	got, ok := authcontext.GetTeamInfo(ginCtx)
	if !ok {
		t.Fatalf("authcontext.GetTeamInfo(ginCtx) ok = false, want true")
	}

	if got.Team.ID != teamID {
		t.Errorf("authcontext.GetTeamInfo(ginCtx).Team.ID = %s, want %s", got.Team.ID, teamID)
	}
}

// A service defining its own scheme gets the same header handling and the
// same 401 stamping as the named ones, and can record something other than a
// user or a team.
func TestNewAuthenticatorAppliesTheConfiguredScheme(t *testing.T) {
	t.Parallel()

	const subjectKey = "provider_subject"

	authenticator := NewAuthenticator(AuthenticatorConfig[string]{
		SchemeName:     "CustomBearerAuth",
		Header:         HeaderAuthorization,
		StrippedPrefix: PrefixBearer,
		Validate: func(_ context.Context, _ *gin.Context, token string) (string, *APIError) {
			if token != "good-token" {
				return "", &APIError{Err: ErrInvalidAuthHeader, ClientMsg: "nope", Code: http.StatusUnauthorized}
			}

			return "subject-1", nil
		},
		SetContext:   func(c *gin.Context, subject string) { c.Set(subjectKey, subject) },
		ErrorMessage: "Invalid custom token.",
	})

	require.Equal(t, "CustomBearerAuth", authenticator.SecuritySchemeName())

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
	req.Header.Set(HeaderAuthorization, PrefixBearer+"good-token")
	ginCtx, _ := gin.CreateTestContext(httptest.NewRecorder())

	require.NoError(t, authenticator.Authenticate(t.Context(), ginCtx, &openapi3filter.AuthenticationInput{
		RequestValidationInput: &openapi3filter.RequestValidationInput{Request: req},
	}))

	subject, ok := ginCtx.Get(subjectKey)
	require.True(t, ok)
	require.Equal(t, "subject-1", subject)
}

// The prefix is stripped before validation, so a scheme sharing the
// Authorization header does not have to strip it again.
func TestNewAuthenticatorStripsTheConfiguredPrefix(t *testing.T) {
	t.Parallel()

	var seen string
	authenticator := NewAuthenticator(AuthenticatorConfig[struct{}]{
		SchemeName:     "CustomBearerAuth",
		Header:         HeaderAuthorization,
		StrippedPrefix: PrefixBearer,
		Validate: func(_ context.Context, _ *gin.Context, token string) (struct{}, *APIError) {
			seen = token

			return struct{}{}, nil
		},
	})

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
	req.Header.Set(HeaderAuthorization, PrefixBearer+"raw-token")
	ginCtx, _ := gin.CreateTestContext(httptest.NewRecorder())

	require.NoError(t, authenticator.Authenticate(t.Context(), ginCtx, &openapi3filter.AuthenticationInput{
		RequestValidationInput: &openapi3filter.RequestValidationInput{Request: req},
	}))
	require.Equal(t, "raw-token", seen)
}

// A present X-API-Key without the e2b_ prefix returns the API-key-specific
// malformed error so the client sees the expected format, while a missing
// header keeps the generic ErrNoAuthHeader.
func TestApiKeyAuthenticatorReturnsMalformedKeyError(t *testing.T) {
	t.Parallel()

	authenticator := NewApiKeyAuthenticator(func(context.Context, *gin.Context, string) (*types.Team, *APIError) {
		return nil, nil
	})

	t.Run("malformed key", func(t *testing.T) {
		t.Parallel()

		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
		req.Header.Set(HeaderAPIKey, "not-a-standard-key")
		recorder := httptest.NewRecorder()
		ginCtx, _ := gin.CreateTestContext(recorder)

		err := authenticator.Authenticate(t.Context(), ginCtx, &openapi3filter.AuthenticationInput{
			RequestValidationInput: &openapi3filter.RequestValidationInput{Request: req},
		})
		require.ErrorIs(t, err, ErrMalformedAPIKey)
		require.Equal(t, http.StatusUnauthorized, ginCtx.Writer.Status())
	})

	t.Run("missing key", func(t *testing.T) {
		t.Parallel()

		ginCtx, _ := gin.CreateTestContext(httptest.NewRecorder())

		err := authenticator.Authenticate(t.Context(), ginCtx, &openapi3filter.AuthenticationInput{
			RequestValidationInput: &openapi3filter.RequestValidationInput{
				Request: httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil),
			},
		})
		require.ErrorIs(t, err, ErrNoAuthHeader)
	})
}

// A missing header stamps 401 rather than leaving the validator's 400
// fallback to win, which is what makes an auth failure look like one.
func TestNewAuthenticatorStamps401OnAMissingHeader(t *testing.T) {
	t.Parallel()

	authenticator := NewAuthenticator(AuthenticatorConfig[struct{}]{
		SchemeName: "CustomBearerAuth",
		Header:     HeaderAuthorization,
		Validate: func(context.Context, *gin.Context, string) (struct{}, *APIError) {
			return struct{}{}, nil
		},
	})

	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)

	err := authenticator.Authenticate(t.Context(), ginCtx, &openapi3filter.AuthenticationInput{
		RequestValidationInput: &openapi3filter.RequestValidationInput{
			Request: httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil),
		},
	})
	require.Error(t, err)
	require.Equal(t, http.StatusUnauthorized, ginCtx.Writer.Status())
}

func TestNewAuthenticatorPreservesExistingStatusOnMissingHeader(t *testing.T) {
	t.Parallel()

	failingAuthenticator := NewAuthenticator(AuthenticatorConfig[struct{}]{
		SchemeName: "ExistingAuth",
		Header:     HeaderAdminToken,
		Validate: func(context.Context, *gin.Context, string) (struct{}, *APIError) {
			return struct{}{}, &APIError{
				Err:       ErrInvalidAuthHeader,
				ClientMsg: "team not found",
				Code:      http.StatusNotFound,
			}
		},
	})
	authenticator := NewAuthenticator(AuthenticatorConfig[struct{}]{
		SchemeName: "CustomBearerAuth",
		Header:     HeaderAuthorization,
		Validate: func(context.Context, *gin.Context, string) (struct{}, *APIError) {
			return struct{}{}, nil
		},
	})

	ginCtx, _ := gin.CreateTestContext(httptest.NewRecorder())
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
	req.Header.Set(HeaderAdminToken, "valid-token")
	require.Error(t, failingAuthenticator.Authenticate(t.Context(), ginCtx, &openapi3filter.AuthenticationInput{
		RequestValidationInput: &openapi3filter.RequestValidationInput{Request: req},
	}))

	err := authenticator.Authenticate(t.Context(), ginCtx, &openapi3filter.AuthenticationInput{
		RequestValidationInput: &openapi3filter.RequestValidationInput{
			Request: req,
		},
	})
	require.ErrorIs(t, err, ErrNoAuthHeader)
	require.Equal(t, http.StatusNotFound, ginCtx.Writer.Status())
}

func TestNewAuthenticatorStamps401OverGinDefaultNotFoundOnMissingHeader(t *testing.T) {
	t.Parallel()

	authenticator := NewAuthenticator(AuthenticatorConfig[struct{}]{
		SchemeName: "CustomBearerAuth",
		Header:     HeaderAuthorization,
		Validate: func(context.Context, *gin.Context, string) (struct{}, *APIError) {
			return struct{}{}, nil
		},
	})

	ginCtx, _ := gin.CreateTestContext(httptest.NewRecorder())
	ginCtx.Status(http.StatusNotFound)

	err := authenticator.Authenticate(t.Context(), ginCtx, &openapi3filter.AuthenticationInput{
		RequestValidationInput: &openapi3filter.RequestValidationInput{
			Request: httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil),
		},
	})

	require.ErrorIs(t, err, ErrNoAuthHeader)
	require.Equal(t, http.StatusUnauthorized, ginCtx.Writer.Status())
}

// SetContext is optional: a scheme that only proves the caller may proceed
// has nothing to record, and must not require a setter to say so.
func TestNewAuthenticatorAllowsNoContextSetter(t *testing.T) {
	t.Parallel()

	authenticator := NewAuthenticator(AuthenticatorConfig[struct{}]{
		SchemeName: "CustomBearerAuth",
		Header:     HeaderAdminToken,
		Validate: func(context.Context, *gin.Context, string) (struct{}, *APIError) {
			return struct{}{}, nil
		},
	})

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
	req.Header.Set(HeaderAdminToken, "anything")
	ginCtx, _ := gin.CreateTestContext(httptest.NewRecorder())

	require.NoError(t, authenticator.Authenticate(t.Context(), ginCtx, &openapi3filter.AuthenticationInput{
		RequestValidationInput: &openapi3filter.RequestValidationInput{Request: req},
	}))
}

// A 401 rejects the credential, so its message leads with the scheme's
// rejection. A 5xx did not get that far, so it drops it and keeps its status.
func TestNewAuthenticatorRejectsTheCredentialOnlyBelowServerErrors(t *testing.T) {
	t.Parallel()

	testCases := map[string]struct {
		validationError *APIError
		wantMessage     string
	}{
		"unknown key keeps the rejection message": {
			validationError: &APIError{ClientMsg: "no such key", Code: http.StatusUnauthorized},
			wantMessage:     "Invalid custom token.\nno such key",
		},
		"failed lookup drops the rejection message": {
			validationError: &APIError{ClientMsg: "try again", Code: http.StatusInternalServerError},
			wantMessage:     "try again",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			authenticator := NewAuthenticator(AuthenticatorConfig[struct{}]{
				SchemeName: "CustomBearerAuth",
				Header:     HeaderAuthorization,
				Validate: func(context.Context, *gin.Context, string) (struct{}, *APIError) {
					return struct{}{}, tc.validationError
				},
				ErrorMessage: "Invalid custom token.",
			})

			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
			req.Header.Set(HeaderAuthorization, "token")
			ginCtx, _ := gin.CreateTestContext(httptest.NewRecorder())

			err := authenticator.Authenticate(t.Context(), ginCtx, &openapi3filter.AuthenticationInput{
				RequestValidationInput: &openapi3filter.RequestValidationInput{Request: req},
			})

			require.EqualError(t, err, tc.wantMessage)
			require.Equal(t, tc.validationError.Code, ginCtx.Writer.Status())
		})
	}
}
