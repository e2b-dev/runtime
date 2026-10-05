package service

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/e2b-dev/infra/packages/auth/pkg/auth/internal/authcontext"
	internalauthteam "github.com/e2b-dev/infra/packages/auth/pkg/auth/internal/team"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth/internal/token"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
	authdb "github.com/e2b-dev/infra/packages/db/pkg/auth"
	"github.com/e2b-dev/infra/packages/shared/pkg/apierrors"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
	"github.com/e2b-dev/infra/packages/shared/pkg/keys"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// authStore abstracts the DB operations needed for auth validation.
type authStore interface {
	GetTeamByHashedAPIKey(ctx context.Context, hashedKey string) (*types.Team, error)
	GetTeamByID(ctx context.Context, teamID uuid.UUID) (*types.Team, error)
	GetTeamByIDAndUserID(ctx context.Context, userID uuid.UUID, teamID string) (*types.Team, error)
	GetTeamAPIKeyHashes(ctx context.Context, teamID uuid.UUID) ([]string, error)
	GetTeamMemberIDs(ctx context.Context, teamID uuid.UUID) ([]uuid.UUID, error)
}

type APIError = apierrors.APIError

// Service is the interface implemented by the internal AuthService. It
// exposes the auth validation, team lookup, and cache invalidation operations
// used by callers such as APIStore and the dashboard-api handlers.
type Service interface {
	ValidateAPIKey(ctx context.Context, ginCtx *gin.Context, apiKey string) (*types.Team, *APIError)
	ValidateAuthProviderToken(ctx context.Context, ginCtx *gin.Context, token string) (uuid.UUID, *APIError)
	ValidateAuthProviderTeam(ctx context.Context, ginCtx *gin.Context, teamID string) (*types.Team, *APIError)
	GetTeamByID(ctx context.Context, teamID uuid.UUID) (*types.Team, error)
	InvalidateTeamMemberCache(ctx context.Context, userID uuid.UUID, teamID string)
	InvalidateTeamCache(ctx context.Context, teamID uuid.UUID) error
	InvalidateAPIKeyCache(ctx context.Context, hashedKey string)
	Close(ctx context.Context) error
}

// AuthService encapsulates the cache, store, and JWT verifier for auth validation.
type AuthService struct {
	store                authStore
	teamCache            *authCache
	authProviderVerifier *token.LinkedOIDCVerifier
}

// Compile-time assertion that *AuthService satisfies the Service interface.
var _ Service = (*AuthService)(nil)

// NewAuthService wires up the team cache, auth store, identity lookup, and JWT
// verifier from the supplied dependencies. The HTTP client is used for OIDC
// discovery and JWKS fetches.
func NewAuthService(
	ctx context.Context,
	redisClient redis.UniversalClient,
	authDB *authdb.Client,
	providerConfig token.ProviderConfig,
	httpClient *http.Client,
) (*AuthService, error) {
	if redisClient == nil {
		return nil, errors.New("redisClient is required")
	}
	if authDB == nil {
		return nil, errors.New("authDB is required")
	}
	if httpClient == nil {
		return nil, errors.New("httpClient is required")
	}

	cache := newAuthCache(redisClient)
	store := newAuthStore(authDB)
	// OIDC bootstrap writes identity rows on the primary immediately before the
	// next authenticated request; using the read replica here races replication lag.
	identityLookup := newAuthIdentityLookup(authDB.Queries)
	v, err := token.NewLinkedOIDCVerifier(ctx, providerConfig, httpClient, identityLookup)
	if err != nil {
		return nil, fmt.Errorf("initializing auth provider JWT verifier: %w", err)
	}

	return &AuthService{
		store:                store,
		teamCache:            cache,
		authProviderVerifier: v,
	}, nil
}

// ValidateAPIKey verifies the API key format and fetches the associated team via cache + store.
func (s *AuthService) ValidateAPIKey(ctx context.Context, ginCtx *gin.Context, apiKey string) (*types.Team, *APIError) {
	hashedKey, err := keys.VerifyKey(keys.ApiKeyPrefix, apiKey)
	if err != nil {
		return nil, &APIError{
			Err:       fmt.Errorf("failed to verify api key: %w", err),
			ClientMsg: "Invalid API key format",
			Code:      http.StatusUnauthorized,
		}
	}

	result, err := s.teamCache.GetOrSet(ctx, hashedKey, func(ctx context.Context, key string) (*types.Team, error) {
		return s.store.GetTeamByHashedAPIKey(ctx, key)
	})
	if err != nil {
		if _, ok := errors.AsType[*internalauthteam.ForbiddenError](err); ok {
			return nil, &APIError{
				Err:       err,
				ClientMsg: err.Error(),
				Code:      http.StatusForbidden,
			}
		}

		return nil, &APIError{
			Err:       fmt.Errorf("failed to get the team from db for an api key: %w", err),
			ClientMsg: "Cannot get the team for the given API key",
			Code:      http.StatusUnauthorized,
		}
	}

	//nolint:contextcheck // We use the gin request context to set attributes on the parent span.
	telemetry.SetAttributes(ginCtx.Request.Context(),
		telemetry.WithMaskedAPIKey(keys.MaskToken(keys.ApiKeyPrefix, apiKey)),
		telemetry.WithTeamID(result.TeamID()),
	)

	return result, nil
}

// GetTeamByID fetches team auth data via cache + store.
func (s *AuthService) GetTeamByID(ctx context.Context, teamID uuid.UUID) (*types.Team, error) {
	return s.teamCache.GetOrSet(ctx, teamCacheKey(teamID), func(ctx context.Context, _ string) (*types.Team, error) {
		return s.store.GetTeamByID(ctx, teamID)
	})
}

// ValidateAuthProviderToken verifies a JWT against the configured auth provider and resolves an internal user ID.
//
// When no auth provider verifier is configured (AUTH_PROVIDER_CONFIG is unset),
// every token is denied with 401. This makes "no auth provider" a valid
// configuration: API key flows keep working, but JWT-based
// flows are universally rejected.
func (s *AuthService) ValidateAuthProviderToken(ctx context.Context, ginCtx *gin.Context, token string) (uuid.UUID, *APIError) {
	if s.authProviderVerifier == nil {
		return uuid.UUID{}, &APIError{
			Err:       errors.New("auth provider is not configured"),
			ClientMsg: "Backend authentication failed",
			Code:      http.StatusUnauthorized,
		}
	}

	return s.validateJWTWithProvider(ctx, ginCtx, s.authProviderVerifier, token, "auth provider")
}

func (s *AuthService) validateJWTWithProvider(ctx context.Context, ginCtx *gin.Context, v *token.LinkedOIDCVerifier, token string, tokenSource string) (uuid.UUID, *APIError) {
	userID, _, err := v.Verify(ctx, token)
	if err != nil {
		return uuid.UUID{}, &APIError{
			Err:       err,
			ClientMsg: "Backend authentication failed",
			Code:      http.StatusUnauthorized,
		}
	}

	if userID == uuid.Nil {
		return uuid.UUID{}, &APIError{
			Err:       fmt.Errorf("%s token user claim is missing or is not an internal UUID", tokenSource),
			ClientMsg: "Backend authentication failed",
			Code:      http.StatusUnauthorized,
		}
	}

	//nolint:contextcheck // We use the gin request context to set attributes on the parent span.
	telemetry.SetAttributes(ginCtx.Request.Context(),
		telemetry.WithUserID(userID.String()),
	)

	return userID, nil
}

// ValidateAuthProviderTeam extracts the user ID from the gin context and fetches the team via cache + store.
func (s *AuthService) ValidateAuthProviderTeam(ctx context.Context, ginCtx *gin.Context, teamID string) (*types.Team, *APIError) {
	userID, ok := authcontext.GetUserID(ginCtx)
	if !ok {
		return nil, &APIError{
			Err:       errors.New("user ID has invalid type"),
			ClientMsg: "Backend authentication failed",
			Code:      http.StatusInternalServerError,
		}
	}

	// The header may carry the public project ID; key the cache on the UUID so
	// membership invalidation, which is keyed by UUID, evicts every spelling.
	parsedTeamID, err := id.ParseTeamID(teamID)
	if err != nil {
		return nil, &APIError{
			Err:       fmt.Errorf("failed parsing team ID: %w", err),
			ClientMsg: "Backend authentication failed",
			Code:      http.StatusUnauthorized,
		}
	}
	canonicalTeamID := parsedTeamID.String()

	cacheKey := teamMemberCacheKey(userID, canonicalTeamID)

	result, err := s.teamCache.GetOrSet(ctx, cacheKey, func(ctx context.Context, _ string) (*types.Team, error) {
		return s.store.GetTeamByIDAndUserID(ctx, userID, canonicalTeamID)
	})
	if err != nil {
		if _, ok := errors.AsType[*internalauthteam.ForbiddenError](err); ok {
			return nil, &APIError{
				Err:       fmt.Errorf("failed getting team: %w", err),
				ClientMsg: fmt.Sprintf("Forbidden: %s", err.Error()),
				Code:      http.StatusForbidden,
			}
		}

		return nil, &APIError{
			Err:       fmt.Errorf("failed getting team: %w", err),
			ClientMsg: "Backend authentication failed",
			Code:      http.StatusUnauthorized,
		}
	}

	//nolint:contextcheck // We use the gin request context to set attributes on the parent span.
	telemetry.SetAttributes(ginCtx.Request.Context(),
		telemetry.WithUserID(userID.String()),
		telemetry.WithTeamID(result.TeamID()),
	)

	return result, nil
}

// InvalidateTeamMemberCache removes the cached auth entry for a specific user-team pair.
// This should be called when team membership changes (member added or removed).
//
// Detached for the same reason as InvalidateAPIKeyCache: the invalidation runs
// after the membership change has committed, and skipping it because the client
// disconnected leaves a removed member authenticating until the cache TTL.
func (s *AuthService) InvalidateTeamMemberCache(ctx context.Context, userID uuid.UUID, teamID string) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), invalidateTimeout)
	defer cancel()

	s.teamCache.Invalidate(ctx, teamMemberCacheKey(userID, teamID))
}

// InvalidateTeamCache removes every cached entry carrying this team's data.
//
// A team is cached under three kinds of key, and a caller changing something
// team-wide -- limits, blocked state -- means all three are stale. Leaving any
// of them means the change lands on some auth paths and not others, which is
// harder to diagnose than not invalidating at all.
//
//	team-<id>          GetTeamByID, the admin and management paths
//	<api key hash>     ApiKeyAuth
//	<user id>-<id>     AuthProviderBearerAuth + AuthProviderTeamAuth, the
//	                   browser session path, one entry per member
//
// Detached from the caller's cancellation, and bounded, because it runs after
// the change it reflects has committed. One budget covers the whole sweep: the
// reads below decide which keys to drop, so a cancelled context part-way
// through would leave an arbitrary subset of them stale.
//
// A failed eviction is returned rather than logged, and the sweep continues
// past it: the caller has already committed the change and needs to know the
// cache still disagrees, while every key that can be dropped now is dropped.
func (s *AuthService) InvalidateTeamCache(ctx context.Context, teamID uuid.UUID) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), invalidateTimeout)
	defer cancel()

	evictions := []error{s.teamCache.TryInvalidate(ctx, teamCacheKey(teamID))}

	hashes, err := s.store.GetTeamAPIKeyHashes(ctx, teamID)
	if err != nil {
		return fmt.Errorf("failed to get team API key hashes: %w", err)
	}

	for _, hash := range hashes {
		evictions = append(evictions, s.teamCache.TryInvalidate(ctx, hash))
	}

	memberIDs, err := s.store.GetTeamMemberIDs(ctx, teamID)
	if err != nil {
		return fmt.Errorf("failed to get team member ids: %w", err)
	}

	for _, userID := range memberIDs {
		evictions = append(evictions, s.teamCache.TryInvalidate(ctx, teamMemberCacheKey(userID, teamID.String())))
	}

	if err := errors.Join(evictions...); err != nil {
		return fmt.Errorf("failed to evict team cache entries: %w", err)
	}

	return nil
}

// InvalidateAPIKeyCache removes the cached auth entry for a specific hashed API key.
// This should be called when the key is deleted so revocation takes effect immediately
// instead of after the cache TTL expires.
//
// The call is synchronous and waits for any in-flight cache writer on the key
// (see RedisCache.Delete), so the caller's request can block for up to
// invalidateTimeout in the worst case — only reached when a concurrent
// refresh of the same key is wedged near the full refresh timeout, which
// requires a multi-second DB stall; the typical case returns in milliseconds.
func (s *AuthService) InvalidateAPIKeyCache(ctx context.Context, hashedKey string) {
	// The invalidation runs after the key's DB delete has committed; if it were
	// skipped because the client disconnected, the revoked key would keep
	// authenticating until the cache TTL expires.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), invalidateTimeout)
	defer cancel()

	s.teamCache.Invalidate(ctx, hashedKey)
}

func teamMemberCacheKey(userID uuid.UUID, teamID string) string {
	return fmt.Sprintf("%s-%s", userID.String(), strings.ToLower(teamID))
}

func teamCacheKey(teamID uuid.UUID) string {
	return fmt.Sprintf("team-%s", teamID.String())
}

// Close stops the underlying cache's background refresh goroutines.
func (s *AuthService) Close(ctx context.Context) error {
	return s.teamCache.Close(ctx)
}
