package handlers

import (
	"context"
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/metric"

	sharedauth "github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
	clickhouse "github.com/e2b-dev/infra/packages/clickhouse/pkg"
	"github.com/e2b-dev/infra/packages/dashboard-api/internal/api"
	"github.com/e2b-dev/infra/packages/dashboard-api/internal/cfg"
	"github.com/e2b-dev/infra/packages/dashboard-api/internal/identity"
	"github.com/e2b-dev/infra/packages/dashboard-api/internal/management"
	sqlcdb "github.com/e2b-dev/infra/packages/db/client"
	authdb "github.com/e2b-dev/infra/packages/db/pkg/auth"
	"github.com/e2b-dev/infra/packages/shared/pkg/apierrors"
)

var _ api.ServerInterface = (*APIStore)(nil)

type APIStore struct {
	config            cfg.Config
	db                *sqlcdb.Client
	authDB            *authdb.Client
	clickhouse        clickhouse.Clickhouse
	authService       sharedauth.Service
	identityService   identity.Service
	managementService *management.Service
}

func NewAPIStore(
	config cfg.Config,
	db *sqlcdb.Client,
	authDB *authdb.Client,
	ch clickhouse.Clickhouse,
	authService sharedauth.Service,
	identityService identity.Service,
	meterProvider metric.MeterProvider,
) *APIStore {
	return &APIStore{
		config:            config,
		db:                db,
		authDB:            authDB,
		clickhouse:        ch,
		authService:       authService,
		identityService:   identityService,
		managementService: management.NewService(authDB, db, authService, meterProvider),
	}
}

func (s *APIStore) sendAPIStoreError(c *gin.Context, code int, message string) {
	apierrors.SendAPIStoreError(c, code, message)
}

// identityProviderUnavailableMessage is the client-facing reason for the 503
// on endpoints backed by the identity provider.
const identityProviderUnavailableMessage = "No identity provider is configured; this endpoint is unavailable"

// abortIfNoIdentityProvider answers 503 when the identity service reports
// that no provider is configured — a missing capability, not a fault — and
// reports whether it handled the request.
func (s *APIStore) abortIfNoIdentityProvider(c *gin.Context, err error) bool {
	if !errors.Is(err, identity.ErrNoIdentityProvider) {
		return false
	}

	s.sendAPIStoreError(c, http.StatusServiceUnavailable, identityProviderUnavailableMessage)

	return true
}

func (s *APIStore) GetHealth(c *gin.Context) {
	c.JSON(http.StatusOK, api.HealthResponse{
		Message: "Health check successful",
	})
}

func (s *APIStore) GetUserIDFromAuthProviderToken(ctx context.Context, ginCtx *gin.Context, token string) (uuid.UUID, *sharedauth.APIError) {
	return s.authService.ValidateAuthProviderToken(ctx, ginCtx, token)
}

func (s *APIStore) GetTeamFromAPIKey(ctx context.Context, ginCtx *gin.Context, apiKey string) (*types.Team, *sharedauth.APIError) {
	return s.authService.ValidateAPIKey(ctx, ginCtx, apiKey)
}

func (s *APIStore) GetTeamFromAuthProviderToken(ctx context.Context, ginCtx *gin.Context, teamID string) (*types.Team, *sharedauth.APIError) {
	return s.authService.ValidateAuthProviderTeam(ctx, ginCtx, teamID)
}
