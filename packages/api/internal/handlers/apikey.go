package handlers

import (
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/team"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/db/pkg/auth/queries"
	"github.com/e2b-dev/infra/packages/db/pkg/dberrors"
	"github.com/e2b-dev/infra/packages/shared/pkg/ginutils"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

func (a *APIStore) PatchApiKeysApiKeyID(c *gin.Context, apiKeyID string) {
	ctx := c.Request.Context()

	body, err := ginutils.ParseBody[api.UpdateTeamAPIKey](ctx, c)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("Error when parsing request: %s", err))

		telemetry.ReportCriticalError(ctx, "error when parsing request", err)

		return
	}

	apiKeyIDParsed, err := uuid.Parse(apiKeyID)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("Error when parsing API key ID: %s", err))

		telemetry.ReportCriticalError(ctx, "error when parsing API key ID", err)

		return
	}

	teamID := auth.MustGetTeamID(c)

	now := time.Now()
	_, err = a.authDB.UpdateTeamApiKey(ctx, authqueries.UpdateTeamApiKeyParams{
		Name:      body.Name,
		UpdatedAt: &now,
		ID:        apiKeyIDParsed,
		TeamID:    teamID,
	})
	if dberrors.IsNotFoundError(err) {
		c.String(http.StatusNotFound, "id not found")

		return
	} else if err != nil {
		a.sendAPIStoreError(c, http.StatusInternalServerError, fmt.Sprintf("Error when updating team API key name: %s", err))

		telemetry.ReportCriticalError(ctx, "error when updating team API key name", err)

		return
	}

	c.Status(http.StatusAccepted)
}

func (a *APIStore) GetApiKeys(c *gin.Context) {
	a.respondTeamAPIKeys(c, auth.MustGetTeamID(c))
}

// respondTeamAPIKeys writes the team's API keys, masked, as the response.
func (a *APIStore) respondTeamAPIKeys(c *gin.Context, teamID uuid.UUID) {
	ctx := c.Request.Context()

	apiKeysDB, err := a.authDB.GetTeamAPIKeysWithCreator(ctx, teamID)
	if err != nil {
		logger.L().Warn(ctx, "error when getting team API keys", zap.Error(err))
		c.String(http.StatusInternalServerError, "Error when getting team API keys")

		return
	}

	teamAPIKeys := make([]api.TeamAPIKey, len(apiKeysDB))
	for i, apiKey := range apiKeysDB {
		var createdBy *api.TeamUser
		if apiKey.CreatedByID != nil {
			createdBy = &api.TeamUser{
				Email: nil,
				Id:    *apiKey.CreatedByID,
			}
		}

		teamAPIKeys[i] = api.TeamAPIKey{
			Id:   apiKey.ID,
			Name: apiKey.Name,
			Mask: api.IdentifierMaskingDetails{
				Prefix:            apiKey.ApiKeyPrefix,
				ValueLength:       int(apiKey.ApiKeyLength),
				MaskedValuePrefix: apiKey.ApiKeyMaskPrefix,
				MaskedValueSuffix: apiKey.ApiKeyMaskSuffix,
			},
			CreatedAt: apiKey.CreatedAt,
			CreatedBy: createdBy,
			LastUsed:  apiKey.LastUsed,
		}
	}
	c.JSON(http.StatusOK, teamAPIKeys)
}

func (a *APIStore) DeleteApiKeysApiKeyID(c *gin.Context, apiKeyID string) {
	ctx := c.Request.Context()

	apiKeyIDParsed, err := uuid.Parse(apiKeyID)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("Error when parsing API key ID: %s", err))

		telemetry.ReportCriticalError(ctx, "error when parsing API key ID", err)

		return
	}

	teamID := auth.MustGetTeamID(c)

	deleted, err := team.DeleteAPIKey(ctx, a.authDB, a.authService, teamID, apiKeyIDParsed)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusInternalServerError, fmt.Sprintf("Error when deleting API key: %s", err))

		telemetry.ReportCriticalError(ctx, "error when deleting API key", err)

		return
	}
	if !deleted {
		c.String(http.StatusNotFound, "id not found")

		return
	}

	c.Status(http.StatusNoContent)
}

func (a *APIStore) PostApiKeys(c *gin.Context) {
	ctx := c.Request.Context()

	userID := auth.MustGetUserID(c)
	teamID := auth.MustGetTeamID(c)

	body, err := ginutils.ParseBody[api.NewTeamAPIKey](ctx, c)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("Error when parsing request: %s", err))

		telemetry.ReportCriticalError(ctx, "error when parsing request", err)

		return
	}

	apiKey, err := team.CreateAPIKey(ctx, a.authDB, teamID, &userID, body.Name)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusInternalServerError, fmt.Sprintf("Error when creating team API key: %s", err))

		telemetry.ReportCriticalError(ctx, "error when creating team API key", err)

		return
	}

	c.JSON(http.StatusCreated, api.CreatedTeamAPIKey{
		Id:   apiKey.ID,
		Name: apiKey.Name,
		Key:  apiKey.RawAPIKey,
		Mask: api.IdentifierMaskingDetails{
			Prefix:            apiKey.ApiKeyPrefix,
			ValueLength:       int(apiKey.ApiKeyLength),
			MaskedValuePrefix: apiKey.ApiKeyMaskPrefix,
			MaskedValueSuffix: apiKey.ApiKeyMaskSuffix,
		},
		CreatedBy: &api.TeamUser{
			Id:    userID,
			Email: nil,
		},
		CreatedAt: apiKey.CreatedAt,
		LastUsed:  apiKey.LastUsed,
	})
}
