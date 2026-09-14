package handlers

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	openapi_types "github.com/oapi-codegen/runtime/types"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/team"
	sharedauth "github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/db/pkg/dberrors"
	"github.com/e2b-dev/infra/packages/shared/pkg/ginutils"
)

func (a *APIStore) PostAdminTeamsTeamIDApiKeys(c *gin.Context, teamID openapi_types.UUID) {
	ctx := c.Request.Context()

	body, err := ginutils.ParseBody[api.NewTeamAPIKey](ctx, c)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("Error when parsing request: %s", err))

		return
	}

	teamInfo, err := a.authService.GetTeamByID(ctx, teamID)
	if err != nil {
		if forbiddenErr, ok := errors.AsType[*sharedauth.TeamForbiddenError](err); ok {
			a.sendAPIStoreError(c, http.StatusForbidden, forbiddenErr.Error())

			return
		}
		if dberrors.IsNotFoundError(err) {
			a.sendAPIStoreError(c, http.StatusNotFound, "Team not found")

			return
		}

		a.sendAPIStoreError(c, http.StatusInternalServerError, fmt.Sprintf("Error when getting team: %s", err))

		return
	}
	if teamInfo == nil {
		a.sendAPIStoreError(c, http.StatusNotFound, "Team not found")

		return
	}

	if err := sharedauth.CheckTeamBlocked(teamInfo); err != nil {
		a.sendAPIStoreError(c, http.StatusForbidden, err.Error())

		return
	}

	apiKey, err := team.CreateAPIKey(ctx, a.authDB, teamID, nil, body.Name)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusInternalServerError, fmt.Sprintf("Error when creating team API key: %s", err))

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
		CreatedBy: nil,
		CreatedAt: apiKey.CreatedAt,
		LastUsed:  apiKey.LastUsed,
	})
}

func (a *APIStore) DeleteAdminTeamsTeamIDApiKeysApiKeyID(c *gin.Context, teamID openapi_types.UUID, apiKeyID api.ApiKeyID) {
	ctx := c.Request.Context()

	apiKeyUUID, err := uuid.Parse(apiKeyID)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("Error when parsing API key ID: %s", err))

		return
	}

	deleted, err := team.DeleteAPIKey(ctx, a.authDB, a.authService, teamID, apiKeyUUID)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusInternalServerError, fmt.Sprintf("Error when deleting API key: %s", err))

		return
	}
	if !deleted {
		a.sendAPIStoreError(c, http.StatusNotFound, "API key not found")

		return
	}

	c.Status(http.StatusNoContent)
}
