package handlers

import (
	"context"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	templatecache "github.com/e2b-dev/infra/packages/api/internal/cache/templates"
	"github.com/e2b-dev/infra/packages/api/internal/db/queries"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
	"github.com/e2b-dev/infra/packages/shared/pkg/ginutils"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/postgres/dberrors"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// PatchTemplatesTemplateID serves to update a template (v1 - deprecated, for older CLIs, creates backward-compatible aliases)
func (a *APIStore) PatchTemplatesTemplateID(c *gin.Context, aliasOrTemplateID api.TemplateID) {
	ctx := c.Request.Context()

	_, _, apiErr := a.updateTemplate(ctx, c, aliasOrTemplateID, true)
	if apiErr != nil {
		telemetry.ReportErrorByCode(ctx, apiErr.Code, apiErr.ClientMsg, apiErr.Err, telemetry.WithTemplateID(aliasOrTemplateID))
		a.sendAPIStoreError(c, apiErr.Code, apiErr.ClientMsg)

		return
	}

	c.JSON(http.StatusOK, nil)
}

// PatchV2TemplatesTemplateID serves to update a template (v2 - for new CLIs)
func (a *APIStore) PatchV2TemplatesTemplateID(c *gin.Context, aliasOrTemplateID api.TemplateID) {
	ctx := c.Request.Context()

	team, aliasInfo, apiErr := a.updateTemplate(ctx, c, aliasOrTemplateID, false)
	if apiErr != nil {
		telemetry.ReportErrorByCode(ctx, apiErr.Code, apiErr.ClientMsg, apiErr.Err, telemetry.WithTemplateID(aliasOrTemplateID))
		a.sendAPIStoreError(c, apiErr.Code, apiErr.ClientMsg)

		return
	}

	template, err := a.sqlcDB.GetTemplateByIDWithAliases(ctx, aliasInfo.TemplateID)
	if err != nil {
		telemetry.ReportError(ctx, "error getting template names after update", err)
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error retrieving template after update")

		return
	}

	c.JSON(http.StatusOK, api.TemplateUpdateResponse{
		Names: template.Names,
	})

	logger.L().Debug(ctx, "Returned template names after update",
		logger.WithTemplateID(aliasInfo.TemplateID),
		logger.WithTeamID(team.ID.String()))
}

// updateTemplate contains the shared logic for updating a template.
// Returns the resolved team and aliasInfo on success, or an APIError on failure.
func (a *APIStore) updateTemplate(ctx context.Context, c *gin.Context, aliasOrTemplateID api.TemplateID, createBackwardCompatAlias bool) (*types.Team, *templatecache.AliasInfo, *api.APIError) {
	body, err := ginutils.ParseBody[api.TemplateUpdateRequest](ctx, c)
	if err != nil {
		return nil, nil, &api.APIError{
			Code:      http.StatusBadRequest,
			ClientMsg: fmt.Sprintf("Invalid request body: %s", err),
			Err:       err,
		}
	}

	identifier, _, err := id.ParseName(aliasOrTemplateID)
	if err != nil {
		return nil, nil, &api.APIError{
			Code:      http.StatusBadRequest,
			ClientMsg: fmt.Sprintf("Invalid template ID: %s", err),
			Err:       err,
		}
	}

	// Resolve template and get the owning team
	team, aliasInfo, apiErr := a.resolveTemplateAndTeam(ctx, c, identifier)
	if apiErr != nil {
		return nil, nil, apiErr
	}

	telemetry.SetAttributes(ctx,
		attribute.String("env.team.id", team.ID.String()),
		attribute.String("env.team.name", team.Name),
		attribute.String("package_version", c.Request.Header.Get("package_version")),
		attribute.Bool("create_backward_compat_alias", createBackwardCompatAlias),
		telemetry.WithTemplateID(aliasInfo.TemplateID),
	)

	// No-op if no update fields provided (empty body is valid per OpenAPI spec)
	if body.Public == nil {
		logger.L().Debug(ctx, "Empty PATCH body, no-op", logger.WithTemplateID(aliasInfo.TemplateID))

		return team, aliasInfo, nil
	}

	_, err = a.sqlcDB.UpdateTemplate(ctx, queries.UpdateTemplateParams{
		TemplateIDOrAlias: aliasInfo.TemplateID,
		TeamID:            team.ID,
		Public:            *body.Public,
	})
	if err != nil {
		if dberrors.IsNotFoundError(err) {
			return nil, nil, &api.APIError{
				Code:      http.StatusNotFound,
				ClientMsg: fmt.Sprintf("Template '%s' not found or you don't have access to it", aliasOrTemplateID),
				Err:       err,
			}
		}

		return nil, nil, &api.APIError{
			Code:      http.StatusInternalServerError,
			ClientMsg: "Error updating template",
			Err:       err,
		}
	}

	// Invalidate cache immediately after successful DB update
	a.templateCache.InvalidateAllTags(context.WithoutCancel(ctx), aliasInfo.TemplateID)

	// For backward compatibility with older CLIs (v1 endpoint), also create a non-namespaced alias
	// when publishing a template, so older CLIs can still find it by bare alias name
	if createBackwardCompatAlias && *body.Public {
		if apiErr := a.createBackwardCompatibleAlias(ctx, identifier, aliasInfo.TemplateID, team.Slug); apiErr != nil {
			return nil, nil, apiErr
		}
	}

	telemetry.ReportEvent(ctx, "updated template")

	properties := a.posthog.GetPackageToPosthogProperties(&c.Request.Header)
	a.posthog.IdentifyAnalyticsTeam(ctx, team.ID.String(), team.Name)
	a.posthog.CreateAnalyticsTeamEvent(ctx, team.ID.String(), "updated environment", properties.Set("environment", aliasInfo.TemplateID))

	logger.L().Info(ctx, "Updated template", logger.WithTemplateID(aliasInfo.TemplateID), logger.WithTeamID(team.ID.String()))

	return team, aliasInfo, nil
}

// createBackwardCompatibleAlias creates a non-namespaced alias for older CLIs
// that don't support namespace-prefixed template names.
// Uses atomic upsert to avoid race conditions.
func (a *APIStore) createBackwardCompatibleAlias(
	ctx context.Context,
	identifier string,
	templateID string,
	teamSlug string,
) *api.APIError {
	alias := id.ExtractAlias(identifier)
	namespacedName := id.WithNamespace(teamSlug, alias)

	// Atomically try to create the alias or get the existing owner
	upsertedTemplateID, err := a.sqlcDB.UpsertTemplateAliasIfNotExists(ctx, queries.UpsertTemplateAliasIfNotExistsParams{
		Alias:      alias,
		TemplateID: templateID,
		Namespace:  nil,
	})
	if err != nil {
		return &api.APIError{
			Code:      http.StatusInternalServerError,
			ClientMsg: "Error creating backward compatible alias",
			Err:       err,
		}
	}

	// Check if the alias belongs to this template (either newly created or already existed)
	if upsertedTemplateID != templateID {
		return &api.APIError{
			Code: http.StatusConflict,
			ClientMsg: fmt.Sprintf(
				"Public template name '%s' is already taken. Your template is available at '%s'. Please update your CLI to remove this error message.",
				alias, namespacedName),
			Err: nil,
		}
	}

	a.templateCache.InvalidateAlias(context.WithoutCancel(ctx), nil, alias)
	logger.L().Info(ctx, "Created or verified backward compatible non-namespaced alias",
		logger.WithTemplateID(templateID),
		zap.String("alias", alias))

	return nil
}
