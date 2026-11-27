package handlers

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	templatecache "github.com/e2b-dev/infra/packages/api/internal/cache/templates"
	"github.com/e2b-dev/infra/packages/api/internal/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
	"github.com/e2b-dev/infra/packages/shared/pkg/postgres/dberrors"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

func (a *APIStore) GetTemplatesAliasesAlias(c *gin.Context, alias string) {
	ctx := c.Request.Context()

	team, apiErr := a.GetTeam(ctx, c, nil)
	if apiErr != nil {
		a.sendAPIStoreError(c, apiErr.Code, apiErr.ClientMsg)
		telemetry.ReportCriticalError(ctx, "error when getting team and tier", apiErr.Err)

		return
	}

	hasExplicitTag := strings.Contains(alias, id.TagSeparator)
	identifier, tag, err := id.ParseName(alias)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("Invalid alias format: %s", err))
		telemetry.ReportError(ctx, "invalid alias format", err)

		return
	}

	if err := id.ValidateNamespaceMatchesTeam(identifier, team.Slug); err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, err.Error())

		return
	}

	aliasInfo, metadata, err := a.templateCache.ResolveAliasWithMetadata(ctx, identifier, team.Slug)
	if err != nil {
		apiErr := templatecache.ErrorToAPIError(err, identifier)
		a.sendAPIStoreError(c, apiErr.Code, apiErr.ClientMsg)

		return
	}

	// Ownership verification (handles edge case where template was transferred).
	// Must run before the tag-existence probe below, otherwise non-owners could
	// distinguish existing tags from missing ones on templates they no longer
	// have access to via 404 vs 403 responses.
	if aliasInfo.TeamID != team.ID {
		a.sendAPIStoreError(c, http.StatusForbidden, "You don't have access to this template alias")

		return
	}

	if hasExplicitTag {
		tagValue := id.DefaultTag
		if tag != nil {
			tagValue = *tag
		}

		_, err = a.sqlcDB.GetTemplateWithBuildByTag(ctx, queries.GetTemplateWithBuildByTagParams{
			TemplateID: aliasInfo.TemplateID,
			Tag:        &tagValue,
		})
		if err != nil {
			if dberrors.IsNotFoundError(err) {
				a.sendAPIStoreError(c, http.StatusNotFound, fmt.Sprintf("tag '%s' does not exist for template '%s'", tagValue, identifier))
				telemetry.ReportError(ctx, "template tag not found", err, telemetry.WithTemplateID(aliasInfo.TemplateID))

				return
			}

			a.sendAPIStoreError(c, http.StatusInternalServerError, "Error when checking template tag existence")
			telemetry.ReportCriticalError(ctx, "error when checking template tag existence", err, telemetry.WithTemplateID(aliasInfo.TemplateID))

			return
		}
	}

	// Team is alias owner
	c.JSON(
		http.StatusOK, api.TemplateAliasResponse{
			Public:     metadata.Public,
			TemplateID: aliasInfo.TemplateID,
		},
	)
}
