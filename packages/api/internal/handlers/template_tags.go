package handlers

import (
	"context"
	"fmt"
	"net/http"
	"slices"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel/attribute"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	templatecache "github.com/e2b-dev/infra/packages/api/internal/cache/templates"
	"github.com/e2b-dev/infra/packages/api/internal/db/queries"
	"github.com/e2b-dev/infra/packages/api/internal/middleware/idempotency"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
	"github.com/e2b-dev/infra/packages/shared/pkg/apierrors"
	"github.com/e2b-dev/infra/packages/shared/pkg/ginutils"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/postgres/dberrors"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

// PostTemplatesTags assigns tags to a template build
// The target template is specified in the request body via the "target" field
func (a *APIStore) PostTemplatesTags(c *gin.Context, _ api.PostTemplatesTagsParams) {
	ctx := c.Request.Context()

	var body api.AssignTemplateTagsRequest
	if err := c.ShouldBind(&body); err != nil {
		telemetry.ReportError(ctx, "error when parsing request", err)
		apierrors.SendAPIError(c, &apierrors.APIError{Code: http.StatusBadRequest, ClientMsg: fmt.Sprintf("Invalid request body: error when parsing request: %s", err), Retryable: apierrors.NoRetry})

		return
	}

	// Validate tags early
	if len(body.Tags) == 0 {
		apierrors.SendAPIError(c, &apierrors.APIError{Code: http.StatusBadRequest, ClientMsg: "At least one tag is required", Retryable: apierrors.NoRetry})
		telemetry.ReportError(ctx, "at least one tag is required", nil)

		return
	}

	team, apiErr := a.GetTeam(ctx, c, nil)
	if apiErr != nil {
		apiErr.Retryable = apierrors.NoRetry
		if apiErr.Code == http.StatusInternalServerError {
			apiErr.Retryable = apierrors.CanRetry
		}
		apierrors.SendAPIError(c, apiErr)
		telemetry.ReportCriticalError(ctx, "error when getting team", apiErr.Err)

		return
	}

	idempotency.Execute(c, body, func() *apierrors.APIError {
		assigned, apiErr := a.assignTemplateTags(c, team, body)
		if apiErr != nil {
			return apiErr
		}
		c.JSON(http.StatusCreated, assigned)

		return nil
	})
}

func (a *APIStore) assignTemplateTags(c *gin.Context, team *types.Team, body api.AssignTemplateTagsRequest) (*api.AssignedTemplateTags, *apierrors.APIError) {
	ctx := c.Request.Context()

	identifier, tag, err := id.ParseName(body.Target)
	if err != nil {
		telemetry.ReportError(ctx, "invalid target template format", err)

		return nil, &apierrors.APIError{Code: http.StatusBadRequest, ClientMsg: fmt.Sprintf("Invalid target template format: %s", err), Retryable: apierrors.NoRetry}
	}

	if err := id.ValidateNamespaceMatchesTeam(identifier, team.Slug); err != nil {
		return nil, &apierrors.APIError{Code: http.StatusBadRequest, ClientMsg: err.Error(), Retryable: apierrors.NoRetry}
	}

	targetTagValue := id.DefaultTag
	if tag != nil {
		targetTagValue = *tag
	}

	aliasInfo, err := a.templateCache.ResolveAlias(ctx, identifier, team.Slug)
	if err != nil {
		apiErr := templatecache.ErrorToAPIError(err, identifier)
		telemetry.ReportError(ctx, "template not found", apiErr.Err, telemetry.WithTemplateID(identifier))

		return nil, apiErr
	}

	if aliasInfo.TeamID != team.ID {
		telemetry.ReportError(ctx, "no access to the template", nil, telemetry.WithTemplateID(aliasInfo.TemplateID))

		return nil, &apierrors.APIError{Code: http.StatusForbidden, ClientMsg: fmt.Sprintf("You don't have access to sandbox template '%s'", identifier), Retryable: apierrors.NoRetry}
	}

	client, tx, err := a.sqlcDB.WithTx(ctx)
	if err != nil {
		telemetry.ReportCriticalError(ctx, "error when beginning transaction", err)

		return nil, &apierrors.APIError{Code: http.StatusInternalServerError, ClientMsg: "Error beginning transaction", Retryable: apierrors.CanRetry}
	}
	defer tx.Rollback(ctx)

	// Step 2: Get template with build by ID and tag
	result, err := client.GetTemplateWithBuildByTag(ctx, queries.GetTemplateWithBuildByTagParams{
		TemplateID: aliasInfo.TemplateID,
		Tag:        &targetTagValue,
	})
	if err != nil {
		if dberrors.IsNotFoundError(err) {
			telemetry.ReportError(ctx, "template tag not found", err, telemetry.WithTemplateID(aliasInfo.TemplateID))

			return nil, &apierrors.APIError{Code: http.StatusNotFound, ClientMsg: fmt.Sprintf("Template '%s' with tag '%s' not found", body.Target, targetTagValue), Retryable: apierrors.NoRetry}
		}

		telemetry.ReportError(ctx, "error when getting template with build", err)

		return nil, &apierrors.APIError{Code: http.StatusInternalServerError, ClientMsg: "Error getting template", Retryable: apierrors.CanRetry}
	}

	template := result.ActiveEnv
	buildID := result.EnvBuild.ID

	telemetry.SetAttributes(ctx,
		attribute.String("env.team.id", team.ID.String()),
		attribute.String("env.team.name", team.Name),
		telemetry.WithTemplateID(template.ID),
	)

	tags, err := id.ValidateAndDeduplicateTags(body.Tags)
	if err != nil {
		telemetry.ReportErrorByCode(ctx, http.StatusBadRequest, "invalid tag", err)

		return nil, &apierrors.APIError{Code: http.StatusBadRequest, ClientMsg: fmt.Sprintf("Invalid tag: %s", err), Retryable: apierrors.NoRetry}
	}

	// Create the tag assignments
	for _, tag := range tags {
		rows, err := client.CreateTemplateBuildAssignment(ctx, queries.CreateTemplateBuildAssignmentParams{
			TemplateID: template.ID,
			BuildID:    buildID,
			Tag:        tag,
		})
		if err != nil {
			telemetry.ReportCriticalError(ctx, "error when creating tag assignment", err)

			return nil, &apierrors.APIError{Code: http.StatusInternalServerError, ClientMsg: "Error creating tag assignment", Retryable: apierrors.CanRetry}
		}
		if rows == 0 {
			return nil, &apierrors.APIError{Code: http.StatusNotFound, ClientMsg: fmt.Sprintf("Template '%s' not found", template.ID), Retryable: apierrors.NoRetry}
		}
	}

	err = tx.Commit(ctx)
	if err != nil {
		telemetry.ReportCriticalError(ctx, "error when committing transaction", err)

		return nil, &apierrors.APIError{Code: http.StatusInternalServerError, ClientMsg: "Error committing transaction", Retryable: apierrors.UndefinedRetry}
	}

	for _, tag := range tags {
		a.templateCache.Invalidate(context.WithoutCancel(ctx), template.ID, &tag)
	}

	telemetry.ReportEvent(ctx, "assigned template tag")

	properties := a.posthog.GetPackageToPosthogProperties(&c.Request.Header)
	a.posthog.IdentifyAnalyticsTeam(ctx, team.ID.String(), team.Name)
	a.posthog.CreateAnalyticsTeamEvent(ctx, team.ID.String(), "assigned template tag",
		properties.
			Set("environment", template.ID).
			Set("tags", tags),
	)

	logger.L().Info(ctx, "Assigned template tag",
		logger.WithTemplateID(template.ID),
		logger.WithTeamID(team.ID.String()),
	)

	return &api.AssignedTemplateTags{
		Tags:    tags,
		BuildID: buildID,
	}, nil
}

// DeleteTemplatesTags deletes multiple tags from a template
func (a *APIStore) DeleteTemplatesTags(c *gin.Context) {
	ctx := c.Request.Context()

	body, err := ginutils.ParseBody[api.DeleteTemplateTagsRequest](ctx, c)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("Invalid request body: %s", err))

		return
	}

	// Validate and normalize tags early
	tags, err := id.ValidateAndDeduplicateTags(body.Tags)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("Invalid tag: %s", err))
		telemetry.ReportErrorByCode(ctx, http.StatusBadRequest, "invalid tag", err)

		return
	}

	if len(tags) == 0 {
		a.sendAPIStoreError(c, http.StatusBadRequest, "At least one tag is required")
		telemetry.ReportError(ctx, "at least one tag is required", nil)

		return
	}

	// Validate that no tag is the default tag
	if slices.Contains(tags, id.DefaultTag) {
		a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("Cannot delete the '%s' tag", id.DefaultTag))
		telemetry.ReportError(ctx, "cannot delete default tag", nil)

		return
	}

	team, apiErr := a.GetTeam(ctx, c, nil)
	if apiErr != nil {
		a.sendAPIStoreError(c, apiErr.Code, apiErr.ClientMsg)
		telemetry.ReportCriticalError(ctx, "error when getting team", apiErr.Err)

		return
	}

	identifier, tag, err := id.ParseName(body.Name)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("Invalid template name format: %s", err))
		telemetry.ReportError(ctx, "invalid template name format", err)

		return
	}

	if err := id.ValidateNamespaceMatchesTeam(identifier, team.Slug); err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, err.Error())

		return
	}

	if tag != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, "Template name should not contain a tag, use the 'tags' field instead")
		telemetry.ReportError(ctx, "template name contains tag", nil)

		return
	}

	aliasInfo, err := a.templateCache.ResolveAlias(ctx, identifier, team.Slug)
	if err != nil {
		apiErr := templatecache.ErrorToAPIError(err, identifier)
		a.sendAPIStoreError(c, apiErr.Code, apiErr.ClientMsg)
		telemetry.ReportError(ctx, "template not found", apiErr.Err, telemetry.WithTemplateID(identifier))

		return
	}

	if aliasInfo.TeamID != team.ID {
		a.sendAPIStoreError(c, http.StatusForbidden, fmt.Sprintf("You don't have access to sandbox template '%s'", identifier))
		telemetry.ReportError(ctx, "no access to the template", nil, telemetry.WithTemplateID(aliasInfo.TemplateID))

		return
	}

	telemetry.SetAttributes(ctx,
		attribute.String("env.team.id", team.ID.String()),
		attribute.String("env.team.name", team.Name),
		telemetry.WithTemplateID(aliasInfo.TemplateID),
	)

	// Delete the tag assignments
	err = a.sqlcDB.DeleteTemplateTags(ctx, queries.DeleteTemplateTagsParams{
		TemplateID: aliasInfo.TemplateID,
		Tags:       tags,
	})
	if err != nil {
		telemetry.ReportCriticalError(ctx, "error when deleting tag assignments", err)
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error deleting tag assignments")

		return
	}

	for _, tag := range tags {
		a.templateCache.Invalidate(context.WithoutCancel(ctx), aliasInfo.TemplateID, &tag)
	}

	telemetry.ReportEvent(ctx, "deleted template tags")

	properties := a.posthog.GetPackageToPosthogProperties(&c.Request.Header)
	a.posthog.IdentifyAnalyticsTeam(ctx, team.ID.String(), team.Name)
	a.posthog.CreateAnalyticsTeamEvent(ctx, team.ID.String(), "deleted template tags", properties.Set("environment", aliasInfo.TemplateID).Set("tags", tags))

	logger.L().Info(ctx, "Deleted template tags",
		logger.WithTemplateID(aliasInfo.TemplateID),
		logger.WithTeamID(team.ID.String()),
	)

	c.Status(http.StatusNoContent)
}

// GetTemplatesTemplateIDTags lists all tags for a template
func (a *APIStore) GetTemplatesTemplateIDTags(c *gin.Context, templateID api.TemplateID) {
	ctx := c.Request.Context()

	team, apiErr := a.GetTeam(ctx, c, nil)
	if apiErr != nil {
		a.sendAPIStoreError(c, apiErr.Code, apiErr.ClientMsg)
		telemetry.ReportCriticalError(ctx, "error when getting team", apiErr.Err)

		return
	}

	telemetry.SetAttributes(ctx,
		telemetry.WithTeamID(team.ID.String()),
	)

	identifier, _, err := id.ParseName(templateID)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("Invalid template reference: %s", err))
		telemetry.ReportError(ctx, "invalid template reference", err)

		return
	}

	aliasInfo, err := a.templateCache.ResolveAlias(ctx, identifier, team.Slug)
	if err != nil {
		apiErr := templatecache.ErrorToAPIError(err, identifier)
		a.sendAPIStoreError(c, apiErr.Code, apiErr.ClientMsg)
		telemetry.ReportError(ctx, "template not found", apiErr.Err, telemetry.WithTemplateID(identifier))

		return
	}

	if aliasInfo.TeamID != team.ID {
		a.sendAPIStoreError(c, http.StatusForbidden, fmt.Sprintf("You don't have access to sandbox template '%s'", templateID))
		telemetry.ReportError(ctx, "no access to the template", nil, telemetry.WithTemplateID(aliasInfo.TemplateID))

		return
	}

	tags, err := a.sqlcDB.ListTemplateTags(ctx, aliasInfo.TemplateID)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error when listing template tags")
		telemetry.ReportCriticalError(ctx, "error when listing template tags", err)

		return
	}

	res := make([]api.TemplateTag, 0, len(tags))
	for _, t := range tags {
		res = append(res, api.TemplateTag{
			Tag:       t.Tag,
			BuildID:   t.BuildID,
			CreatedAt: t.CreatedAt.Time,
		})
	}

	c.JSON(http.StatusOK, res)
}
