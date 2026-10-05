package handlers

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	templatecache "github.com/e2b-dev/infra/packages/api/internal/cache/templates"
	"github.com/e2b-dev/infra/packages/api/internal/template"
	"github.com/e2b-dev/infra/packages/shared/pkg/clusters"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/ginutils"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/templates"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// PostV3Templates triggers a new template build
func (a *APIStore) PostV3Templates(c *gin.Context) {
	ctx := c.Request.Context()

	body, err := ginutils.ParseBody[api.TemplateBuildRequestV3](ctx, c)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("Invalid request body: %s", err))
		telemetry.ReportCriticalError(ctx, "invalid request body", err)

		return
	}

	t := requestTemplateBuild(ctx, c, a, body)
	if t != nil {
		c.JSON(http.StatusAccepted, t)
	}
}

func requestTemplateBuild(ctx context.Context, c *gin.Context, a *APIStore, body api.TemplateBuildRequestV3) *api.TemplateRequestResponseV3 {
	telemetry.ReportEvent(ctx, "started environment build")

	// Prepare info for rebuilding env
	team, apiErr := a.GetTeam(ctx, c, body.TeamID)
	if apiErr != nil {
		a.sendAPIStoreError(c, apiErr.Code, apiErr.ClientMsg)
		telemetry.ReportCriticalError(ctx, "error when getting team, limits", apiErr.Err)

		return nil
	}
	// Determine the input based on which field is provided
	var input string
	switch {
	case body.Name != nil:
		input = *body.Name
	case body.Alias != nil:
		// Deprecated: handle alias field for backward compatibility
		input = *body.Alias
	default:
		a.sendAPIStoreError(c, http.StatusBadRequest, "Name is required")
		telemetry.ReportError(ctx, "name is required", nil)

		return nil
	}

	identifier, tag, err := id.ParseName(input)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("Invalid name: %s", err))
		telemetry.ReportError(ctx, "invalid name", err)

		return nil
	}

	if err := id.ValidateNamespaceMatchesTeam(identifier, team.Slug); err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, err.Error())

		return nil
	}

	allTags := utils.DerefOrDefault(body.Tags, nil)
	if tag != nil {
		allTags = append([]string{*tag}, allTags...)
	}

	tags, err := id.ValidateAndDeduplicateTags(allTags)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("Invalid tag: %s", err))
		telemetry.ReportErrorByCode(ctx, http.StatusBadRequest, "invalid tag", err)

		return nil
	}

	currentCluster, err := a.sqlcDB.GetTeamClusterForTemplateBuild(ctx, team.ID)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error when checking the team's cluster")
		telemetry.ReportCriticalError(ctx, "error when getting team cluster", err)

		return nil
	}
	clusterID := clusters.WithClusterFallback(currentCluster)
	findTemplateCtx, span := tracer.Start(ctx, "find-template-alias")
	defer span.End()
	templateID := id.Generate()
	public := false
	replacesTemplateID := ""

	aliasInfo, metadata, err := a.templateCache.ResolveAliasWithMetadata(findTemplateCtx, identifier, team.Slug)
	switch {
	case err == nil && aliasInfo.TeamID == team.ID:
		public = metadata.Public
		if metadata.ClusterID != clusterID {
			if aliasInfo.MatchedIdentifier == id.WithNamespace(team.Slug, id.ExtractAlias(identifier)) {
				replacesTemplateID = aliasInfo.TemplateID
			}
		} else {
			templateID = aliasInfo.TemplateID
		}
	case err == nil || errors.Is(err, templatecache.ErrTemplateNotFound):
		// Either alias not found, or found but owned by different team (e.g. promoted template)
		// Team can create their own template with this alias in their namespace
	default:
		apiErr := templatecache.ErrorToAPIError(err, identifier)
		a.sendAPIStoreError(c, apiErr.Code, apiErr.ClientMsg)
		telemetry.ReportCriticalError(findTemplateCtx, "error when getting template alias", apiErr.Err)

		return nil
	}
	span.End()

	// TODO(ENG-3852): Stop sending the firecracker/kernel versions from the API.
	// The orchestrator resolves them itself via the BuildFirecrackerVersion /
	// BuildKernelVersion feature flags (see packages/orchestrator/pkg/template/server/create_template.go).
	firecrackerVersion := a.featureFlags.StringFlag(ctx, featureflags.BuildFirecrackerVersion)
	kernelVersion := a.featureFlags.StringFlag(ctx, featureflags.BuildKernelVersion)
	buildReq := template.RegisterBuildData{
		ReplacesTemplateID: replacesTemplateID,
		Public:             public,
		ClusterID:          clusterID,
		TemplateID:         templateID,
		UserID:             nil,
		Team:               team,
		Alias:              &identifier,
		Tags:               tags,
		CpuCount:           body.CpuCount,
		MemoryMB:           body.MemoryMB,
		MinFreeDiskMb:      body.MinFreeDiskMb,
		Version:            templates.TemplateV2LatestVersion,
		KernelVersion:      kernelVersion,
		FirecrackerVersion: firecrackerVersion,
	}

	template, apiError := template.RegisterBuild(ctx, a.templateCache, a.sqlcDB, buildReq)
	if apiError != nil {
		if replacesTemplateID != "" {
			a.templateCache.InvalidateAlias(context.WithoutCancel(ctx), &team.Slug, id.ExtractAlias(identifier))
		}
		a.sendAPIStoreError(c, apiError.Code, apiError.ClientMsg)
		telemetry.ReportErrorByCode(ctx, apiError.Code, "error when requesting template build", apiError.Err, telemetry.WithTemplateID(templateID))

		return nil
	}

	// Invalidate aliases to prevent stale NotFound entries
	for _, alias := range template.Aliases {
		a.templateCache.InvalidateAlias(context.WithoutCancel(ctx), &team.Slug, alias)
	}

	posthogCtx, span := tracer.Start(ctx, "posthog-analytics")
	defer span.End()
	properties := a.posthog.GetPackageToPosthogProperties(&c.Request.Header)
	a.posthog.IdentifyAnalyticsTeam(posthogCtx, team.ID.String(), team.Name)
	a.posthog.CreateAnalyticsTeamEvent(posthogCtx, team.ID.String(), "submitted environment build request", properties.
		Set("environment", template.TemplateID).
		Set("build_id", template.BuildID).
		Set("alias", identifier).
		Set("tags", tags),
	)
	span.End()

	return &api.TemplateRequestResponseV3{
		TemplateID: template.TemplateID,
		BuildID:    template.BuildID,
		Aliases:    template.Aliases,
		Names:      template.Names,
		Tags:       template.Tags,
		Public:     template.Public,
	}
}
