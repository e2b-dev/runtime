package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/template"
	"github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/clusters"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/templates"
)

// refreshEnvdPlan is the decision derived from a source template's latest ready
// build: what to register and build so the host envd replaces the baked-in one.
type refreshEnvdPlan struct {
	// Steps is the single trivial RUN step. A FROM TEMPLATE base layer is always
	// cached, so this step gets UpdateEnvd=true and the swap is guaranteed rather
	// than left to finalize (see phases/steps/builder.go).
	Steps []api.TemplateStep
	// FromTemplate is the source the derived build bases on: the template itself,
	// resolved to its latest READY build, so the old layer is reused.
	FromTemplate string
	// CPU and RAM are inherited from the source build so the refresh does not
	// silently drop specs to the RegisterBuild defaults (2 vCPU / 1024 MiB).
	CPU int32
	RAM int32
	// Alias, when set, keeps the same name pointing at the new build (in-place).
	Alias *string
	// FromEnvdVersion is the source build's envd version, echoed back to the caller.
	FromEnvdVersion string
}

// buildRefreshEnvdPlan validates the source build and derives the refresh plan.
// Pure (no I/O) so the decisions are unit-tested without a DB. Returns an
// *api.APIError for the caller to surface. Cross-team access is NOT enforced
// here -- it is enforced upstream by GetTeamTemplate's team-scoped query, which
// returns no row (-> 404) for a template the key's team does not own.
func buildRefreshEnvdPlan(templateID string, src queries.GetTeamTemplateRow) (refreshEnvdPlan, *api.APIError) {
	if src.BuildStatus != types.BuildStatusGroupReady {
		return refreshEnvdPlan{}, &api.APIError{
			Code:      http.StatusBadRequest,
			ClientMsg: "Template has no ready build to refresh from",
			Err:       fmt.Errorf("template '%s' build status is %s", templateID, src.BuildStatus),
		}
	}
	if src.BuildVcpu == 0 || src.BuildRamMb == 0 {
		return refreshEnvdPlan{}, &api.APIError{
			Code:      http.StatusBadRequest,
			ClientMsg: fmt.Sprintf("Source build has no usable specs (cpu=%d ram=%d)", src.BuildVcpu, src.BuildRamMb),
			Err:       fmt.Errorf("source build missing specs: cpu=%d ram=%d", src.BuildVcpu, src.BuildRamMb),
		}
	}

	fromEnvdVersion := "unknown"
	if src.BuildEnvdVersion != nil {
		fromEnvdVersion = *src.BuildEnvdVersion
	}

	var alias *string
	if len(src.Aliases) > 0 {
		alias = &src.Aliases[0]
	}

	return refreshEnvdPlan{
		Steps:           []api.TemplateStep{{Type: "RUN", Args: new([]string{"true"})}},
		FromTemplate:    templateID,
		CPU:             int32(src.BuildVcpu),
		RAM:             int32(src.BuildRamMb),
		Alias:           alias,
		FromEnvdVersion: fromEnvdVersion,
	}, nil
}

// PostV2TemplatesTemplateIDRefreshEnvd rebuilds a template with the host's current
// envd. It derives a new build FROM the template's own latest ready build, which
// keeps the base layer cached so only a trivial RUN step plus finalize run -- and
// a cached source layer is exactly what drives UpdateEnvd=true in the step phase
// (see packages/orchestrator/pkg/template/build/phases/steps/builder.go), so the
// host envd binary replaces the one baked into the original build.
//
// Caveat: the step phase gates the swap on UpdateEnvd = sourceLayer.Cached &&
// !guestTemplate.KeepGuestEnvd (steps/builder.go). A template built with
// KeepGuestEnvd keeps its baked envd even through this endpoint, so the refresh
// is a no-op for it; that is intended (the template opted out of envd swaps).
// Specs and the
// alias are inherited from the source, in-place: the new build supersedes the
// default tag under the SAME templateID.
func (a *APIStore) PostV2TemplatesTemplateIDRefreshEnvd(c *gin.Context, templateID api.TemplateID) {
	ctx := c.Request.Context()

	telemetry.ReportEvent(ctx, "started envd refresh build")

	team, apiErr := a.GetTeam(ctx, c, nil)
	if apiErr != nil {
		a.sendAPIStoreError(c, apiErr.Code, apiErr.ClientMsg)
		telemetry.ReportCriticalError(ctx, "error when getting team", apiErr.Err)

		return
	}

	ctx = featureflags.AddToContext(ctx, featureflags.TemplateContext(templateID))
	telemetry.SetAttributes(ctx,
		telemetry.WithTeamID(team.ID.String()),
		telemetry.WithTemplateID(templateID),
	)

	// Team-scoped: the query filters on team_id and source='template', so another
	// team's template returns no row and 404s here.
	src, err := a.sqlcDB.GetTeamTemplate(ctx, queries.GetTeamTemplateParams{
		ID:     templateID,
		TeamID: team.ID,
	})
	if err != nil {
		a.sendAPIStoreError(c, http.StatusNotFound, fmt.Sprintf("Template '%s' not found", templateID))
		telemetry.ReportErrorByCode(ctx, http.StatusNotFound, "template not found", err, telemetry.WithTemplateID(templateID))

		return
	}

	plan, apiErr := buildRefreshEnvdPlan(templateID, src)
	if apiErr != nil {
		a.sendAPIStoreError(c, apiErr.Code, apiErr.ClientMsg)
		telemetry.ReportErrorByCode(ctx, apiErr.Code, "refresh plan rejected", apiErr.Err, telemetry.WithTemplateID(templateID))

		return
	}

	firecrackerVersion := a.featureFlags.StringFlag(ctx, featureflags.BuildFirecrackerVersion)
	kernelVersion := a.featureFlags.StringFlag(ctx, featureflags.BuildKernelVersion)

	stepsMarshalled, err := json.Marshal(dockerfileStore{FromTemplate: &plan.FromTemplate, Steps: &plan.Steps})
	if err != nil {
		a.sendAPIStoreError(c, http.StatusInternalServerError, fmt.Sprintf("Error when processing steps: %s", err))
		telemetry.ReportCriticalError(ctx, "error when processing steps", err, telemetry.WithTemplateID(templateID))

		return
	}

	reg, apiErr := template.RegisterBuild(ctx, a.templateCache, a.sqlcDB, a.featureFlags, template.RegisterBuildData{
		ClusterID:          clusters.WithClusterFallback(team.ClusterID),
		TemplateID:         templateID,
		Team:               team,
		Alias:              plan.Alias,
		Dockerfile:         string(stepsMarshalled),
		CpuCount:           &plan.CPU,
		MemoryMB:           &plan.RAM,
		Version:            templates.TemplateV2LatestVersion,
		KernelVersion:      kernelVersion,
		FirecrackerVersion: firecrackerVersion,
	})
	if apiErr != nil {
		a.sendAPIStoreError(c, apiErr.Code, apiErr.ClientMsg)
		telemetry.ReportErrorByCode(ctx, apiErr.Code, "error when registering refresh build", apiErr.Err, telemetry.WithTemplateID(templateID))

		return
	}
	for _, al := range reg.Aliases {
		a.templateCache.InvalidateAlias(context.WithoutCancel(ctx), &team.Slug, al)
	}

	buildUUID, err := uuid.Parse(reg.BuildID)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error when parsing new build ID")
		telemetry.ReportCriticalError(ctx, "invalid build ID", err, telemetry.WithTemplateID(templateID))

		return
	}

	if _, err := a.CheckAndCancelConcurrentBuilds(ctx, templateID, buildUUID, clusters.WithClusterFallback(team.ClusterID)); err != nil {
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error during refresh build request")
		telemetry.ReportCriticalError(ctx, "error cancelling concurrent builds", err, telemetry.WithTemplateID(templateID))

		return
	}

	builderNode, err := a.templateManager.GetAvailableBuildClient(ctx, clusters.WithClusterFallback(team.ClusterID), nil)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusServiceUnavailable, "Error when getting available build client")
		telemetry.ReportCriticalError(ctx, "error getting build client", err, telemetry.WithTemplateID(templateID))

		return
	}

	machineInfo := builderNode.GetMachineInfo()
	err = a.sqlcDB.UpdateTemplateBuild(ctx, queries.UpdateTemplateBuildParams{
		Dockerfile:      new(string(stepsMarshalled)),
		ClusterNodeID:   new(builderNode.NodeID),
		CpuArchitecture: new(machineInfo.CPUArchitecture),
		CpuFamily:       new(machineInfo.CPUFamily),
		CpuModel:        new(machineInfo.CPUModel),
		CpuModelName:    new(machineInfo.CPUModelName),
		CpuFlags:        machineInfo.CPUFlags,
		BuildUuid:       buildUUID,
	})
	if err != nil {
		a.sendAPIStoreError(c, http.StatusInternalServerError, fmt.Sprintf("Error when updating build: %s", err))
		telemetry.ReportCriticalError(ctx, "error updating build", err, telemetry.WithTemplateID(templateID))

		return
	}

	// Reload the freshly-registered build for its resolved free-disk target and the
	// seeded kernel/fc versions, exactly as the v2 start handler passes them on.
	built, err := a.sqlcDB.GetTemplateBuildWithTemplate(ctx, queries.GetTemplateBuildWithTemplateParams{
		TemplateID: templateID,
		BuildID:    buildUUID,
	})
	if err != nil {
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error when reloading registered build")
		telemetry.ReportCriticalError(ctx, "error reloading build", err, telemetry.WithTemplateID(templateID))

		return
	}
	build := built.EnvBuild

	version, err := userAgentToTemplateVersion(ctx, logger.L().With(logger.WithTemplateID(templateID), logger.WithBuildID(reg.BuildID)), c.Request.UserAgent())
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("Error when parsing user agent: %s", err))
		telemetry.ReportErrorByCode(ctx, http.StatusBadRequest, "error parsing user agent", err, telemetry.WithTemplateID(templateID))

		return
	}

	err = a.templateManager.CreateTemplate(
		ctx,
		team.ID,
		team.Slug,
		templateID,
		buildUUID,
		build.KernelVersion,
		build.FirecrackerVersion,
		nil, // startCmd: the derived build re-runs the source's baked config
		build.Vcpu,
		team.Limits.DiskMb,
		build.FreeDiskSizeMb,
		build.RamMb,
		build.MaxVcpu, // maxVCpuCount: inherit the source build's autoscale ceiling
		nil,           // readyCmd
		nil,           // fromImage
		&plan.FromTemplate,
		nil, // fromImageRegistry
		nil, // force
		&plan.Steps,
		clusters.WithClusterFallback(team.ClusterID),
		builderNode.NodeID,
		version,
	)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusInternalServerError, fmt.Sprintf("Error when starting refresh build: %s", err))
		telemetry.ReportCriticalError(ctx, "refresh build failed", err, telemetry.WithTemplateID(templateID))

		return
	}

	c.JSON(http.StatusAccepted, api.TemplateRefreshEnvdResponse{
		TemplateID:      templateID,
		BuildID:         reg.BuildID,
		FromEnvdVersion: plan.FromEnvdVersion,
		Aliases:         &reg.Aliases,
	})
}
