package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/posthog/posthog-go"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	templatecache "github.com/e2b-dev/infra/packages/api/internal/cache/templates"
	templatemanager "github.com/e2b-dev/infra/packages/api/internal/template-manager"
	"github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/clusters"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/ginutils"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/templates"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

const (
	jsSDKPrefix     = "e2b-js-sdk/"
	pythonSDKPrefix = "e2b-python-sdk/"
)

type dockerfileStore struct {
	FromImage    *string             `json:"from_image"`
	FromTemplate *string             `json:"from_template"`
	Steps        *[]api.TemplateStep `json:"steps"`
}

// CheckAndCancelConcurrentBuilds checks for concurrent builds and cancels them if found
func (a *APIStore) CheckAndCancelConcurrentBuilds(ctx context.Context, templateID api.TemplateID, buildID uuid.UUID, teamClusterID uuid.UUID) error {
	concurrentBuilds, err := a.sqlcDB.GetConcurrentTemplateBuilds(ctx, queries.GetConcurrentTemplateBuildsParams{
		TemplateID:     templateID,
		CurrentBuildID: buildID,
	})
	if err != nil {
		telemetry.ReportErrorByCode(ctx, http.StatusInternalServerError, "Error when getting running builds", err, telemetry.WithTemplateID(templateID), telemetry.WithBuildID(buildID.String()))

		return fmt.Errorf("error when getting running builds: %w", err)
	}

	// make sure there is no other build in progress for the same template
	if len(concurrentBuilds) > 0 {
		concurrentRunningBuilds := utils.Filter(concurrentBuilds, func(b queries.EnvBuild) bool {
			return b.StatusGroup == types.BuildStatusGroupInProgress
		})
		buildIDs := make([]templatemanager.DeleteBuild, 0, len(concurrentRunningBuilds))
		for _, b := range concurrentRunningBuilds {
			clusterNodeID := b.ClusterNodeID
			if clusterNodeID == nil {
				continue
			}

			buildIDs = append(buildIDs, templatemanager.DeleteBuild{
				TemplateID: templateID,
				BuildID:    b.ID,
				ClusterID:  teamClusterID,
				NodeID:     *clusterNodeID,
			})
		}
		telemetry.ReportEvent(ctx, "canceling running builds", attribute.StringSlice("ids", utils.Map(buildIDs, func(b templatemanager.DeleteBuild) string {
			return fmt.Sprintf("%s/%s", b.TemplateID, b.BuildID)
		})))

		deleteJobErr := a.templateManager.DeleteBuilds(ctx, buildIDs)
		if deleteJobErr != nil {
			telemetry.ReportErrorByCode(ctx, http.StatusInternalServerError, "Error when canceling running build", deleteJobErr, telemetry.WithTemplateID(templateID), telemetry.WithBuildID(buildID.String()))

			return fmt.Errorf("error when canceling running build: %w", deleteJobErr)
		}

		telemetry.ReportEvent(ctx, "canceled running builds")
	}

	return nil
}

// PostV2TemplatesTemplateIDBuildsBuildID triggers a new build
func (a *APIStore) PostV2TemplatesTemplateIDBuildsBuildID(c *gin.Context, templateID api.TemplateID, buildID api.BuildID) {
	ctx := c.Request.Context()

	body, err := ginutils.ParseBody[api.TemplateBuildStartV2](ctx, c)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("Invalid request body: %s", err))
		telemetry.ReportCriticalError(ctx, "invalid request body", err)

		return
	}

	buildUUID, err := uuid.Parse(buildID)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("Invalid build ID: %s", buildID))

		telemetry.ReportCriticalError(ctx, "invalid build ID", err)

		return
	}

	hasImage := body.FromImage != nil && *body.FromImage != ""
	hasTemplate := body.FromTemplate != nil && *body.FromTemplate != ""
	if hasImage == hasTemplate {
		message := "must specify either fromImage or fromTemplate"
		if hasImage {
			message = "cannot specify both fromImage and fromTemplate"
		}

		a.sendAPIStoreError(c, http.StatusBadRequest, message)
		telemetry.ReportErrorByCode(ctx, http.StatusBadRequest, "template build source rejected", errors.New(message), telemetry.WithTemplateID(templateID))

		return
	}

	telemetry.ReportEvent(ctx, "started environment build")

	// Check if the user has access to the template, load the template with build info
	templateBuildDB, err := a.sqlcDB.GetTemplateBuildWithTemplate(ctx, queries.GetTemplateBuildWithTemplateParams{
		TemplateID: templateID,
		BuildID:    buildUUID,
	})
	if err != nil {
		a.sendAPIStoreError(c, http.StatusNotFound, fmt.Sprintf("Error when getting template: %s", err))
		telemetry.ReportCriticalError(ctx, "error when getting env", err, telemetry.WithTemplateID(templateID))

		return
	}

	dbTeamID := templateBuildDB.ActiveEnv.TeamID.String()
	team, apiErr := a.GetTeam(ctx, c, &dbTeamID)
	if apiErr != nil {
		a.sendAPIStoreError(c, apiErr.Code, apiErr.ClientMsg)
		telemetry.ReportCriticalError(ctx, "error when getting team and tier", apiErr.Err)

		return
	}

	if team.ID != templateBuildDB.ActiveEnv.TeamID {
		a.sendAPIStoreError(c, http.StatusForbidden, "User does not have access to the template")

		telemetry.ReportCriticalError(ctx, "user does not have access to the template", err, telemetry.WithTemplateID(templateID))

		return
	}

	currentCluster, err := a.sqlcDB.GetTeamClusterForTemplateBuild(ctx, team.ID)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error when checking the team's cluster")
		telemetry.ReportCriticalError(ctx, "error when getting team cluster", err)

		return
	}
	clusterID := clusters.WithClusterFallback(templateBuildDB.ActiveEnv.ClusterID)
	if clusterID != clusters.WithClusterFallback(currentCluster) {
		apiErr := templatecache.ErrorToAPIError(templatecache.ErrClusterMismatch, templateID)
		a.sendAPIStoreError(c, apiErr.Code, apiErr.ClientMsg)
		telemetry.ReportErrorByCode(ctx, apiErr.Code, "template build start cluster mismatch", apiErr.Err, telemetry.WithTemplateID(templateID))

		return
	}

	telemetry.SetAttributes(ctx,
		telemetry.WithTeamID(team.ID.String()),
		telemetry.WithTemplateID(templateID),
	)

	// setup launch darkly context
	ctx = featureflags.AddToContext(ctx, featureflags.TemplateContext(templateID))

	// Check and cancel concurrent builds
	if err := a.CheckAndCancelConcurrentBuilds(ctx, templateID, buildUUID, clusterID); err != nil {
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error during template build request")

		return
	}

	startTime := time.Now()
	build := templateBuildDB.EnvBuild

	// only waiting builds can be triggered
	if build.StatusGroup != types.BuildStatusGroupPending {
		a.sendAPIStoreError(c, http.StatusBadRequest, "build is not in waiting state")
		telemetry.ReportErrorByCode(ctx, http.StatusBadRequest, "build is not in waiting state", fmt.Errorf("build is not in waiting state - current state: %s", build.StatusGroup), telemetry.WithTemplateID(templateID))

		return
	}

	stepsMarshalled, err := json.Marshal(dockerfileStore{
		FromImage:    body.FromImage,
		FromTemplate: body.FromTemplate,
		Steps:        body.Steps,
	})
	if err != nil {
		a.sendAPIStoreError(c, http.StatusInternalServerError, fmt.Sprintf("Error when processing steps: %s", err))
		telemetry.ReportCriticalError(ctx, "error when processing steps", err, telemetry.WithTemplateID(templateID))

		return
	}

	version, err := userAgentToTemplateVersion(ctx, logger.L().With(logger.WithTemplateID(templateID), logger.WithBuildID(buildID)), c.Request.UserAgent())
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("Error when parsing user agent: %s", err))
		telemetry.ReportErrorByCode(ctx, http.StatusBadRequest, "error when parsing user agent", err, telemetry.WithTemplateID(templateID))

		return
	}

	builderNode, err := a.templateManager.GetAvailableBuildClient(ctx, clusterID)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusServiceUnavailable, "Error when getting available build client")
		telemetry.ReportCriticalError(ctx, "error when getting available build client", err, telemetry.WithTemplateID(templateID))

		return
	}

	machineInfo := builderNode.GetMachineInfo()
	err = a.sqlcDB.UpdateTemplateBuild(ctx, queries.UpdateTemplateBuildParams{
		StartCmd:        body.StartCmd,
		ReadyCmd:        body.ReadyCmd,
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
		telemetry.ReportCriticalError(ctx, "error when updating build", err)
		a.sendAPIStoreError(c, http.StatusInternalServerError, fmt.Sprintf("Error when updating build: %s", err))

		return
	}

	// Call the Template Manager to build the environment
	err = a.templateManager.CreateTemplate(
		ctx,
		team.ID,
		team.Slug,
		templateID,
		buildUUID,
		build.KernelVersion,
		build.FirecrackerVersion,
		body.StartCmd,
		build.Vcpu,
		team.Limits.DiskMb,   // Build-time working space available before customer steps run.
		build.FreeDiskSizeMb, // Customer's free-space target after customer steps run.
		build.RamMb,
		body.ReadyCmd,
		body.FromImage,
		body.FromTemplate,
		body.FromImageRegistry,
		body.Force,
		body.Steps,
		clusterID,
		builderNode.NodeID,
		version,
	)

	a.posthog.CreateAnalyticsTeamEvent(ctx, team.ID.String(), "built environment", posthog.NewProperties().
		Set("environment", templateID).
		Set("build_id", buildID).
		Set("duration", time.Since(startTime).String()).
		Set("success", err == nil),
	)

	if err != nil {
		telemetry.ReportCriticalError(ctx, "build failed", err, telemetry.WithTemplateID(templateID))
		a.sendAPIStoreError(c, http.StatusInternalServerError, fmt.Sprintf("Error when starting template build: %s", err))

		return
	}

	c.Status(http.StatusAccepted)
}

// userAgentToTemplateVersion returns the template semver version based on the user agent string.
// If the user agent is not recognized, it defaults to the latest stable version.
func userAgentToTemplateVersion(ctx context.Context, logger logger.Logger, userAgent string) (string, error) {
	version := templates.TemplateV2LatestVersion

	for agent := range strings.FieldsSeq(userAgent) {
		switch {
		case strings.HasPrefix(agent, jsSDKPrefix):
			sdk := strings.TrimPrefix(agent, jsSDKPrefix)

			// Check if the SDK version supports the latest template version
			ok, err := utils.IsGTEVersion(sdk, templates.SDKTemplateReleaseVersion)
			if err != nil {
				return "", fmt.Errorf("parsing JS SDK version: %w", err)
			}
			if !ok {
				version = templates.TemplateV2BetaVersion
			}

			return version, nil
		case strings.HasPrefix(agent, pythonSDKPrefix):
			sdk := strings.TrimPrefix(agent, pythonSDKPrefix)

			// Check if the SDK version supports the latest template version
			ok, err := utils.IsGTEVersion(sdk, templates.SDKTemplateReleaseVersion)
			if err != nil {
				return "", fmt.Errorf("parsing Python SDK version: %w", err)
			}
			if !ok {
				version = templates.TemplateV2BetaVersion
			}

			return version, nil
		}
	}

	logger.Debug(ctx, "Unrecognized user agent, defaulting to the latest template version", zap.String("user_agent", userAgent), zap.String("version", version))

	return version, nil
}
