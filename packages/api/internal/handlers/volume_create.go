package handlers

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/clusters"
	"github.com/e2b-dev/infra/packages/auth/pkg/types"
	"github.com/e2b-dev/infra/packages/db/pkg/dberrors"
	"github.com/e2b-dev/infra/packages/db/queries"
	clustershared "github.com/e2b-dev/infra/packages/shared/pkg/clusters"
	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/ginutils"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

func (a *APIStore) PostVolumes(c *gin.Context) {
	ctx := c.Request.Context()

	// get team
	team, apiErr := a.GetTeam(ctx, c, nil)
	if apiErr != nil {
		a.sendAPIStoreError(c, apiErr.Code, apiErr.ClientMsg)
		telemetry.ReportCriticalError(ctx, "error when getting team", apiErr.Err)

		return
	}

	telemetry.SetAttributes(ctx,
		telemetry.WithTeamID(team.ID.String()),
	)

	if !a.featureFlags.BoolFlag(ctx, featureflags.PersistentVolumesFlag) {
		a.sendAPIStoreError(c, http.StatusForbidden, "use of volumes is not enabled")

		return
	}

	// Fail fast before allocating any resources if token signing is not configured,
	// otherwise we would persist a volume that we cannot mint a content token for.
	if !a.config.VolumesToken.IsConfigured() {
		a.sendAPIStoreError(c, http.StatusNotImplemented, ErrVolumesTokenNotConfigured.Error())
		telemetry.ReportError(ctx, "volumes content token signing is not configured", ErrVolumesTokenNotConfigured)

		return
	}

	// parse body
	body, err := ginutils.ParseBody[api.PostVolumesJSONRequestBody](ctx, c)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("Error when parsing request: %s", err))

		telemetry.ReportCriticalError(ctx, "error when parsing request", err)

		return
	}

	telemetry.ReportEvent(ctx, "Parsed body")

	// validate body
	if !isValidVolumeName(body.Name) {
		a.sendAPIStoreError(c, http.StatusBadRequest, "Invalid volume name")
		telemetry.ReportError(ctx, "invalid volume name", nil)

		return
	}

	ctx = featureflags.AddToContext(ctx,
		featureflags.VolumeContext(body.Name),
		featureflags.TeamContext(team.ID.String()),
	)

	volumeType := a.getVolumeType(ctx, team)
	if volumeType == "" {
		a.sendAPIStoreError(c, http.StatusInternalServerError, "No persistent volume type is configured")
		telemetry.ReportCriticalError(ctx, "default persistent volume type is not configured", nil)

		return
	}

	clusterID := clustershared.WithClusterFallback(team.ClusterID)

	// Resolve the BYOC domain up front so we fail before allocating any
	// resources if the team's cluster can't be found.
	domain, err := a.volumeContentDomain(team)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusServiceUnavailable, "Cluster not found")
		telemetry.ReportError(ctx, "cluster not found", err)

		return
	}

	// The volume identity we intend to create. We generate the ID up front so we
	// can hand it to the orchestrator, which is given the chance to adjust these
	// values (e.g. resolve a placeholder volume type) and returns the definitive
	// values we then persist.
	volume := queries.Volume{
		ID:         uuid.New(),
		TeamID:     team.ID,
		Name:       body.Name,
		VolumeType: volumeType,
	}

	response, err := a.createVolume(ctx, clusterID, volume)
	if err != nil {
		if errors.Is(err, ErrClusterNotFound) {
			a.sendAPIStoreError(c, http.StatusServiceUnavailable, "Cluster not found")
			telemetry.ReportError(ctx, "cluster not found", err)

			return
		}

		if errors.Is(err, ErrUnknownVolumeType) {
			a.sendAPIStoreError(c, http.StatusInternalServerError, "Unknown volume type")
			telemetry.ReportError(ctx, "Unknown volume type", err)

			return
		}

		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error when creating directory")
		telemetry.ReportCriticalError(ctx, "error when creating directory", err)

		return
	}

	// The orchestrator may have changed the volume type; persist
	// whatever it returned. Older orchestrators leave this empty, in which case
	// we fall back to the values we sent.
	if volumeType := response.GetVolumeType(); volumeType != "" {
		volume.VolumeType = volumeType
	}

	created, err := a.sqlcDB.CreateVolume(ctx, queries.CreateVolumeParams{
		ID:         volume.ID,
		TeamID:     volume.TeamID,
		Name:       volume.Name,
		VolumeType: volume.VolumeType,
	})

	switch {
	case dberrors.IsUniqueConstraintViolation(err):
		a.cleanupOrchestratorVolume(ctx, clusterID, volume)

		a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("Volume with name '%s' already exists", body.Name))
		telemetry.ReportError(ctx, "volume already exists", err)

		return
	case err != nil:
		a.cleanupOrchestratorVolume(ctx, clusterID, volume)

		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error when creating volume")
		telemetry.ReportCriticalError(ctx, "error when creating volume", err)

		return
	default:
	}

	volume = created

	a.posthog.IdentifyAnalyticsTeam(ctx, team.ID.String(), team.Name)
	properties := a.posthog.GetPackageToPosthogProperties(&c.Request.Header)
	a.posthog.CreateAnalyticsTeamEvent(ctx, team.ID.String(), "created_volume", properties.
		Set("volume_id", volume.ID.String()).
		Set("volume_name", volume.Name).
		Set("volume_type", volume.VolumeType),
	)

	token, apiErr := generateVolumeContentToken(a.config.VolumesToken, volume, team, a.volumeTokenAudience(domain))
	if apiErr != nil {
		a.sendAPIStoreError(c, apiErr.Code, apiErr.ClientMsg)
		telemetry.ReportCriticalError(ctx, apiErr.ClientMsg, apiErr.Err)

		return
	}

	result := api.VolumeAndToken{
		VolumeID: volume.ID.String(),
		Name:     volume.Name,
		Token:    token,
		Domain:   domain,
	}

	c.JSON(http.StatusCreated, result)
}

const (
	// regionNodeLabelPrefix marks the node label naming the region a node runs
	// in, e.g. "region=us-west3". Regions live on nodes, never on teams:
	// Terraform appends the label to every client cluster automatically.
	regionNodeLabelPrefix = "region="

	// defaultSchedulingLabel is the pool a team without scheduling labels of
	// its own lands on.
	defaultSchedulingLabel = "default"
)

// getVolumeType resolves the volume type a new volume of the given team gets,
// in order of precedence: the LaunchDarkly override, the per-region default of
// the region the team schedules into, and finally the deployment-wide default.
//
// Node labels only answer *where* the team runs; *what* a new volume there
// should be is policy and comes exclusively from the region map. A region
// mounting several volume types therefore never needs runtime guessing - the
// map names its default explicitly.
func (a *APIStore) getVolumeType(ctx context.Context, team *types.Team) string {
	if volumeType := a.featureFlags.StringFlag(ctx, featureflags.DefaultPersistentVolumeType); volumeType != "" {
		return volumeType
	}

	if team != nil && team.ClusterID != nil {
		return a.config.PlaceholderPersistentVolumeType
	}

	// Regional defaulting is opt-in: without a map there is nothing to look
	// up, so don't walk the cluster.
	if len(a.config.DefaultPersistentVolumeTypeByRegion) == 0 || a.orchestrator == nil {
		return a.config.DefaultPersistentVolumeType
	}

	// Mirrors generateRequiredNodeLabels: a team without labels of its own
	// runs on the "default" pool, so we resolve the region over the same set
	// of nodes that placement would choose from.
	requiredLabels := team.SandboxSchedulingLabels
	if len(requiredLabels) == 0 {
		requiredLabels = []string{defaultSchedulingLabel}
	}

	// Collect the distinct regions advertised by the ready nodes carrying all
	// of requiredLabels; the same subset semantics as sandbox placement.
	regions := make(map[string]struct{})
	for _, node := range a.orchestrator.GetClusterNodes(clustershared.WithClusterFallback(team.ClusterID)) {
		if !node.CanAcceptNewRequests() {
			continue
		}

		labels := node.Labels()
		if !hasAllLabels(labels, requiredLabels) {
			continue
		}

		for label := range labels {
			if region, ok := strings.CutPrefix(label, regionNodeLabelPrefix); ok && region != "" {
				regions[region] = struct{}{}
			}
		}
	}

	// Exactly one region with a mapped default is the only affirmative
	// answer. Zero regions (labels match nothing yet) or several (labels do
	// not pin a region) fall open to the deployment-wide default.
	if len(regions) == 1 {
		for region := range regions {
			if volumeType, ok := a.config.DefaultPersistentVolumeTypeByRegion[region]; ok {
				return volumeType
			}
		}
	}

	return a.config.DefaultPersistentVolumeType
}

func hasAllLabels(labels map[string]struct{}, requiredLabels []string) bool {
	for _, required := range requiredLabels {
		if _, ok := labels[required]; !ok {
			return false
		}
	}

	return true
}

var validVolumeNameRegex = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

func isValidVolumeName(name string) bool {
	return validVolumeNameRegex.MatchString(name)
}

// cleanupOrchestratorVolume best-effort deletes an orchestrator volume that was
// created before the database row could be persisted, so we don't leak the
// underlying directory. It runs in the background and only logs on failure.
func (a *APIStore) cleanupOrchestratorVolume(ctx context.Context, clusterID uuid.UUID, volume queries.Volume) {
	bgCtx := context.WithoutCancel(ctx)
	a.orchestrator.GoTrackedTail(func() {
		if err := a.deleteVolume(bgCtx, clusterID, volume); err != nil {
			telemetry.ReportCriticalError(bgCtx, "failed to clean up volume after failing to persist it", err)
		}
	})
}

func (a *APIStore) createVolume(ctx context.Context, clusterID uuid.UUID, volume queries.Volume) (response *orchestrator.CreateVolumeResponse, err error) {
	err = a.executeOnOrchestratorByClusterID(ctx, clusterID, volume, func(ctx context.Context, client *clusters.GRPCClient) error {
		response, err = client.Volumes.CreateVolume(ctx, &orchestrator.CreateVolumeRequest{
			Volume: toVolumeKey(volume),
		})

		return err
	})

	return
}
