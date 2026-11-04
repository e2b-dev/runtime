package handlers

import (
	"context"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/posthog/posthog-go"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/clusters"
	"github.com/e2b-dev/infra/packages/db/queries"
	clustershared "github.com/e2b-dev/infra/packages/shared/pkg/clusters"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
)

func (a *APIStore) DeleteVolumesVolumeID(c *gin.Context, volumeID api.VolumeID) {
	ctx := c.Request.Context()

	volume, team, ok := a.getVolume(c, volumeID)
	if !ok {
		return
	}

	clusterID := clustershared.WithClusterFallback(team.ClusterID)

	if err := a.sqlcDB.DeleteVolume(ctx, queries.DeleteVolumeParams{
		TeamID:   team.ID,
		VolumeID: volume.ID,
	}); err != nil {
		telemetry.ReportCriticalError(ctx, "error when recording volume deletion", err)
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error recording volume deletion")

		return
	}

	a.posthog.CreateAnalyticsTeamEvent(ctx, team.ID.String(), "deleted_volume", posthog.NewProperties().
		Set("volume_id", volume.ID.String()).
		Set("volume_name", volume.Name),
	)

	bgCtx := context.WithoutCancel(ctx)
	a.orchestrator.GoTrackedTail(func() {
		// if this fails, we can clean it up later
		if err := a.deleteVolume(bgCtx, clusterID, volume); err != nil {
			telemetry.ReportCriticalError(bgCtx, fmt.Sprintf("failed to delete data in volume %q", volume.ID.String()), err)
		}
	})

	c.Status(http.StatusNoContent)
}

func (a *APIStore) deleteVolume(ctx context.Context, clusterID uuid.UUID, volume queries.Volume) error {
	return a.executeOnOrchestratorByClusterID(ctx, clusterID, volume, func(ctx context.Context, client *clusters.GRPCClient) error {
		_, err := client.Volumes.DeleteVolume(ctx, &orchestrator.DeleteVolumeRequest{
			Volume: toVolumeKey(volume),
		})

		return err
	})
}
