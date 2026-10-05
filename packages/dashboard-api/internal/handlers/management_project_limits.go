package handlers

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel/attribute"

	"github.com/e2b-dev/infra/packages/dashboard-api/internal/api"
	"github.com/e2b-dev/infra/packages/dashboard-api/internal/management"
	"github.com/e2b-dev/infra/packages/shared/pkg/ginutils"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

// ManagementUpsertProjectLimits records a project's effective limits.
//
// Idempotent, because the caller retries. The revision is what makes that hold
// for a delayed retry too: a delivery at or below the one this side has recorded
// arrived after a newer one and is dropped, and the caller is told 204 either
// way -- what it asked for is stored, and a newer answer is not this call's to
// undo.
func (s *APIStore) ManagementUpsertProjectLimits(c *gin.Context, projectID api.ProjectID) {
	ctx := c.Request.Context()
	attrs := []attribute.KeyValue{telemetry.WithTeamID(projectID.String())}

	body, err := ginutils.ParseBody[api.ManagementProjectLimits](ctx, c)
	if err != nil {
		telemetry.ReportErrorByCode(ctx, http.StatusBadRequest, "upsert project limits failed",
			fmt.Errorf("parse project limits request: %w", err), attrs...)
		s.sendAPIStoreError(c, http.StatusBadRequest, "Invalid request body")

		return
	}

	maxFreeDiskSizeMB, err := maxFreeDiskSize(body)
	if err != nil {
		telemetry.ReportErrorByCode(ctx, http.StatusBadRequest, "upsert project limits failed", err, attrs...)
		s.sendAPIStoreError(c, http.StatusBadRequest, err.Error())

		return
	}

	if err := s.managementService.ApplyProjectLimits(ctx, management.ProjectLimitsProjection{
		ProjectID:                projectID,
		Revision:                 body.Revision,
		DecidedAt:                utils.DerefOrDefault(body.DecidedAt, time.Time{}),
		MaxLengthHours:           int64(body.MaxSandboxLengthHours),
		ConcurrentSandboxes:      int64(body.ConcurrentSandboxes),
		ConcurrentTemplateBuilds: int64(body.ConcurrentTemplateBuilds),
		MaxVCPU:                  int64(body.MaxVcpu),
		MaxRAMMB:                 body.MaxRamMb,
		DiskMB:                   body.DiskMb,
		EventsTTLDays:            int64(body.EventsTtlDays),
		DefaultFreeDiskSizeMB:    body.DefaultFreeDiskSizeMb,
		MaxFreeDiskSizeMB:        maxFreeDiskSizeMB,
		APITeamRPSList:           body.ApiTeamRpsList,
	}); err != nil {
		s.sendProjectLimitsError(c, err, attrs...)

		return
	}

	c.Status(http.StatusNoContent)
}

// One ceiling under two names, so a caller may send either. Sending both is what
// a caller does while receivers of both vintages are running, and then they have
// to agree: picking a side would store a limit nobody asked for.
func maxFreeDiskSize(limits api.ManagementProjectLimits) (int64, error) {
	switch {
	case limits.MaxFreeDiskSizeMb != nil && limits.MaxDiskSizeMb != nil:
		if *limits.MaxFreeDiskSizeMb != *limits.MaxDiskSizeMb {
			return 0, fmt.Errorf("max_free_disk_size_mb %d and max_disk_size_mb %d must be equal",
				*limits.MaxFreeDiskSizeMb, *limits.MaxDiskSizeMb)
		}

		return *limits.MaxFreeDiskSizeMb, nil
	case limits.MaxFreeDiskSizeMb != nil:
		return *limits.MaxFreeDiskSizeMb, nil
	case limits.MaxDiskSizeMb != nil:
		return *limits.MaxDiskSizeMb, nil
	default:
		return 0, errors.New("one of max_free_disk_size_mb or max_disk_size_mb is required")
	}
}
