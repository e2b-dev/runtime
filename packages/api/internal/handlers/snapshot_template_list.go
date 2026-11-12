package handlers

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel/trace"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/utils"
	"github.com/e2b-dev/infra/packages/auth/pkg/auth"
	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
	"github.com/e2b-dev/infra/packages/shared/pkg/telemetry"
	sharedUtils "github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

const (
	snapshotTemplatesDefaultLimit = 100
	snapshotTemplatesMaxLimit     = 100
	maxSnapshotTemplateID         = "zzzzzzzzzzzzzzzzzzzzzzzz"
)

func (a *APIStore) GetSnapshots(c *gin.Context, params api.GetSnapshotsParams) {
	ctx := c.Request.Context()

	teamInfo := auth.MustGetTeamInfo(c)
	teamID := teamInfo.Team.ID

	span := trace.SpanFromContext(ctx)
	traceID := span.SpanContext().TraceID().String()
	c.Set("traceID", traceID)

	telemetry.SetAttributes(ctx, telemetry.WithTeamID(teamID.String()))
	telemetry.ReportEvent(ctx, "Listing snapshot templates")

	pagination, err := utils.NewPagination[queries.ListTeamSnapshotTemplatesRow](
		utils.PaginationParams{
			Limit:     params.Limit,
			NextToken: params.NextToken,
		},
		utils.PaginationConfig{
			DefaultLimit: snapshotTemplatesDefaultLimit,
			MaxLimit:     snapshotTemplatesMaxLimit,
			DefaultID:    maxSnapshotTemplateID,
		},
	)
	if err != nil {
		telemetry.ReportError(ctx, "error parsing pagination cursor", err)
		a.sendAPIStoreError(c, http.StatusBadRequest, "Invalid next token")

		return
	}

	var sandboxIDFilter *string
	if params.SandboxID != nil {
		short, err := utils.ShortID(*params.SandboxID)
		if err != nil {
			a.sendAPIStoreError(c, http.StatusBadRequest, "Invalid sandbox ID")

			return
		}

		sandboxIDFilter = &short
	}

	var envIDFilter, tagFilter *string
	if params.Name != nil {
		identifier, tag, err := id.ParseName(*params.Name)
		if err != nil {
			a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("Invalid name: %s", err))

			return
		}

		if err := id.ValidateNamespaceMatchesTeam(identifier, teamInfo.Slug); err != nil {
			a.sendAPIStoreError(c, http.StatusBadRequest, err.Error())

			return
		}

		// ParseName normalizes an explicit ":default" tag to nil; re-check the raw
		// input so "name:default" filters by the default tag, while a bare "name"
		// matches builds of any tag.
		tagFilter = tag
		if _, _, hasTag := strings.Cut(*params.Name, id.TagSeparator); hasTag && tagFilter == nil {
			defaultTag := id.DefaultTag
			tagFilter = &defaultTag
		}

		// Resolve alias using the cache — same pattern as template builds.
		templateID, outcome := a.resolveTemplateFilter(c, identifier, teamInfo.Slug, "error resolving snapshot template alias")
		switch outcome {
		case templateFilterResolved:
			envIDFilter = &templateID
		case templateFilterNoMatch:
			c.JSON(http.StatusOK, []api.SnapshotInfo{})

			return
		case templateFilterFailed:
			return
		}
	}

	snapshots, err := a.sqlcDB.ListTeamSnapshotTemplates(ctx, queries.ListTeamSnapshotTemplatesParams{
		TeamID:     teamID,
		SandboxID:  sandboxIDFilter,
		EnvID:      envIDFilter,
		Tag:        tagFilter,
		CursorTime: pagination.CursorTime(),
		CursorID:   pagination.CursorID(),
		PageLimit:  pagination.QueryLimit(),
	})
	if err != nil {
		telemetry.ReportCriticalError(ctx, "Error listing snapshot templates", err, telemetry.WithTeamID(teamID.String()))
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Error listing snapshot templates")

		return
	}

	snapshots = pagination.ProcessResultsWithHeader(c, snapshots, func(s queries.ListTeamSnapshotTemplatesRow) (time.Time, string) {
		return s.CreatedAt, s.SnapshotID
	})

	result := sharedUtils.Map(snapshots, func(snap queries.ListTeamSnapshotTemplatesRow) api.SnapshotInfo {
		snapshotID := id.WithTag(snap.SnapshotID, snap.Tag)
		if len(snap.Names) > 0 {
			snapshotID = id.WithTag(snap.Names[0], snap.Tag)
		}

		cpuCount := int32(snap.Vcpu)
		memoryMB := int32(snap.RamMb)
		createdAt := snap.CreatedAt
		var diskSizeMB *int32
		if snap.TotalDiskSizeMb != nil {
			d := int32(*snap.TotalDiskSizeMb)
			diskSizeMB = &d
		}

		return api.SnapshotInfo{
			SnapshotID: snapshotID,
			Names:      snap.Names,
			CpuCount:   &cpuCount,
			MemoryMB:   &memoryMB,
			DiskSizeMB: diskSizeMB,
			CreatedAt:  &createdAt,
		}
	})

	c.JSON(http.StatusOK, result)
}
