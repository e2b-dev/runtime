package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/utils"
	"github.com/e2b-dev/infra/packages/db/pkg/dberrors"
	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
)

func (a *APIStore) GetUnstableAdminSandboxesSandboxIDOverview(c *gin.Context, sandboxID api.SandboxID) {
	ctx := c.Request.Context()

	sandboxID, err := utils.ShortID(sandboxID)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, "Invalid sandbox ID")

		return
	}

	// Reads the primary rather than the replica, so a restore shows at once.
	var logTeamID *uuid.UUID
	teamID, err := a.sqlcDB.GetAdminSandboxOverviewLogTeamID(ctx, sandboxID)
	switch {
	case err == nil:
		logTeamID = &teamID
	case dberrors.IsNotFoundError(err), dberrors.IsUndefinedTable(err):
		// A database without the stopped-sandbox log falls back to the snapshot.
	default:
		logger.L().Error(ctx, "Failed to get sandbox team from the stopped-sandbox log", zap.Error(err), logger.WithSandboxID(sandboxID))
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to get sandbox overview")

		return
	}

	team, err := a.sqlcDB.GetAdminSandboxOverviewTeam(ctx, queries.GetAdminSandboxOverviewTeamParams{LogTeamID: logTeamID, SandboxID: sandboxID})
	if dberrors.IsNotFoundError(err) {
		a.sendAPIStoreError(c, http.StatusNotFound, "Sandbox not found")

		return
	}
	if err != nil {
		logger.L().Error(ctx, "Failed to get sandbox team", zap.Error(err), logger.WithSandboxID(sandboxID))
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to get sandbox overview")

		return
	}

	overview := api.AdminSandboxOverview{
		SandboxId: sandboxID,
		Team: api.AdminSandboxOverviewTeam{
			Id:    team.ID,
			Name:  &team.Name,
			Email: &team.Email,
		},
	}

	snapshot, err := a.sqlcDB.GetAdminSandboxOverviewLastPause(ctx, sandboxID)
	if dberrors.IsNotFoundError(err) {
		c.JSON(http.StatusOK, overview)

		return
	}
	if err != nil {
		logger.L().Error(ctx, "Failed to get sandbox snapshot", zap.Error(err), logger.WithSandboxID(sandboxID))
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to get sandbox overview")

		return
	}

	overview.SnapshotTemplateId = &snapshot.EnvID
	overview.LastPause = adminSandboxLastPause(snapshot)

	builds, err := a.sqlcDB.ListAdminSandboxOverviewBuilds(ctx, queries.ListAdminSandboxOverviewBuildsParams{TemplateID: snapshot.EnvID, Tag: id.DefaultTag})
	if err != nil {
		logger.L().Error(ctx, "Failed to list sandbox builds", zap.Error(err), logger.WithSandboxID(sandboxID))
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to get sandbox overview")

		return
	}

	overview.BuildCount = int64(len(builds))
	if len(builds) > 0 {
		overview.LatestBuild = adminSandboxLatestBuild(builds[0])
	}

	c.JSON(http.StatusOK, overview)
}

// adminSandboxLastPause reads the pause kinds from the snapshot config. Rows
// written before filesystem-only pauses existed have neither flag, which reads
// as a memory snapshot, the only kind there was.
func adminSandboxLastPause(snapshot queries.GetAdminSandboxOverviewLastPauseRow) *api.AdminSandboxLastPause {
	lastPause := &api.AdminSandboxLastPause{
		Kind:          api.AdminSandboxPauseKindMemory,
		AutoPause:     snapshot.AutoPause,
		AutoPauseKind: api.AdminSandboxPauseKindMemory,
	}
	if snapshot.Config == nil {
		return lastPause
	}
	if snapshot.Config.FilesystemOnly {
		lastPause.Kind = api.AdminSandboxPauseKindFilesystem
	}
	if snapshot.Config.AutoPauseFilesystemOnly {
		lastPause.AutoPauseKind = api.AdminSandboxPauseKindFilesystem
	}

	return lastPause
}

// adminSandboxLatestBuild describes the newest assignment, the build the
// template resolves to, so it is current and never restorable.
func adminSandboxLatestBuild(row queries.ListAdminSandboxOverviewBuildsRow) *api.AdminSandboxOverviewBuild {
	build := &api.AdminSandboxOverviewBuild{
		BuildId:       row.BuildID,
		TemplateId:    row.EnvID,
		Tag:           row.Tag,
		Status:        string(row.StatusGroup),
		ClusterNodeId: row.ClusterNodeID,
		AssignedAt:    row.AssignedAt,
		IsCurrent:     true,
		Restorable:    false,
	}
	// A group outside the enum is reported as null rather than as an
	// undeclared value.
	if group := api.AdminSandboxBuildStatusGroup(row.StatusGroup); group.Valid() {
		build.StatusGroup = &group
	}

	return build
}
