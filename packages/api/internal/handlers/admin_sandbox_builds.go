package handlers

import (
	"context"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/utils"
	"github.com/e2b-dev/infra/packages/db/pkg/dberrors"
	"github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/db/queries"
	"github.com/e2b-dev/infra/packages/shared/pkg/ginutils"
	"github.com/e2b-dev/infra/packages/shared/pkg/id"
	"github.com/e2b-dev/infra/packages/shared/pkg/logger"
	sharedutils "github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

const adminSandboxBuildListDefaultLimit = 50

func (a *APIStore) GetUnstableAdminSandboxesSandboxIDBuilds(c *gin.Context, sandboxID api.SandboxID, params api.GetUnstableAdminSandboxesSandboxIDBuildsParams) {
	ctx := c.Request.Context()

	sandboxID, err := utils.ShortID(sandboxID)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, "Invalid sandbox ID")

		return
	}

	list := api.AdminSandboxBuildList{Items: []api.AdminSandboxBuild{}}

	// Reads the primary rather than the replica, so a restore shows at once.
	snapshot, err := a.sqlcDB.GetAdminSandboxBuildsTemplate(ctx, sandboxID)
	if dberrors.IsNotFoundError(err) {
		c.JSON(http.StatusOK, list)

		return
	}
	if err != nil {
		logger.L().Error(ctx, "Failed to get sandbox snapshot", zap.Error(err), logger.WithSandboxID(sandboxID))
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to get sandbox snapshot")

		return
	}

	templateID := snapshot.EnvID
	list.SnapshotTemplateID = &templateID
	tag := sharedutils.DerefOrDefault(params.Tag, id.DefaultTag)

	// The status filter never applies here, so it cannot make an older build
	// current.
	currentBuildID, err := a.sqlcDB.GetAdminSandboxCurrentBuildID(ctx, queries.GetAdminSandboxCurrentBuildIDParams{TemplateID: templateID, Tag: tag})
	if err != nil && !dberrors.IsNotFoundError(err) {
		logger.L().Error(ctx, "Failed to get current sandbox build", zap.Error(err), logger.WithSandboxID(sandboxID))
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to list sandbox builds")

		return
	}

	var statusGroups []string
	for _, group := range sharedutils.FromPtr(params.StatusGroup) {
		statusGroups = append(statusGroups, string(group))
	}

	rows, err := a.sqlcDB.ListAdminSandboxBuilds(ctx, queries.ListAdminSandboxBuildsParams{
		TemplateID:     templateID,
		Tag:            tag,
		ByStatusGroups: len(statusGroups) > 0,
		StatusGroups:   statusGroups,
		Descending:     sharedutils.DerefOrDefault(params.Order, api.Desc) == api.Desc,
		OffsetCount:    int64(sharedutils.FromPtr(params.Offset)),
		PageSize:       int64(sharedutils.DerefOrDefault(params.Limit, adminSandboxBuildListDefaultLimit)),
	})
	if err != nil {
		logger.L().Error(ctx, "Failed to list sandbox builds", zap.Error(err), logger.WithSandboxID(sandboxID))
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to list sandbox builds")

		return
	}

	list.Total, err = a.sqlcDB.CountAdminSandboxBuilds(ctx, queries.CountAdminSandboxBuildsParams{
		TemplateID:     templateID,
		Tag:            tag,
		ByStatusGroups: len(statusGroups) > 0,
		StatusGroups:   statusGroups,
	})
	if err != nil {
		logger.L().Error(ctx, "Failed to count sandbox builds", zap.Error(err), logger.WithSandboxID(sandboxID))
		a.sendAPIStoreError(c, http.StatusInternalServerError, "Failed to list sandbox builds")

		return
	}

	for _, row := range rows {
		isCurrent := row.BuildID == currentBuildID
		list.Items = append(list.Items, api.AdminSandboxBuild{
			BuildID:       row.BuildID,
			TemplateID:    row.EnvID,
			Tag:           row.Tag,
			Status:        string(row.StatusGroup),
			StatusGroup:   api.AdminSandboxBuildStatusGroup(row.StatusGroup),
			ClusterNodeID: row.ClusterNodeID,
			AssignedAt:    row.AssignedAt,
			IsCurrent:     isCurrent,
			// The restore route only looks up default-tag assignments, and
			// refuses a deleted template, which is never resumable.
			Restorable: !isCurrent && row.StatusGroup == types.BuildStatusGroupReady && row.Tag == id.DefaultTag && snapshot.Resumable,
		})
	}

	c.JSON(http.StatusOK, list)
}

func (a *APIStore) PostUnstableAdminSandboxesSandboxIDBuildsRestore(c *gin.Context, sandboxID api.SandboxID) {
	ctx := c.Request.Context()

	sandboxID, err := utils.ShortID(sandboxID)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, "Invalid sandbox ID")

		return
	}

	body, err := ginutils.ParseBody[api.AdminSandboxBuildRestoreRequest](ctx, c)
	if err != nil {
		a.sendAPIStoreError(c, http.StatusBadRequest, fmt.Sprintf("Error when parsing request: %s", err))

		return
	}

	result, apiErr := a.restoreAdminSandboxBuild(ctx, sandboxID, body.BuildID)
	if apiErr != nil {
		if apiErr.Code == http.StatusInternalServerError {
			logger.L().Error(ctx, "Failed to restore sandbox build", zap.Error(apiErr.Err), logger.WithSandboxID(sandboxID), logger.WithBuildID(body.BuildID.String()))
		}

		a.sendAPIStoreError(c, apiErr.Code, apiErr.ClientMsg)

		return
	}

	// Resume and connect read the build from the snapshot cache, which still
	// holds the previous build.
	a.snapshotCache.Invalidate(context.WithoutCancel(ctx), sandboxID)

	c.JSON(http.StatusOK, result)
}

// restoreAdminSandboxBuild checks and writes the new default-tag assignment in
// one transaction, holding the template row so a concurrent restore waits and
// then sees this one's build as current.
func (a *APIStore) restoreAdminSandboxBuild(ctx context.Context, sandboxID string, buildID uuid.UUID) (api.AdminSandboxBuildRestoreResult, *api.APIError) {
	client, tx, err := a.sqlcDB.WithTx(ctx)
	if err != nil {
		return api.AdminSandboxBuildRestoreResult{}, adminSandboxBuildRestoreFailed(fmt.Errorf("begin transaction: %w", err))
	}
	defer tx.Rollback(ctx)

	templateID, err := client.LockAdminSandboxSnapshotTemplate(ctx, sandboxID)
	if dberrors.IsNotFoundError(err) {
		return api.AdminSandboxBuildRestoreResult{}, &api.APIError{Code: http.StatusNotFound, ClientMsg: "Sandbox has no snapshot", Err: err}
	}
	if err != nil {
		return api.AdminSandboxBuildRestoreResult{}, adminSandboxBuildRestoreFailed(fmt.Errorf("lock snapshot template: %w", err))
	}

	build, err := client.GetAdminSandboxRestoreBuild(ctx, queries.GetAdminSandboxRestoreBuildParams{
		TemplateID: templateID,
		BuildID:    buildID,
		Tag:        id.DefaultTag,
	})
	if dberrors.IsNotFoundError(err) {
		return api.AdminSandboxBuildRestoreResult{}, &api.APIError{Code: http.StatusNotFound, ClientMsg: "Build is not assigned to the sandbox's snapshot template", Err: err}
	}
	if err != nil {
		return api.AdminSandboxBuildRestoreResult{}, adminSandboxBuildRestoreFailed(fmt.Errorf("get build status: %w", err))
	}
	if build.StatusGroup != types.BuildStatusGroupReady {
		return api.AdminSandboxBuildRestoreResult{}, &api.APIError{Code: http.StatusConflict, ClientMsg: "Only ready builds can be restored", Err: fmt.Errorf("build status group is %q", build.StatusGroup)}
	}

	var previousBuildID *uuid.UUID
	currentBuildID, err := client.GetAdminSandboxCurrentBuildID(ctx, queries.GetAdminSandboxCurrentBuildIDParams{TemplateID: templateID, Tag: id.DefaultTag})
	switch {
	case dberrors.IsNotFoundError(err):
	case err != nil:
		return api.AdminSandboxBuildRestoreResult{}, adminSandboxBuildRestoreFailed(fmt.Errorf("get current build: %w", err))
	case currentBuildID == buildID:
		return api.AdminSandboxBuildRestoreResult{}, &api.APIError{Code: http.StatusConflict, ClientMsg: "Build is already the current build", Err: fmt.Errorf("build %s is current", buildID)}
	default:
		previousBuildID = &currentBuildID
	}

	assignedAt, err := client.CreateAdminSandboxBuildAssignment(ctx, queries.CreateAdminSandboxBuildAssignmentParams{
		TemplateID: templateID,
		BuildID:    buildID,
		Tag:        id.DefaultTag,
	})
	if err != nil {
		return api.AdminSandboxBuildRestoreResult{}, adminSandboxBuildRestoreFailed(fmt.Errorf("create build assignment: %w", err))
	}

	// The snapshot row's kind must describe its latest ready build, which is
	// now this one. A build older than its own kind column reads as a memory
	// snapshot whatever it was, since nothing else records its kind.
	if err := client.UpdateSnapshotFilesystemOnly(ctx, queries.UpdateSnapshotFilesystemOnlyParams{
		SandboxID:      sandboxID,
		FilesystemOnly: build.FilesystemOnly,
	}); err != nil {
		return api.AdminSandboxBuildRestoreResult{}, adminSandboxBuildRestoreFailed(fmt.Errorf("record snapshot kind: %w", err))
	}

	if err := tx.Commit(ctx); err != nil {
		return api.AdminSandboxBuildRestoreResult{}, adminSandboxBuildRestoreFailed(fmt.Errorf("commit transaction: %w", err))
	}

	return api.AdminSandboxBuildRestoreResult{
		SandboxID:       sandboxID,
		TemplateID:      templateID,
		BuildID:         buildID,
		AssignedAt:      assignedAt,
		PreviousBuildID: previousBuildID,
	}, nil
}

func adminSandboxBuildRestoreFailed(err error) *api.APIError {
	return &api.APIError{Code: http.StatusInternalServerError, ClientMsg: "Failed to restore sandbox build", Err: err}
}
