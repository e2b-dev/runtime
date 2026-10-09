package template_manager

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	dbtypes "github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/db/queries"
	templatemanagergrpc "github.com/e2b-dev/infra/packages/shared/pkg/grpc/template-manager"
)

var _ buildCanceller = (*cancelRecorder)(nil)

// cancelRecorder answers the fenced write the way the query does: a build that
// another writer already ended reports the loss.
type cancelRecorder struct {
	lost           bool
	setStatusErr   error
	deleteBuildErr error
	// onSetStatus runs once the status write has been made, so a test can end
	// the request the way a caller hanging up does.
	onSetStatus func()

	statusCalls  int
	reason       string
	deleteCalls  int
	deleteCtxErr error
}

func (c *cancelRecorder) SetTerminalStatus(_ context.Context, _ uuid.UUID, _ dbtypes.BuildStatusGroup, reason *templatemanagergrpc.TemplateBuildStatusReason) (bool, error) {
	c.statusCalls++
	c.reason = reason.GetMessage()

	if c.onSetStatus != nil {
		c.onSetStatus()
	}

	return !c.lost, c.setStatusErr
}

func (c *cancelRecorder) DeleteBuild(ctx context.Context, _ uuid.UUID, _ string, _ uuid.UUID, _ string) error {
	c.deleteCalls++
	c.deleteCtxErr = ctx.Err()

	return c.deleteBuildErr
}

func cancellableBuild() queries.GetCancellableTemplateBuildsByTeamRow {
	nodeID := "node-1"

	return queries.GetCancellableTemplateBuildsByTeamRow{
		BuildID:       uuid.New(),
		TemplateID:    "tmpl-1",
		ClusterID:     nil,
		ClusterNodeID: &nodeID,
	}
}

func TestCancelBuild_StopsTheBuildItEnded(t *testing.T) {
	t.Parallel()

	tm := &cancelRecorder{}

	require.NoError(t, cancelBuild(t.Context(), tm, cancellableBuild(), "cancelled by admin"))

	require.Equal(t, 1, tm.statusCalls)
	require.Equal(t, 1, tm.deleteCalls, "a build this call ended must be stopped on its node")
}

func TestCancelBuild_RecordsTheCallersReason(t *testing.T) {
	t.Parallel()

	tm := &cancelRecorder{}

	require.NoError(t, cancelBuild(t.Context(), tm, cancellableBuild(), "cancelled: team deleted"))

	require.Equal(t, "cancelled: team deleted", tm.reason)
}

func TestCancelBuild_LeavesABuildThatEndedOnItsOwn(t *testing.T) {
	t.Parallel()

	// The build reached a terminal state between the listing and this call. Its
	// artifacts belong to that outcome, and the node-side delete would remove
	// them while the row still points at them.
	tm := &cancelRecorder{lost: true}

	require.NoError(t, cancelBuild(t.Context(), tm, cancellableBuild(), "cancelled by admin"))

	require.Equal(t, 1, tm.statusCalls)
	require.Zero(t, tm.deleteCalls, "a build this call did not end must keep its artifacts")
}

func TestCancelBuild_StopsTheBuildAfterTheCallerHasGone(t *testing.T) {
	t.Parallel()

	// The caller hangs up once the build is recorded as failed. That write has
	// already dropped the build from every listing, so nothing would come back
	// for it: the stop it owes the node has to outlive the request.
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	tm := &cancelRecorder{onSetStatus: cancel}

	require.NoError(t, cancelBuild(ctx, tm, cancellableBuild(), "cancelled by admin"))

	require.Equal(t, 1, tm.deleteCalls)
	require.NoError(t, tm.deleteCtxErr, "the delete must not be issued on the request context")
}

func TestCancelBuild_DoesNotDeleteWhenTheStatusWriteFails(t *testing.T) {
	t.Parallel()

	tm := &cancelRecorder{setStatusErr: errors.New("database is down")}

	require.Error(t, cancelBuild(t.Context(), tm, cancellableBuild(), "cancelled by admin"))

	require.Zero(t, tm.deleteCalls, "a build still recorded as running must keep its artifacts")
}

func TestCancelBuild_ReportsAFailedNodeDelete(t *testing.T) {
	t.Parallel()

	// The build is failed but still holds its node slot, which the operator
	// needs to see.
	tm := &cancelRecorder{deleteBuildErr: errors.New("node unreachable")}

	require.Error(t, cancelBuild(t.Context(), tm, cancellableBuild(), "cancelled by admin"))
}
