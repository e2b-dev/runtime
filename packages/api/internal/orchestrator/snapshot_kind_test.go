package orchestrator

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
)

// pauseSucceedingSandboxClient accepts every Pause.
type pauseSucceedingSandboxClient struct {
	orchestrator.SandboxServiceClient
}

func (*pauseSucceedingSandboxClient) Pause(context.Context, *orchestrator.SandboxPauseRequest, ...grpc.CallOption) (*orchestrator.SandboxPauseResponse, error) {
	return &orchestrator.SandboxPauseResponse{}, nil
}

type noopSnapshotCache struct{}

func (noopSnapshotCache) Invalidate(context.Context, string) {}

// seedFilesystemOnlyBuild leaves the sandbox with one ready filesystem-only build, as
// a completed memory:false pause or snapshot would.
func seedFilesystemOnlyBuild(t *testing.T, o *Orchestrator, node *nodemanager.Node, sbx sandbox.Sandbox) {
	t.Helper()

	row, err := o.throttledUpsertSnapshot(t.Context(), buildUpsertSnapshotParams(sbx, node, true))
	require.NoError(t, err)
	require.NoError(t, o.finishSnapshotBuild(t.Context(), row.BuildID, types.BuildStatusSuccess))
}

// A pause the node refuses leaves the sandbox running on its previous build,
// so the row must keep describing that build's kind: here a filesystem-only
// build must not be relabelled as a memory one by a refused memory pause.
func TestPauseSandbox_RefusedPauseKeepsSnapshotKind(t *testing.T) {
	t.Parallel()

	o, _, node, sbx := newPauseFixture(t, errors.New("node exploded"))
	seedFilesystemOnlyBuild(t, o, node, sbx)

	require.Error(t, o.pauseSandbox(t.Context(), node, sbx, false))

	kind, err := o.sqlcDB.GetSnapshotFilesystemOnly(t.Context(), sbx.SandboxID)
	require.NoError(t, err)
	assert.True(t, kind, "refused pause must not change the kind of the ready build")
}

// A pause that succeeds records the requested kind together with the ready
// build, in both directions.
func TestPauseSandbox_SuccessRecordsRequestedKind(t *testing.T) {
	t.Parallel()

	o, db, node, sbx := newPauseFixture(t, nil)
	node.SetSandboxClient(&pauseSucceedingSandboxClient{})
	o.snapshotCache = noopSnapshotCache{}
	seedFilesystemOnlyBuild(t, o, node, sbx)

	require.NoError(t, o.pauseSandbox(t.Context(), node, sbx, false))
	kind, err := o.sqlcDB.GetSnapshotFilesystemOnly(t.Context(), sbx.SandboxID)
	require.NoError(t, err)
	assert.False(t, kind, "a memory pause records a memory build")
	buildStatus, _ := snapshotBuildStatus(t, db, sbx.SandboxID)
	assert.Equal(t, string(types.BuildStatusSuccess), buildStatus)

	require.NoError(t, o.pauseSandbox(t.Context(), node, sbx, true))
	kind, err = o.sqlcDB.GetSnapshotFilesystemOnly(t.Context(), sbx.SandboxID)
	require.NoError(t, err)
	assert.True(t, kind, "a filesystem-only pause records a filesystem-only build")
}
