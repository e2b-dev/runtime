package orchestrator

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/e2b-dev/infra/packages/api/internal/sandbox"
	"github.com/e2b-dev/infra/packages/db/pkg/types"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
)

// A memory pause the node took over an unresponsive guest agent, on a
// sandbox whose row still describes a filesystem-only build, changes the
// kind and writes the mark in the same finalization: the row and the build
// say memory, the build is marked, and it is ready.
func TestPauseSandbox_MarkAndKindChangeLandTogether(t *testing.T) {
	t.Parallel()

	o, db, node, sbx := newPauseFixture(t, nil)
	node.SetSandboxClient(&pauseCapturingSandboxClient{envdUnresponsive: true})
	o.snapshotCache = noopSnapshotCache{}
	seedFilesystemOnlyBuild(t, o, node, sbx)

	require.NoError(t, o.pauseSandbox(t.Context(), node, sbx, sandbox.RemoveOpts{Action: sandbox.StateActionPause}))
	kind, err := o.sqlcDB.GetSnapshotFilesystemOnly(t.Context(), sbx.SandboxID)
	require.NoError(t, err)
	assert.False(t, kind, "the row follows the memory pause")
	assert.False(t, lastSnapshotBuildKind(t, o, sbx.SandboxID), "the build is the memory one")
	mark := snapshotBuildEnvdUnresponsive(t, db, sbx.SandboxID)
	require.NotNil(t, mark, "the mark is written with the kind change")
	assert.True(t, *mark)
	buildStatus, hasFinishedAt := snapshotBuildStatus(t, db, sbx.SandboxID)
	assert.Equal(t, types.BuildStatusGroupReady, buildStatus)
	assert.True(t, hasFinishedAt)
}

// pauseDiskOnlyMarkedClient answers every Pause as a disk-only capture that
// also claims the miss, which no node sends; the API must not trust it.
type pauseDiskOnlyMarkedClient struct {
	orchestrator.SandboxServiceClient
}

func (*pauseDiskOnlyMarkedClient) Pause(context.Context, *orchestrator.SandboxPauseRequest, ...grpc.CallOption) (*orchestrator.SandboxPauseResponse, error) {
	return &orchestrator.SandboxPauseResponse{FilesystemOnly: true, EnvdUnresponsive: true}, nil
}

// A full pause the node captured disk-only carries no mark even when the
// node's answer claims one: there is no memory for it to describe, and the
// build is rekinded alone.
func TestPauseSandbox_DiskOnlyCaptureCarriesNoMark(t *testing.T) {
	t.Parallel()

	o, db, node, sbx := newPauseFixture(t, nil)
	node.SetSandboxClient(&pauseDiskOnlyMarkedClient{})
	o.snapshotCache = noopSnapshotCache{}

	require.NoError(t, o.pauseSandbox(t.Context(), node, sbx, sandbox.RemoveOpts{Action: sandbox.StateActionPause}))
	assert.True(t, lastSnapshotBuildKind(t, o, sbx.SandboxID), "the build carries the captured kind")
	assert.Nil(t, snapshotBuildEnvdUnresponsive(t, db, sbx.SandboxID), "a disk-only capture is never marked")
}
