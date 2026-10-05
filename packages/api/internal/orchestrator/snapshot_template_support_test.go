package orchestrator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/api/internal/orchestrator/nodemanager"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
)

// A memory:false snapshot is refused on a node whose orchestrator predates
// the request field (it would take a memory checkpoint and answer as if it had
// not); a full snapshot never consults the release.
func TestCheckSnapshotSupport(t *testing.T) {
	t.Parallel()

	oldNode := nodemanager.NewTestNode("old", api.NodeStatusReady, 0, 8, nodemanager.WithOrchestratorVersion("0.16.202609291011-9f0b9d75926"))
	newNode := nodemanager.NewTestNode("new", api.NodeStatusReady, 0, 8, nodemanager.WithOrchestratorVersion("0.16.202609301732-6d975d1ebc8"))
	unversioned := nodemanager.NewTestNode("unversioned", api.NodeStatusReady, 0, 8, nodemanager.WithOrchestratorVersion(""))

	assert.NoError(t, checkSnapshotSupport(oldNode, SnapshotTemplateOpts{}))
	assert.NoError(t, checkSnapshotSupport(unversioned, SnapshotTemplateOpts{}))
	assert.NoError(t, checkSnapshotSupport(newNode, SnapshotTemplateOpts{FilesystemOnly: true}))

	for _, node := range []*nodemanager.Node{oldNode, unversioned} {
		err := checkSnapshotSupport(node, SnapshotTemplateOpts{FilesystemOnly: true})
		require.Error(t, err)
		var featErr NodeFeatureMissingError
		require.ErrorAs(t, err, &featErr)
		assert.Equal(t, node.ID, featErr.NodeID)
		assert.Equal(t, "filesystem-only-checkpoint", featErr.Feature)
		assert.Equal(t, "0.16.202609301732", featErr.MinVersion)
	}
}

// The orchestrator's own flag refusal is told apart from every other
// FailedPrecondition by its typed detail, never by its message.
func TestIsFilesystemOnlyDisabled(t *testing.T) {
	t.Parallel()

	plain := status.New(codes.FailedPrecondition, "a checkpoint is already in progress")
	assert.False(t, isFilesystemOnlyDisabled(plain.Err()))

	withDetail, err := status.New(codes.FailedPrecondition, "filesystem-only checkpoint of sandbox 'x' is disabled").WithDetails(
		&orchestrator.UserError{Code: orchestrator.UserErrorCode_FILESYSTEM_ONLY_CHECKPOINT_DISABLED, HttpStatus: 400},
	)
	require.NoError(t, err)
	assert.True(t, isFilesystemOnlyDisabled(withDetail.Err()))

	other, err := status.New(codes.FailedPrecondition, "path").WithDetails(
		&orchestrator.UserError{Code: orchestrator.UserErrorCode_PATH_NOT_FOUND},
	)
	require.NoError(t, err)
	assert.False(t, isFilesystemOnlyDisabled(other.Err()))
}
