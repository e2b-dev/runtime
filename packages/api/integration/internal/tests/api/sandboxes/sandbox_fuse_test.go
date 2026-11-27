package sandboxes

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/integration/internal/setup"
	"github.com/e2b-dev/infra/packages/api/integration/internal/utils"
)

func TestFuseDevicePermissions(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	client := setup.GetAPIClient()
	sbx := utils.SetupSandboxWithCleanup(t, client)

	envdClient := setup.GetEnvdClient(t, ctx)

	// Log detailed /dev/fuse info for debugging
	lsOutput, _ := utils.ExecCommandAsRootWithOutput(t, ctx, sbx, envdClient, "ls", "-la", "/dev/fuse")
	t.Logf("/dev/fuse listing: %s", strings.TrimSpace(lsOutput))

	// Check /dev/fuse permissions - should be 0666 (crw-rw-rw-)
	output, err := utils.ExecCommandAsRootWithOutput(t, ctx, sbx, envdClient, "stat", "-c", "%a", "/dev/fuse")
	require.NoError(t, err, "Failed to stat /dev/fuse")
	t.Logf("/dev/fuse permissions: %s", strings.TrimSpace(output))
	assert.Equal(t, "666", strings.TrimSpace(output), "Expected /dev/fuse to have permissions 666")
}

func TestFuseNonRootAccess(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	client := setup.GetAPIClient()
	sbx := utils.SetupSandboxWithCleanup(t, client)

	envdClient := setup.GetEnvdClient(t, ctx)

	// Test that non-root user can open /dev/fuse for reading
	// This verifies the device permissions are set correctly
	err := utils.ExecCommand(t, ctx, sbx, envdClient, "test", "-r", "/dev/fuse")
	require.NoError(t, err, "Non-root user should be able to read /dev/fuse")

	// Test that non-root user can open /dev/fuse for writing
	err = utils.ExecCommand(t, ctx, sbx, envdClient, "test", "-w", "/dev/fuse")
	require.NoError(t, err, "Non-root user should be able to write to /dev/fuse")
}
