package orchestrator

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/api/integration/internal/setup"
	"github.com/e2b-dev/infra/packages/api/integration/internal/utils"
)

func TestHardwareEntropyDeviceAvailability(t *testing.T) {
	t.Parallel()

	c := setup.GetAPIClient()

	// Create a sandbox
	sbx := utils.SetupSandboxWithCleanup(t, c)

	envdClient := setup.GetEnvdClient(t, t.Context())

	cmd := "cat /sys/class/misc/hw_random/rng_current"
	output, err := utils.ExecCommandAsRootWithOutput(t, t.Context(), sbx, envdClient, "bash", "-c", cmd)
	require.NoError(t, err)
	require.NotEqual(t, "none", strings.TrimSpace(output), "Should have a hardware random number generator available")

	// Read some random bytes and verify they're not all zeros
	cmd = "sudo dd if=/dev/hwrng bs=1 count=100 2>/dev/null | od -An -tu1 | tr -d '[:space:]'"
	output, err = utils.ExecCommandAsRootWithOutput(t, t.Context(), sbx, envdClient, "bash", "-c", cmd)
	require.NoError(t, err)

	// Output should contain non-zero bytes
	assert.NotEmpty(t, output, "Should receive random data")
	// Check that not all bytes are zero
	assert.NotContains(t, output, strings.Repeat("0", 100), "Random data should not be all zeros")
}
