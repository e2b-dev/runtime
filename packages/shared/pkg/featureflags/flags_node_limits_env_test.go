package featureflags

import (
	"os"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The fallbacks are read at package init, so the test reruns itself with the
// variables set.
func TestNodeLimitFallbacksReadTheEnvironment(t *testing.T) {
	t.Parallel()

	const child = "NODE_LIMIT_FALLBACKS_TEST_CHILD"
	if os.Getenv(child) == "1" {
		assert.Equal(t, 250, MaxSandboxesPerNode.Fallback())
		assert.Equal(t, 12, MaxStartingInstancesPerNode.Fallback())

		return
	}

	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^"+t.Name()+"$", "-test.v")
	cmd.Env = append(os.Environ(), child+"=1", "MAX_SANDBOXES_PER_NODE=250", "MAX_STARTING_INSTANCES_PER_NODE=12")
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, string(out))
	assert.Contains(t, string(out), "--- PASS: "+t.Name(), string(out))
}
