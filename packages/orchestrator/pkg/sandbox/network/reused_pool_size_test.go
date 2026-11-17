//go:build linux

package network

import (
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

// A slot returned to a full reuse pool is torn down, so a node that recycles
// more sandboxes at once than the pool holds has to be able to size it.
func TestParseConfig_ReusedSlotsPoolSize(t *testing.T) { //nolint:paralleltest // the subtests set process environment with t.Setenv
	t.Run("absent keeps the default", func(t *testing.T) {
		t.Setenv("NETWORK_REUSED_SLOTS_POOL_SIZE", "")
		require.NoError(t, os.Unsetenv("NETWORK_REUSED_SLOTS_POOL_SIZE"))

		config, err := ParseConfig()
		require.NoError(t, err)
		require.Equal(t, ReusedSlotsPoolSize, config.ReusedPoolSize())
	})

	t.Run("set value is used", func(t *testing.T) {
		t.Setenv("NETWORK_REUSED_SLOTS_POOL_SIZE", "250")

		config, err := ParseConfig()
		require.NoError(t, err)
		require.Equal(t, 250, config.ReusedPoolSize())
	})

	t.Run("negative fails loudly", func(t *testing.T) {
		t.Setenv("NETWORK_REUSED_SLOTS_POOL_SIZE", "-1")

		_, err := ParseConfig()
		require.ErrorContains(t, err, "NETWORK_REUSED_SLOTS_POOL_SIZE=-1")
	})

	t.Run("more slots than the host can address fails loudly", func(t *testing.T) {
		over := strconv.Itoa(vrtSlotsSize + 1)
		t.Setenv("NETWORK_REUSED_SLOTS_POOL_SIZE", over)

		_, err := ParseConfig()
		require.ErrorContains(t, err, "NETWORK_REUSED_SLOTS_POOL_SIZE="+over)
	})
}

// A config built in code leaves the size unset.
func TestConfig_ReusedPoolSizeDefaultsWhenUnset(t *testing.T) {
	t.Parallel()

	require.Equal(t, ReusedSlotsPoolSize, Config{}.ReusedPoolSize())
}
