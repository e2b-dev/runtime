//go:build linux

package v2

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewV2Pool_ReusePoolTakesItsSizeFromTheConfig(t *testing.T) {
	t.Parallel()

	config := testConfig()
	config.ReusedSlotsPoolSize = 250
	pool := NewV2Pool(&failingStorage{}, config, nil, nil)

	require.Equal(t, 250, cap(pool.reusedSlots))
}
