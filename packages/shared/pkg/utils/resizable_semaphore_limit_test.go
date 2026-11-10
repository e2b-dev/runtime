package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAdjustableSemaphore_Limit verifies Limit() reflects the initial capacity
// and any later SetLimit, independent of how many slots are currently held.
func TestAdjustableSemaphore_Limit(t *testing.T) {
	t.Parallel()

	s, err := NewAdjustableSemaphore(3)
	require.NoError(t, err)
	assert.Equal(t, int64(3), s.Limit())

	// Taking slots does not change the limit.
	require.True(t, s.TryAcquire(2))
	assert.Equal(t, int64(3), s.Limit())

	require.NoError(t, s.SetLimit(5))
	assert.Equal(t, int64(5), s.Limit())
}
