//go:build linux

package startupreclaim

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// A cache dir the sweep leaves alone keeps only files, so it must not mark the
// reclaim unclean for the slot reconciliation that follows.
func TestSummaryHasRuntimeFailuresIgnoresTheCacheSweep(t *testing.T) {
	t.Parallel()

	cacheOnly := Summary{Failed: map[string]int{resourceCacheDir: 2, resourceNetwork: 0}}
	assert.True(t, cacheOnly.HasFailures())
	assert.False(t, cacheOnly.HasRuntimeFailures())

	network := Summary{Failed: map[string]int{resourceCacheDir: 1, resourceNetwork: 1}}
	assert.True(t, network.HasRuntimeFailures())
}
