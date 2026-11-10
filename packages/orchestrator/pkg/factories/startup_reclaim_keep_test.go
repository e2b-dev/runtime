//go:build linux

package factories

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
)

// Local template and build-cache storage hold the only copies of their
// artifacts and may share a cache path, so the startup sweep must keep them.
func TestStartupReclaimKeepPathsProtectsLocalStorage(t *testing.T) {
	t.Setenv("TEMPLATE_STORAGE_URL", "file:///orchestrator/template")
	t.Setenv("BUILD_CACHE_STORAGE_URL", "file:///orchestrator/build-cache-storage")

	templateSpec, err := cfg.TemplateStorage()
	require.NoError(t, err)

	keep, err := startupReclaimKeepPaths(cfg.Config{}, templateSpec)
	require.NoError(t, err)
	assert.Contains(t, keep, "/orchestrator/template")
	assert.Contains(t, keep, "/orchestrator/build-cache-storage")
}

func TestStartupReclaimKeepPathsSkipsBucketStorage(t *testing.T) {
	t.Setenv("TEMPLATE_STORAGE_URL", "gs://templates")
	t.Setenv("BUILD_CACHE_STORAGE_URL", "gs://build-cache")

	templateSpec, err := cfg.TemplateStorage()
	require.NoError(t, err)

	keep, err := startupReclaimKeepPaths(cfg.Config{}, templateSpec)
	require.NoError(t, err)
	assert.NotContains(t, keep, "templates")
	assert.NotContains(t, keep, "build-cache")
}

// A node that never builds leaves the build-cache bucket unset; that must not
// stop the sweep, since such a build cache holds nothing on this disk.
func TestStartupReclaimKeepPathsIgnoresAnUnconfiguredBucketBuildCache(t *testing.T) {
	t.Setenv("TEMPLATE_STORAGE_URL", "gs://templates")
	t.Setenv("BUILD_CACHE_STORAGE_URL", "")
	t.Setenv("STORAGE_PROVIDER", "GCPBucket")
	t.Setenv("BUILD_CACHE_BUCKET_NAME", "")

	templateSpec, err := cfg.TemplateStorage()
	require.NoError(t, err)

	_, err = startupReclaimKeepPaths(cfg.Config{}, templateSpec)
	require.NoError(t, err)
}

// A local build cache that cannot be resolved may still name a path on this
// disk, so the sweep must not run without it.
func TestStartupReclaimKeepPathsFailsOnAnUnresolvableLocalBuildCache(t *testing.T) {
	t.Setenv("TEMPLATE_STORAGE_URL", "gs://templates")
	t.Setenv("BUILD_CACHE_STORAGE_URL", "file://host/build-cache")

	templateSpec, err := cfg.TemplateStorage()
	require.NoError(t, err)

	_, err = startupReclaimKeepPaths(cfg.Config{}, templateSpec)
	require.Error(t, err)
}
