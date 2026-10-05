//go:build linux

package base

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/buildcontext"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/config"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/storage/cache"
)

// The resolved inputs every key test builds on. Hash itself needs an index and
// a feature-flag client to produce them; the properties worth pinning are in
// how baseLayerKey combines them with the build context.
const (
	testIndexVersion     = "index-v1"
	testProvisionVersion = "provision-1"
	testBaseSource       = "ubuntu:22.04"
	testDiskSizeMB       = 1024
)

// testLayerKey is the base-layer key for a build context under the fixed
// resolved inputs, computed by the same function Hash uses.
func testLayerKey(buildContext buildcontext.BuildContext) string {
	return baseLayerKey(testIndexVersion, testProvisionVersion, testBaseSource, buildContext)
}

// legacyLayerKey is the digest a build with nothing opted in hashed to before
// any conditional contribution existed. Holding the provision version fixed, it
// is the key a cached base layer is stored under.
func legacyLayerKey() string {
	return cache.HashKeys(testIndexVersion, testProvisionVersion, "1024", testBaseSource)
}

func cmdlineContext(args map[string]string) buildcontext.BuildContext {
	return buildcontext.BuildContext{
		Config: config.TemplateConfig{
			DiskSizeMB:  testDiskSizeMB,
			CmdlineArgs: args,
		},
	}
}

func TestCmdlineArgsCacheKey(t *testing.T) {
	t.Parallel()

	unTargeted := testLayerKey(cmdlineContext(nil))

	t.Run("an untargeted team's key is unchanged", func(t *testing.T) {
		t.Parallel()

		// The fleet-wide-rebuild guard: a team with no variant must hash exactly as it
		// did before the field existed, or every cached base layer everywhere is invalidated.
		assert.Equal(t, legacyLayerKey(), unTargeted)
	})

	t.Run("no parameters does not change the key", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, unTargeted, testLayerKey(cmdlineContext(nil)))
	})

	t.Run("arguments change the key", func(t *testing.T) {
		t.Parallel()

		withPSI := testLayerKey(cmdlineContext(map[string]string{"psi": "1"}))
		assert.NotEqual(t, unTargeted, withPSI)

		// Removing a team's variant returns them to the key their old layers are under.
		assert.Equal(t, unTargeted, testLayerKey(cmdlineContext(nil)))
	})

	t.Run("different arguments differ", func(t *testing.T) {
		t.Parallel()

		a := testLayerKey(cmdlineContext(map[string]string{"psi": "1"}))
		b := testLayerKey(cmdlineContext(map[string]string{"nokaslr": ""}))
		assert.NotEqual(t, a, b)
	})

	t.Run("the same arguments hash the same regardless of map order", func(t *testing.T) {
		t.Parallel()

		// Go randomises map iteration, so an unsorted key would cache nothing at all.
		want := testLayerKey(cmdlineContext(map[string]string{"psi": "1", "nokaslr": "", "a": "b"}))
		for range 20 {
			assert.Equal(t, want, testLayerKey(cmdlineContext(map[string]string{"a": "b", "nokaslr": "", "psi": "1"})))
		}
	})
}
