//go:build linux

package base

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/buildcontext"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/config"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/storage/cache"
	templatemanager "github.com/e2b-dev/infra/packages/shared/pkg/grpc/template-manager"
)

func envdMemoryContext(on bool, memoryMB int64) buildcontext.BuildContext {
	return buildcontext.BuildContext{
		Config: config.TemplateConfig{
			DiskSizeMB: testDiskSizeMB,
			MemoryMB:   memoryMB,
		},
		Rootfs: buildcontext.RootfsOptions{EnvdMemoryProtection: on},
	}
}

func fromTemplate(buildContext buildcontext.BuildContext) buildcontext.BuildContext {
	buildContext.Config.FromTemplate = &templatemanager.FromTemplateConfig{Alias: "parent", BuildID: "parent-build"}

	return buildContext
}

func TestEnvdMemoryProtectionCacheKey(t *testing.T) {
	t.Parallel()

	off := testLayerKey(envdMemoryContext(false, 4096))
	on := testLayerKey(envdMemoryContext(true, 4096))

	t.Run("off contributes nothing, so the key is the legacy digest for the same provision version", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, legacyLayerKey(), off)
	})

	t.Run("on never reuses a layer built off", func(t *testing.T) {
		t.Parallel()

		assert.NotEqual(t, off, on)
	})

	t.Run("off again returns to the key the old layers are under", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, legacyLayerKey(), testLayerKey(envdMemoryContext(false, 4096)))
	})

	t.Run("off does not vary with the template's RAM", func(t *testing.T) {
		t.Parallel()

		// Today's key, kept: off contributes nothing that varies with RAM, so
		// no existing cached layer's key changes. (The off files do vary with
		// RAM through GOMEMLIMIT, and always have; that is not keyed today.)
		assert.Equal(t, off, testLayerKey(envdMemoryContext(false, 128)))
	})

	t.Run("on does not vary with the template's RAM either", func(t *testing.T) {
		t.Parallel()

		// The request is the same constant on every template, so templates of
		// one team that differ only in RAM share the on layer as they share the
		// off one.
		assert.Equal(t, on, testLayerKey(envdMemoryContext(true, 128)))
		assert.Equal(t, on, testLayerKey(envdMemoryContext(true, 16384)))
	})

	t.Run("on carries the two values rendered, not the fact of the option", func(t *testing.T) {
		t.Parallel()

		// The constants live in Go, not in the template files FilesHash covers,
		// so the key is what keeps a layer rendered under one value from being
		// served to a build rendering another. Spelled out as literals so that
		// a token that stops carrying them fails here.
		assert.Equal(t, cache.HashKeys(testIndexVersion, testProvisionVersion, "1024", testBaseSource, "envd-memory-protection:128:256"), on)
	})

	t.Run("a build from another template contributes nothing whatever its flag", func(t *testing.T) {
		t.Parallel()

		// It inherits the parent's base layer and renders no files; a
		// contribution here would rotate every layer derived from it for no
		// change in bytes.
		assert.Equal(t, testLayerKey(fromTemplate(envdMemoryContext(false, 4096))), testLayerKey(fromTemplate(envdMemoryContext(true, 4096))))
	})

	t.Run("the option and a cmdline variant are independent contributions", func(t *testing.T) {
		t.Parallel()

		psi := map[string]string{"psi": "1"}
		withArgs := func(buildContext buildcontext.BuildContext) buildcontext.BuildContext {
			buildContext.Config.CmdlineArgs = psi

			return buildContext
		}
		cmdline := testLayerKey(withArgs(envdMemoryContext(false, 4096)))
		both := testLayerKey(withArgs(envdMemoryContext(true, 4096)))

		keys := []string{off, on, cmdline, both}
		for i := range keys {
			for j := i + 1; j < len(keys); j++ {
				assert.NotEqual(t, keys[i], keys[j], "keys %d and %d collide", i, j)
			}
		}
	})
}

func TestEnvdMemoryMinMiBAttribute(t *testing.T) {
	t.Parallel()

	t.Run("off reports 0 whatever the template's RAM", func(t *testing.T) {
		t.Parallel()

		// The units then carry their legacy request, which the chain grants
		// nothing of; reporting that request would claim a protection that
		// does not exist.
		assert.Equal(t, int64(0), envdMemoryMinMiB(envdMemoryContext(false, 4096)))
		assert.Equal(t, int64(0), envdMemoryMinMiB(envdMemoryContext(false, 128)))
	})

	t.Run("on reports the request rendered, whatever the template's RAM", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, int64(128), envdMemoryMinMiB(envdMemoryContext(true, 4096)))
		assert.Equal(t, int64(128), envdMemoryMinMiB(envdMemoryContext(true, 128)))
	})
}

func TestRendersRootfsFiles(t *testing.T) {
	t.Parallel()

	assert.True(t, rendersRootfsFiles(envdMemoryContext(true, 4096)))
	assert.False(t, rendersRootfsFiles(fromTemplate(envdMemoryContext(true, 4096))),
		"a build from another template inherits its parent's files and reports nothing about them")
}
