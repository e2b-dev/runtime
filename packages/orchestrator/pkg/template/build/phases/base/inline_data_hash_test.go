//go:build linux

package base

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/buildcontext"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/config"
)

func inlineDataContext(inlineData bool) buildcontext.BuildContext {
	return buildcontext.BuildContext{
		Config: config.TemplateConfig{DiskSizeMB: testDiskSizeMB},
		Rootfs: buildcontext.RootfsOptions{InlineData: inlineData},
	}
}

func TestInlineDataCacheKey(t *testing.T) {
	t.Parallel()

	kept := testLayerKey(inlineDataContext(true))
	dropped := testLayerKey(inlineDataContext(false))

	t.Run("keeping inline_data contributes nothing, so existing layers keep their key", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, legacyLayerKey(), kept)
	})

	t.Run("dropping inline_data never reuses a layer made with it", func(t *testing.T) {
		t.Parallel()

		assert.NotEqual(t, kept, dropped)
	})

	t.Run("a build from another template contributes nothing whatever its flag", func(t *testing.T) {
		t.Parallel()

		// It takes its parent's filesystem as it is and runs no mkfs of its own.
		assert.Equal(t, testLayerKey(fromTemplate(inlineDataContext(true))), testLayerKey(fromTemplate(inlineDataContext(false))))
	})

	t.Run("dropping inline_data and envd memory protection are independent contributions", func(t *testing.T) {
		t.Parallel()

		withEnvd := func(buildContext buildcontext.BuildContext) buildcontext.BuildContext {
			buildContext.Rootfs.EnvdMemoryProtection = true

			return buildContext
		}
		envd := testLayerKey(withEnvd(inlineDataContext(true)))
		both := testLayerKey(withEnvd(inlineDataContext(false)))

		keys := []string{kept, dropped, envd, both}
		for i := range keys {
			for j := i + 1; j < len(keys); j++ {
				assert.NotEqual(t, keys[i], keys[j], "keys %d and %d collide", i, j)
			}
		}
	})
}
