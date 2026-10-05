//go:build linux

package rootfs

import (
	"testing"
	"text/template"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/buildcontext"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/config"
)

func TestGenerateFile(t *testing.T) {
	t.Parallel()

	render := func(t *testing.T, text string) ([]byte, *templateModel, error) {
		t.Helper()

		tpl := template.Must(template.New("probe.tpl").Parse(text))
		model := newTemplateModel(buildcontext.BuildContext{}, "provision.log", "provision.result")
		data, err := generateFile(tpl, model)

		return data, model, err
	}

	t.Run("a template that writes a path renders its body", func(t *testing.T) {
		t.Parallel()

		data, model, err := render(t, `{{ .WriteFile "/etc/probe" 0o644 }}
body`)
		require.NoError(t, err)
		assert.Equal(t, "body", string(data))
		require.Len(t, model.paths, 1)
		assert.Equal(t, "etc/probe", model.paths[0].path)
	})

	t.Run("a template that renders nothing installs nothing", func(t *testing.T) {
		t.Parallel()

		data, model, err := render(t, `{{ if false }}{{ .WriteFile "/etc/probe" 0o644 }}
body{{ end }}
`)
		require.NoError(t, err)
		assert.Nil(t, data)
		assert.Empty(t, model.paths)
	})

	t.Run("a template that renders a body without a path fails the build", func(t *testing.T) {
		t.Parallel()

		// The guard that catches a removed or misplaced WriteFile before a body
		// with nowhere to go installs nothing in silence.
		_, _, err := render(t, "body")
		require.ErrorContains(t, err, "did not set path")
	})
}

func TestEnvdMemoryProtectionValues(t *testing.T) {
	t.Parallel()

	// Literals on purpose: a test that read the constants back would pass for
	// any value. The model test below only checks that the model hands the
	// constants through.
	assert.Equal(t, int64(128), EnvdMemoryMinMiB)
	assert.Equal(t, int64(256), EnvdMemoryLowMiB)
}

func TestTemplateModelEnvdMemory(t *testing.T) {
	t.Parallel()

	m := newTemplateModel(buildcontext.BuildContext{
		Config: config.TemplateConfig{MemoryMB: 4096},
		Rootfs: buildcontext.RootfsOptions{EnvdMemoryProtection: true},
	}, "provision.log", "provision.result")

	assert.True(t, m.EnvdMemoryProtection())
	assert.Equal(t, EnvdMemoryMinMiB, m.EnvdMemoryMinMiB())
	assert.Equal(t, EnvdMemoryLowMiB, m.EnvdMemoryLowMiB())
}
