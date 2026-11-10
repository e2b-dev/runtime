//go:build linux

package base

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc/cputemplate"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/buildcontext"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/config"
)

func cpuTemplateContext(tmpl *cputemplate.Template) buildcontext.BuildContext {
	return buildcontext.BuildContext{
		Config: config.TemplateConfig{
			DiskSizeMB:  testDiskSizeMB,
			CPUTemplate: tmpl,
		},
	}
}

// A build step can observe the CPU template (e.g. the TSC rate), so two builds that differ
// only in it must not share a cached base layer, while builds without one keep their key.
func TestCPUTemplateCacheKey(t *testing.T) {
	t.Parallel()

	unTargeted := testLayerKey(cpuTemplateContext(nil))

	t.Run("an untargeted team's key is unchanged", func(t *testing.T) {
		t.Parallel()

		// The fleet-wide-rebuild guard.
		assert.Equal(t, legacyLayerKey(), unTargeted)
	})

	t.Run("a template changes the key", func(t *testing.T) {
		t.Parallel()

		withTemplate := testLayerKey(cpuTemplateContext(&cputemplate.Template{
			X86TscKhz: 3_200_000,
		}))
		assert.NotEqual(t, unTargeted, withTemplate)
	})

	t.Run("a template that changes nothing does not change the key", func(t *testing.T) {
		t.Parallel()

		// Parse never returns one, but an empty template is still treated as none.
		assert.Equal(t, unTargeted, testLayerKey(cpuTemplateContext(&cputemplate.Template{})))
	})

	t.Run("different settings differ", func(t *testing.T) {
		t.Parallel()

		a := testLayerKey(cpuTemplateContext(&cputemplate.Template{X86TscKhz: 3_200_000}))
		b := testLayerKey(cpuTemplateContext(&cputemplate.Template{X86TscKhz: 2_400_000}))
		assert.NotEqual(t, a, b)
	})

	t.Run("the same settings spelled differently share a key", func(t *testing.T) {
		t.Parallel()

		// Reordering the flag's JSON must not orphan every layer cached under the old spelling.
		a, err := cputemplate.Parse([]byte(`{"x86_tsc_khz": 3200000, "kvm_capabilities": ["!121"]}`))
		require.NoError(t, err)
		b, err := cputemplate.Parse([]byte(`{"kvm_capabilities":["!121"],"x86_tsc_khz":3200000}`))
		require.NoError(t, err)

		assert.Equal(t, testLayerKey(cpuTemplateContext(a)), testLayerKey(cpuTemplateContext(b)))
	})

	t.Run("a CPU template beyond the TSC frequency is part of the key", func(t *testing.T) {
		t.Parallel()

		a := testLayerKey(cpuTemplateContext(&cputemplate.Template{X86TscKhz: 3_200_000}))
		b := testLayerKey(cpuTemplateContext(&cputemplate.Template{X86TscKhz: 3_200_000, KvmCapabilities: []string{"!121"}}))
		assert.NotEqual(t, a, b)
	})

	t.Run("a CPU template and cmdline args contribute independently", func(t *testing.T) {
		t.Parallel()

		both := buildcontext.BuildContext{
			Config: config.TemplateConfig{
				DiskSizeMB:  testDiskSizeMB,
				CmdlineArgs: map[string]string{"psi": "1"},
				CPUTemplate: &cputemplate.Template{X86TscKhz: 3_200_000},
			},
		}

		assert.NotEqual(t, testLayerKey(cmdlineContext(map[string]string{"psi": "1"})), testLayerKey(both))
		assert.NotEqual(t, testLayerKey(cpuTemplateContext(&cputemplate.Template{X86TscKhz: 3_200_000})), testLayerKey(both))
	})
}
