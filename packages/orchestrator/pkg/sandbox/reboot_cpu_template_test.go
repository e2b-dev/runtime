//go:build linux

package sandbox

import (
	"runtime"
	"testing"

	"github.com/launchdarkly/go-sdk-common/v3/ldvalue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc/cputemplate"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
)

func TestCheckRebootCPUTemplate(t *testing.T) {
	t.Parallel()

	tsc := &cputemplate.Template{X86TscKhz: 3_200_000}
	kvm := &cputemplate.Template{KvmCapabilities: []string{"!121"}}

	// v1.14-0.2.0 predates x86_tsc_khz, so the reboot's version must reject it.
	err := checkRebootCPUTemplate(tsc, "v1.14-0.2.0")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "v1.14-0.2.0")

	if runtime.GOARCH == "amd64" {
		require.NoError(t, checkRebootCPUTemplate(tsc, "v1.14-0.3.0"))
	}

	require.NoError(t, checkRebootCPUTemplate(kvm, "v1.14-0.3.0"))
	require.Error(t, checkRebootCPUTemplate(kvm, "not-a-version"))

	// No template needs no version.
	require.NoError(t, checkRebootCPUTemplate(nil, "not-a-version"))
	require.NoError(t, checkRebootCPUTemplate(&cputemplate.Template{}, "not-a-version"))
}

// The override replaces the build's template for one boot; with it cleared, the next boot is
// back on the build's template even though the last boot ran the override.
func TestRebootCPUTemplate(t *testing.T) {
	t.Parallel()

	a := &cputemplate.Template{KvmCapabilities: []string{"!121"}}
	b := &cputemplate.Template{KvmCapabilities: []string{"!122"}}
	// The last boot ran B under the override.
	meta := metadata.Template{BuildCPUTemplate: a, CPUTemplate: b}

	got, overridden, err := rebootCPUTemplate(meta, ldvalue.Null())
	require.NoError(t, err)
	assert.False(t, overridden)
	assert.Equal(t, a, got, "with no override the build's template returns")

	got, overridden, err = rebootCPUTemplate(meta, ldvalue.Parse([]byte(`{"kvm_capabilities":["!122"]}`)))
	require.NoError(t, err)
	assert.True(t, overridden)
	assert.Equal(t, b, got)

	got, overridden, err = rebootCPUTemplate(meta, ldvalue.Parse([]byte(`{}`)))
	require.NoError(t, err)
	assert.True(t, overridden)
	assert.Nil(t, got, "{} boots with no template")

	got, overridden, err = rebootCPUTemplate(meta, ldvalue.Parse([]byte(`{"bogus":1}`)))
	require.Error(t, err)
	assert.False(t, overridden)
	assert.Equal(t, a, got, "an invalid override falls back to the build's template")
}
