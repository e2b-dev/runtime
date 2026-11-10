//go:build linux

package fc

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc/cputemplate"
)

// Firecracker accepts an omitted collection but rejects null, so unset fields must not be sent.
func TestCPUConfigBodyOmitsUnsetCollections(t *testing.T) {
	t.Parallel()

	raw, err := json.Marshal(cpuConfigBody(cputemplate.Template{X86TscKhz: 3_200_000}))
	require.NoError(t, err)

	assert.JSONEq(t, `{"x86_tsc_khz":3200000}`, string(raw))
}

func TestCPUConfigBodySendsSetCollections(t *testing.T) {
	t.Parallel()

	raw, err := json.Marshal(cpuConfigBody(cputemplate.Template{KvmCapabilities: []string{"!121"}}))
	require.NoError(t, err)

	assert.JSONEq(t, `{"kvm_capabilities":["!121"]}`, string(raw))
}

func TestCPUConfigBodyForAnEmptyTemplate(t *testing.T) {
	t.Parallel()

	raw, err := json.Marshal(cpuConfigBody(cputemplate.Template{}))
	require.NoError(t, err)

	assert.JSONEq(t, `{}`, string(raw))
}
