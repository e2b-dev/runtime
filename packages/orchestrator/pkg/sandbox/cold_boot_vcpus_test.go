//go:build linux

package sandbox

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
)

// A cold boot of a guest that onlines every vCPU is refused before any
// resource when its VM would be larger than its target, however the size
// arrived; the default profile caps the online set with maxcpus= and is left alone.
func TestRefuseColdBootVcpus(t *testing.T) {
	t.Parallel()

	firmware := fc.Config{Guest: firmwareProfile{}}
	require.ErrorIs(t, refuseColdBootVcpus(NewConfig(Config{Vcpu: 2, ConfiguredVmVcpus: 4, FirecrackerConfig: firmware})), ErrColdBootVcpus)
	require.NoError(t, refuseColdBootVcpus(NewConfig(Config{Vcpu: 2, ConfiguredVmVcpus: 2, FirecrackerConfig: firmware})))
	require.NoError(t, refuseColdBootVcpus(NewConfig(Config{Vcpu: 2, FirecrackerConfig: firmware})), "no maximum boots at the target")
	require.NoError(t, refuseColdBootVcpus(NewConfig(Config{Vcpu: 2, ConfiguredVmVcpus: 4})), "a default-profile cold boot")

	_, err := (&Factory{}).CreateSandbox(
		t.Context(),
		NewConfig(Config{Vcpu: 2, ConfiguredVmVcpus: 4, FirecrackerConfig: firmware}),
		sandboxtypes.RuntimeMetadata{},
		capacityOnlyTemplate{},
		0,
		"",
		fc.ProcessOptions{},
		nil,
	)
	require.ErrorIs(t, err, ErrColdBootVcpus, "refused before the template's files are touched")
}
