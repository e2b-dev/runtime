//go:build linux

package resumebuild

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/guest"
)

func TestRefuseVcpusAboveTarget(t *testing.T) {
	t.Parallel()

	all := guest.BootConfig{RefuseVcpusAboveTarget: true}
	err := refuseVcpusAboveTarget(all, true, 2, 4)
	require.ErrorContains(t, err, "pass -max-vcpus 2, or -vcpu 4", "a bigger VM is refused with the flags that fix it")
	require.NoError(t, refuseVcpusAboveTarget(all, true, 2, 2), "a VM of the target's size")
	require.NoError(t, refuseVcpusAboveTarget(all, false, 2, 4), "a memory resume keeps the snapshot's VM")
	require.NoError(t, refuseVcpusAboveTarget(guest.BootConfig{}, true, 2, 4), "the default guest caps the online set with maxcpus=")
}
