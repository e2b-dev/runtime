//go:build linux

package resumebuild

import (
	"fmt"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/guest"
)

// refuseVcpusAboveTarget refuses, before the boot, a cold boot of a guest that
// onlines every vCPU of its VM when the VM is larger than the target, whether
// the extra vCPUs come from -max-vcpus or from the size a paused descendant
// recorded. The factory refuses the same shape; checking here names the flags.
func refuseVcpusAboveTarget(boot guest.BootConfig, coldBoot bool, vcpu, vmVcpus int64) error {
	if coldBoot && boot.RefuseVcpusAboveTarget && vmVcpus > vcpu {
		return fmt.Errorf("this guest's cold boot onlines every vCPU of its VM: the VM would have %d vCPUs for a target of %d (pass -max-vcpus %d, or -vcpu %d)", vmVcpus, vcpu, vcpu, vmVcpus)
	}

	return nil
}
