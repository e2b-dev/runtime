//go:build linux

package sandbox

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	buildconfig "github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/config"
	"github.com/e2b-dev/infra/packages/shared/pkg/units"
)

// coldBootMemoryHotplug gives a cold boot the hotplug region its config carries, or refuses one
// the boot cannot carry, before any resource is taken. A cold boot configures the device and boots
// the kernel again, so every rule a build applies holds here too, unlike a memory resume.
func coldBootMemoryHotplug(config *Config, options fc.ProcessOptions) (fc.ProcessOptions, error) {
	if err := config.FirecrackerConfig.RefuseMemoryHotplug(config.MemoryHotplugMB); err != nil {
		return options, err
	}
	options.MemoryHotplugMB = config.MemoryHotplugMB

	return options, nil
}

// NewColdBootMemfile is the empty memfile a cold boot starts from: boot RAM followed by the
// hotplug region, as the guest memory file lays them out, in the config's page size.
func NewColdBootMemfile(config *Config, buildID uuid.UUID) (*block.Empty, error) {
	return block.NewEmpty(units.MBToBytes(config.RamMB+config.MemoryHotplugMB), buildconfig.MemfilePageSize(config.HugePages), buildID)
}

// pluggedMemoryReadTimeout bounds the hotplug status read a pause admission makes, so a slow
// Firecracker cannot hold the admission; a read that times out falls back to the whole region.
const pluggedMemoryReadTimeout = 2 * time.Second

// PluggedMemoryMB is the memory plugged into the VM's hotplug region now, as Firecracker reports
// it; 0 without a region, which asks Firecracker nothing.
func (s *Sandbox) PluggedMemoryMB(ctx context.Context) (int64, error) {
	if s.memoryHotplugMB == 0 {
		return 0, nil
	}
	ctx, cancel := context.WithTimeout(ctx, pluggedMemoryReadTimeout)
	defer cancel()
	if s.readPluggedMemory != nil {
		return s.readPluggedMemory(ctx)
	}

	return s.process.PluggedMemoryMB(ctx)
}
