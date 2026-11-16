//go:build linux

package sandbox

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/block"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	buildconfig "github.com/e2b-dev/infra/packages/orchestrator/pkg/template/build/config"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/template/metadata"
	"github.com/e2b-dev/infra/packages/shared/pkg/units"
)

// coldBootMemoryHotplug gives a cold boot the hotplug region its config carries, or refuses one
// the boot cannot carry, before any resource is taken. A cold boot configures the device and boots
// the kernel again, so every rule a build applies holds here too, unlike a memory resume.
func coldBootMemoryHotplug(config *Config, options fc.ProcessOptions) (fc.ProcessOptions, error) {
	if err := config.FirecrackerConfig.RefuseMemoryHotplug(config.MemoryHotplugMB); err != nil {
		return options, err
	}
	if _, err := requestedMemoryHotplug(config.MemoryMB, config.RamMB, config.MemoryHotplugMB, 0, false); err != nil {
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

// RequestedMemoryHotplug checks a start's requested memory against the template it starts from,
// before the start takes anything, and returns how much of the hotplug region it asks to plug, in
// MiB. A cold boot configures the template's region with nothing plugged; a memory resume restores
// it with what its snapshot had asked for.
func RequestedMemoryHotplug(memoryMB, ramMB int64, meta metadata.Template, coldBoot bool) (int64, error) {
	if coldBoot {
		return requestedMemoryHotplug(memoryMB, ramMB, meta.MemoryHotplugMB, 0, false)
	}

	return requestedMemoryHotplug(memoryMB, ramMB, meta.MemoryHotplugMB, meta.MemoryHotplugRequestedMB, true)
}

// restoreMemoryHotplugTarget is the hotplug target a memory resume sets on the restored device, if
// any. A named size is checked against the device, which refuses one below its own request whatever
// the metadata said. A kept size is the one the snapshot recorded, which the restore plugs up to, so
// a sandbox paused before its cold boot's plug landed gets its memory back. A snapshot without a
// region has no device to read: the only size it may name is boot RAM, which needs no target.
// Otherwise only a start that names nothing over a snapshot that asked for nothing reads nothing.
func restoreMemoryHotplugTarget(regionMB, memoryMB, requestMB int64) (target int64, explicit, ok bool) {
	if regionMB == 0 || (memoryMB == 0 && requestMB == 0) {
		return 0, false, false
	}

	return requestMB, memoryMB > 0, true
}

// requestedMemoryHotplug is how much of the hotplug region, in MiB, a start asks Firecracker to
// plug: memoryMB minus boot RAM, or what the snapshot had asked for (recordedMB) when memoryMB is 0.
// The request must be a whole number of hotplug slots within the region. A memory resume may
// only keep or grow its snapshot's request; a cold boot starts with nothing plugged and may ask
// for any size in the region.
func requestedMemoryHotplug(memoryMB, ramMB, regionMB, recordedMB int64, memoryResume bool) (int64, error) {
	if memoryMB == 0 {
		return recordedMB, nil
	}
	want := memoryMB - ramMB
	switch {
	case want < 0:
		return 0, fmt.Errorf("%w: memory of %d MiB is below the %d MiB of boot RAM", fc.ErrMemoryRequestInvalid, memoryMB, ramMB)
	case want == 0:
	case regionMB == 0:
		return 0, fmt.Errorf("%w: memory of %d MiB needs a hotplug region and the template has none", fc.ErrMemoryRequestInvalid, memoryMB)
	case want%fc.MemoryHotplugSlotMiB != 0:
		// The guest's memory is reserved in whole slots, so a request in whole slots is what
		// both the orchestrator's and the API's counts agree on.
		return 0, fmt.Errorf("%w: %d MiB above boot RAM is not a whole number of %d MiB slots", fc.ErrMemoryRequestInvalid, want, fc.MemoryHotplugSlotMiB)
	case want > regionMB:
		return 0, fmt.Errorf("%w: %d MiB above boot RAM exceeds the %d MiB hotplug region", fc.ErrMemoryRequestInvalid, want, regionMB)
	}
	if memoryResume && want < recordedMB {
		return 0, fmt.Errorf("%w: it asks for %d MiB above boot RAM and its snapshot had %d MiB", fc.ErrMemoryHotplugShrink, want, recordedMB)
	}

	return want, nil
}

// memoryHotplugUnusedBytes is the part of a hotplug region of regionMB, with slots of slotMB, that
// the guest cannot plug while Firecracker asks for requestedMB, and so needs no huge pages. The
// region is the last memory in Firecracker's memfd, so this is the file's tail. It holds while
// the request is only raised before the vCPUs run: a raise on a running sandbox would let the
// guest plug past a mapping it can no longer safely extend.
func memoryHotplugUnusedBytes(regionMB, slotMB, requestedMB int64) int64 {
	usableMB := fc.MemoryHotplugUsableMB(requestedMB, slotMB)
	if usableMB >= regionMB {
		return 0
	}

	return (regionMB - usableMB) << 20
}
