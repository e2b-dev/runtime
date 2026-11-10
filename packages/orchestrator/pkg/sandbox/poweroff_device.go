package sandbox

import (
	"context"

	"github.com/e2b-dev/infra/packages/shared/pkg/featureflags"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxtypes"
)

// acpiPowerOffFor is guest-poweroff-end for the sandbox: whether a cold boot
// declares the ACPI power-off device, and the flag half of the restore gate.
// Off, a guest power-off halts the vCPUs and the sandbox keeps its disk until
// its timeout action, as it always has; the device would end the process and
// drop the disk instead.
func acpiPowerOffFor(ctx context.Context, ff *featureflags.Client, runtime sandboxtypes.RuntimeMetadata, config *Config) bool {
	return ff != nil && ff.BoolFlag(ctx, featureflags.GuestPowerOffEndFlag, sandboxLDContext(runtime, config))
}

// acpiPowerOffOnRestoreFor is whether a restore declares the device before
// the load: when guest-poweroff-end is on and the guest's profile sends the
// declaration. The default guest does not: the fleet's Firecracker refuses it.
func acpiPowerOffOnRestoreFor(ctx context.Context, ff *featureflags.Client, runtime sandboxtypes.RuntimeMetadata, config *Config) bool {
	return config.GuestProfile().Restore().PowerOffPatch && acpiPowerOffFor(ctx, ff, runtime, config)
}

// exitDrain is exitCountersWanted, asked at the process's exit.
func (f *Factory) exitDrain(ctx context.Context, runtime sandboxtypes.RuntimeMetadata, config *Config) func() bool {
	ctx = context.WithoutCancel(ctx)

	return func() bool { return f.exitCountersWanted(ctx, runtime, config) }
}

// exitCountersWanted is whether a process's exit waits for its final guest
// counters: only when something reads them, a reboot in place, a power-off
// hold, an ended-on-node listing, or a profile that reports crashes. Off,
// an exit is classified as it was before the counters, with no wait.
func (f *Factory) exitCountersWanted(ctx context.Context, runtime sandboxtypes.RuntimeMetadata, config *Config) bool {
	if config.GuestProfile().Reset().Crash.Group != "" {
		return true
	}
	if f.featureFlags == nil {
		return false
	}
	ld := sandboxLDContext(runtime, config)

	return f.featureFlags.BoolFlag(ctx, featureflags.GuestRebootInPlaceFlag, ld) ||
		f.featureFlags.BoolFlag(ctx, featureflags.GuestPowerOffEndFlag, ld) ||
		f.featureFlags.BoolFlag(ctx, featureflags.SandboxEndedOnNodeFlag, ld)
}
