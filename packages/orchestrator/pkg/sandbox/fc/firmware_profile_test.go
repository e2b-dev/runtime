package fc

import (
	"encoding/json"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/guest"
)

// firmwareProfile is a guest profile that asks for every boot and restore
// difference the profile can declare.
type firmwareProfile struct {
	guest.Profile
}

func (firmwareProfile) Name() string  { return "firmware" }
func (firmwareProfile) Label() string { return "firmware" }

func (firmwareProfile) Boot() guest.BootConfig {
	return guest.BootConfig{
		NoKernelArgs:       true,
		PCI:                true,
		MachineConfigExtra: map[string]json.RawMessage{"x_extension": json.RawMessage(`true`)},
		NoRootDevice:       true,
		BlockSectorSize:    512,
		NoEntropy:          true,
		NoBalloon:          true,
		NoCPUTemplate:      true,
	}
}

func (firmwareProfile) Restore() guest.RestoreConfig {
	return guest.RestoreConfig{ClockRealtime: true, MmdsStoreOptional: true}
}
