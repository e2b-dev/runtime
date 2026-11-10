//go:build linux

package fc

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A cold boot declares the ACPI power-off device only when asked; a
// Firecracker that refuses the field gets the machine again without it and
// the boot goes on; a restore declares it with a PATCH that carries nothing
// else, only when its guest asks for it.
func TestACPIPowerOffWireBodies(t *testing.T) {
	t.Parallel()

	versions := Config{KernelVersion: "vmlinux-6.1", FirecrackerVersion: "v1.14.1_431f1fc"}
	on := ProcessOptions{InitScriptPath: "/sbin/init", ACPIPowerOff: true}

	p, rec, _ := goldenProcess(t, versions, 41)
	require.NoError(t, p.configureGuestDevices(t.Context(), 4, 512, false, false, false, on, noRateLimit, noRateLimit))
	require.Len(t, rec.all("PUT /machine-config"), 1)
	assert.JSONEq(t, machineBody(4, 512, `,"acpi_poweroff":true`), rec.all("PUT /machine-config")[0])
	assert.True(t, p.ACPIPowerOffDeclared(), "the process took the device")

	p, rec, _ = goldenProcess(t, versions, 42)
	rec.refuse("acpi_poweroff")
	require.NoError(t, p.configureGuestDevices(t.Context(), 2, 512, false, false, false, on, noRateLimit, noRateLimit), "the boot goes on without the device")
	sent := rec.all("PUT /machine-config")
	require.Len(t, sent, 2, "refused once, then sent without the field")
	assert.JSONEq(t, machineBody(2, 512, ""), sent[1])
	assert.False(t, p.ACPIPowerOffDeclared(), "a refused device is not declared")

	p, rec, _ = goldenProcess(t, versions, 43)
	require.NoError(t, p.configureGuestDevices(t.Context(), 2, 512, false, false, false, ProcessOptions{InitScriptPath: "/sbin/init"}, noRateLimit, noRateLimit))
	assert.JSONEq(t, machineBody(2, 512, ""), rec.all("PUT /machine-config")[0], "off, the field stays out of the body")
	assert.False(t, p.ACPIPowerOffDeclared())

	p, rec, _ = goldenProcess(t, versions, 44)
	p.declareRestorePowerOff(t.Context(), GuestOptions{})
	assert.Empty(t, rec.keys(), "a restore whose guest does not ask sends nothing")
	assert.False(t, p.ACPIPowerOffDeclared())

	p.declareRestorePowerOff(t.Context(), GuestOptions{ACPIPowerOff: true})
	patches := rec.all("PATCH /machine-config")
	require.Len(t, patches, 1)
	assert.JSONEq(t, `{"acpi_poweroff":true,"mem_size_mib":null,"vcpu_count":null}`, patches[0])
	assert.True(t, p.ACPIPowerOffDeclared())

	p, rec, _ = goldenProcess(t, versions, 45)
	rec.refuse("acpi_poweroff")
	p.declareRestorePowerOff(t.Context(), GuestOptions{ACPIPowerOff: true})
	require.Len(t, rec.all("PATCH /machine-config"), 1)
	assert.False(t, p.ACPIPowerOffDeclared(), "a refused declaration restores without the device")
}
