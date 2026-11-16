//go:build linux

package fc

import (
	"errors"
	"fmt"
	"net/http"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateMemoryHotplug(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		memory, limit int64
		wantErr       string
	}{
		{name: "equal is no region", memory: 1024, limit: 1024},
		{name: "one slot", memory: 1024, limit: 1152},
		{name: "whole slots above boot RAM that is not a slot multiple", memory: 1000, limit: 1000 + 3*MemoryHotplugSlotMiB},
		{name: "region at the cap", memory: 1024, limit: 1024 + MaxMemoryHotplugMiB},
		{name: "no region never meets the cap", memory: 2 * MaxMemoryHotplugMiB, limit: 2 * MaxMemoryHotplugMiB},
		{name: "a small region above large boot RAM", memory: 2 * MaxMemoryHotplugMiB, limit: 2*MaxMemoryHotplugMiB + 128},
		{name: "partial slot", memory: 1024, limit: 1100, wantErr: "multiple of 128 MiB"},
		{name: "below boot RAM", memory: 2048, limit: 1024, wantErr: "must be at least memoryMB 2048"},
		{name: "zero", memory: 1024, limit: 0, wantErr: "must be at least memoryMB 1024"},
		{name: "no boot RAM", memory: 0, limit: 1024, wantErr: "memoryMB 0 must be at least 1"},
		{name: "region above the cap", memory: 1024, limit: 1024 + MaxMemoryHotplugMiB + MemoryHotplugSlotMiB, wantErr: "above the 131072 MiB a region can have"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := ValidateMemoryHotplug(tt.memory, tt.limit, "memoryMB", "maxMemoryMB")
			if tt.wantErr == "" {
				require.NoError(t, err)

				return
			}
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

// No caller can give a VM a region without the capability and the online policy.
func TestRefuseMemoryHotplug(t *testing.T) {
	t.Parallel()

	ok := Config{KernelVersion: "vmlinux-6.1.158", FirecrackerVersion: "v1.17-0.4.1"}
	require.NoError(t, ok.RefuseMemoryHotplug(0))
	require.NoError(t, ok.RefuseMemoryHotplug(3072))
	require.NoError(t, Config{FirecrackerVersion: "v1.14-0.3.1"}.RefuseMemoryHotplug(0), "no region needs no capability")
	require.ErrorIs(t, Config{KernelVersion: "vmlinux-5.10.186", FirecrackerVersion: "v1.17-0.4.1"}.RefuseMemoryHotplug(3072), ErrMemoryHotplugKernel)
	require.ErrorIs(t, Config{FirecrackerVersion: "v1.14-0.3.1"}.RefuseMemoryHotplug(3072), ErrMemoryHotplugFirecracker)

	for name, tc := range map[string]struct {
		cfg    Config
		region int64
	}{
		"old release":            {Config{FirecrackerVersion: "v1.14-0.3.1"}, 3072},
		"legacy build":           {Config{FirecrackerVersion: "v1.14.1_431f1fc"}, 3072},
		"unparseable version":    {Config{FirecrackerVersion: "nope"}, 3072},
		"old guest kernel":       {Config{KernelVersion: "vmlinux-5.10.186_abc1234", FirecrackerVersion: "v1.17-0.4.1"}, 3072},
		"no kernel command line": {Config{FirecrackerVersion: "v1.17-0.4.1", Guest: firmwareProfile{}}, 3072},
		"partial slot":           {ok, 100},
		"negative":               {ok, -128},
		"above the cap":          {ok, MaxMemoryHotplugMiB + MemoryHotplugSlotMiB},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			require.ErrorIs(t, tc.cfg.RefuseMemoryHotplug(tc.region), ErrMemoryHotplugUnsupported)
		})
	}
}

// A boot with a region sends it with Firecracker's default sizes before the VM starts, and the
// guest gets the online policy on its command line.
func TestMemoryHotplugCreateWireBodies(t *testing.T) {
	t.Parallel()

	p, rec, slot := goldenProcess(t, Config{KernelVersion: "vmlinux-6.1", FirecrackerVersion: "v1.17-0.4.1"}, 51)
	options := ProcessOptions{InitScriptPath: "/sbin/init", MemoryHotplugMB: 3072}

	require.NoError(t, p.configureGuestDevices(t.Context(), 2, 1024, true, false, false, options, noRateLimit, noRateLimit))

	assert.JSONEq(t, `{"total_size_mib":3072,"block_size_mib":2,"slot_size_mib":128}`, rec.body(t, "PUT /hotplug/memory"))
	assert.JSONEq(t, machineBody(2, 1024, `,"huge_pages":"2M"`), rec.body(t, "PUT /machine-config"), "boot RAM stays the machine's memory")
	ipv4 := fmt.Sprintf("%s::%s:%s:instance:%s:off:%s", slot.NamespaceIP(), slot.TapIPString(), slot.TapMaskString(), slot.VpeerName(), slot.TapName())
	assert.Contains(t, buildKernelArgs(ipv4, options).String(), "memhp_default_state=online")
	assert.Contains(t, rec.body(t, "PUT /boot-source"), "memory_hotplug.online_policy=auto-movable")

	p, rec, _ = goldenProcess(t, Config{KernelVersion: "vmlinux-6.1", FirecrackerVersion: "v1.17-0.4.1"}, 52)
	require.NoError(t, p.configureGuestDevices(t.Context(), 2, 1024, true, false, false, ProcessOptions{InitScriptPath: "/sbin/init"}, noRateLimit, noRateLimit))
	assert.Empty(t, rec.all("PUT /hotplug/memory"), "no region, no device")
	assert.NotContains(t, rec.body(t, "PUT /boot-source"), "memhp_default_state")
}

// A Firecracker that refuses the region stops the process and fails the boot.
func TestMemoryHotplugRefusedStopsTheProcess(t *testing.T) {
	t.Parallel()

	p, rec, _ := goldenProcess(t, Config{KernelVersion: "vmlinux-6.1", FirecrackerVersion: "v1.17-0.4.1"}, 53)
	rec.mu.Lock()
	rec.answers["PUT /hotplug/memory"] = stubAnswer{status: http.StatusBadRequest, body: `{"fault_message":"region does not fit"}`}
	rec.mu.Unlock()
	p.cmd = &exec.Cmd{}

	err := p.configureGuestDevices(t.Context(), 2, 1024, true, false, false, ProcessOptions{InitScriptPath: "/sbin/init", MemoryHotplugMB: 3072}, noRateLimit, noRateLimit)
	require.ErrorContains(t, err, "error setting fc memory hotplug")
	require.ErrorContains(t, err, "fc process not started", "the error carries the Stop of the process")
}

// Create refuses a region its boot cannot carry before it starts anything.
func TestCreateRefusesAnUnsupportedRegion(t *testing.T) {
	t.Parallel()

	require.ErrorIs(t, (&Process{Versions: Config{FirecrackerVersion: "v1.14-0.3.1"}}).Create(
		t.Context(), nil, 1024, true, false, false, ProcessOptions{MemoryHotplugMB: 3072}, noRateLimit, noRateLimit, nil, 2, 0),
		ErrMemoryHotplugUnsupported, "Create refuses before it starts anything")
}

// Only a kernel name that reads as too old is refused; one that does not read is let through.
func TestKernelTooOldForMemoryHotplug(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		arch, kernel string
		old          bool
	}{
		{"amd64", "vmlinux-6.1.158", false},
		{"amd64", "vmlinux-6.1.158_b15756f", false},
		{"amd64", "vmlinux-5.16.0", false},
		{"amd64", "vmlinux-5.15.120", true},
		{"amd64", "vmlinux-5.10.186", true},
		{"amd64", "vmlinux-4.14.174", true},
		{"arm64", "vmlinux-5.17.1", true},
		{"arm64", "vmlinux-5.18.0", false},
		{"amd64", "firmware-fd", false},
		{"amd64", "", false},
		{"riscv64", "vmlinux-5.10.186", false},
	} {
		assert.Equal(t, tc.old, kernelTooOldForMemoryHotplug(tc.arch, tc.kernel), "%s %s", tc.arch, tc.kernel)
	}
}

func TestValidateMemoryHotplugRatio(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                    string
		memory, limit, maxRatio int64
		wantErr                 string
	}{
		{name: "at the default cap", memory: 1024, limit: 4096, maxRatio: DefaultMemoryHotplugMaxRatio},
		{name: "one slot above the cap", memory: 1024, limit: 4224, maxRatio: 3, wantErr: "the largest allowed maxMemoryMB is 4096"},
		{name: "names the cap", memory: 1024, limit: 8192, maxRatio: 3, wantErr: "above the hotplug cap of 3 times boot RAM (1024 MiB)"},
		{name: "largest allowed is whole slots", memory: 1000, limit: 8000, maxRatio: 3, wantErr: "the largest allowed maxMemoryMB is 3944"},
		{name: "ratio 0 is no cap", memory: 512, limit: 65536, maxRatio: 0},
		{name: "ratio 1", memory: 2048, limit: 4096, maxRatio: 1},
		{name: "ratio 1 exceeded", memory: 2048, limit: 4224, maxRatio: 1, wantErr: "the largest allowed maxMemoryMB is 4096"},
		{name: "no region", memory: 1024, limit: 1024, maxRatio: 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := ValidateMemoryHotplugRatio(tt.memory, tt.limit, tt.maxRatio, "maxMemoryMB")
			if tt.wantErr == "" {
				require.NoError(t, err)

				return
			}
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

// A restore needs only the release: kernel, profile and size rules belong to a boot.
func TestRefuseMemoryHotplugRestore(t *testing.T) {
	t.Parallel()

	require.NoError(t, Config{FirecrackerVersion: "v1.14-0.3.1"}.RefuseMemoryHotplugRestore(0))
	require.NoError(t, Config{KernelVersion: "vmlinux-5.10.186", FirecrackerVersion: "v1.17-0.4.1", Guest: firmwareProfile{}}.RefuseMemoryHotplugRestore(100))
	require.ErrorIs(t, Config{KernelVersion: "vmlinux-6.1.158", FirecrackerVersion: "v1.14-0.3.1"}.RefuseMemoryHotplugRestore(3072), ErrMemoryHotplugFirecracker)
	require.ErrorIs(t, Config{FirecrackerVersion: "v1.14.1_431f1fc"}.RefuseMemoryHotplugRestore(3072), ErrMemoryHotplugFirecracker)
}

// PluggedMemoryMB reads what Firecracker reports as plugged now.
func TestPluggedMemoryMB(t *testing.T) {
	t.Parallel()

	p, rec, _ := goldenProcess(t, Config{KernelVersion: "vmlinux-6.1", FirecrackerVersion: "v1.17-0.4.1"}, 55)
	rec.mu.Lock()
	rec.answers["GET /hotplug/memory"] = stubAnswer{status: http.StatusOK, body: `{"total_size_mib":3072,"block_size_mib":2,"slot_size_mib":128,"plugged_size_mib":1024,"requested_size_mib":1024}`}
	rec.mu.Unlock()

	plugged, err := p.PluggedMemoryMB(t.Context())
	require.NoError(t, err)
	assert.Equal(t, int64(1024), plugged)

	rec.mu.Lock()
	rec.answers["GET /hotplug/memory"] = stubAnswer{status: http.StatusOK, body: `{"total_size_mib":3072,"plugged_size_mib":512,"requested_size_mib":2048}`}
	rec.mu.Unlock()
	status, err := p.MemoryHotplugStatus(t.Context())
	require.NoError(t, err)
	assert.Equal(t, MemoryHotplugStatus{PluggedMB: 512, RequestedMB: 2048, TotalMB: 3072}, status)

	rec.mu.Lock()
	rec.answers["GET /hotplug/memory"] = stubAnswer{status: http.StatusBadRequest, body: `{"fault_message":"no hotplug device"}`}
	rec.mu.Unlock()
	_, err = p.PluggedMemoryMB(t.Context())
	require.Error(t, err)
}

func TestMemoryRequestErrorsNameOnlyTheRequest(t *testing.T) {
	t.Parallel()

	for _, err := range []error{ErrMemoryRequestInvalid, ErrMemoryHotplugShrink} {
		require.ErrorIs(t, err, ErrMemoryRequestInvalid)
		require.ErrorIs(t, err, ErrMemoryHotplugUnsupported)
		require.NotContains(t, err.Error(), "cannot carry a memory hotplug region")
	}
	require.Equal(t, "the requested memory does not fit the sandbox: a memory resume cannot shrink the sandbox", ErrMemoryHotplugShrink.Error())
}

// A restore sends its hotplug target only when it is above the one the snapshot restored. An
// explicit target is always checked against the restored device, which refuses one below its own
// request whatever the metadata said; a kept one only plugs up to the recorded size.
func TestApplyMemoryHotplugTarget(t *testing.T) {
	t.Parallel()

	patches := func(rec *wireRecorder) []string {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		var bodies []string
		for _, r := range rec.sent {
			if r.key == "PATCH /hotplug/memory" {
				bodies = append(bodies, r.body)
			}
		}

		return bodies
	}

	for name, tc := range map[string]struct {
		noTarget  bool
		target    int64
		explicit  bool
		restored  int64
		wantPatch bool
		wantErr   error
	}{
		"no target keeps the snapshot's":                   {noTarget: true, restored: 1024},
		"the same explicit target is not resent":           {target: 1024, explicit: true, restored: 1024},
		"a larger explicit target is sent":                 {target: 2048, explicit: true, restored: 1024, wantPatch: true},
		"an explicit target below the device's is refused": {target: 1024, explicit: true, restored: 2048, wantErr: ErrMemoryHotplugShrink},
		"boot RAM named over stale metadata is refused":    {target: 0, explicit: true, restored: 1024, wantErr: ErrMemoryHotplugShrink},
		"a kept size the device lost is plugged again":     {target: 2048, restored: 0, wantPatch: true},
		"a kept size the device already has is not resent": {target: 2048, restored: 2048},
		"a device above the kept size is left as it is":    {target: 1024, restored: 2048},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			p, rec, _ := goldenProcess(t, Config{KernelVersion: "vmlinux-6.1", FirecrackerVersion: "v1.17-0.4.1"}, 56)
			rec.mu.Lock()
			rec.answers["GET /hotplug/memory"] = stubAnswer{status: http.StatusOK, body: fmt.Sprintf(`{"total_size_mib":3072,"plugged_size_mib":%d,"requested_size_mib":%d}`, tc.restored, tc.restored)}
			rec.mu.Unlock()

			if !tc.noTarget {
				p.SetMemoryHotplugTarget(tc.target, tc.explicit)
			}
			err := p.applyMemoryHotplugTarget(t.Context())
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tc.wantPatch, p.MemoryHotplugResized())
			if tc.noTarget {
				rec.mu.Lock()
				for _, r := range rec.sent {
					assert.NotEqual(t, "GET /hotplug/memory", r.key, "no target, no hotplug query")
				}
				rec.mu.Unlock()
			}
			if tc.wantPatch {
				assert.JSONEq(t, fmt.Sprintf(`{"requested_size_mib":%d}`, tc.target), patches(rec)[0])
			} else {
				assert.Empty(t, patches(rec))
			}
		})
	}

	p, rec, _ := goldenProcess(t, Config{KernelVersion: "vmlinux-6.1", FirecrackerVersion: "v1.17-0.4.1"}, 57)
	rec.mu.Lock()
	rec.answers["GET /hotplug/memory"] = stubAnswer{status: http.StatusBadRequest, body: `{"fault_message":"no hotplug device"}`}
	rec.mu.Unlock()
	p.SetMemoryHotplugTarget(2048, true)
	err := p.applyMemoryHotplugTarget(t.Context())
	require.ErrorIs(t, err, errMemoryHotplugStatusUnread, "a status that cannot be read fails the restore")
	assert.Contains(t, err.Error(), "memory hotplug status")
}

// The restore tells its callback the device's state with the size the guest is asked to plug: the
// new target when one was sent, the device's own request otherwise, even when a kept target is
// below it. Without a target the callback still gets the device's state.
func TestApplyMemoryHotplugTargetTellsTheCallback(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		noTarget      bool
		target        int64
		explicit      bool
		restored      int64
		wantRequested int64
		wantErr       error
		wantCalled    bool
	}{
		"no target reports the device":                      {noTarget: true, restored: 1024, wantRequested: 1024, wantCalled: true},
		"a sent target is reported":                         {target: 2048, explicit: true, restored: 1024, wantRequested: 2048, wantCalled: true},
		"a kept target below the device reports the device": {target: 1024, restored: 2048, wantRequested: 2048, wantCalled: true},
		"a refused shrink never reaches it":                 {target: 1024, explicit: true, restored: 2048, wantErr: ErrMemoryHotplugShrink},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			p, rec, _ := goldenProcess(t, Config{KernelVersion: "vmlinux-6.1", FirecrackerVersion: "v1.17-0.4.1"}, 58)
			rec.mu.Lock()
			rec.answers["GET /hotplug/memory"] = stubAnswer{status: http.StatusOK, body: fmt.Sprintf(`{"total_size_mib":3072,"slot_size_mib":128,"plugged_size_mib":%d,"requested_size_mib":%d}`, tc.restored, tc.restored)}
			rec.mu.Unlock()

			var got *MemoryHotplugStatus
			p.OnMemoryHotplugRestored(func(s MemoryHotplugStatus) error {
				got = &s

				return nil
			})
			if !tc.noTarget {
				p.SetMemoryHotplugTarget(tc.target, tc.explicit)
			}
			err := p.applyMemoryHotplugTarget(t.Context())
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}
			if !tc.wantCalled {
				assert.Nil(t, got)

				return
			}
			require.NotNil(t, got)
			assert.Equal(t, MemoryHotplugStatus{PluggedMB: tc.restored, RequestedMB: tc.wantRequested, TotalMB: 3072, SlotMB: 128}, *got)
		})
	}

	p, rec, _ := goldenProcess(t, Config{KernelVersion: "vmlinux-6.1", FirecrackerVersion: "v1.17-0.4.1"}, 59)
	rec.mu.Lock()
	rec.answers["GET /hotplug/memory"] = stubAnswer{status: http.StatusOK, body: `{"total_size_mib":3072,"slot_size_mib":128,"plugged_size_mib":0,"requested_size_mib":0}`}
	rec.mu.Unlock()
	wantErr := errors.New("extend failed")
	p.OnMemoryHotplugRestored(func(MemoryHotplugStatus) error { return wantErr })
	require.ErrorIs(t, p.applyMemoryHotplugTarget(t.Context()), wantErr, "a callback failure fails the restore")
}

// Firecracker's answers to a hotplug call are classed: an inactive device passes once the guest is
// up, any other 4xx is permanent, and a 5xx or a transport failure may pass.
func TestMemoryHotplugErrorClasses(t *testing.T) {
	t.Parallel()

	p, rec, _ := goldenProcess(t, Config{KernelVersion: "vmlinux-6.1", FirecrackerVersion: "v1.17-0.4.1"}, 59)
	answer := func(status int, body string) error {
		rec.mu.Lock()
		rec.answers["PATCH /hotplug/memory"] = stubAnswer{status: status, body: body}
		rec.mu.Unlock()

		return p.RequestMemoryHotplug(t.Context(), 2048)
	}

	err := answer(http.StatusBadRequest, `{"fault_message":"Virtio memory error: Device is not active"}`)
	assert.True(t, MemoryHotplugDeviceNotActive(err))
	assert.False(t, MemoryHotplugErrorPermanent(err))

	err = answer(http.StatusBadRequest, `{"fault_message":"Virtio memory error: Size 3 is invalid"}`)
	assert.False(t, MemoryHotplugDeviceNotActive(err))
	assert.True(t, MemoryHotplugErrorPermanent(err))

	err = answer(http.StatusInternalServerError, `{"fault_message":"internal"}`)
	assert.False(t, MemoryHotplugErrorPermanent(err))

	assert.False(t, MemoryHotplugErrorPermanent(errors.New("dial unix: connection refused")))
	assert.False(t, MemoryHotplugDeviceNotActive(nil))
}
