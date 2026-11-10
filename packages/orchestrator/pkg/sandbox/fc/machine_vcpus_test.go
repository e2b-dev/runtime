package fc

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidMachineVcpus(t *testing.T) {
	t.Parallel()

	for _, n := range []int64{1, 2, 4, MaxVcpus} {
		assert.True(t, ValidMachineVcpus(n), "VM size %d", n)
	}
	for _, n := range []int64{0, -1, MaxVcpus + 1} {
		assert.False(t, ValidMachineVcpus(n), "VM size %d", n)
	}
	assert.Equal(t, runtime.GOARCH == "arm64", ValidMachineVcpus(3))
}

func TestValidateVcpus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		onlineVcpus int64
		vmVcpus     int64
		onlineName  string
		vmName      string
		wantErr     string
	}{
		{name: "vm size unset", onlineVcpus: 2},
		{name: "vm size equal", onlineVcpus: 2, vmVcpus: 2},
		{name: "vm size larger", onlineVcpus: 2, vmVcpus: 8},
		{name: "odd vm size is a boot concern, not a range one", onlineVcpus: 1, vmVcpus: 3},
		{name: "online at the maximum", onlineVcpus: MaxVcpus, vmVcpus: MaxVcpus},
		{name: "zero online", onlineVcpus: 0, vmVcpus: 2, wantErr: "vcpu 0 must be at least 1"},
		{name: "negative online", onlineVcpus: -1, wantErr: "vcpu -1 must be at least 1"},
		{name: "online above the maximum", onlineVcpus: MaxVcpus + 1, wantErr: "vcpu 33 is above the 32 a VM can have"},
		{name: "vm size below online", onlineVcpus: 4, vmVcpus: 2, wantErr: "vcpu 4 must be 1..max_vcpus 2"},
		{name: "vm size above the maximum", onlineVcpus: 2, vmVcpus: MaxVcpus + 2, wantErr: "max_vcpus 34 is above the 32 a VM can have"},
		{name: "caller names below online", onlineVcpus: 4, vmVcpus: 2, onlineName: "vCpuCount", vmName: "maxVCpuCount", wantErr: "vCpuCount 4 must be 1..maxVCpuCount 2"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			onlineName, vmName := "vcpu", "max_vcpus"
			if tt.onlineName != "" {
				onlineName, vmName = tt.onlineName, tt.vmName
			}

			err := ValidateVcpus(tt.onlineVcpus, tt.vmVcpus, onlineName, vmName)
			if tt.wantErr == "" {
				assert.NoError(t, err)

				return
			}
			assert.EqualError(t, err, tt.wantErr)
		})
	}
}
