//go:build linux

package server

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/sandbox/fc"
	templatemanager "github.com/e2b-dev/infra/packages/shared/pkg/grpc/template-manager"
)

// A VM size the host cannot boot must fail the request, not the build mid-way.
func TestValidateMaxVCpuCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		vcpu    int32
		max     int32
		unset   bool
		wantErr bool
		wantMsg string
	}{
		{name: "unset keeps the VM at vCpuCount", vcpu: 2, unset: true},
		{name: "explicit zero", vcpu: 2, max: 0, wantErr: true},
		{name: "negative", vcpu: 2, max: -1, wantErr: true, wantMsg: "maxVCpuCount must be at least 1"},
		{name: "equal to vCpuCount", vcpu: 2, max: 2},
		{name: "larger even", vcpu: 2, max: 8},
		{name: "odd online with even maximum", vcpu: 3, max: 4, wantErr: runtime.GOARCH != "arm64"},
		{name: "below vCpuCount", vcpu: 4, max: 2, wantErr: true, wantMsg: "maxVCpuCount"},
		{name: "no online vCPUs", vcpu: 0, max: 8, wantErr: true},
		{name: "odd on x86", vcpu: 2, max: 3, wantErr: runtime.GOARCH != "arm64"},
		{name: "above the Firecracker maximum", vcpu: 2, max: fc.MaxVcpus + 2, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := &templatemanager.TemplateConfig{VCpuCount: tt.vcpu}
			if !tt.unset {
				cfg.MaxVCpuCount = &tt.max
			}

			err := validateMaxVCpuCount(cfg)
			if !tt.wantErr {
				require.NoError(t, err)

				return
			}
			require.Error(t, err)
			assert.Equal(t, codes.InvalidArgument, status.Code(err))
			assert.Contains(t, status.Convert(err).Message(), tt.wantMsg)
		})
	}
}
