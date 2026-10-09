package server

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
)

func TestCreate_RefusesInvalidVcpuLimits(t *testing.T) {
	t.Parallel()

	for name, cfg := range map[string]*orchestrator.SandboxConfig{
		"zero vcpu without maximum":     {Vcpu: 0},
		"negative vcpu without maximum": {Vcpu: -1},
		"maximum below vcpu":            {Vcpu: 4, MaxVcpus: 2},
		"maximum above firecracker's":   {Vcpu: 2, MaxVcpus: 33},
		"vcpu above firecracker's":      {Vcpu: 33},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			s := duplicateCreateTestServer()
			cfg.SandboxId = "sbx-vcpu"
			cfg.ExecutionId = "exec-vcpu"
			cfg.Snapshot = true

			_, err := s.Create(t.Context(), &orchestrator.SandboxCreateRequest{Sandbox: cfg})
			require.Error(t, err)
			st, ok := status.FromError(err)
			require.True(t, ok)
			assert.Equal(t, codes.InvalidArgument, st.Code())
			assert.Equal(t, 0, s.sandboxFactory.Sandboxes.Count(), "nothing is reserved for a refused request")
		})
	}
}

func TestValidateColdBootVcpus(t *testing.T) {
	t.Parallel()

	require.NoError(t, validateColdBootVcpus(3, 4, 0), "the active target may be odd when the VM is even-sized")
	require.NoError(t, validateColdBootVcpus(2, 2, 4), "an explicit maximum may shrink the VM")
	require.NoError(t, validateColdBootVcpus(2, 0, 4), "an omitted maximum inherits the recorded capacity")
	if runtime.GOARCH != "arm64" {
		assert.Equal(t, codes.InvalidArgument, status.Code(validateColdBootVcpus(2, 3, 4)), "validate the explicit VM size, not the old one")
		assert.Equal(t, codes.InvalidArgument, status.Code(validateColdBootVcpus(2, 3, 0)))
		assert.Equal(t, codes.InvalidArgument, status.Code(validateColdBootVcpus(3, 0, 0)))
	}
}
