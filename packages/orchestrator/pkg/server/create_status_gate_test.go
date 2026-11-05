//go:build linux

package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/cfg"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/service"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/service/machineinfo"
	"github.com/e2b-dev/infra/packages/orchestrator/pkg/servicestatus"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	orchestratorinfo "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator-info"
)

// statusGateTestServer is duplicateCreateTestServer with a status that can
// change; a zero ServiceInfo reports Healthy and panics on a status change.
func statusGateTestServer(t *testing.T) *Server {
	t.Helper()

	s := duplicateCreateTestServer()
	s.info = service.NewInfoContainer(t.Context(), "node", "version", "commit", "instance", machineinfo.MachineInfo{}, cfg.Config{BuilderConfig: cfg.BuilderConfig{OrchestratorBaseDir: t.TempDir()}})
	// The limit the replaced info had; at 0 the capacity check would refuse
	// with the same code before the gate is reached.
	s.info.MaxSandboxes.Store(10)

	return s
}

// A parked node admits nothing new. The refusal is a bare ResourceExhausted,
// the code the caller retries elsewhere without excluding this node, and it
// leaves no hold on the ID and no outstanding work behind. Once the owner
// releases the park, the same create reaches the reservation.
func TestCreate_RefusesWhileNotHealthy(t *testing.T) {
	t.Parallel()

	s := statusGateTestServer(t)
	require.NoError(t, s.info.OverrideStatus(t.Context(), servicestatus.Request{Status: orchestratorinfo.ServiceInfoStatus_Standby, Owner: "sizer"}))
	req := &orchestrator.SandboxCreateRequest{Sandbox: &orchestrator.SandboxConfig{SandboxId: "sandbox-1", Vcpu: 1}}

	_, err := s.Create(t.Context(), req)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.ResourceExhausted, st.Code())
	assert.Empty(t, st.Details(), "a UserError detail would make the caller give up on the sandbox")
	assert.Zero(t, s.info.OutstandingWork())

	// The refusal left the ID free. This hold stops the next create at the
	// reservation, before it reaches the template cache (nil here).
	r, err := s.sandboxFactory.Sandboxes.Reserve("sandbox-1")
	require.NoError(t, err)
	t.Cleanup(r.Release)

	require.NoError(t, s.info.OverrideStatus(t.Context(), servicestatus.Request{Status: orchestratorinfo.ServiceInfoStatus_Healthy, Owner: "sizer"}))
	_, err = s.Create(t.Context(), req)
	assert.Equal(t, codes.AlreadyExists, status.Code(err), "a Healthy node reaches the reservation")
}

// The gate is on "not Healthy", not on Standby: a draining node refuses too,
// and before the reservation, so the held ID is never the reason.
func TestCreate_RefusesWhileDraining(t *testing.T) {
	t.Parallel()

	s := statusGateTestServer(t)
	require.NoError(t, s.info.OverrideStatus(t.Context(), servicestatus.Request{Status: orchestratorinfo.ServiceInfoStatus_Draining}))
	r, err := s.sandboxFactory.Sandboxes.Reserve("sandbox-1")
	require.NoError(t, err)
	t.Cleanup(r.Release)

	_, err = s.Create(t.Context(), &orchestrator.SandboxCreateRequest{Sandbox: &orchestrator.SandboxConfig{SandboxId: "sandbox-1", Vcpu: 1}})
	assert.Equal(t, codes.ResourceExhausted, status.Code(err))
}
