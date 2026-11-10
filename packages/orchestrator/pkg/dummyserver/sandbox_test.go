package dummyserver

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/servicestatus"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
)

// Creates are refused with the code the real orchestrator gives while the node
// is not Healthy or is at its sandbox limit.
func TestDummySandbox_RefusesWhileStandbyAndAtCapacity(t *testing.T) {
	t.Parallel()

	machine := servicestatus.New(servicestatus.NopStore{}, time.Now)
	s := NewSandbox(machine, 1)
	create := func(id string) error {
		_, err := s.Create(t.Context(), &orchestrator.SandboxCreateRequest{Sandbox: &orchestrator.SandboxConfig{SandboxId: id}})

		return err
	}

	require.NoError(t, machine.Override(t.Context(), servicestatus.Request{Status: standby, Owner: "sizer-1"}))
	refused, ok := status.FromError(create("sbx-1"))
	require.True(t, ok)
	assert.Equal(t, codes.ResourceExhausted, refused.Code())
	require.Len(t, refused.Details(), 1, "the status detail the api skips the node on")
	detail, ok := refused.Details()[0].(*orchestrator.UserError)
	require.True(t, ok)
	assert.Equal(t, orchestrator.UserErrorCode_NODE_NOT_ACCEPTING_SANDBOXES, detail.GetCode())
	assert.Zero(t, s.Count())

	require.NoError(t, machine.Override(t.Context(), servicestatus.Request{Status: healthy, Owner: "sizer-1"}))
	require.NoError(t, create("sbx-1"))
	assert.Equal(t, uint32(1), s.Count())
	full, ok := status.FromError(create("sbx-2"))
	require.True(t, ok)
	assert.Equal(t, codes.ResourceExhausted, full.Code())
	assert.Empty(t, full.Details(), "a capacity refusal carries no detail: the api retries the node")
	assert.Equal(t, uint32(1), s.Count())

	closed := NewSandbox(machine, 0)
	_, err := closed.Create(t.Context(), &orchestrator.SandboxCreateRequest{Sandbox: &orchestrator.SandboxConfig{SandboxId: "sbx-3"}})
	assert.Equal(t, codes.ResourceExhausted, status.Code(err), "a limit of zero closes the node")
}

func TestDummySandbox_CreateListDelete(t *testing.T) {
	t.Parallel()

	s := NewSandbox(servicestatus.New(servicestatus.NopStore{}, time.Now), 100)
	_, err := s.Create(t.Context(), &orchestrator.SandboxCreateRequest{Sandbox: &orchestrator.SandboxConfig{SandboxId: "sbx-1", TeamId: "team"}})
	require.NoError(t, err)

	list, err := s.List(t.Context(), &emptypb.Empty{})
	require.NoError(t, err)
	require.Len(t, list.GetSandboxes(), 1)
	assert.Equal(t, "sbx-1", list.GetSandboxes()[0].GetSandboxId())
	assert.Equal(t, s.ClientID(), list.GetSandboxes()[0].GetClientId())

	_, err = s.Delete(t.Context(), &orchestrator.SandboxDeleteRequest{SandboxId: "sbx-1"})
	require.NoError(t, err)
	assert.Zero(t, s.Count())
	list, err = s.List(t.Context(), &emptypb.Empty{})
	require.NoError(t, err)
	assert.Empty(t, list.GetSandboxes())
}
