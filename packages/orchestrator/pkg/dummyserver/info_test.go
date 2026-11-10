package dummyserver

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/e2b-dev/infra/packages/orchestrator/pkg/servicestatus"
	orchestratorinfo "github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator-info"
)

const (
	healthy  = orchestratorinfo.ServiceInfoStatus_Healthy
	standby  = orchestratorinfo.ServiceInfoStatus_Standby
	draining = orchestratorinfo.ServiceInfoStatus_Draining
	notReady = orchestratorinfo.ServiceInfoStatus_NotReady
)

// The dummy answers overrides with the shared rules, so a test cluster running
// it gets the codes a real orchestrator gives.
func TestDummyInfo_RunsTheSharedRules(t *testing.T) {
	t.Parallel()

	machine := servicestatus.New(servicestatus.NopStore{}, time.Now)
	info := NewInfo(InfoOptions{NodeID: "dummy", Machine: machine})
	override := func(s orchestratorinfo.ServiceInfoStatus, owner string, force bool) codes.Code {
		t.Helper()

		_, err := info.ServiceStatusOverride(t.Context(), &orchestratorinfo.ServiceStatusChangeRequest{ServiceStatus: s, Owner: owner, Force: force})

		return status.Code(err)
	}

	assert.Equal(t, codes.InvalidArgument, override(standby, "Bad_Owner", false))
	assert.Equal(t, codes.OK, override(standby, "sizer-1", false))
	assert.Equal(t, codes.FailedPrecondition, override(standby, "sizer-2", false), "another owner cannot take the park")
	assert.Equal(t, codes.FailedPrecondition, override(notReady, "", false), "process-only status")
	assert.Equal(t, codes.OK, override(healthy, "", false), "a person releases without the owner")
	assert.Equal(t, codes.OK, override(standby, "sizer-2", false), "the released node parks under a new owner")
	assert.Equal(t, codes.OK, override(draining, "", false), "a person drains an owned park")
	assert.Equal(t, codes.OK, override(healthy, "", true), "force applies too")
	assert.Equal(t, servicestatus.Snapshot{Status: healthy, ChangedAt: machine.Snapshot().ChangedAt}, machine.Snapshot())
}

func TestDummyInfo_ReportsOwnerCapabilitiesAndCounts(t *testing.T) {
	t.Parallel()

	machine := servicestatus.New(servicestatus.NopStore{}, time.Now)
	info := NewInfo(InfoOptions{
		NodeID: "dummy", ServiceID: "svc", Version: "v", Commit: "c", Labels: []string{"gpu"},
		Machine: machine, MaxSandboxes: 7, SandboxCount: func() uint32 { return 3 },
	})
	require.NoError(t, machine.Override(t.Context(), servicestatus.Request{Status: standby, Owner: "sizer-1"}))

	resp, err := info.ServiceInfo(t.Context(), &emptypb.Empty{})
	require.NoError(t, err)
	assert.Equal(t, "dummy", resp.GetNodeId())
	assert.Equal(t, "svc", resp.GetServiceId())
	assert.Equal(t, standby, resp.GetServiceStatus())
	assert.Equal(t, "sizer-1", resp.GetStatusOwner())
	assert.Equal(t, []string{"park-owner"}, resp.GetCapabilities())
	assert.Equal(t, []string{"gpu"}, resp.GetLabels())
	assert.Equal(t, int64(7), resp.GetMaxSandboxes())
	assert.Equal(t, uint32(3), resp.GetMetricSandboxesRunning())
	assert.Zero(t, resp.GetOutstandingWork())
	assert.True(t, resp.GetServiceStatusChangedAt().IsValid())
}

// A Standby survives a restart through the state dir, created when missing;
// without one nothing persists; a corrupt record is reported and the node
// starts Healthy.
func TestDummyInfo_RestoresFromStateDir(t *testing.T) {
	t.Parallel()

	dir := filepath.Join(t.TempDir(), "state")
	first, err := NewMachine(t.Context(), dir)
	require.NoError(t, err)
	require.NoError(t, first.Override(t.Context(), servicestatus.Request{Status: standby, Owner: "sizer-1"}))

	second, err := NewMachine(t.Context(), dir)
	require.NoError(t, err)
	resp, err := NewInfo(InfoOptions{NodeID: "dummy", Machine: second}).ServiceInfo(t.Context(), &emptypb.Empty{})
	require.NoError(t, err)
	assert.Equal(t, standby, resp.GetServiceStatus())
	assert.Equal(t, "sizer-1", resp.GetStatusOwner())

	none, err := NewMachine(t.Context(), "")
	require.NoError(t, err)
	assert.Equal(t, healthy, none.Snapshot().Status)

	require.NoError(t, os.WriteFile(filepath.Join(dir, servicestatus.FileName), []byte("{"), 0o600))
	corrupt, err := NewMachine(t.Context(), dir)
	require.Error(t, err)
	assert.Equal(t, healthy, corrupt.Snapshot().Status)

	blocked := filepath.Join(dir, "blocked")
	require.NoError(t, os.WriteFile(blocked, nil, 0o600))
	none, err = NewMachine(t.Context(), blocked)
	require.Error(t, err, "a state dir that cannot be created")
	assert.Nil(t, none)
}
