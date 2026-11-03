package nodemanager

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/e2b-dev/infra/packages/api/internal/api"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
)

// mockSandboxListClient implements orchestrator.SandboxServiceClient and returns
// a canned List response, so GetOrphanCandidates can be tested without a live
// orchestrator.
type mockSandboxListClient struct {
	orchestrator.SandboxServiceClient

	resp *orchestrator.SandboxListResponse
}

func (m *mockSandboxListClient) List(_ context.Context, _ *emptypb.Empty, _ ...grpc.CallOption) (*orchestrator.SandboxListResponse, error) {
	return m.resp, nil
}

func newTestNodeWithList(t *testing.T, sandboxes ...*orchestrator.RunningSandbox) *TestNode {
	t.Helper()

	node := NewTestNode("test-node", api.NodeStatusReady, 0, 4)
	node.SetSandboxClient(&mockSandboxListClient{
		resp: &orchestrator.SandboxListResponse{Sandboxes: sandboxes},
	})

	return node
}

// TestGetOrphanCandidates_ReadsScalarFields covers the current orchestrator,
// which sends the identity and resource fields directly on RunningSandbox.
func TestGetOrphanCandidates_ReadsScalarFields(t *testing.T) {
	t.Parallel()

	now := time.Now()
	teamID := uuid.New()
	node := newTestNodeWithList(t, &orchestrator.RunningSandbox{
		StartTime:   timestamppb.New(now),
		EndTime:     timestamppb.New(now.Add(time.Hour)),
		SandboxId:   "sbx-1",
		TeamId:      teamID.String(),
		ExecutionId: "exec-1",
		Vcpu:        2,
		RamMb:       512,
	})

	sandboxes, err := node.GetOrphanCandidates(t.Context())
	require.NoError(t, err)
	require.Len(t, sandboxes, 1)

	got := sandboxes[0]
	assert.Equal(t, "sbx-1", got.SandboxID)
	assert.Equal(t, teamID, got.TeamID)
	assert.Equal(t, "exec-1", got.ExecutionID)
	assert.Equal(t, int64(2), got.VCpu)
	assert.Equal(t, int64(512), got.RamMB)
	assert.Equal(t, now.UTC(), got.StartTime.UTC())
	assert.Equal(t, "test-node", got.NodeID)
}

// TestGetOrphanCandidates_FallsBackToDeprecatedConfig covers an orchestrator
// that predates the scalar fields and only sends the deprecated config message.
// Without the fallback the whole node would look orphaned and get killed.
func TestGetOrphanCandidates_FallsBackToDeprecatedConfig(t *testing.T) {
	t.Parallel()

	now := time.Now()
	teamID := uuid.New()
	node := newTestNodeWithList(t, &orchestrator.RunningSandbox{
		StartTime: timestamppb.New(now),
		EndTime:   timestamppb.New(now.Add(time.Hour)),
		Config: &orchestrator.SandboxConfig{
			SandboxId:   "sbx-legacy",
			TeamId:      teamID.String(),
			ExecutionId: "exec-legacy",
			Vcpu:        4,
			RamMb:       1024,
		},
	})

	sandboxes, err := node.GetOrphanCandidates(t.Context())
	require.NoError(t, err)
	require.Len(t, sandboxes, 1)

	got := sandboxes[0]
	assert.Equal(t, "sbx-legacy", got.SandboxID)
	assert.Equal(t, teamID, got.TeamID)
	assert.Equal(t, "exec-legacy", got.ExecutionID)
	assert.Equal(t, int64(4), got.VCpu)
	assert.Equal(t, int64(1024), got.RamMB)
}

// TestGetOrphanCandidates_PrefersScalarFieldsOverConfig pins the precedence for
// the shape seen throughout the rollout, where the orchestrator sends both. The
// scalar fields are what the orchestrator knows about the running sandbox; the
// config is the request the API stored when it created it.
func TestGetOrphanCandidates_PrefersScalarFieldsOverConfig(t *testing.T) {
	t.Parallel()

	teamID := uuid.New()
	node := newTestNodeWithList(t, &orchestrator.RunningSandbox{
		StartTime:   timestamppb.New(time.Now()),
		SandboxId:   "sbx-scalar",
		TeamId:      teamID.String(),
		ExecutionId: "exec-scalar",
		Vcpu:        2,
		RamMb:       512,
		Config: &orchestrator.SandboxConfig{
			SandboxId:   "sbx-config",
			TeamId:      uuid.NewString(),
			ExecutionId: "exec-config",
			Vcpu:        8,
			RamMb:       4096,
		},
	})

	sandboxes, err := node.GetOrphanCandidates(t.Context())
	require.NoError(t, err)
	require.Len(t, sandboxes, 1)

	got := sandboxes[0]
	assert.Equal(t, "sbx-scalar", got.SandboxID)
	assert.Equal(t, teamID, got.TeamID)
	assert.Equal(t, "exec-scalar", got.ExecutionID)
	assert.Equal(t, int64(2), got.VCpu)
	assert.Equal(t, int64(512), got.RamMB)
}

// TestGetOrphanCandidates_ToleratesMissingConfig verifies that a response with
// no config message at all is accepted, so the orchestrator can stop sending it.
func TestGetOrphanCandidates_ToleratesMissingConfig(t *testing.T) {
	t.Parallel()

	teamID := uuid.New()
	node := newTestNodeWithList(t, &orchestrator.RunningSandbox{
		StartTime: timestamppb.New(time.Now()),
		SandboxId: "sbx-no-config",
		TeamId:    teamID.String(),
	})

	sandboxes, err := node.GetOrphanCandidates(t.Context())
	require.NoError(t, err)
	require.Len(t, sandboxes, 1)
	assert.Equal(t, "sbx-no-config", sandboxes[0].SandboxID)
}

// TestGetOrphanCandidates_ReadsHugePagesFromConfig covers the one field with no
// scalar counterpart. Killing an orphan releases its RAM from the node's
// hugepage count only when the flag is set.
func TestGetOrphanCandidates_ReadsHugePagesFromConfig(t *testing.T) {
	t.Parallel()

	teamID := uuid.New()
	node := newTestNodeWithList(t,
		&orchestrator.RunningSandbox{
			StartTime: timestamppb.New(time.Now()),
			SandboxId: "sbx-hugepages",
			TeamId:    teamID.String(),
			Config:    &orchestrator.SandboxConfig{HugePages: true},
		},
		&orchestrator.RunningSandbox{
			StartTime: timestamppb.New(time.Now()),
			SandboxId: "sbx-no-config",
			TeamId:    teamID.String(),
		},
	)

	sandboxes, err := node.GetOrphanCandidates(t.Context())
	require.NoError(t, err)
	require.Len(t, sandboxes, 2)
	assert.True(t, sandboxes[0].HugePages)
	assert.False(t, sandboxes[1].HugePages)
}

// TestGetOrphanCandidates_SkipsUnparseableTeamID verifies that one malformed
// entry does not abort the sync for the whole node. A sandbox with no usable
// team ID has no store key, so it can be neither confirmed nor killed.
func TestGetOrphanCandidates_SkipsUnparseableTeamID(t *testing.T) {
	t.Parallel()

	teamID := uuid.New()
	node := newTestNodeWithList(t,
		&orchestrator.RunningSandbox{
			StartTime: timestamppb.New(time.Now()),
			SandboxId: "sbx-bad",
			TeamId:    "not-a-uuid",
		},
		&orchestrator.RunningSandbox{
			StartTime: timestamppb.New(time.Now()),
			SandboxId: "sbx-good",
			TeamId:    teamID.String(),
		},
	)

	sandboxes, err := node.GetOrphanCandidates(t.Context())
	require.NoError(t, err)
	require.Len(t, sandboxes, 1)
	assert.Equal(t, "sbx-good", sandboxes[0].SandboxID)
}

// TestGetOrphanCandidates_SkipsEntryWithoutSandboxID verifies that an entry with
// no sandbox ID never becomes a kill candidate. Its store lookup would always
// miss, so it would read as an orphan, and the kill would subtract its
// resources from the node's accounting without stopping anything.
func TestGetOrphanCandidates_SkipsEntryWithoutSandboxID(t *testing.T) {
	t.Parallel()

	teamID := uuid.New()
	node := newTestNodeWithList(t,
		&orchestrator.RunningSandbox{
			StartTime: timestamppb.New(time.Now()),
			TeamId:    teamID.String(),
			Vcpu:      2,
			RamMb:     512,
		},
		&orchestrator.RunningSandbox{
			StartTime: timestamppb.New(time.Now()),
			SandboxId: "sbx-good",
			TeamId:    teamID.String(),
		},
	)

	sandboxes, err := node.GetOrphanCandidates(t.Context())
	require.NoError(t, err)
	require.Len(t, sandboxes, 1)
	assert.Equal(t, "sbx-good", sandboxes[0].SandboxID)
}

// An ended sandbox carries its guest event, in log form, and whether the node
// kept its disk.
func TestGetOrphanCandidates_ReadsEnded(t *testing.T) {
	t.Parallel()

	teamID := uuid.New()
	node := newTestNodeWithList(t,
		&orchestrator.RunningSandbox{
			SandboxId:  "sbx-ended",
			TeamId:     teamID.String(),
			Ended:      true,
			GuestEvent: orchestrator.GuestEvent_GUEST_EVENT_REBOOT_OR_CRASH,
		},
		&orchestrator.RunningSandbox{
			SandboxId:  "sbx-kept",
			TeamId:     teamID.String(),
			Ended:      true,
			GuestEvent: orchestrator.GuestEvent_GUEST_EVENT_POWER_OFF,
			DiskKept:   true,
		},
		&orchestrator.RunningSandbox{
			SandboxId: "sbx-running",
			TeamId:    teamID.String(),
		},
	)

	sandboxes, err := node.GetOrphanCandidates(t.Context())
	require.NoError(t, err)
	require.Len(t, sandboxes, 3)

	assert.True(t, sandboxes[0].Ended)
	assert.Equal(t, "reboot_or_crash", sandboxes[0].GuestEvent)
	assert.False(t, sandboxes[0].DiskKept)
	assert.True(t, sandboxes[0].EndedOnNode())

	assert.True(t, sandboxes[1].Ended)
	assert.Equal(t, "power_off", sandboxes[1].GuestEvent)
	assert.True(t, sandboxes[1].DiskKept)
	assert.False(t, sandboxes[1].EndedOnNode())

	assert.False(t, sandboxes[2].Ended)
	assert.Empty(t, sandboxes[2].GuestEvent)
	assert.False(t, sandboxes[2].EndedOnNode())
}
