package server_test

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/scheduling"
)

func TestClaimStaleTransition(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	client, pool := newClient(t)
	paused := staleTransition(t, client, pool, uuid.NewString(), sandboxID, scheduling.TransitionAction_TRANSITION_ACTION_PAUSE, time.Hour)

	newID := uuid.NewString()
	claimed, err := client.ClaimTransition(ctx, claimRequest(paused, newID))
	require.NoError(t, err)
	require.Equal(t, scheduling.ClaimTransitionOutcome_CLAIM_TRANSITION_OUTCOME_CLAIMED, claimed.GetOutcome())
	require.Equal(t, newID, claimed.GetTransitionId())
	require.Equal(t, scheduling.TransitionAction_TRANSITION_ACTION_PAUSE, claimed.GetAction())
	require.True(t, claimed.HasTransitionAt())
	require.Equal(t, paused.sandboxID, claimed.GetSandbox().GetSandboxId())
	require.Equal(t, paused.executionID, claimed.GetSandbox().GetExecutionId())
	require.Equal(t, scheduling.SandboxState_SANDBOX_STATE_PAUSING, claimed.GetSandbox().GetState())

	again, err := client.ClaimTransition(ctx, claimRequest(paused, uuid.NewString()))
	require.NoError(t, err)
	require.Equal(t, scheduling.ClaimTransitionOutcome_CLAIM_TRANSITION_OUTCOME_CONFLICT, again.GetOutcome())
	require.Equal(t, newID, again.GetTransitionId())
	require.False(t, again.HasSandbox())

	waited, err := client.WaitForTransition(ctx, scheduling.WaitForTransitionRequest_builder{
		ProjectId: paused.project, SandboxId: paused.sandboxID, TransitionId: paused.tid, MaxWait: durationpb.New(time.Second),
	}.Build())
	require.NoError(t, err)
	require.Equal(t, scheduling.WaitForTransitionOutcome_WAIT_FOR_TRANSITION_OUTCOME_UNKNOWN, waited.GetOutcome())
	finished, err := client.FinishTransition(ctx, scheduling.FinishTransitionRequest_builder{
		ProjectId: paused.project, SandboxId: paused.sandboxID, TransitionId: newID,
	}.Build())
	require.NoError(t, err)
	require.Equal(t, scheduling.FinishTransitionOutcome_FINISH_TRANSITION_OUTCOME_FINISHED, finished.GetOutcome())
	require.Equal(t, int32(0), slotCount(t, pool, paused.project, 0))
}

func TestClaimTransitionInvalidRequests(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	client, _ := newClient(t)
	tid := uuid.NewString()
	valid := staleSandbox{project: uuid.NewString(), sandboxID: sandboxID, tid: tid, executionID: "execution"}

	for name, req := range map[string]*scheduling.ClaimTransitionRequest{
		"same transition IDs": claimRequest(valid, tid),
		"no execution":        claimRequest(staleSandbox{project: valid.project, sandboxID: sandboxID, tid: tid}, uuid.NewString()),
		"NUL in execution":    claimRequest(staleSandbox{project: valid.project, sandboxID: sandboxID, tid: tid, executionID: "a\x00b"}, uuid.NewString()),
		"bad new ID":          claimRequest(valid, "not-a-uuid"),
		"bad sandbox ID":      claimRequest(staleSandbox{project: valid.project, sandboxID: "sandbox", tid: tid, executionID: "e"}, uuid.NewString()),
	} {
		_, err := client.ClaimTransition(ctx, req)
		require.Equal(t, codes.InvalidArgument, status.Code(err), name)
	}
	_, err := client.ClaimTransition(ctx, claimRequest(valid, tid))
	require.Equal(t, "new_transition_id: must differ from old_transition_id", status.Convert(err).Message())
}

func TestClaimTransitionInternalError(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	client, pool := newClient(t)
	pool.Close()

	_, err := client.ClaimTransition(ctx, claimRequest(staleSandbox{
		project: uuid.NewString(), sandboxID: sandboxID, tid: uuid.NewString(), executionID: "e",
	}, uuid.NewString()))
	require.Equal(t, codes.Internal, status.Code(err))
	require.Equal(t, "internal error", status.Convert(err).Message())
}

func claimRequest(s staleSandbox, newID string) *scheduling.ClaimTransitionRequest {
	return scheduling.ClaimTransitionRequest_builder{
		ProjectId:           s.project,
		SandboxId:           s.sandboxID,
		OldTransitionId:     s.tid,
		NewTransitionId:     newID,
		ExpectedExecutionId: s.executionID,
	}.Build()
}
