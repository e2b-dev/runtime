package server_test

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/scheduling"
)

func TestLiveResponsesPreserveStoredFields(t *testing.T) {
	t.Parallel()
	client, pool := newClient(t)
	ctx := t.Context()
	project, rid := uuid.NewString(), uuid.NewString()
	start(t, client, project, rid, 10)
	initial, err := client.Get(ctx, scheduling.GetRequest_builder{ProjectId: project, SandboxId: sandboxID}.Build())
	require.NoError(t, err)
	require.False(t, initial.GetSandbox().HasRefusedUntil())
	require.False(t, initial.GetSandbox().HasRefusedSince())
	require.False(t, initial.GetSandbox().HasEnvdRefusedUntil())
	require.False(t, initial.GetSandbox().HasEnvdRefusedSince())
	refusedSince := endTime.Add(-time.Minute)
	refusedUntil := endTime.Add(time.Minute)
	envdSince := endTime.Add(-2 * time.Minute)
	envdUntil := endTime.Add(2 * time.Minute)
	_, err = pool.Exec(ctx, `UPDATE scheduling_actor_run SET refused_since = $1, refused_until = $2, envd_refused_since = $3, envd_refused_until = $4 WHERE project_id = $5`, refusedSince, refusedUntil, envdSince, envdUntil, project)
	require.NoError(t, err)
	get, err := client.Get(ctx, scheduling.GetRequest_builder{ProjectId: project, SandboxId: sandboxID}.Build())
	require.NoError(t, err)
	page, err := client.ListLive(ctx, scheduling.ListLiveRequest_builder{ProjectId: project}.Build())
	require.NoError(t, err)
	require.Len(t, page.GetSandboxes(), 1)
	joined, err := client.Reserve(ctx, reserveRequest(project, sandboxID, uuid.NewString(), 10))
	require.NoError(t, err)
	inspected, err := client.InspectReservation(ctx, scheduling.InspectReservationRequest_builder{ProjectId: project, SandboxId: sandboxID}.Build())
	require.NoError(t, err)
	for _, actor := range []*scheduling.Sandbox{get.GetSandbox(), page.GetSandboxes()[0], joined.GetSandbox(), inspected.GetSandbox()} {
		requireSandbox(t, project, actor)
		require.Equal(t, refusedSince, actor.GetRefusedSince().AsTime())
		require.Equal(t, refusedUntil, actor.GetRefusedUntil().AsTime())
		require.Equal(t, envdSince, actor.GetEnvdRefusedSince().AsTime())
		require.Equal(t, envdUntil, actor.GetEnvdRefusedUntil().AsTime())
	}
	updatedEnd := endTime.Add(-time.Hour)
	updated, err := client.UpdateDeadline(ctx, scheduling.UpdateDeadlineRequest_builder{ProjectId: project, SandboxId: sandboxID, ExpectedExecutionId: putRequest(project, rid).GetExecutionId(), ExpectedEndTime: timestamppb.New(endTime), EndTime: timestamppb.New(updatedEnd)}.Build())
	require.NoError(t, err)
	require.Equal(t, updatedEnd, updated.GetSandbox().GetEndTime().AsTime())
	require.Equal(t, envdUntil, updated.GetSandbox().GetEnvdRefusedUntil().AsTime())
	begun, err := client.BeginTransition(ctx, beginRequest(project, uuid.NewString(), scheduling.TransitionAction_TRANSITION_ACTION_KILL))
	require.NoError(t, err)
	require.Equal(t, scheduling.SandboxState_SANDBOX_STATE_KILLING, begun.GetSandbox().GetState())
	require.Equal(t, updatedEnd, begun.GetSandbox().GetEndTime().AsTime())
	require.Equal(t, putRequest(project, rid).GetMetadata(), begun.GetSandbox().GetMetadata())
	require.JSONEq(t, string(putRequest(project, rid).GetRecord()), string(begun.GetSandbox().GetRecord()))
	require.Equal(t, envdSince, begun.GetSandbox().GetEnvdRefusedSince().AsTime())
	_, err = client.FinishTransition(ctx, finishTransitionRequest(project, begun.GetTransitionId()))
	require.NoError(t, err)
	inspected, err = client.InspectReservation(ctx, scheduling.InspectReservationRequest_builder{ProjectId: project, SandboxId: sandboxID}.Build())
	require.NoError(t, err)
	require.False(t, inspected.HasSandbox())
}

func TestLivePagesAreFilteredAndExcludeStarts(t *testing.T) {
	t.Parallel()
	client, _ := newClient(t)
	ctx := t.Context()
	project := uuid.NewString()
	start(t, client, project, uuid.NewString(), 10)
	otherID := "i00000000000000000001"
	reservation := uuid.NewString()
	_, err := client.Reserve(ctx, reserveRequest(project, otherID, reservation, 10))
	require.NoError(t, err)
	page, err := client.ListLive(ctx, scheduling.ListLiveRequest_builder{ProjectId: project, PageSize: 1}.Build())
	require.NoError(t, err)
	require.Len(t, page.GetSandboxes(), 1)
	put := putRequest(project, reservation)
	put.SetSandboxId(otherID)
	_, err = client.Put(ctx, put)
	require.NoError(t, err)
	first, err := client.ListLive(ctx, scheduling.ListLiveRequest_builder{ProjectId: project, PageSize: 1}.Build())
	require.NoError(t, err)
	require.Len(t, first.GetSandboxes(), 1)
	require.NotEmpty(t, first.GetNextPageToken())
	second, err := client.ListLive(ctx, scheduling.ListLiveRequest_builder{ProjectId: project, PageSize: 1, PageToken: first.GetNextPageToken()}.Build())
	require.NoError(t, err)
	require.Len(t, second.GetSandboxes(), 1)
	require.NotEqual(t, first.GetSandboxes()[0].GetSandboxId(), second.GetSandboxes()[0].GetSandboxId())
	empty, err := client.ListLive(ctx, scheduling.ListLiveRequest_builder{ProjectId: uuid.NewString()}.Build())
	require.NoError(t, err)
	require.Empty(t, empty.GetSandboxes())
	transition := uuid.NewString()
	_, err = client.BeginTransition(ctx, beginRequest(project, transition, scheduling.TransitionAction_TRANSITION_ACTION_PAUSE))
	require.NoError(t, err)
	running, err := client.ListLive(ctx, scheduling.ListLiveRequest_builder{ProjectId: project, States: []scheduling.SandboxState{scheduling.SandboxState_SANDBOX_STATE_RUNNING}}.Build())
	require.NoError(t, err)
	require.Len(t, running.GetSandboxes(), 1)
	require.Equal(t, otherID, running.GetSandboxes()[0].GetSandboxId())
	_, err = client.ListLive(ctx, scheduling.ListLiveRequest_builder{PageToken: "broken"}.Build())
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}
