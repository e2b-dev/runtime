package remote_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	reservationpb "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/scheduling/reservation"
	statepb "github.com/e2b-dev/infra/packages/shared/pkg/grpc/contracts/scheduling/state"
	"github.com/e2b-dev/infra/packages/shared/pkg/grpc/orchestrator"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxstore/remote"
	"github.com/e2b-dev/infra/packages/shared/pkg/sandboxstore/sandboxtypes"
)

func TestRequestsCarryTheCallersValues(t *testing.T) {
	t.Parallel()

	sbx := testRecord()
	end := time.Date(2026, 11, 13, 10, 0, 0, 987654321, time.UTC)
	transition := sandboxtypes.StateTransition{Sandbox: sbx, OriginalEndTime: &end, ID: "transition-1", InFlightID: "transition-0"}
	requireNoZeroField(t, transition)
	wireTransition := statepb.StateTransition_builder{Record: encode(t, sbx), OriginalEndTime: timestamppb.New(end), Id: "transition-1", InFlightId: "transition-0"}.Build()
	hold := sandboxtypes.Hold{TeamID: teamID, SandboxID: sandboxID, ExecutionID: sbx.ExecutionID, SourceNodeID: "node-a", ID: "hold-1"}
	requireNoZeroField(t, hold)
	wireHold := statepb.Hold_builder{TeamId: teamID.String(), SandboxId: sandboxID, ExecutionId: sbx.ExecutionID, SourceNodeId: "node-a", Id: "hold-1"}.Build()
	token := sandboxtypes.UpdateToken("\x00opaque\xff token")
	removeOpts := sandboxtypes.RemoveOpts{
		Action: sandboxtypes.StateActionSnapshot, Eviction: true, Reason: sandboxtypes.KillReasonTimeout, FilesystemOnly: true,
		RequireEnvdHealthy: true, ExpectExecutionID: sbx.ExecutionID, BeforeEndTime: true,
		// The API's store reads it; the request does not carry it.
		WaitForLifecycleHold: true,
	}
	requireNoZeroField(t, removeOpts)
	restoreOpts := sandboxtypes.RestoreOpts{RetryAfter: 90*time.Second + 123*time.Nanosecond, HoldOnly: true, EnvdRefusal: true}
	requireNoZeroField(t, restoreOpts)
	outcome := sandboxtypes.TransitionOutcome{Failed: true, Error: "boom", SkipRestore: true, Restored: true, RefusalClass: "unsupported"}
	requireNoZeroField(t, outcome)
	placement := &orchestrator.SandboxNetworkPlacement{ExecutionId: sbx.ExecutionID, NetworkMode: "mode", RouterId: "router", IngressAddress: "10.0.0.2", IngressPort: 5010}
	until := time.Date(2026, 11, 13, 11, 0, 0, 5, time.UTC)

	tests := map[string]struct {
		resp proto.Message
		call func(ctx context.Context, s *remote.Storage, r *remote.ReservationStorage)
		want proto.Message
	}{
		"Add": {
			call: func(ctx context.Context, s *remote.Storage, _ *remote.ReservationStorage) { _ = s.Add(ctx, sbx) },
			want: statepb.AddRequest_builder{Record: encode(t, sbx)}.Build(),
		},
		"Get": {
			call: func(ctx context.Context, s *remote.Storage, _ *remote.ReservationStorage) {
				_, _ = s.Get(ctx, teamID, sandboxID)
			},
			want: statepb.GetRequest_builder{TeamId: teamID.String(), SandboxId: sandboxID}.Build(),
		},
		"Remove": {
			call: func(ctx context.Context, s *remote.Storage, _ *remote.ReservationStorage) {
				_ = s.Remove(ctx, teamID, sandboxID, sbx.ExecutionID)
			},
			want: statepb.RemoveRequest_builder{TeamId: teamID.String(), SandboxId: sandboxID, ExpectedExecutionId: sbx.ExecutionID}.Build(),
		},
		"TeamItems": {
			call: func(ctx context.Context, s *remote.Storage, _ *remote.ReservationStorage) {
				_, _ = s.TeamItems(ctx, teamID, []sandboxtypes.State{sandboxtypes.StateSnapshotting, sandboxtypes.StateRunning, sandboxtypes.StatePausing, sandboxtypes.StateKilling})
			},
			want: statepb.TeamItemsRequest_builder{TeamId: teamID.String(), States: []statepb.SandboxState{
				statepb.SandboxState_SANDBOX_STATE_SNAPSHOTTING, statepb.SandboxState_SANDBOX_STATE_RUNNING,
				statepb.SandboxState_SANDBOX_STATE_PAUSING, statepb.SandboxState_SANDBOX_STATE_KILLING,
			}}.Build(),
		},
		"ExpiredItems": {
			call: func(ctx context.Context, s *remote.Storage, _ *remote.ReservationStorage) { _, _ = s.ExpiredItems(ctx) },
			want: statepb.ExpiredItemsRequest_builder{}.Build(),
		},
		"TeamsWithSandboxCount": {
			call: func(ctx context.Context, s *remote.Storage, _ *remote.ReservationStorage) {
				_, _ = s.TeamsWithSandboxCount(ctx)
			},
			want: statepb.TeamsWithSandboxCountRequest_builder{}.Build(),
		},
		"BeginUpdate": {
			call: func(ctx context.Context, s *remote.Storage, _ *remote.ReservationStorage) {
				_, _, _ = s.BeginUpdate(ctx, teamID, sandboxID)
			},
			want: statepb.BeginUpdateRequest_builder{TeamId: teamID.String(), SandboxId: sandboxID}.Build(),
		},
		"CommitUpdate": {
			call: func(ctx context.Context, s *remote.Storage, _ *remote.ReservationStorage) {
				_, _ = s.CommitUpdate(ctx, teamID, sandboxID, token, sbx)
			},
			want: statepb.CommitUpdateRequest_builder{TeamId: teamID.String(), SandboxId: sandboxID, Token: []byte(token), Record: encode(t, sbx)}.Build(),
		},
		"AbortUpdate": {
			call: func(ctx context.Context, s *remote.Storage, _ *remote.ReservationStorage) {
				_ = s.AbortUpdate(ctx, teamID, sandboxID, token)
			},
			want: statepb.AbortUpdateRequest_builder{TeamId: teamID.String(), SandboxId: sandboxID, Token: []byte(token)}.Build(),
		},
		"HoldExecution": {
			call: func(ctx context.Context, s *remote.Storage, _ *remote.ReservationStorage) {
				_ = s.HoldExecution(ctx, hold, until)
			},
			want: statepb.HoldExecutionRequest_builder{Hold: wireHold, Until: timestamppb.New(until)}.Build(),
		},
		"MoveHeldExecution": {
			call: func(ctx context.Context, s *remote.Storage, _ *remote.ReservationStorage) {
				_, _ = s.MoveHeldExecution(ctx, hold, "node-b")
			},
			want: statepb.MoveHeldExecutionRequest_builder{Hold: wireHold, NodeId: "node-b"}.Build(),
		},
		"ReleaseHeldExecution": {
			call: func(ctx context.Context, s *remote.Storage, _ *remote.ReservationStorage) {
				_ = s.ReleaseHeldExecution(ctx, hold)
			},
			want: statepb.ReleaseHeldExecutionRequest_builder{Hold: wireHold}.Build(),
		},
		"RemoveHeldExecution": {
			call: func(ctx context.Context, s *remote.Storage, _ *remote.ReservationStorage) {
				_, _ = s.RemoveHeldExecution(ctx, hold)
			},
			want: statepb.RemoveHeldExecutionRequest_builder{Hold: wireHold}.Build(),
		},
		"BeginRemoving": {
			call: func(ctx context.Context, s *remote.Storage, _ *remote.ReservationStorage) {
				_, _, _ = s.BeginRemoving(ctx, teamID, sandboxID, removeOpts)
			},
			want: statepb.BeginRemovingRequest_builder{
				TeamId: teamID.String(), SandboxId: sandboxID,
				Options: statepb.RemoveOptions_builder{
					Action: statepb.RemoveAction_REMOVE_ACTION_SNAPSHOT, Eviction: true, Reason: "timeout", FilesystemOnly: true,
					RequireEnvdHealthy: true, ExpectExecutionId: sbx.ExecutionID, BeforeEndTime: true,
				}.Build(),
			}.Build(),
		},
		"FinishTransition": {
			call: func(ctx context.Context, s *remote.Storage, _ *remote.ReservationStorage) {
				_ = s.FinishTransition(ctx, teamID, sandboxID, "transition-1", sandboxtypes.StateActionKill, outcome)
			},
			want: statepb.FinishTransitionRequest_builder{
				TeamId: teamID.String(), SandboxId: sandboxID, TransitionId: "transition-1", Action: statepb.RemoveAction_REMOVE_ACTION_KILL,
				Result: statepb.TransitionResult_builder{Failed: true, Error: "boom", SkipRestore: true, Restored: true, RefusalClass: "unsupported"}.Build(),
			}.Build(),
		},
		"RestoreRunning": {
			call: func(ctx context.Context, s *remote.Storage, _ *remote.ReservationStorage) {
				_, _ = s.RestoreRunning(ctx, transition, restoreOpts)
			},
			want: statepb.RestoreRunningRequest_builder{
				Transition: wireTransition,
				Options:    statepb.RestoreOptions_builder{RetryAfter: durationpb.New(restoreOpts.RetryAfter), HoldOnly: true, EnvdRefusal: true}.Build(),
			}.Build(),
		},
		"RestoreCheckpointed": {
			call: func(ctx context.Context, s *remote.Storage, _ *remote.ReservationStorage) {
				_, _ = s.RestoreCheckpointed(ctx, transition, placement)
			},
			want: statepb.RestoreCheckpointedRequest_builder{
				Transition: wireTransition,
				Placement: statepb.NetworkPlacement_builder{
					ExecutionId: sbx.ExecutionID, NetworkMode: "mode", RouterId: "router", IngressAddress: "10.0.0.2", IngressPort: 5010,
				}.Build(),
			}.Build(),
		},
		"RestoreCheckpointed without a placement": {
			call: func(ctx context.Context, s *remote.Storage, _ *remote.ReservationStorage) {
				_, _ = s.RestoreCheckpointed(ctx, transition, nil)
			},
			want: statepb.RestoreCheckpointedRequest_builder{Transition: wireTransition}.Build(),
		},
		"WaitForTransition": {
			resp: statepb.WaitForTransitionResponse_builder{Outcome: statepb.WaitForTransitionOutcome_WAIT_FOR_TRANSITION_OUTCOME_FINISHED}.Build(),
			call: func(ctx context.Context, s *remote.Storage, _ *remote.ReservationStorage) {
				_, _ = s.WaitForTransition(ctx, teamID, sandboxID, "transition-1")
			},
			want: statepb.WaitForTransitionRequest_builder{TeamId: teamID.String(), SandboxId: sandboxID, TransitionId: "transition-1"}.Build(),
		},
		"Reserve": {
			call: func(ctx context.Context, _ *remote.Storage, r *remote.ReservationStorage) {
				_, _ = r.Reserve(ctx, teamID, sandboxID, -1)
			},
			want: reservationpb.ReserveRequest_builder{TeamId: teamID.String(), SandboxId: sandboxID, Limit: -1}.Build(),
		},
		"FinishStart": {
			call: func(ctx context.Context, _ *remote.Storage, r *remote.ReservationStorage) {
				_ = r.FinishStart(ctx, teamID, sandboxID, []byte("\x00result\xff"))
			},
			want: reservationpb.FinishStartRequest_builder{TeamId: teamID.String(), SandboxId: sandboxID, Result: []byte("\x00result\xff")}.Build(),
		},
		"WaitForStart": {
			resp: reservationpb.WaitForStartResponse_builder{Outcome: reservationpb.WaitForStartOutcome_WAIT_FOR_START_OUTCOME_FINISHED}.Build(),
			call: func(ctx context.Context, _ *remote.Storage, r *remote.ReservationStorage) {
				_, _ = r.WaitForStart(ctx, teamID, sandboxID)
			},
			want: reservationpb.WaitForStartRequest_builder{TeamId: teamID.String(), SandboxId: sandboxID}.Build(),
		},
		"Release": {
			call: func(ctx context.Context, _ *remote.Storage, r *remote.ReservationStorage) {
				_ = r.Release(ctx, teamID, sandboxID)
			},
			want: reservationpb.ReleaseRequest_builder{TeamId: teamID.String(), SandboxId: sandboxID}.Build(),
		},
		"ClaimKill": {
			call: func(ctx context.Context, _ *remote.Storage, r *remote.ReservationStorage) {
				_, _, _ = r.ClaimKill(ctx, teamID, sandboxID)
			},
			want: reservationpb.ClaimKillRequest_builder{TeamId: teamID.String(), SandboxId: sandboxID}.Build(),
		},
		"ReleaseKillClaim": {
			call: func(ctx context.Context, _ *remote.Storage, r *remote.ReservationStorage) {
				_ = r.ReleaseKillClaim(ctx, teamID, sandboxID, "claim-1")
			},
			want: reservationpb.ReleaseKillClaimRequest_builder{TeamId: teamID.String(), SandboxId: sandboxID, ClaimId: "claim-1"}.Build(),
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			rec := &recorder{}
			s, r := newStores(dial(t, func(_ context.Context, _ string, req proto.Message, _ func(proto.Message) error) (proto.Message, error) {
				rec.record(req)
				if tt.resp != nil {
					return tt.resp, nil
				}

				return nil, status.Error(codes.Unavailable, "recorded")
			}))
			tt.call(callContext(t), s, r)
			requireProtoEqual(t, tt.want, rec.last(t))
		})
	}
}

func requireEveryFieldSet(t *testing.T, message protoreflect.Message) {
	t.Helper()

	fields := message.Descriptor().Fields()
	for i := range fields.Len() {
		require.True(t, message.Has(fields.Get(i)), "field %s is set", fields.Get(i).Name())
	}
}

func TestPlacementCoversEveryField(t *testing.T) {
	t.Parallel()

	placement := &orchestrator.SandboxNetworkPlacement{ExecutionId: "execution", NetworkMode: "mode", RouterId: "router", IngressAddress: "10.0.0.2", IngressPort: 5010}
	requireEveryFieldSet(t, placement.ProtoReflect())
	mirror := &statepb.NetworkPlacement{}
	require.Equal(t, placement.ProtoReflect().Descriptor().Fields().Len(), mirror.ProtoReflect().Descriptor().Fields().Len(), "the mirror has every field of the orchestrator's placement")

	rec := &recorder{}
	s, _ := newStores(dial(t, func(_ context.Context, _ string, req proto.Message, _ func(proto.Message) error) (proto.Message, error) {
		rec.record(req)

		return statepb.RestoreCheckpointedResponse_builder{}.Build(), nil
	}))
	end := time.Now()
	_, _ = s.RestoreCheckpointed(callContext(t), sandboxtypes.StateTransition{Sandbox: testRecord(), OriginalEndTime: &end}, placement)

	sent := rec.last(t).(*statepb.RestoreCheckpointedRequest).GetPlacement()
	require.NotNil(t, sent)
	requireEveryFieldSet(t, sent.ProtoReflect())
	sent.ProtoReflect().Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		original := placement.ProtoReflect().Descriptor().Fields().ByName(field.Name())
		require.NotNil(t, original, "field %s exists in the orchestrator's placement", field.Name())
		assert.True(t, value.Equal(placement.ProtoReflect().Get(original)), "field %s carries the caller's value", field.Name())

		return true
	})
}
